"""File sharing part of the peer (uses stream 1 of our protocol).

Step 5: /share a file -> cut it into chunks, choose 2 peers for every chunk,
        and announce the file to every peer. /files shows the shared files.

File packets use their own sequence numbers and their own ACK table,
so they never mix with the chat messages of peer.py (stream 0).
"""

import base64
import hashlib
import os
import threading
import time

import protocol

HOST = "127.0.0.1"
CHUNK_SIZE = 64 * 1024    # 64 KB per chunk
ACK_TIMEOUT = 1           # seconds before sending a packet again
MAX_ATTEMPTS = 5
FILE_STREAM = 1

my_port = 0
session_number = 0
lock = threading.Lock()

files = {}            # file name -> information about the file
next_seq = {}         # peer port -> last seq number used on stream 1
waiting_ack = {}      # (peer port, seq) -> packet waiting for its ACK
already_seen = set()  # (peer port, session, seq) of packets already handled


def setup(port, session):
    """Called once by peer.py when the peer starts."""

    global my_port, session_number

    my_port = port
    session_number = session

    os.makedirs(storage_folder(), exist_ok=True)


def storage_folder():
    """Folder where this peer keeps its chunks, e.g. storage_5001."""

    return f"storage_{my_port}"


def calculate_hash(data):
    """SHA-256 fingerprint of some bytes."""

    return hashlib.sha256(data).hexdigest()


# ---------------- reliable sending on stream 1 ----------------

def send_reliable(sock, port, msg_type, data):
    """Send a file packet and remember it until its ACK arrives."""

    with lock:
        seq = next_seq.get(port, 0) + 1
        next_seq[port] = seq

        packet = protocol.make_packet(
            msg_type, session_number, seq, data,
            reliable=True, stream=FILE_STREAM
        )

        waiting_ack[(port, seq)] = {
            "packet": packet,
            "last_sent": time.monotonic(),
            "attempts": 1
        }

    sock.sendto(packet, (HOST, port))


def resend_missing(sock):
    """Send again every file packet whose ACK did not come back in time.

    peer.py calls this every 0.2 seconds.
    """

    now = time.monotonic()
    to_resend = []

    with lock:
        for key, record in list(waiting_ack.items()):

            if now - record["last_sent"] < ACK_TIMEOUT:
                continue

            if record["attempts"] >= MAX_ATTEMPTS:
                del waiting_ack[key]      # give up on this packet
                continue

            record["attempts"] += 1
            record["last_sent"] = now
            to_resend.append((key[0], record["packet"]))

    for port, packet in to_resend:
        try:
            sock.sendto(packet, (HOST, port))
        except OSError:
            pass


# ---------------- receiving on stream 1 ----------------

def handle_packet(sock, sender_port, packet):
    """peer.py gives every stream-1 packet to this function."""

    # An ACK: the other peer got our packet, stop waiting for it
    if packet["type"] == protocol.ACK:
        with lock:
            waiting_ack.pop((sender_port, packet["seq"]), None)
        return

    # Always send an ACK, even for a duplicate (our first ACK may have been lost)
    ack = protocol.make_packet(
        protocol.ACK, session_number, packet["seq"], stream=FILE_STREAM
    )
    sock.sendto(ack, (HOST, sender_port))

    # Ignore a packet we already handled (a duplicate)
    key = (sender_port, packet["session"], packet["seq"])

    with lock:
        if key in already_seen:
            return
        already_seen.add(key)

    data = packet["data"]

    if packet["type"] == protocol.FILE_ANNOUNCE:

        with lock:
            files[data["name"]] = data

        print(
            f"\nNew shared file: {data['name']} "
            f"({data['size']} bytes, {len(data['chunks'])} chunks)"
        )
        print("You: ", end="", flush=True)

    if packet["type"] == protocol.STORE_CHUNK:
        receive_piece(data)


# ---------------- commands ----------------

def share_file(sock, path, peer_ports):
    """/share <path>: cut the file into chunks and announce it."""

    if not os.path.isfile(path):
        print("File not found:", path)
        return

    with open(path, "rb") as f:
        file_data = f.read()

    name = os.path.basename(path)

    # 1. Cut the file into chunks
    chunks = []
    for start in range(0, len(file_data), CHUNK_SIZE):
        chunks.append(file_data[start:start + CHUNK_SIZE])

    # 2. Placement rule: chunk i -> peer (i mod n) and peer ((i+1) mod n)
    all_peers = sorted(peer_ports + [my_port])
    n = len(all_peers)

    holders = []
    for i in range(len(chunks)):
        first = all_peers[i % n]
        second = all_peers[(i + 1) % n]
        holders.append([first, second])

    # 3. Information about the file (like Umar's manifest.json)
    info = {
        "name": name,
        "size": len(file_data),
        "sha256": calculate_hash(file_data),
        "chunk_size": CHUNK_SIZE,
        "chunks": [calculate_hash(chunk) for chunk in chunks],
        "holders": holders
    }

    with lock:
        files[name] = info

    # 4. Keep the chunks that the rule gives to me
    for i in range(len(chunks)):
        if my_port in holders[i]:
            chunk_path = os.path.join(storage_folder(), f"{name}.chunk{i}")
            with open(chunk_path, "wb") as f:
                f.write(chunks[i])

    # 5. Tell every peer about the file
    for port in peer_ports:
        send_reliable(sock, port, protocol.FILE_ANNOUNCE, info)

    # 6. Send every chunk to its holders (each chunk goes to 2 peers)
    wait_for_acks()          # first make sure everyone got the announcement

    sent = 0
    for i in range(len(chunks)):
        for port in holders[i]:
            if port != my_port:
                send_chunk(sock, port, name, i, chunks[i])
                sent += 1

    print(f"Shared {name}: {len(chunks)} chunks, announced to {len(peer_ports)} peer(s), "
          f"sent {sent} chunk copies.")


def show_files():
    """/files: list shared files and how many chunks each peer should store."""

    with lock:
        if not files:
            print("No shared files yet.")
            return

        for name, info in files.items():
            print(f"{name}: {info['size']} bytes, {len(info['chunks'])} chunks")

            count = {}
            for pair in info["holders"]:
                for port in pair:
                    count[port] = count.get(port, 0) + 1

            for port in sorted(count):
                print(f"   peer {port} stores {count[port]} chunks")


# ---------------- step 6: sending and storing chunks ----------------

PIECE_SIZE = 1024     # bytes of file data in one packet
incoming = {}         # (file name, chunk number) -> {piece number: bytes}


def wait_for_acks(port=None, timeout=10):
    """Wait until our packets (to one peer, or to everyone) are acknowledged."""

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:

        with lock:
            still_waiting = 0
            for key in waiting_ack:
                if port is None or key[0] == port:
                    still_waiting += 1

        if still_waiting == 0:
            return

        time.sleep(0.01)


def send_chunk(sock, port, name, index, chunk):
    """Send one chunk to a peer as 1 KB pieces (STORE_CHUNK messages)."""

    # Cut the chunk into small pieces that fit in one UDP packet
    pieces = []
    for start in range(0, len(chunk), PIECE_SIZE):
        pieces.append(chunk[start:start + PIECE_SIZE])

    for number in range(len(pieces)):
        data = {
            "file": name,
            "chunk": index,
            "piece": number,
            "total_pieces": len(pieces),
            "data": base64.b64encode(pieces[number]).decode("ascii")
        }
        send_reliable(sock, port, protocol.STORE_CHUNK, data)

    # Wait until this chunk has arrived before sending the next one,
    # so we do not flood the other peer with thousands of packets at once
    wait_for_acks(port)


def receive_piece(data):
    """Collect the pieces of a chunk. When it is complete, check it and save it."""

    name = data["file"]
    index = data["chunk"]
    key = (name, index)

    with lock:
        if key not in incoming:
            incoming[key] = {}

        incoming[key][data["piece"]] = base64.b64decode(data["data"])

        # Not all pieces yet: wait for more
        if len(incoming[key]) < data["total_pieces"]:
            return

        pieces = incoming.pop(key)
        info = files.get(name)

    # Join the pieces in the right order
    chunk = b""
    for number in range(data["total_pieces"]):
        chunk = chunk + pieces[number]

    # Check the fingerprint before saving
    if info is None or calculate_hash(chunk) != info["chunks"][index]:
        print(f"\nChunk {index} of {name} is damaged, not saved.")
        print("You: ", end="", flush=True)
        return

    chunk_path = os.path.join(storage_folder(), f"{name}.chunk{index}")
    with open(chunk_path, "wb") as f:
        f.write(chunk)

    # When all my chunks of this file are stored, tell the user
    my_total = 0
    for pair in info["holders"]:
        if my_port in pair:
            my_total += 1

    stored = 0
    for file_name in os.listdir(storage_folder()):
        if file_name.startswith(f"{name}.chunk"):
            stored += 1

    if stored == my_total:
        print(f"\nStored all {stored} of my chunks of {name} (SHA-256 checked).")
        print("You: ", end="", flush=True)
