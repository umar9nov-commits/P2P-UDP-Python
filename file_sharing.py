"""File sharing part of the peer (uses stream 1 of our protocol).

Step 5: /share a file -> cut it into chunks, choose 2 peers for every chunk,
        and announce the file to every peer. /files shows the shared files.

File packets use their own sequence numbers and their own ACK table,
so they never mix with the chat messages of peer.py (stream 0).
"""

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

    print(f"Shared {name}: {len(chunks)} chunks, announced to {len(peer_ports)} peer(s).")


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
