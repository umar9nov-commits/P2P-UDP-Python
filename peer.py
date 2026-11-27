
import socket
import threading
import sys
import time
import json
import uuid

import protocol

HOST = "127.0.0.1"
FIRST_PORT = 5001
LAST_PORT = 5010

VERSION = 1
HEARTBEAT_INTERVAL = 2
FAILURE_TIMEOUT = 6
ACK_TIMEOUT = 1
MAX_ATTEMPTS = 5

SESSION_ID = uuid.uuid4().hex

# TEMPORARY TEST: Peer 5002 drops its first CHAT ACK



# Message names used in this file -> type numbers in our protocol header
TYPE_CODES = {
    "JOIN": protocol.JOIN,
    "WELCOME": protocol.WELCOME,
    "HEARTBEAT": protocol.HEARTBEAT,
    "ACK": protocol.ACK,
    "CHAT": protocol.CHAT
}

TYPE_NAMES = {}
for name, code in TYPE_CODES.items():
    TYPE_NAMES[code] = name


def send_packet(sock, port, packet):
    """Send a text or JSON packet using UDP, with our protocol header in front."""

    # The header needs the session as a number, so use part of the session text
    session_number = int(SESSION_ID[:8], 16)

    if isinstance(packet, dict):
        msg_type = TYPE_CODES[packet["type"]]
        seq = packet.get("seq", 0)
        data = packet
        reliable = packet["type"] == "CHAT"
    else:
        msg_type = TYPE_CODES[packet]
        seq = 0
        data = None
        reliable = False

    header_and_data = protocol.make_packet(
        msg_type, session_number, seq, data, reliable
    )

    sock.sendto(header_and_data, (HOST, port))


def decode_packet(data):
    """Remove our header. Gives back the same text or dictionary as before."""

    packet = protocol.read_packet(data)

    if packet is None:
        return None

    # CHAT and ACK carry a dictionary
    if packet["data"]:
        return packet["data"]

    # JOIN, WELCOME and HEARTBEAT carry no data, so give back their name
    return TYPE_NAMES.get(packet["type"])


def discover_peers(sock, my_port):
    """Send JOIN to possible local peers."""

    for port in range(FIRST_PORT, LAST_PORT + 1):
        if port != my_port:
            send_packet(sock, port, "JOIN")


def receive_messages(
    sock, my_port, active_peers,
    pending, receive_state, lock, stop_event
):
    """Receive peer messages and acknowledgments."""


    while not stop_event.is_set():

        try:
            data, address = sock.recvfrom(4096)
            sender_ip, sender_port = address

            if sender_ip != HOST or sender_port == my_port:
                continue

            if not FIRST_PORT <= sender_port <= LAST_PORT:
                continue

            message = decode_packet(data)

            if message is None:
                continue

            # Handle discovery and heartbeat
            if message in ("JOIN", "WELCOME", "HEARTBEAT"):

                with lock:
                    is_new = sender_port not in active_peers
                    active_peers[sender_port] = time.monotonic()

                if is_new:
                    print(f"\nPeer {sender_port} is now active.")
                    print("You: ", end="", flush=True)

                if message == "JOIN":
                    send_packet(sock, sender_port, "WELCOME")

                continue

            # CHAT and ACK messages are dictionaries
            packet = message

            if not isinstance(packet, dict):
                continue

            if packet.get("version") != VERSION:
                continue

            packet_type = packet.get("type")
            session = packet.get("session")
            seq = packet.get("seq")

            if not isinstance(session, str) or not session:
                continue

            if type(seq) is not int or seq < 1:
                continue

            # Handle ACK packets
            if packet_type == "ACK":

                if packet.get("ack_session") != SESSION_ID:
                    continue

                with lock:
                    active_peers[sender_port] = time.monotonic()

                    key = (sender_port, seq)

                    if key in pending:
                        del pending[key]

                        print(
                            f"\nACK received from Peer "
                            f"{sender_port} for seq={seq}"
                        )
                        print("You: ", end="", flush=True)

                continue

            if packet_type != "CHAT":
                continue

            chat_text = packet.get("message")

            if not isinstance(chat_text, str):
                continue

            # Prepare acknowledgment
            ack = {
                "version": VERSION,
                "type": "ACK",
                "session": SESSION_ID,
                "ack_session": session,
                "seq": seq
            }

            # TEST: Deliberately drop one ACK on Peer B
            send_packet(sock, sender_port, ack)

            # Maintain ordering per sender session
            key = (sender_port, session)
            delivered = []

            with lock:

                is_new = sender_port not in active_peers
                active_peers[sender_port] = time.monotonic()

                if key not in receive_state:
                    receive_state[key] = {
                        "expected": 1,
                        "buffer": {}
                    }

                state = receive_state[key]

                # Store messages not already delivered
                if seq >= state["expected"]:
                    state["buffer"][seq] = chat_text

                # Deliver only consecutive messages
                while state["expected"] in state["buffer"]:

                    current = state["expected"]
                    text = state["buffer"].pop(current)

                    delivered.append((current, text))
                    state["expected"] += 1

            if is_new:
                print(f"\nPeer {sender_port} is now active.")

            for number, text in delivered:

                print(
                    f"\n[Peer {sender_port}, "
                    f"seq={number}]: {text}"
                )

            if is_new or delivered:
                print("You: ", end="", flush=True)

        except socket.timeout:
            continue

        except (ValueError, UnicodeDecodeError):
            continue

        except OSError:
            if not stop_event.is_set():
                print("\nReceive error.")
            break


def maintain_peers(
    sock, my_port, active_peers,
    pending, lock, stop_event
):
    """Send heartbeats and retry unacknowledged packets."""

    last_heartbeat = time.monotonic()

    while not stop_event.wait(0.2):

        now = time.monotonic()

        resend = []
        failed_messages = []
        failed_peers = []

        with lock:

            # Detect inactive peers
            for port, last_seen in list(active_peers.items()):

                if now - last_seen > FAILURE_TIMEOUT:

                    del active_peers[port]
                    failed_peers.append(port)

            # Check messages waiting for ACK
            for key, record in list(pending.items()):

                port, seq = key

                if port in failed_peers:
                    del pending[key]
                    failed_messages.append(key)
                    continue

                if now - record["last_sent"] < ACK_TIMEOUT:
                    continue

                if record["attempts"] >= MAX_ATTEMPTS:
                    del pending[key]
                    failed_messages.append(key)
                    continue

                record["attempts"] += 1
                record["last_sent"] = now

                resend.append((
                    port,
                    seq,
                    record["packet"],
                    record["attempts"]
                ))

            active_ports = list(active_peers.keys())

        # Report disconnected peers
        for port in failed_peers:
            print(f"\nPeer {port} inactive (timeout).")
            print("You: ", end="", flush=True)

        # Report unconfirmed deliveries
        for port, seq in failed_messages:
            print(
                f"\nDelivery not confirmed: "
                f"Peer {port}, seq={seq}"
            )
            print("You: ", end="", flush=True)

        # Retransmit missing packets
        for port, seq, packet, attempt in resend:

            try:
                send_packet(sock, port, packet)

                print(
                    f"\nRetransmitting to Peer {port}: "
                    f"seq={seq}, attempt={attempt}"
                )
                print("You: ", end="", flush=True)

            except OSError:
                pass

        # Regular heartbeat and rediscovery
        if now - last_heartbeat >= HEARTBEAT_INTERVAL:

            for port in active_ports:
                try:
                    send_packet(sock, port, "HEARTBEAT")
                except OSError:
                    pass

            discover_peers(sock, my_port)
            last_heartbeat = now


def main():

    if len(sys.argv) != 2:
        print("Usage: python3 peer.py <my_port>")
        return

    try:
        my_port = int(sys.argv[1])
    except ValueError:
        print("Port must be an integer.")
        return

    if not FIRST_PORT <= my_port <= LAST_PORT:
        print("Port must be between 5001 and 5010.")
        return

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    active_peers = {}
    send_seq = {}
    pending = {}
    receive_state = {}

    lock = threading.Lock()
    stop_event = threading.Event()
    threads = []

    try:
        sock.bind((HOST, my_port))
        sock.settimeout(0.2)

        receiver_thread = threading.Thread(
            target=receive_messages,
            args=(
                sock, my_port, active_peers,
                pending, receive_state,
                lock, stop_event
            )
        )

        maintenance_thread = threading.Thread(
            target=maintain_peers,
            args=(
                sock, my_port, active_peers,
                pending, lock, stop_event
            )
        )

        threads = [
            receiver_thread,
            maintenance_thread
        ]

        receiver_thread.start()
        maintenance_thread.start()

        print(f"Reliable UDP Peer started: {my_port}")
        print("Commands: /peers, /quit")

        discover_peers(sock, my_port)

        while True:

            try:
                message = input("You: ")
            except (EOFError, KeyboardInterrupt):
                break

            if message.strip() == "/quit":
                break

            if message.strip() == "/peers":

                with lock:
                    peers_copy = sorted(active_peers.keys())

                print("Active peers:", peers_copy)
                continue

            if not message:
                continue

            with lock:

                peers_copy = list(active_peers.keys())
                packets_to_send = []

                for port in peers_copy:

                    seq = send_seq.get(port, 0) + 1
                    send_seq[port] = seq

                    packet = {
                        "version": VERSION,
                        "type": "CHAT",
                        "session": SESSION_ID,
                        "seq": seq,
                        "message": message
                    }

                    pending[(port, seq)] = {
                        "packet": packet,
                        "last_sent": time.monotonic(),
                        "attempts": 1
                    }

                    packets_to_send.append(
                        (port, packet)
                    )

            for port, packet in packets_to_send:
                send_packet(sock, port, packet)

            print(
                f"Sent reliably to {len(peers_copy)} peer(s)."
            )

    except OSError as error:
        print(f"Network error: {error}")

    finally:

        stop_event.set()

        for thread in threads:
            if thread.ident is not None:
                thread.join()

        sock.close()
        print("Peer closed.")


if __name__ == "__main__":
    main()
