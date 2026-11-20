
import socket
import threading
import sys

HOST = "127.0.0.1"

FIRST_PORT = 5001
LAST_PORT = 5010


def send_message(udp_socket, port, message):
    """Send a UDP message to another local peer."""

    udp_socket.sendto(
        message.encode("utf-8"),
        (HOST, port)
    )


def discover_peers(udp_socket, my_port):
    """Announce ourselves to possible peers."""

    for port in range(FIRST_PORT, LAST_PORT + 1):

        if port != my_port:
            send_message(udp_socket, port, "JOIN")


def receive_messages(
    udp_socket, my_port, active_peers, peers_lock, stop_event
):
    """Receive messages and update the peer list."""

    while not stop_event.is_set():

        try:
            data, address = udp_socket.recvfrom(1024)

            message = data.decode("utf-8")
            sender_port = address[1]

            if address[0] != HOST:
                continue

            if sender_port == my_port:
                continue

            if not (FIRST_PORT <= sender_port <= LAST_PORT):
                continue

            # New peer announces itself.
            if message == "JOIN":

                with peers_lock:
                    is_new = sender_port not in active_peers
                    active_peers.add(sender_port)

                if is_new:
                    print(f"\nPeer {sender_port} joined.")

                send_message(udp_socket, sender_port, "WELCOME")

            # Existing peer responds to JOIN.
            elif message == "WELCOME":

                with peers_lock:
                    is_new = sender_port not in active_peers
                    active_peers.add(sender_port)

                if is_new:
                    print(f"\nDiscovered Peer {sender_port}")

            # Normal group chat message.
            elif message.startswith("CHAT:"):

                with peers_lock:
                    known_peer = sender_port in active_peers

                if known_peer:
                    chat_text = message[5:]

                    print(
                        f"\n[Peer {sender_port}]: {chat_text}"
                    )

            print("You: ", end="", flush=True)

        except socket.timeout:
            continue

        except UnicodeDecodeError:
            continue

        except OSError:
            if not stop_event.is_set():
                print("\nError receiving message.")
            break


def main():

    if len(sys.argv) != 2:
        print("Usage: python3 peer.py <my_port>")
        return

    try:
        my_port = int(sys.argv[1])

    except ValueError:
        print("Error: Port must be an integer.")
        return

    if not (FIRST_PORT <= my_port <= LAST_PORT):
        print("Port must be between 5001 and 5010.")
        return

    udp_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    stop_event = threading.Event()
    active_peers = set()
    peers_lock = threading.Lock()

    try:
        udp_socket.bind((HOST, my_port))
        udp_socket.settimeout(0.2)

        receiver_thread = threading.Thread(
            target=receive_messages,
            args=(
                udp_socket,
                my_port,
                active_peers,
                peers_lock,
                stop_event
            )
        )

        receiver_thread.start()

        print(f"Peer started on port {my_port}")
        print("Commands: /peers, /quit")

        # Discover currently running peers.
        discover_peers(udp_socket, my_port)

        while True:

            try:
                message = input("You: ")

            except (EOFError, KeyboardInterrupt):
                break

            if message == "/quit":
                break

            if message == "/peers":

                with peers_lock:
                    peers_copy = sorted(active_peers)

                print("Known peers:", peers_copy)
                continue

            if not message:
                continue

            # Copy the peer list safely.
            with peers_lock:
                peers_copy = list(active_peers)

            for port in peers_copy:
                send_message(
                    udp_socket,
                    port,
                    "CHAT:" + message
                )

            print(
                f"Message sent to {len(peers_copy)} peer(s)."
            )

    except OSError as error:
        print(f"Network error: {error}")

    finally:
        stop_event.set()

        if "receiver_thread" in locals():
            receiver_thread.join()

        udp_socket.close()
        print("Peer closed.")


if __name__ == "__main__":
    main()
