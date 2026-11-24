
import socket
import threading
import sys
import time

# Network configuration
HOST = "127.0.0.1"

FIRST_PORT = 5001
LAST_PORT = 5010

HEARTBEAT_INTERVAL = 2
FAILURE_TIMEOUT = 6


def send_message(udp_socket, port, message):
    """Send a UDP message to another peer."""

    try:
        udp_socket.sendto(
            message.encode("utf-8"),
            (HOST, port)
        )

    except OSError as error:
        print(f"\nSend error to Peer {port}: {error}")


def discover_peers(udp_socket, my_port):
    """Search for other peers using JOIN messages."""

    for port in range(FIRST_PORT, LAST_PORT + 1):

        if port != my_port:
            send_message(udp_socket, port, "JOIN")


def receive_messages(
    udp_socket,
    my_port,
    active_peers,
    peers_lock,
    stop_event
):
    """Receive UDP messages and update membership."""

    while not stop_event.is_set():

        try:
            data, address = udp_socket.recvfrom(1024)

            sender_ip, sender_port = address

            # Accept only valid local peers
            if sender_ip != HOST:
                continue

            if sender_port == my_port:
                continue

            if not (FIRST_PORT <= sender_port <= LAST_PORT):
                continue

            message = data.decode("utf-8")

            # Recognize supported message types
            if message not in ("JOIN", "WELCOME", "HEARTBEAT") \
                    and not message.startswith("CHAT:"):
                continue

            # Update the peer's last-seen timestamp
            with peers_lock:

                is_new = sender_port not in active_peers

                active_peers[sender_port] = time.monotonic()

            if is_new:
                print(f"\nPeer {sender_port} is now active.")
                print("You: ", end="", flush=True)

            # Handle JOIN
            if message == "JOIN":

                send_message(
                    udp_socket,
                    sender_port,
                    "WELCOME"
                )

            # Handle WELCOME
            elif message == "WELCOME":

                pass

            # Handle HEARTBEAT
            elif message == "HEARTBEAT":

                pass

            # Handle CHAT
            elif message.startswith("CHAT:"):

                chat_text = message[5:]

                print(f"\n[Peer {sender_port}]: {chat_text}")
                print("You: ", end="", flush=True)

        except socket.timeout:
            continue

        except UnicodeDecodeError:
            continue

        except OSError as error:

            if not stop_event.is_set():
                print(f"\nReceive error: {error}")

            break


def maintain_peers(
    udp_socket,
    my_port,
    active_peers,
    peers_lock,
    stop_event
):
    """Send heartbeats and remove inactive peers."""

    while not stop_event.wait(HEARTBEAT_INTERVAL):

        current_time = time.monotonic()

        active_ports = []
        failed_ports = []

        # Access the shared peer list safely
        with peers_lock:

            for port, last_seen in list(active_peers.items()):

                elapsed = current_time - last_seen

                # Peer has been silent for too long
                if elapsed > FAILURE_TIMEOUT:

                    failed_ports.append(port)

                    del active_peers[port]

                else:

                    active_ports.append(port)

        # Report failed peers
        for port in failed_ports:

            print(
                f"\nPeer {port} is inactive (timeout)."
            )

            print("You: ", end="", flush=True)

        # Send heartbeats to active peers
        for port in active_ports:

            send_message(
                udp_socket,
                port,
                "HEARTBEAT"
            )

        # Repeat discovery to find new or restarted peers
        discover_peers(udp_socket, my_port)


def main():

    # Read the peer's own UDP port
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

    # Create the UDP socket
    udp_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    # Dictionary: port -> last-seen time
    active_peers = {}

    # Protect shared peer information
    peers_lock = threading.Lock()

    # Signal for stopping background threads
    stop_event = threading.Event()

    threads = []

    try:

        # Bind to our local port
        udp_socket.bind((HOST, my_port))

        # Receiver checks periodically for shutdown
        udp_socket.settimeout(0.2)

        # Create receiver thread
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

        # Create maintenance thread
        maintenance_thread = threading.Thread(
            target=maintain_peers,
            args=(
                udp_socket,
                my_port,
                active_peers,
                peers_lock,
                stop_event
            )
        )

        threads = [receiver_thread, maintenance_thread]

        # Start both background threads
        receiver_thread.start()
        maintenance_thread.start()

        print(f"Peer started on port {my_port}")

        print(
            f"Heartbeat interval: {HEARTBEAT_INTERVAL} seconds"
        )

        print(
            f"Failure timeout: {FAILURE_TIMEOUT} seconds"
        )

        print("Commands: /peers, /quit")

        # Initial discovery
        discover_peers(udp_socket, my_port)

        # Main chat loop
        while True:

            try:
                message = input("You: ")

            except (EOFError, KeyboardInterrupt):
                break

            if message == "/quit":
                break

            # Display active peers
            if message == "/peers":

                with peers_lock:
                    peers_copy = sorted(active_peers.keys())

                print("Active peers:", peers_copy)

                continue

            if not message:
                continue

            # Copy the membership list
            with peers_lock:
                peers_copy = list(active_peers.keys())

            # Send chat to every active peer
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

        # Stop both background threads
        stop_event.set()

        for thread in threads:

            if thread.ident is not None:
                thread.join()

        # Close the socket
        udp_socket.close()

        print("Peer closed.")


if __name__ == "__main__":
    main()
