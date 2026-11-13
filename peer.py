
import socket
import threading
import sys

HOST = "127.0.0.1"


def receive_messages(udp_socket, stop_event):
    """Receive UDP messages in the background."""

    while not stop_event.is_set():
        try:
            data, address = udp_socket.recvfrom(1024)

            message = data.decode("utf-8")

            print(f"\n[Peer {address[1]}]: {message}")
            print("You: ", end="", flush=True)

        except socket.timeout:
            continue

        except OSError:
            if not stop_event.is_set():
                print("\nError receiving message.")
            break


def main():

    # We need our port and at least one peer port.
    if len(sys.argv) < 3:
        print(
            "Usage: python3 peer.py "
            "<my_port> <peer_port1> [peer_port2] ..."
        )
        return

    try:
        # First argument is our own port.
        my_port = int(sys.argv[1])

        # Remaining arguments are destination ports.
        peer_ports = [
            int(port) for port in sys.argv[2:]
        ]

    except ValueError:
        print("Error: Ports must be integers.")
        return

    # Validate our own port.
    if not (1 <= my_port <= 65535):
        print("Invalid local port.")
        return

    # Validate destination ports.
    for port in peer_ports:
        if not (1 <= port <= 65535):
            print(f"Invalid destination port: {port}")
            return

        if port == my_port:
            print("A peer cannot send to itself.")
            return

    # Remove duplicate destination ports.
    peer_ports = list(dict.fromkeys(peer_ports))

    # Create UDP socket.
    udp_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    try:
        # Bind to our listening port.
        udp_socket.bind((HOST, my_port))

        # Allow receiver to check for shutdown.
        udp_socket.settimeout(0.2)

        stop_event = threading.Event()

        # Start receiver thread.
        receiver_thread = threading.Thread(
            target=receive_messages,
            args=(udp_socket, stop_event)
        )

        receiver_thread.start()

        print(f"Peer started on port {my_port}")
        print(f"Group members: {peer_ports}")
        print("Type /quit to exit.")

        # Main thread handles outgoing messages.
        while True:
            try:
                message = input("You: ")

            except (EOFError, KeyboardInterrupt):
                break

            if message == "/quit":
                break

            if not message:
                continue

            # Send the message to every known peer.
            for port in peer_ports:
                udp_socket.sendto(
                    message.encode("utf-8"),
                    (HOST, port)
                )

            print(
                f"Message sent to {len(peer_ports)} peers."
            )

    except OSError as error:
        print(f"Network error: {error}")

    finally:
        if "stop_event" in locals():
            stop_event.set()
            receiver_thread.join()

        udp_socket.close()
        print("Peer closed.")


if __name__ == "__main__":
    main()
