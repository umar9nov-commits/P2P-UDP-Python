
import socket
import threading
import sys

HOST = "127.0.0.1"


def receive_messages(udp_socket, stop_event):
    """Receive UDP messages in a background thread."""

    while not stop_event.is_set():
        try:
            data, address = udp_socket.recvfrom(1024)

            message = data.decode("utf-8")

            print(f"\nPeer {address[1]}: {message}")
            print("You: ", end="", flush=True)

        except socket.timeout:
            continue

        except OSError:
            if not stop_event.is_set():
                print("\nError receiving message.")
            break


def main():

    if len(sys.argv) != 3:
        print("Usage: python3 peer.py <my_port> <peer_port>")
        return

    try:
        my_port = int(sys.argv[1])
        peer_port = int(sys.argv[2])
    except ValueError:
        print("Error: Port numbers must be integers.")
        return

    if not (1 <= my_port <= 65535):
        print("Invalid local port.")
        return

    if not (1 <= peer_port <= 65535):
        print("Invalid destination port.")
        return

    if my_port == peer_port:
        print("Both ports must be different.")
        return

    udp_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    try:
        udp_socket.bind((HOST, my_port))
        udp_socket.settimeout(0.2)

        stop_event = threading.Event()

        receiver_thread = threading.Thread(
            target=receive_messages,
            args=(udp_socket, stop_event)
        )

        receiver_thread.start()

        print(f"Peer started on port {my_port}")
        print(f"Destination peer: {peer_port}")
        print("Type /quit to exit.")

        while True:
            try:
                message = input("You: ")
            except (EOFError, KeyboardInterrupt):
                break

            if message == "/quit":
                break

            if not message:
                continue

            udp_socket.sendto(
                message.encode("utf-8"),
                (HOST, peer_port)
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
