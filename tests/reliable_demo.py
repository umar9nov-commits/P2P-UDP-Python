
import socket
import json
import sys
import uuid

HOST = "127.0.0.1"

ACK_TIMEOUT = 1
MAX_ATTEMPTS = 5
VERSION = 1


def send_packet(udp_socket, packet, address):
    """Convert a dictionary to JSON and send it."""

    data = json.dumps(packet).encode("utf-8")
    udp_socket.sendto(data, address)


def receive_mode(my_port):
    """Receive chat messages and send acknowledgments."""

    udp_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    udp_socket.bind((HOST, my_port))

    # Remember messages already delivered.
    received_messages = set()

    print(f"Receiver started on port {my_port}")
    print("Press Ctrl+C to stop.")

    try:
        while True:

            data, address = udp_socket.recvfrom(4096)

            try:
                packet = json.loads(
                    data.decode("utf-8")
                )

                if not isinstance(packet, dict):
                    continue

                if packet.get("version") != VERSION:
                    continue

                if packet.get("type") != "CHAT":
                    continue

                session = packet["session"]
                seq = packet["seq"]
                message = packet["message"]

                if not isinstance(session, str):
                    continue

                if type(seq) is not int or seq < 1:
                    continue

                if not isinstance(message, str):
                    continue

            except (
                ValueError,
                UnicodeDecodeError,
                KeyError,
                TypeError
            ):
                continue

            # Identify this message uniquely.
            message_id = (
                address[0],
                address[1],
                session,
                seq
            )

            # Display only previously unseen messages.
            if message_id not in received_messages:

                received_messages.add(message_id)

                print(
                    f"Received seq={seq}: {message}"
                )

            else:
                print(
                    f"Duplicate seq={seq}, not displayed again."
                )

            # Acknowledge even duplicate messages.
            ack_packet = {
                "version": VERSION,
                "type": "ACK",
                "session": uuid.uuid4().hex,
                "ack_session": session,
                "seq": seq
            }

            send_packet(
                udp_socket,
                ack_packet,
                address
            )

            print(f"ACK sent for seq={seq}")

    except KeyboardInterrupt:
        print("\nReceiver stopped.")

    finally:
        udp_socket.close()


def send_mode(my_port, peer_port, message):
    """Send one message reliably using retries."""

    udp_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    udp_socket.bind((HOST, my_port))
    udp_socket.settimeout(ACK_TIMEOUT)

    # Unique identifier for this sender session.
    session = uuid.uuid4().hex

    # First message in this experiment.
    seq = 1

    packet = {
        "version": VERSION,
        "type": "CHAT",
        "session": session,
        "seq": seq,
        "message": message
    }

    destination = (HOST, peer_port)

    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):

            print(
                f"Sending seq={seq}, attempt={attempt}"
            )

            send_packet(
                udp_socket,
                packet,
                destination
            )

            try:
                while True:

                    data, address = udp_socket.recvfrom(4096)

                    # Ignore replies from other addresses.
                    if address != destination:
                        continue

                    try:
                        ack = json.loads(
                            data.decode("utf-8")
                        )

                    except (
                        ValueError,
                        UnicodeDecodeError
                    ):
                        continue

                    if not isinstance(ack, dict):
                        continue

                    # Check that ACK matches our message.
                    if (
                        ack.get("version") == VERSION
                        and ack.get("type") == "ACK"
                        and ack.get("ack_session") == session
                        and ack.get("seq") == seq
                    ):

                        print(
                            f"ACK received for seq={seq}"
                        )

                        print("Message delivery confirmed!")
                        return

            except socket.timeout:

                print("ACK timeout. Retrying...")

        print("Delivery not confirmed after retries.")

    finally:
        udp_socket.close()


def main():

    if len(sys.argv) < 3:
        print("Usage:")
        print("  python3 reliable_demo.py receive 5002")
        print(
            '  python3 reliable_demo.py '
            'send 5001 5002 "Hello"'
        )
        return

    mode = sys.argv[1]

    try:

        if mode == "receive" and len(sys.argv) == 3:

            my_port = int(sys.argv[2])

            if not (1 <= my_port <= 65535):
                raise ValueError

            receive_mode(my_port)

        elif mode == "send" and len(sys.argv) >= 5:

            my_port = int(sys.argv[2])
            peer_port = int(sys.argv[3])

            if not (
                1 <= my_port <= 65535
                and 1 <= peer_port <= 65535
                and my_port != peer_port
            ):
                raise ValueError

            message = " ".join(sys.argv[4:])

            send_mode(my_port, peer_port, message)

        else:
            print("Invalid arguments.")

    except ValueError:
        print("Invalid port number.")

    except OSError as error:
        print(f"Network error: {error}")


if __name__ == "__main__":
    main()
