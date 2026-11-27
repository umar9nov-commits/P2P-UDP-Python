
import socket
import json
import sys
import time

HOST = "127.0.0.1"
PORT = 6000


def receiver():

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    sock.settimeout(10)

    expected_seq = 1
    buffer = {}

    print("UDP receiver started on port 6000")

    try:
        while expected_seq <= 5:

            data, address = sock.recvfrom(4096)
            packet = json.loads(data.decode("utf-8"))

            seq = packet["seq"]
            message = packet["message"]

            print(f"\nArrived: seq={seq}, message={message}")

            if seq < expected_seq:
                print("Duplicate packet ignored.")
                continue

            buffer[seq] = message

            while expected_seq in buffer:
                text = buffer.pop(expected_seq)
                print(f"Delivered seq={expected_seq}: {text}")
                expected_seq += 1

            print("Next expected:", expected_seq)
            print("Buffer:", sorted(buffer.keys()))

    except socket.timeout:
        print("Timed out waiting for missing packets.")

    finally:
        sock.close()


def sender():

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    packets = [
        (1, "Hello"),
        (3, "Good morning"),
        (2, "How are you?"),
        (5, "See you"),
        (4, "I am fine")
    ]

    try:
        for seq, message in packets:

            packet = {
                "seq": seq,
                "message": message
            }

            data = json.dumps(packet).encode("utf-8")

            sock.sendto(data, (HOST, PORT))

            print(f"Sent seq={seq}: {message}")

            time.sleep(0.5)

    finally:
        sock.close()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: python3 udp_ordering_test.py receive|send")

    elif sys.argv[1] == "receive":
        receiver()

    elif sys.argv[1] == "send":
        sender()

    else:
        print("Invalid mode.")
