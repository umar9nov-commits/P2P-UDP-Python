
import socket
import json
import base64
import hashlib
import uuid
import sys
from pathlib import Path

# Network settings
HOST = "127.0.0.1"

PACKET_SIZE = 512
ACK_TIMEOUT = 1
MAX_ATTEMPTS = 5

# Project files and folders
MANIFEST_FILE = Path("manifest.json")
CHUNK_FOLDER = Path("chunks")
RECEIVED_FOLDER = Path("network_received")

# False = Normal operation
# True = Simulate one lost acknowledgment
DROP_FIRST_ACK = False


def calculate_hash(data):
    """Calculate SHA-256 checksum."""
    return hashlib.sha256(data).hexdigest()


def send_json(sock, address, packet):
    """Convert a dictionary to JSON and send over UDP."""

    data = json.dumps(packet).encode("utf-8")
    sock.sendto(data, address)


def receive_chunk(my_port):
    """Receive UDP packets and reconstruct chunks."""

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    sock.bind((HOST, my_port))

    transfers = {}

    # Controls the optional ACK-loss experiment
    drop_first_ack = DROP_FIRST_ACK

    # Create receiver storage folder
    peer_folder = RECEIVED_FOLDER / str(my_port)
    peer_folder.mkdir(parents=True, exist_ok=True)

    print(f"UDP chunk receiver running on port {my_port}")

    if drop_first_ack:
        print("TEST MODE: First ACK will be dropped.")
    else:
        print("NORMAL MODE: All ACKs enabled.")

    print("Press Ctrl+C to stop.")

    try:

        while True:

            data, address = sock.recvfrom(4096)

            try:

                packet = json.loads(data.decode("utf-8"))

                if not isinstance(packet, dict):
                    continue

                if packet.get("type") != "CHUNK_DATA":
                    continue

                transfer_id = packet["transfer_id"]
                chunk_index = packet["chunk_index"]
                packet_index = packet["packet_index"]
                total_packets = packet["total_packets"]
                expected_hash = packet["sha256"]

                if not isinstance(transfer_id, str):
                    continue

                if not isinstance(expected_hash, str):
                    continue

                if any(
                    type(value) is not int
                    for value in (
                        chunk_index,
                        packet_index,
                        total_packets
                    )
                ):
                    continue

                if not (
                    0 <= chunk_index
                    and 0 <= packet_index < total_packets <= 1024
                ):
                    continue

                payload = base64.b64decode(
                    packet["data"],
                    validate=True
                )

                if len(payload) > PACKET_SIZE:
                    continue

            except (
                ValueError,
                KeyError,
                TypeError
            ):
                continue

            # Identify this transfer
            key = (address, transfer_id)

            if key not in transfers:

                transfers[key] = {
                    "chunk_index": chunk_index,
                    "total_packets": total_packets,
                    "expected_hash": expected_hash,
                    "packets": {},
                    "completed": False
                }

            transfer = transfers[key]

            # Check that packet metadata is consistent
            if (
                transfer["chunk_index"] != chunk_index
                or transfer["total_packets"] != total_packets
                or transfer["expected_hash"] != expected_hash
            ):
                continue

            # Store data only if transfer is incomplete
            if not transfer["completed"]:

                transfer["packets"][packet_index] = payload

            print(
                f"Received packet "
                f"{packet_index + 1}/{total_packets}"
            )

            # Check whether all packets have arrived
            if (
                not transfer["completed"]
                and len(transfer["packets"]) == total_packets
            ):

                chunk_data = b"".join(
                    transfer["packets"][i]
                    for i in range(total_packets)
                )

                actual_hash = calculate_hash(chunk_data)

                if actual_hash != expected_hash:

                    print("ERROR: Chunk checksum mismatch!")
                    continue

                filename = f"chunk_{chunk_index:04d}.bin"

                output_path = peer_folder / filename

                # Save the complete verified chunk
                output_path.write_bytes(chunk_data)

                transfer["completed"] = True

                print(
                    f"Chunk saved successfully: {output_path}"
                )

                print("SHA-256 verified!")

            # Prepare acknowledgment
            ack = {
                "type": "CHUNK_ACK",
                "transfer_id": transfer_id,
                "packet_index": packet_index
            }

            # Optional ACK-loss simulation
            if drop_first_ack:

                print("TEST: Dropping first CHUNK_ACK")

                drop_first_ack = False

            else:

                send_json(sock, address, ack)

                print(
                    f"ACK sent for packet {packet_index + 1}"
                )

    except KeyboardInterrupt:

        print("\nReceiver stopped.")

    finally:

        sock.close()


def send_chunk(my_port, peer_port, chunk_index):
    """Send a chunk with ACKs and retransmission."""

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    try:

        sock.bind((HOST, my_port))
        sock.settimeout(ACK_TIMEOUT)

        # Load the manifest
        manifest = json.loads(
            MANIFEST_FILE.read_text(encoding="utf-8")
        )

        # Find requested chunk metadata
        chunk_info = next(
            (
                item for item in manifest["chunks"]
                if item["index"] == chunk_index
            ),
            None
        )

        if chunk_info is None:
            print("ERROR: Chunk not listed in manifest.")
            return

        chunk_path = CHUNK_FOLDER / chunk_info["filename"]

        if not chunk_path.is_file():
            print("ERROR: Source chunk not found.")
            return

        chunk_data = chunk_path.read_bytes()

        expected_hash = chunk_info["sha256"]

        # Verify source before transmission
        if (
            len(chunk_data) != chunk_info["size"]
            or calculate_hash(chunk_data) != expected_hash
        ):

            print("ERROR: Source chunk failed verification.")
            return

        # Divide chunk into 512-byte packets
        packets = [
            chunk_data[i:i + PACKET_SIZE]
            for i in range(0, len(chunk_data), PACKET_SIZE)
        ]

        transfer_id = uuid.uuid4().hex

        destination = (HOST, peer_port)

        print(f"Sending Chunk {chunk_index}")
        print(f"Destination Peer: {peer_port}")
        print(f"Total packets: {len(packets)}")

        # Send each packet reliably
        for packet_index, payload in enumerate(packets):

            packet = {
                "type": "CHUNK_DATA",
                "transfer_id": transfer_id,
                "chunk_index": chunk_index,
                "packet_index": packet_index,
                "total_packets": len(packets),
                "sha256": expected_hash,
                "data": base64.b64encode(
                    payload
                ).decode("ascii")
            }

            acknowledged = False

            # Retry if acknowledgment is missing
            for attempt in range(1, MAX_ATTEMPTS + 1):

                send_json(sock, destination, packet)

                print(
                    f"Sent packet {packet_index + 1}/"
                    f"{len(packets)}, attempt {attempt}"
                )

                try:

                    while True:

                        data, address = sock.recvfrom(4096)

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

                        if (
                            ack.get("type") == "CHUNK_ACK"
                            and ack.get("transfer_id") == transfer_id
                            and ack.get("packet_index") == packet_index
                        ):

                            acknowledged = True
                            break

                except socket.timeout:

                    print(
                        f"ACK timeout for packet "
                        f"{packet_index + 1}"
                    )

                if acknowledged:
                    break

            # Stop if packet cannot be acknowledged
            if not acknowledged:

                print(
                    f"ERROR: Packet {packet_index + 1} "
                    "was not acknowledged."
                )

                return

        print("\nSUCCESS: All packets acknowledged!")

    finally:

        sock.close()


def main():

    if len(sys.argv) < 3:

        print("Usage:")
        print("  python3 udp_chunk_transfer.py receive 5002")
        print(
            "  python3 udp_chunk_transfer.py "
            "send 5001 5002 0"
        )

        return

    try:

        mode = sys.argv[1]

        if mode == "receive" and len(sys.argv) == 3:

            receive_chunk(
                int(sys.argv[2])
            )

        elif mode == "send" and len(sys.argv) == 5:

            send_chunk(
                int(sys.argv[2]),
                int(sys.argv[3]),
                int(sys.argv[4])
            )

        else:

            print("ERROR: Invalid arguments.")

    except (
        ValueError,
        OSError,
        FileNotFoundError,
        json.JSONDecodeError
    ) as error:

        print("ERROR:", error)


if __name__ == "__main__":
    main()
