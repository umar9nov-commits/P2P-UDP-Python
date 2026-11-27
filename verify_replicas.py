
import json
import hashlib
from pathlib import Path

MANIFEST_FILE = Path("manifest.json")
STORAGE_FOLDER = Path("peer_storage")

PEERS = [5001, 5002, 5003, 5004]
REQUIRED_REPLICAS = 2


def calculate_hash(data):
    """Calculate SHA-256 checksum."""
    return hashlib.sha256(data).hexdigest()


def verify_replicas():

    if not MANIFEST_FILE.is_file():
        print("ERROR: Manifest not found.")
        return False

    with MANIFEST_FILE.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    chunks = manifest["chunks"]
    total_chunks = len(chunks)

    all_valid = True

    # Track which valid chunks each peer stores
    peer_chunks = {port: set() for port in PEERS}

    # Verify every replica
    for chunk in chunks:

        index = chunk["index"]
        filename = chunk["filename"]
        expected_hash = chunk["sha256"]
        expected_size = chunk["size"]

        valid_replicas = []

        print(f"\nChecking Chunk {index}")

        for port in PEERS:

            path = STORAGE_FOLDER / str(port) / filename

            if not path.is_file():
                continue

            data = path.read_bytes()

            if len(data) != expected_size:
                print(f"  Peer {port}: SIZE ERROR")
                all_valid = False
                continue

            if calculate_hash(data) != expected_hash:
                print(f"  Peer {port}: CORRUPTED")
                all_valid = False
                continue

            print(f"  Peer {port}: VERIFIED")

            valid_replicas.append(port)
            peer_chunks[port].add(index)

        print(
            f"  Valid replicas: {len(valid_replicas)}"
        )

        if len(valid_replicas) < REQUIRED_REPLICAS:
            print("  ERROR: Insufficient replicas!")
            all_valid = False

    # Check that no peer stores all chunks
    print("\nChecking peer storage distribution")

    for port in PEERS:

        stored = peer_chunks[port]

        print(
            f"Peer {port}: {len(stored)} "
            f"of {total_chunks} valid chunks"
        )

        if len(stored) == total_chunks:
            print(
                f"ERROR: Peer {port} stores the complete file."
            )
            all_valid = False

    if all_valid:
        print("\nSUCCESS: Replica verification passed!")
    else:
        print("\nERROR: Replica verification failed.")

    return all_valid


if __name__ == "__main__":
    verify_replicas()
