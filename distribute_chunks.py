
import json
import hashlib
import shutil
from pathlib import Path

# Project folders
CHUNK_FOLDER = Path("chunks")
STORAGE_FOLDER = Path("peer_storage")
MANIFEST_FILE = Path("manifest.json")

# Define which peers store each chunk
DISTRIBUTION = {
    0: [5001, 5002],
    1: [5002, 5003],
    2: [5003, 5004]
}


def calculate_hash(data):
    """Calculate SHA-256 checksum."""

    return hashlib.sha256(data).hexdigest()


def distribute_chunks():

    # Load the file manifest
    with MANIFEST_FILE.open("r") as file:
        manifest = json.load(file)

    chunks = manifest["chunks"]

    # Check that every chunk has a placement plan
    expected_indices = {
        chunk["index"] for chunk in chunks
    }

    if expected_indices != set(DISTRIBUTION):
        print("ERROR: Distribution plan does not match chunks.")
        return

    # Check replica count and prevent full-file storage
    peer_chunks = {}

    for index, ports in DISTRIBUTION.items():

        if len(ports) < 2 or len(ports) != len(set(ports)):
            print(f"ERROR: Invalid replicas for chunk {index}.")
            return

        for port in ports:
            peer_chunks.setdefault(port, set()).add(index)

    total_chunks = len(chunks)

    if any(
        len(indices) >= total_chunks
        for indices in peer_chunks.values()
    ):
        print("ERROR: A peer would store the complete file.")
        return

    # Validate all source chunks before copying anything
    for chunk in chunks:

        path = CHUNK_FOLDER / chunk["filename"]

        if not path.is_file():
            print(f"ERROR: Missing source {path}")
            return

        data = path.read_bytes()

        if len(data) != chunk["size"]:
            print(f"ERROR: Size mismatch in {path}")
            return

        if calculate_hash(data) != chunk["sha256"]:
            print(f"ERROR: Corrupted source {path}")
            return

    # Create the storage directories
    for port in sorted(peer_chunks):
        (STORAGE_FOLDER / str(port)).mkdir(
            parents=True,
            exist_ok=True
        )

    # Copy chunks to their assigned peer folders
    for chunk in chunks:

        index = chunk["index"]
        filename = chunk["filename"]
        source = CHUNK_FOLDER / filename

        for port in DISTRIBUTION[index]:

            destination = STORAGE_FOLDER / str(port) / filename

            shutil.copyfile(source, destination)

            print(
                f"Chunk {index} copied to Peer {port}"
            )

    print("\nDistribution completed successfully.")


if __name__ == "__main__":
    distribute_chunks()
