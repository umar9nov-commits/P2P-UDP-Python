
import json
import hashlib
from pathlib import Path

# File and folder locations
ORIGINAL_FILE = Path("sample.bin")
CHUNK_FOLDER = Path("chunks")
MANIFEST_FILE = Path("manifest.json")

CHUNK_SIZE = 4096


def calculate_hash(data):
    """Calculate the SHA-256 checksum of bytes."""

    return hashlib.sha256(data).hexdigest()


def create_manifest():

    # Check whether the original file exists
    if not ORIGINAL_FILE.is_file():
        print("Error: Original file not found.")
        return

    # Get all chunk files in order
    chunks = sorted(CHUNK_FOLDER.glob("chunk_*.bin"))

    if not chunks:
        print("Error: No chunks found.")
        return

    # Read original file information
    original_data = ORIGINAL_FILE.read_bytes()

    # Create the file manifest
    manifest = {
        "version": 1,
        "filename": ORIGINAL_FILE.name,
        "file_size": len(original_data),
        "file_sha256": calculate_hash(original_data),
        "chunk_size": CHUNK_SIZE,
        "total_chunks": len(chunks),
        "chunks": []
    }

    # Add information about each chunk
    for index, chunk in enumerate(chunks):

        chunk_data = chunk.read_bytes()

        # Verify each chunk against the original file
        expected_data = original_data[
            index * CHUNK_SIZE:(index + 1) * CHUNK_SIZE
        ]

        if chunk_data != expected_data:
            print(f"Error: {chunk.name} does not match original file.")
            return

        chunk_info = {
            "index": index,
            "filename": chunk.name,
            "size": len(chunk_data),
            "sha256": calculate_hash(chunk_data)
        }

        manifest["chunks"].append(chunk_info)

    # Verify the expected number of chunks
    expected_count = (
        len(original_data) + CHUNK_SIZE - 1
    ) // CHUNK_SIZE

    if len(chunks) != expected_count:
        print("Error: Incorrect number of chunks.")
        return

    # Save the manifest as JSON
    with MANIFEST_FILE.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=4)

    print("Manifest created successfully.")
    print("File:", MANIFEST_FILE)
    print("Total chunks:", len(chunks))
    print("Original file size:", len(original_data), "bytes")
    print("Original SHA-256:", manifest["file_sha256"])


if __name__ == "__main__":
    create_manifest()
