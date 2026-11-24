
from pathlib import Path
import hashlib

CHUNK_FOLDER = Path("chunks")
ORIGINAL_FILE = Path("sample.bin")
RECOVERED_FILE = Path("recovered_sample.bin")


def calculate_hash(filename):
    """Calculate the SHA-256 checksum of a file."""

    sha256 = hashlib.sha256()

    with open(filename, "rb") as file:
        while True:
            data = file.read(4096)

            if not data:
                break

            sha256.update(data)

    return sha256.hexdigest()


def reassemble_file():
    """Combine chunks into the original file."""

    chunks = sorted(CHUNK_FOLDER.glob("chunk_*.bin"))

    if not chunks:
        print("No chunks found.")
        return

    with open(RECOVERED_FILE, "wb") as output:

        for chunk in chunks:

            data = chunk.read_bytes()
            output.write(data)

            print(
                f"Added {chunk.name}: {len(data)} bytes"
            )

    print("\nFile reassembly completed.")

    print(
        "Recovered file size:",
        RECOVERED_FILE.stat().st_size,
        "bytes"
    )

    if ORIGINAL_FILE.exists():

        original_hash = calculate_hash(ORIGINAL_FILE)
        recovered_hash = calculate_hash(RECOVERED_FILE)

        print("\nOriginal SHA-256:", original_hash)
        print("Recovered SHA-256:", recovered_hash)

        if original_hash == recovered_hash:
            print("\nSUCCESS: Files are identical!")
        else:
            print("\nERROR: Files do not match!")


if __name__ == "__main__":
    reassemble_file()
