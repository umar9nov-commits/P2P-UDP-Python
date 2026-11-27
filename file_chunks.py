
from pathlib import Path
import hashlib

CHUNK_SIZE = 4096


def split_file(filename):
    """Split a file into smaller chunks."""

    source = Path(filename)

    if not source.is_file():
        print("File not found.")
        return

    output_folder = Path("chunks")
    output_folder.mkdir(exist_ok=True)

    chunk_number = 0

    with source.open("rb") as file:

        while True:
            data = file.read(CHUNK_SIZE)

            if not data:
                break

            chunk_path = output_folder / (
                f"chunk_{chunk_number:04d}.bin"
            )

            chunk_path.write_bytes(data)

            checksum = hashlib.sha256(data).hexdigest()

            print(
                f"Chunk {chunk_number}: "
                f"{len(data)} bytes, "
                f"SHA256={checksum}"
            )

            chunk_number += 1

    print(f"Total chunks created: {chunk_number}")


def main():

    filename = input("Enter file name: ").strip()
    split_file(filename)


if __name__ == "__main__":
    main()
