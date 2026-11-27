
import json
import hashlib
from pathlib import Path

MANIFEST_FILE = Path("manifest.json")
CHUNK_FOLDER = Path("chunks")


def calculate_hash(data):
    return hashlib.sha256(data).hexdigest()


def verify_chunks():

    with open(MANIFEST_FILE, "r") as file:
        manifest = json.load(file)

    all_valid = True

    for chunk in manifest["chunks"]:

        filename = chunk["filename"]
        expected_hash = chunk["sha256"]
        expected_size = chunk["size"]

        path = CHUNK_FOLDER / filename

        if not path.exists():
            print(f"MISSING: {filename}")
            all_valid = False
            continue

        data = path.read_bytes()

        actual_hash = calculate_hash(data)
        actual_size = len(data)

        if actual_size != expected_size:
            print(f"SIZE ERROR: {filename}")
            all_valid = False

        elif actual_hash != expected_hash:
            print(f"CORRUPTED: {filename}")
            all_valid = False

        else:
            print(f"VERIFIED: {filename}")

    if all_valid:
        print("\nSUCCESS: All chunks verified!")
    else:
        print("\nERROR: Chunk verification failed.")


if __name__ == "__main__":
    verify_chunks()
