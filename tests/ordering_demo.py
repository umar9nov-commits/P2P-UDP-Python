
# Simulate UDP packets arriving out of order

packets = [
    (1, "Hello"),
    (3, "Good morning"),
    (2, "How are you?"),
    (5, "See you"),
    (4, "I am fine")
]

expected_seq = 1
buffer = {}

for seq, message in packets:

    print(f"\nPacket arrived: seq={seq}")

    if seq < expected_seq:
        print("Duplicate or old packet. Ignored.")
        continue

    # Store packet until its turn comes
    buffer[seq] = message

    # Display all consecutive packets available
    while expected_seq in buffer:

        text = buffer.pop(expected_seq)

        print(
            f"Delivered seq={expected_seq}: {text}"
        )

        expected_seq += 1

    print("Waiting for sequence:", expected_seq)
    print("Buffered sequences:", sorted(buffer.keys()))

print("\nFinal delivery complete.")
