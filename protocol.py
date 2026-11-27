"""Our packet format (see PROTOCOL.md).

Every packet = 14-byte header + payload.
Header fields: version, type, flags, stream, session, seq, length
"""

import struct
import json

VERSION = 1

# ! = network byte order (big-endian)
# B = 1 byte, I = 4 bytes, H = 2 bytes
HEADER_FORMAT = "!BBBBIIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)   # 14 bytes

# Message types
JOIN = 1
WELCOME = 2
HELLO = 3
LEAVE = 4
HEARTBEAT = 5
ACK = 6
CHAT = 16

# Flags
RELIABLE = 1    # the receiver must send an ACK


def make_packet(msg_type, session, seq=0, data=None, reliable=False, stream=0):
    """Build a packet: header + JSON payload."""

    if data is None:
        payload = b""
    else:
        payload = json.dumps(data).encode("utf-8")

    if reliable:
        flags = RELIABLE
    else:
        flags = 0

    header = struct.pack(
        HEADER_FORMAT,
        VERSION, msg_type, flags, stream,
        session, seq, len(payload)
    )

    return header + payload


def read_packet(packet):
    """Read a packet. Returns a dictionary, or None if the packet is invalid."""

    if len(packet) < HEADER_SIZE:
        return None

    header = packet[:HEADER_SIZE]
    payload = packet[HEADER_SIZE:]

    version, msg_type, flags, stream, session, seq, length = struct.unpack(
        HEADER_FORMAT, header
    )

    if version != VERSION:
        return None

    if length != len(payload):
        return None

    if payload:
        data = json.loads(payload.decode("utf-8"))
    else:
        data = {}

    return {
        "type": msg_type,
        "reliable": flags == RELIABLE,
        "stream": stream,
        "session": session,
        "seq": seq,
        "data": data
    }


# Run "python3 protocol.py" to test this file
if __name__ == "__main__":

    packet = make_packet(CHAT, 1234, 7, {"text": "hi"}, reliable=True)
    print("Packet bytes:", packet)
    print("Header size:", HEADER_SIZE)

    result = read_packet(packet)
    print("Read back:", result)

    assert result["type"] == CHAT
    assert result["session"] == 1234
    assert result["seq"] == 7
    assert result["data"]["text"] == "hi"
    assert read_packet(packet[:-1]) is None    # broken length is rejected

    print("protocol.py test passed")

# File-sharing message types (sent on stream 1)
FILE_ANNOUNCE = 32
HAVE_CHUNK = 33
CHUNK_REQUEST = 34
CHUNK_DATA = 35
CHUNK_ERROR = 36
STORE_CHUNK = 37
