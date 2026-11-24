"""Packet format of the P2P-UDP protocol (see PROTOCOL.md)."""

import json
import struct

VERSION = 1

# version, type, flags, stream, seq, length  (big-endian, 10 bytes)
HEADER = struct.Struct("!BBBBIH")
HEADER_SIZE = HEADER.size

# Message types
JOIN = 0x01
WELCOME = 0x02
HELLO = 0x03
LEAVE = 0x04
HEARTBEAT = 0x05
ACK = 0x06
CHAT = 0x10
FILE_ANNOUNCE = 0x20
HAVE_CHUNK = 0x21
CHUNK_REQUEST = 0x22
CHUNK_DATA = 0x23
CHUNK_ERROR = 0x24
STORE_CHUNK = 0x25

TYPE_NAMES = {
    JOIN: "JOIN", WELCOME: "WELCOME", HELLO: "HELLO", LEAVE: "LEAVE",
    HEARTBEAT: "HEARTBEAT", ACK: "ACK", CHAT: "CHAT",
    FILE_ANNOUNCE: "FILE_ANNOUNCE", HAVE_CHUNK: "HAVE_CHUNK",
    CHUNK_REQUEST: "CHUNK_REQUEST", CHUNK_DATA: "CHUNK_DATA",
    CHUNK_ERROR: "CHUNK_ERROR", STORE_CHUNK: "STORE_CHUNK",
}

# Flags
FLAG_RELIABLE = 0x01

# Streams
STREAM_CONTROL = 0
STREAM_FILE = 1

# Sizes
MAX_DATAGRAM = 2048
PIECE_SIZE = 1024
CHUNK_SIZE = 64 * 1024

# Timers and limits
WINDOW_SIZE = 32
RETRANSMIT_TIMEOUT = 0.3
MAX_RETRIES = 10
HEARTBEAT_INTERVAL = 1.0
DEAD_TIMEOUT = 5.0
REPLICATION = 2


def pack(msg_type, payload=b"", flags=0, stream=STREAM_CONTROL, seq=0):
    """Build a packet: header + payload bytes."""
    return HEADER.pack(VERSION, msg_type, flags, stream, seq, len(payload)) + payload


def unpack(data):
    """Parse a packet. Returns (type, flags, stream, seq, payload) or None if invalid."""
    if len(data) < HEADER_SIZE:
        return None
    version, msg_type, flags, stream, seq, length = HEADER.unpack_from(data)
    if version != VERSION or length != len(data) - HEADER_SIZE:
        return None
    return msg_type, flags, stream, seq, data[HEADER_SIZE:]


def to_json(obj):
    return json.dumps(obj).encode("utf-8")


def from_json(payload):
    return json.loads(payload.decode("utf-8")) if payload else {}


# Chunk piece: name_len(1) name chunk_index(4) offset(4) chunk_length(4) data
_PIECE_HEAD = struct.Struct("!III")


def pack_piece(file_name, chunk_index, offset, chunk_length, data):
    name = file_name.encode("utf-8")
    return bytes([len(name)]) + name + _PIECE_HEAD.pack(chunk_index, offset, chunk_length) + data


def unpack_piece(payload):
    """Returns (file_name, chunk_index, offset, chunk_length, data)."""
    name_len = payload[0]
    name = payload[1:1 + name_len].decode("utf-8")
    pos = 1 + name_len
    chunk_index, offset, chunk_length = _PIECE_HEAD.unpack_from(payload, pos)
    return name, chunk_index, offset, chunk_length, payload[pos + _PIECE_HEAD.size:]


if __name__ == "__main__":
    # Self-test
    pkt = pack(CHAT, to_json({"text": "hi"}), FLAG_RELIABLE, STREAM_CONTROL, 7)
    assert len(pkt) == HEADER_SIZE + len(b'{"text": "hi"}')
    t, f, s, q, p = unpack(pkt)
    assert (t, f, s, q, from_json(p)) == (CHAT, FLAG_RELIABLE, 0, 7, {"text": "hi"})
    assert pkt[:4] == bytes([1, CHAT, 1, 0]) and pkt[4:8] == b"\x00\x00\x00\x07"
    assert unpack(pkt[:-1]) is None              # wrong length
    assert unpack(b"\x02" + pkt[1:]) is None     # wrong version
    piece = pack_piece("a.bin", 3, 1024, 65536, b"x" * PIECE_SIZE)
    assert unpack_piece(piece) == ("a.bin", 3, 1024, 65536, b"x" * PIECE_SIZE)
    assert len(pack(CHUNK_DATA, piece)) < 1500
    print("protocol.py self-test passed")
