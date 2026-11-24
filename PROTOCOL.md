# P2P-UDP Protocol Specification (version 1)

This document defines the application-layer protocol used by every peer.
Any implementation that follows it exactly can interoperate with ours.

## 1. Transport and addressing

- Transport: UDP over IPv4.
- A peer is identified by its address string `"ip:port"` (e.g. `"192.168.1.5:5001"`).
- The list of members is always kept **sorted by this string**, so every peer
  computes the same order (used for chunk placement).
- All multi-byte integers are **big-endian (network byte order)**.
- Maximum datagram size: 2048 bytes. File data is sent in pieces of
  1024 bytes so a packet never exceeds a normal Ethernet MTU (1500 bytes).

## 2. Packet format

Every packet = 10-byte header + payload.

    0        1        2        3        4                 8        10
    +--------+--------+--------+--------+-----------------+--------+---------...
    |version |  type  | flags  | stream |   seq (32 bit)  | length | payload
    +--------+--------+--------+--------+-----------------+--------+---------...

| Field   | Size | Description |
|---------|------|-------------|
| version | 1 B  | Protocol version. Always `1`. Packets with another version are dropped. |
| type    | 1 B  | Message type (table in section 3). |
| flags   | 1 B  | Bit 0 = `RELIABLE` (receiver must ACK). Other bits are 0. |
| stream  | 1 B  | Logical stream: `0` = control/chat, `1` = file data. |
| seq     | 4 B  | Sequence number of this packet in its stream (starts at 1). `0` for unreliable packets. In an ACK it is the seq being acknowledged. |
| length  | 2 B  | Payload length in bytes. Must equal datagram size - 10, otherwise the packet is dropped. |
| payload | var  | JSON (UTF-8) for control messages, binary for chunk data (section 4). |

Python `struct` format: `"!BBBBIH"`.

## 3. Message types

| Code | Name          | Stream | Reliable | Payload |
|------|---------------|--------|----------|---------|
| 0x01 | JOIN          | 0 | yes | `{}` |
| 0x02 | WELCOME       | 0 | yes | `{"you": addr, "peers": [addr,...], "files": {name: FileInfo}}` |
| 0x03 | HELLO         | 0 | yes | `{}` |
| 0x04 | LEAVE         | 0 | yes | `{}` |
| 0x05 | HEARTBEAT     | 0 | no  | empty |
| 0x06 | ACK           | same as acked packet | no | empty (seq = acked seq) |
| 0x10 | CHAT          | 0 | yes | `{"text": str}` |
| 0x20 | FILE_ANNOUNCE | 0 | yes | `{"name": str, "info": FileInfo}` |
| 0x21 | HAVE_CHUNK    | 0 | yes | `{"file": str, "chunk": int}` |
| 0x22 | CHUNK_REQUEST | 0 | yes | `{"file": str, "chunk": int}` |
| 0x23 | CHUNK_DATA    | 1 | yes | binary, section 4 |
| 0x24 | CHUNK_ERROR   | 0 | yes | `{"file": str, "chunk": int}` (sender does not have that chunk) |
| 0x25 | STORE_CHUNK   | 1 | yes | binary, section 4 (receiver must store the chunk) |

`FileInfo` (JSON object):

    {
      "size": 2500000,
      "sha256": "<hex of whole file>",
      "chunk_size": 65536,
      "chunks": ["<hex sha256 of chunk 0>", "<chunk 1>", "..."],
      "holders": [["ip:port", "ip:port"], ["ip:port", "ip:port"], "..."]
    }

`holders[i]` is the list of peers that store chunk `i`.

## 4. Chunk data payload (CHUNK_DATA and STORE_CHUNK)

A chunk (64 KiB, the last one may be smaller) is sent as a series of pieces
of at most 1024 bytes. Each piece is one packet:

    +----------+-----------+-------------+------------+-------------+--------...
    | name_len | file name | chunk index |   offset   | chunk length| data
    |   1 B    | name_len B|    4 B      |    4 B     |     4 B     | <=1024 B
    +----------+-----------+-------------+------------+-------------+--------...

- `offset` = position of this piece inside the chunk.
- The receiver has the whole chunk when it has received `chunk length` bytes.
- It then checks the chunk's SHA-256 against `FileInfo.chunks[i]`.
  If it does not match, the chunk is discarded and requested again.

## 5. Reliability (TCP-like, without congestion control)

Applies to every packet with the `RELIABLE` flag.

- **Separate sequence space** per (sender, receiver, stream). Streams 0 and 1
  are independent, so a big file transfer never delays chat messages.
- **Sender**
  - Gives each packet the next seq (1, 2, 3, ...).
  - Keeps up to **32 unacknowledged packets** per (receiver, stream) (sliding
    window). If the window is full, sending waits.
  - Retransmits a packet if no ACK arrives within **0.3 s**.
  - After **10** retransmissions of the same packet, the receiver is declared dead.
- **Receiver**
  - Sends an ACK for every reliable packet it gets, including duplicates
    (selective ACK: the ACK names exactly one seq).
  - `seq` lower than the next expected seq: duplicate, ACKed and dropped.
  - `seq` equal to next expected: delivered, then any buffered packets that
    now follow in order are delivered too.
  - `seq` higher than expected: buffered until the gap is filled.
- Result: per sender and stream, messages arrive **in order, without
  duplicates, and without loss**.

## 6. Membership

- **Timers**: every peer sends HEARTBEAT to every member every **1 s**.
  Any packet from a peer counts as proof it is alive. A peer silent for
  **5 s** is declared crashed.
- When a peer is declared dead or leaves, all state about it (sequence
  numbers, buffers) is deleted and it is removed from every `holders` list.

### Join sequence (bootstrap peer B is any running peer)

    New peer N                Bootstrap B              Other member M
        | ---- JOIN ------------> |                         |
        | <--- WELCOME ---------- |  (peer list + file table)
        | ---- HELLO --------------------------------------> |   (to every member)
        |                         |                          | adds N to its list

After WELCOME, N no longer needs B; B can leave.
The first peer of a group starts with no bootstrap and is alone in the group.

### Leave sequence

    Leaving peer L ---- LEAVE ----> every member (members remove L)

### Crash

No message. Members detect it with the 5 s heartbeat timeout (or a
retransmission failure).

## 7. Group chat

    Sender S ---- CHAT {"text"} ----> every member (reliable, stream 0)

Each member displays messages from S in the order of S's seq numbers.

## 8. File sharing

### Placement rule

`members` = sorted list of all peers (including the sharer) at share time, `n` = its length.
Chunk `i` is stored on `members[i mod n]` and `members[(i+1) mod n]`.
With n >= 3, no peer stores every chunk, and every chunk has 2 replicas.

### Share sequence

    Sharer S: split file, compute hashes, compute holders
    S ---- FILE_ANNOUNCE ----> every member
    S ---- STORE_CHUNK pieces (stream 1) ----> each holder of each chunk
    Holder H, once the chunk is complete and its hash is correct:
    H ---- HAVE_CHUNK ----> every member   (members add H to holders[i])

S keeps only the chunks the rule assigns to S. The original file is just the
input and is not part of the shared storage.

### Download sequence (multi-source)

    Downloader D: for each chunk choose a holder, spreading chunks over
                  different holders, and fetch from several holders in parallel
    D ---- CHUNK_REQUEST {file, chunk} ----> holder H
    H ---- CHUNK_DATA pieces (stream 1) ----> D
    D checks chunk hash; when all chunks are done, checks whole-file hash

- If H replies CHUNK_ERROR, sends a bad chunk, or dies (timeout), D requests
  the same chunk from another holder in `holders[i]`.

### New peer taking its share

After WELCOME, new peer N at position `k` in the sorted member list (size `n`)
fetches every chunk `i` with `i mod n == k` (same as a download),
stores it, and sends HAVE_CHUNK to every member.

### Re-replication after a peer leaves or crashes

For every chunk that now has only one holder, that holder picks the first
member after itself (in sorted order) that does not have the chunk and sends it
STORE_CHUNK. The new holder then sends HAVE_CHUNK as usual.

## 9. Constants summary

| Constant            | Value |
|---------------------|-------|
| Protocol version    | 1 |
| Header size         | 10 bytes |
| Chunk size          | 65536 bytes |
| Piece size          | 1024 bytes |
| Window size         | 32 packets per (peer, stream) |
| Retransmit timeout  | 0.3 s |
| Max retransmissions | 10 |
| Heartbeat interval  | 1 s |
| Dead-peer timeout   | 5 s |
| Replication factor  | 2 |
