# How RFTP Works Under the Hood

RFTP (**Reliable File Transfer Protocol**) is a lightweight, connection-oriented reliability layer implemented entirely in user space on top of UDP.

This document breaks down how the system works end-to-end—from the moment you pick a file to the final cryptographic check.

---

## 1. High-Level Lifecycle

Every transfer goes through three distinct stages:

```
[ Handshake ]  --->  [ Sliding Window Data Transfer ]  --->  [ Verification & FIN ]
 (START / ACK)         (DATA chunks + SACK packets)          (SHA-256 Check & FIN_ACK)
```

### Phase 1: Connection & Negotiation (`START`)
1. The **Sender** opens the target file, calculates its total length, chunk count, and whole-file SHA-256 hash upfront.
2. It sends a `START` packet containing:
   - File name
   - File size in bytes
   - Total number of chunks
   - Chunk size (default: 1,400 bytes, chosen to safely fit within typical 1,500-byte MTUs without IP fragmentation)
   - 32-byte SHA-256 hash
3. The **Receiver** verifies directory permissions, pre-allocates the file on disk using `set_len()`, and responds with `START_ACK`.
4. If `START_ACK` gets lost, the sender retransmits `START` up to 20 times. The receiver handles duplicate `START` packets idempotently without wiping written data.

---

## 2. Phase 2: Sliding Window & Selective Repeat

Once the handshake is confirmed, the sender streams chunks using a **Sliding Window**:

```
               [------- Window Size: W = 4 -------]
Chunk:   [ 0 ]   [ 1 ]   [ 2 ]   [ 3 ]   [ 4 ]   [ 5 ]   [ 6 ] ...
Status:  ACKed   ACKed   SENT    LOST    SENT    PENDING PENDING
                         ^
                        Base
```

### Sending Chunks
- The sender transmits all chunks from `base` up to `base + window_size`.
- As long as `next_seq < base + window_size`, new chunks are read directly from disk via file seeking and sent over the UDP socket.
- Each transmitted chunk is tracked in memory (`InFlightChunk`) with its transmission timestamp and retry count.

### Processing Acknowledgements (Cumulative + SACK)
Instead of plain Go-Back-N (which re-transmits everything from the lost packet onwards), RFTP uses **Selective Repeat** with a 64-bit SACK bitmask:
- `ack_num`: The cumulative base. All chunks strictly `< ack_num` have been received and written to disk.
- `sack_bitmask`: A 64-bit mask where bit `i` is set to `1` if chunk `ack_num + 1 + i` was received out of order.

When an ACK packet arrives at the sender:
1. Chunks below `ack_num` are removed from the in-flight table.
2. The sender's `base` slides forward.
3. Chunks flagged in the `sack_bitmask` are marked as delivered, so they will **not** be retransmitted.
4. RTT is sampled for smooth RTO calculation (using Karn's algorithm: only packets sent exactly once are used for timing).

### Retransmission & Timeout Logic
- Every cycle, the sender scans its in-flight list:
  ```rust
  if now.duration_since(chunk.sent_at) >= rto {
      retransmit(chunk);
  }
  ```
- If a chunk is missing while later chunks have arrived, the receiver's SACK bitmask indicates the gap immediately.
- The retransmission timer adapts dynamically based on round-trip time, clamped between **80 ms** and **800 ms**.

---

## 3. Phase 3: Verification & Termination

1. Once all chunks are cumulatively acknowledged (`base >= total_chunks`), the sender emits a `FIN` packet containing the file's expected SHA-256 hash.
2. The receiver:
   - Flushes all pending writes to disk.
   - Truncates the file to the exact byte count.
   - Reads the file from disk and computes its SHA-256 checksum.
   - Compares the calculated hash with the sender's expected hash.
3. If the hashes match, the receiver sends `FIN_ACK(success: true)`.
4. If a mismatch is detected, the receiver returns an error code and keeps the temporary file for inspection.

---

## 4. Socket Architecture & Non-Blocking Polling

Both the Sender and Receiver threads use non-blocking UDP sockets (`socket.set_nonblocking(true)`):
- During heavy transfer loops, draining incoming ACKs doesn't block the thread when the socket queue is empty.
- A tiny sub-millisecond yield (`thread::sleep(Duration::from_micros(500))`) prevents 100% CPU core spinning while keeping latency low.
- Inter-thread communication with the GUI uses lock-free crossbeam channels, ensuring the UI remains responsive at 60 FPS even during maximum throughput transfers.
