# Understanding the Tricky Parts (Documentation++)

Building reliable transport protocols on top of datagram sockets involves classic distributed systems subtleties. This guide dives into the trickier parts of the codebase.

---

## 1. Selective ACK (SACK) 64-Bit Bitmask

### The Problem
If the receiver gets chunks `0, 1, 3, 4, 5` (chunk `2` was lost), standard cumulative ACK can only say:
> *"I have received everything up to chunk 1."* (`ack_num = 2`)

The sender wouldn't know whether chunks `3, 4, 5` made it, and in a pure Go-Back-N scheme, it would wastefully retransmit `2, 3, 4, 5`.

### How Our SACK Mask Works
In `rftp/protocol/packet.py` and `rftp/protocol/window.py`, our ACK header has:
- `ack_num: int`: First missing chunk (`base`).
- `sack_bitmask: int`: A 64-bit window relative to `ack_num`.

Each bit `i` (from `0` to `63`) represents whether chunk `ack_num + 1 + i` was received:

```
ack_num = 2  (Chunk 2 is missing)

Bit index:      0     1     2     3   ...   63
Target chunk: (2+1) (2+2) (2+3) (2+4) ... (2+64)
Chunk #:        3     4     5     6   ...   66
Bit value:      1     1     1     0   ...    0
```

#### On the Receiver Side (`rftp/protocol/window.py`):
```python
ack_num = self.base
sack_bitmask = 0

for i in range(64):
    check_seq = ack_num + 1 + i
    if check_seq in self.out_of_order:
        sack_bitmask |= 1 << i
```

#### On the Sender Side (`rftp/protocol/window.py`):
```python
for i in range(64):
    if (sack_bitmask & (1 << i)) != 0:
        sack_seq = ack_num + 1 + i
        if sack_seq in self.in_flight:
            self.in_flight[sack_seq].acked = True  # Delivered!
```
**Result**: The sender immediately knows that chunks `3, 4, 5` are safe, and retransmits **only chunk 2**.

---

## 2. Karn's Algorithm for RTT & RTO Calculation

### The Ambiguity Problem
Suppose chunk `5` is sent at `t = 0ms`. At `t = 200ms`, the timer expires, so chunk `5` is retransmitted. At `t = 210ms`, an ACK arrives.
- Did the ACK correspond to the **first** transmission (which was just delayed by 210ms)?
- Or did it correspond to the **second** transmission (which took only 10ms)?

If you calculate RTT based on retransmissions, you either artificially inflate or severely underestimate round-trip time.

### The Solution: Karn's Rule
In `rftp/protocol/window.py`:
```python
# Only take RTT samples for packets that were never retransmitted
if entry.retransmit_count == 0 and rtt_sample is None:
    rtt_sample = max(0.0001, now - entry.sent_at)
```
When a clean sample is obtained, the dynamic retransmission timeout (RTO) adapts:
```python
rto = min(0.8, max(0.08, rtt_sample * 2))
```
This ensures the timer is responsive to fast local networks (~1ms RTT) without premature timeouts during bursts.

---

## 3. Direct Offset Disk Streaming (Zero-RAM Overhead)

### Why Not Buffer Everything in Memory?
A common beginner mistake in custom transfer protocols is collecting chunks in a `bytearray` or hash table in RAM until the transfer completes.
- For a 50 MB file, that consumes 50 MB RAM.
- For a 4 GB video or ISO, it will crash low-memory laptops with out-of-memory (OOM) errors.

### Seekable Chunker & Direct Writer
In `rftp/file/chunker.py` and `rftp/file/writer.py`:
1. **Reading**: The sender opens the file once and seeks to the chunk's offset:
   ```python
   offset = chunk_index * self.chunk_size
   self._file.seek(offset)
   data = self._file.read(bytes_to_read)
   ```
2. **Writing**: When the receiver gets chunk `seq = 42` before chunk `seq = 2`, it writes chunk 42 straight to disk at `offset = 42 * chunk_size`:
   ```python
   offset = chunk_index * self.chunk_size
   self._file.seek(offset)
   self._file.write(data)
   ```
Because the OS filesystem supports seekable random-access writes, memory usage remains virtually constant (~12 MB total process footprint), whether transferring a 10 KB text snippet or an 8 GB disk image.

---

## 4. Idempotent Handshake Handling

### The Problem
The sender sends `START`. The receiver creates the file on disk and responds with `START_ACK`.
However, Wi-Fi interference drops the `START_ACK` datagram.
The sender times out after 300 ms and retransmits `START`.

If the receiver blindly executed:
```python
# NAIVE APPROACH:
FileWriter(...)  # <-- Truncates existing file back to 0 bytes!
```
It would erase any chunks already in flight!

### The Solution
In `rftp/network/receiver.py`:
```python
if writer is not None and sender_addr == from_addr:
    # We are already actively receiving this transfer from this peer.
    # Just re-emit the START_ACK and keep our existing disk writer intact!
    ack = Packet.new_start_ack(True, "Ready to receive").serialize()
    sock.send_to(ack, from_addr)
    continue
```
This makes the handshake completely idempotent under severe initial packet loss.

---

## 5. Clean Asynchronous GUI Architecture

The UI runs on standard Tkinter on the main thread while network workers run on separate background threads:

```
[ GUI Main Thread (Tkinter event loop) ]
       ^                  |
       |                  | threading.Event (cancel_event)
       | (thread-safe queue.Queue)
       | TransferEvent { Progress, Log, Success, Error }
       |
[ Network Worker Thread (run_sender / run_receiver) ]
       |
[ Non-blocking UDP Socket (socket.SOCK_DGRAM) ]
```

- The GUI never blocks on socket I/O, preventing application unresponsiveness.
- Clicking "Cancel" simply sets an atomic event (`cancel_event.set()`), which prompts the worker thread to send a `CANCEL` datagram to the peer and cleanly shut down.
