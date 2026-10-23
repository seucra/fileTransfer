# Understanding the Tricky Parts (Documentation++)

Building reliable transport protocols on top of datagram sockets involves classic distributed systems subtleties. This guide dives into the trickier parts of the codebase.

---

## 1. Selective ACK (SACK) 64-Bit Bitmask

### The Problem
If the receiver gets chunks `0, 1, 3, 4, 5` (chunk `2` was lost), standard cumulative ACK can only say:
> *"I have received everything up to chunk 1."* (`ack_num = 2`)

The sender wouldn't know whether chunks `3, 4, 5` made it, and in a pure Go-Back-N scheme, it would wastefully retransmit `2, 3, 4, 5`.

### How Our SACK Mask Works
In `src/protocol/packet.rs` and `src/protocol/window.rs`, our ACK header has:
- `ack_num: u32`: First missing chunk (`base`).
- `sack_bitmask: u64`: A 64-bit window relative to `ack_num`.

Each bit `i` (from `0` to `63`) represents whether chunk `ack_num + 1 + i` was received:

```
ack_num = 2  (Chunk 2 is missing)

Bit index:      0     1     2     3   ...   63
Target chunk: (2+1) (2+2) (2+3) (2+4) ... (2+64)
Chunk #:        3     4     5     6   ...   66
Bit value:      1     1     1     0   ...    0
```

#### On the Receiver Side:
```rust
let ack_num = self.base;
let mut sack_bitmask: u64 = 0;

for i in 0..64 {
    let check_seq = ack_num + 1 + (i as u32);
    if self.out_of_order.contains(&check_seq) {
        sack_bitmask |= 1u64 << i;
    }
}
```

#### On the Sender Side:
```rust
for i in 0..64 {
    if (sack_bitmask & (1u64 << i)) != 0 {
        let sack_seq = ack_num + 1 + (i as u32);
        if let Some(entry) = self.in_flight.get_mut(&sack_seq) {
            entry.acked = true; // Mark as safely delivered!
        }
    }
}
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
In `src/protocol/window.rs`:
```rust
// Only take RTT samples for packets that were never retransmitted
if entry.retransmit_count == 0 && rtt_sample.is_none() {
    rtt_sample = Some(now.saturating_duration_since(entry.sent_at));
}
```
When a clean sample is obtained, the dynamic retransmission timeout (RTO) adapts:
```rust
rto = (rtt * 2).clamp(Duration::from_millis(80), Duration::from_millis(800));
```
This ensures the timer is responsive to fast local networks (~1ms RTT) without premature timeouts during bursts.

---

## 3. Direct Offset Disk Streaming (Zero-RAM Overhead)

### Why Not Buffer Everything in Memory?
A common beginner mistake in custom transfer protocols is collecting chunks in a `Vec<u8>` or hash table in RAM until the transfer completes.
- For a 50 MB file, that consumes 50 MB RAM.
- For a 4 GB video or ISO, it will crash low-memory laptops with out-of-memory (OOM) errors.

### Seekable Chunker & Direct Writer
In `src/file/chunker.rs` and `src/file/writer.rs`:
1. **Reading**: The sender opens the file once and seeks to the chunk's offset:
   ```rust
   let offset = (chunk_index as u64) * (self.chunk_size as u64);
   self.file.seek(SeekFrom::Start(offset))?;
   self.file.read_exact(&mut buf)?;
   ```
2. **Writing**: When the receiver gets chunk `seq = 42` before chunk `seq = 2`, it writes chunk 42 straight to disk at `offset = 42 * chunk_size`:
   ```rust
   let offset = (chunk_index as u64) * (self.chunk_size as u64);
   self.file.seek(SeekFrom::Start(offset))?;
   self.file.write_all(data)?;
   ```
Because the OS filesystem supports sparse files and seekable random-access writes, memory usage remains virtually constant (~8 MB total process footprint), whether transferring a 10 KB text snippet or an 8 GB disk image.

---

## 4. Idempotent Handshake Handling

### The Problem
The sender sends `START`. The receiver creates the file on disk and responds with `START_ACK`.
However, Wi-Fi interference drops the `START_ACK` datagram.
The sender times out after 300 ms and retransmits `START`.

If the receiver blindly executed:
```rust
// NAIVE APPROACH:
FileWriter::new(...) // <-- Truncates existing file back to 0 bytes!
```
It would erase any chunks already in flight!

### The Solution
In `src/network/receiver.rs`:
```rust
if writer.is_some() && sender_addr == Some(from_addr) {
    // We are already actively receiving this transfer from this peer.
    // Just re-emit the START_ACK and keep our existing disk writer intact!
    let ack = Packet::new_start_ack(true, "Ready to receive").serialize();
    let _ = socket.send_to(&ack, from_addr);
    continue;
}
```
This makes the handshake completely idempotent under severe initial packet loss.

---

## 5. Clean Asynchronous GUI Architecture

The UI runs on `eframe` (Rust immediate-mode GUI) while the network workers run on separate OS threads:

```
[ GUI Thread (eframe/egui 60fps) ]
       ^                  |
       |                  | Arc<AtomicBool> (cancel_flag)
       | (unbounded mpsc channel)
       | TransferEvent { Progress, Log, Success, Error }
       |
[ Network Worker Thread (run_sender / run_receiver) ]
       |
[ Non-blocking UDP Socket ]
```

- The GUI never touches network sockets directly, preventing UI freezing or frame drops.
- Clicking "Cancel" simply sets an atomic bool (`cancel_flag.store(true)`), which prompts the worker thread to send a `CANCEL` datagram to the peer and cleanly shut down.
