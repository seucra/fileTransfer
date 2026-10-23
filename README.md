# Reliable UDP File Transfer (RFTP)

> A modern, peer-to-peer file transfer tool written in **Rust** that sends files reliably between two laptops over local Wi-Fi hotspots using custom **UDP** instead of TCP.

No router, internet connection, or external servers needed. Just connect both machines to the same Wi-Fi hotspot and send.

---

## Highlights

- **Zero-Internet P2P**: Direct laptop-to-laptop transfer over local Wi-Fi or ad-hoc hotspots.
- **Custom Reliability Layer**: Built on raw UDP with sequence numbers, a sliding window, Selective ACKs (SACK), and dynamic timeouts.
- **Bit-Level Integrity**: Per-packet CRC32 checksums drop corrupted packets immediately, followed by whole-file SHA-256 verification after transfer.
- **Modern Desktop GUI**: Clean dark-mode interface built with `eframe`/`egui`, featuring live progress bars, speed metrics, ETA, and event logs.
- **Headless CLI & Benchmarks**: Full CLI support for terminal workflows, plus an automated benchmark tool comparing custom UDP against TCP.
- **Simulated Packet Loss**: Built-in packet drop slider (0% to 50%) to demonstrate and test retransmission recovery in real time.

---

## Quickstart

### 1. Build from Source
Make sure you have Rust installed (`cargo` and `rustc >= 1.75`):

```bash
cargo build --release
```

### 2. Launch Desktop GUI (Default)
```bash
./target/release/file_transfer
```

### 3. Or Use the CLI
```bash
# On the receiver machine (Laptop B):
./target/release/file_transfer recv --dest ./downloads --bind 0.0.0.0:9999

# On the sender machine (Laptop A):
./target/release/file_transfer send --file video.mp4 --target 192.168.43.50:9999
```

---

## Demo Walkthrough: Two Laptops on a Hotspot

1. **Create Hotspot**: On Laptop A, turn on Mobile Hotspot (or run `nmcli dev wifi hotspot ssid "FileTransfer" password "password123"` on Linux).
2. **Connect**: Connect Laptop B to that hotspot.
3. **Find IP**: Note Laptop B's IP address (shown directly in the app's top-right corner, or run `ipconfig` / `hostname -I`).
4. **Firewall Note**: If prompted, allow UDP traffic on port `9999` (`sudo ufw allow 9999/udp` on Linux, or click "Allow" on Windows).
5. **Receive**: On Laptop B, switch to **Receiver Mode** and click **Start Listening**.
6. **Send**: On Laptop A, pick your file, enter Laptop B's IP, and click **Start Transfer**. Watch the real-time speed, progress bar, and SHA-256 verification banner.

---

## Documentation & Deeper Dives

Everything is documented in detail in the [`docs/`](docs/) directory:

- 📖 **[How It Works (`docs/WORKING.md`)](docs/WORKING.md)**: End-to-end lifecycle, handshake, sliding window flow, and non-blocking socket design.
- 🧠 **[Tricky Parts & Deep Dive (`docs/UNDERSTANDING_HARD_PARTS.md`)](docs/UNDERSTANDING_HARD_PARTS.md)**: 64-bit SACK bitmask math, Karn's RTT estimation, zero-RAM disk streaming, and handshake idempotency.
- 💡 **[Use Cases (`docs/USECASES.md`)](docs/USECASES.md)**: Air-gapped research labs, travel transfers, field robotics, and teaching.
- 📊 **[Project Report & Benchmarks (`docs/PROJECT_REPORT.md`)](docs/PROJECT_REPORT.md)**: Semester project report, protocol specification, and empirical TCP vs. UDP benchmark evaluation.

---

## Automated Tests

Run the test suite covering packet encoding, SACK bitmasks, timeouts, and multi-threaded transfers:

```bash
cargo test
```

To run the local empirical TCP vs. UDP benchmark:
```bash
./target/release/file_transfer benchmark --size-mb 5
```

---

## License

Distributed under the [MIT License](LICENSE).
