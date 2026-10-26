# Practical Use Cases for RFTP

RFTP is designed to solve a specific, recurring problem: **how to move large files quickly and reliably between two devices when you don't have internet access, a router, or an external server.**

Here are primary scenarios where this architecture shines.

---

## 1. Laptop-to-Laptop File Sharing (Dorms, Travel & Commutes)

### The Scenario
You are on a train, flight, or in a college common room without Wi-Fi. You need to send a 2 GB lecture video, virtual machine image, or codebase zip to a classmate's laptop.

### Why RFTP Excels
- Laptop A turns on its mobile hotspot. Laptop B connects.
- No cellular data is used, and no router or internet connection is required.
- Unlike cloud-based file drop services (Google Drive, WeTransfer), data transfers directly device-to-device at high local Wi-Fi speeds without uploading to an external server.

---

## 2. Air-Gapped & Secure Environments

### The Scenario
Industrial control systems, embedded testbenches, and secure academic research labs often operate on isolated, air-gapped subnets where connecting to an external network or installing proprietary cloud clients is strictly prohibited by security policy.

### Why RFTP Excels
- **Zero External Dependencies**: Pure standard library in Python 3 (or single static binary in the Rust `dev` branch) with no runtime dependencies or third-party packages required.
- **No Telemetry or Accounts**: No tracking, phone-home mechanisms, or external licensing servers.
- **Cryptographic Guarantees**: Built-in SHA-256 verification guarantees bit-for-bit file authenticity before acknowledging the transfer as successful.

---

## 3. Network Education & Academic Demonstrations

### The Scenario
Computer Networks courses routinely teach transport-layer concepts (Go-Back-N vs Selective Repeat, cumulative ACKs, RTT estimation, timeouts, and checksums) purely on a whiteboard or via abstract packet simulators (like NS-3 or Cisco Packet Tracer).

### Why RFTP Excels
- **Visual & Tangible**: Students can watch the sliding window in action in the GUI.
- **Built-in Loss Simulation**: The simulated packet loss slider (0% to 50%) lets instructors demonstrate real-time packet drops, retransmission counters, and recovery behavior without needing `iptables`, `tc`, or external disruption tools.
- **Side-by-Side TCP Benchmark**: Students can run `python3 main.py benchmark` (or `cargo run -- benchmark` on `dev`) to compare how an application-space UDP reliability layer behaves compared to the kernel's native TCP stack.

---

## 4. Headless Systems & Automated Backup Scripts

### The Scenario
You have a headless Linux server or Raspberry Pi that captures sensor data or camera recordings and needs to offload archives to an operator laptop over an ad-hoc Wi-Fi link.

### Why RFTP Excels
- The CLI mode requires zero display server:
  ```bash
  # On the logging server (receiver):
  python3 main.py recv --dest /var/data/ingest --bind 0.0.0.0:9999

  # On the operator station (sender):
  python3 main.py send --file sensor_dump.tar.gz --target 192.168.43.10:9999
  ```
  *(Or with the compiled Rust binary from branch `dev`: `./target/release/file_transfer ...`)*
- Easily scriptable in bash or systemd services.
