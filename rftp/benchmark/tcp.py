import hashlib
import os
import socket
import threading
import time
from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    protocol: str
    file_size_bytes: int
    elapsed_sec: float
    throughput_mb_s: float
    retransmissions: int
    integrity_verified: bool


def run_tcp_transfer(
    src_file: str, dest_file: str, expected_hash: bytes
) -> BenchmarkResult:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    file_size = os.path.getsize(src_file)
    recv_error = []
    recv_verified = [False]

    def receiver_worker():
        try:
            conn, _ = listener.accept()
            hasher = hashlib.sha256()
            with open(dest_file, "wb") as f:
                while chunk := conn.recv(65536):
                    f.write(chunk)
                    hasher.update(chunk)
            conn.close()
            actual_hash = hasher.digest()
            if actual_hash == expected_hash:
                recv_verified[0] = True
            else:
                recv_error.append("TCP SHA-256 hash mismatch")
        except Exception as e:
            recv_error.append(str(e))
        finally:
            listener.close()

    t = threading.Thread(target=receiver_worker, daemon=True)
    t.start()

    start_time = time.time()

    sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sender.connect(("127.0.0.1", port))
    with open(src_file, "rb") as f:
        while chunk := f.read(65536):
            sender.sendall(chunk)
    sender.shutdown(socket.SHUT_WR)
    sender.close()

    t.join(timeout=30)
    elapsed = max(0.0001, time.time() - start_time)

    if recv_error:
        raise RuntimeError(f"TCP transfer error: {recv_error[0]}")

    throughput_mb_s = (file_size / (1024.0 * 1024.0)) / elapsed

    return BenchmarkResult(
        protocol="TCP (Baseline)",
        file_size_bytes=file_size,
        elapsed_sec=elapsed,
        throughput_mb_s=throughput_mb_s,
        retransmissions=0,
        integrity_verified=recv_verified[0],
    )
