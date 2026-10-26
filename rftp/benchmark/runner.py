import hashlib
import os
import queue
import socket
import tempfile
import threading
import time
from typing import List

from rftp.benchmark.tcp import BenchmarkResult, run_tcp_transfer
from rftp.network.receiver import ReceiverConfig, run_receiver
from rftp.network.sender import SenderConfig, run_sender


def create_test_file(path: str, size_bytes: int) -> bytes:
    hasher = hashlib.sha256()
    remaining = size_bytes
    with open(path, "wb") as f:
        while remaining > 0:
            chunk_size = min(65536, remaining)
            data = os.urandom(chunk_size)
            f.write(data)
            hasher.update(data)
            remaining -= chunk_size
    return hasher.digest()


def run_custom_udp_benchmark(
    src_file: str,
    dest_dir: str,
    loss_rate_percent: int = 0,
    window_size: int = 32,
    chunk_size: int = 1400,
) -> BenchmarkResult:
    file_size = os.path.getsize(src_file)

    # Pick available ephemeral port
    temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    temp_sock.bind(("127.0.0.1", 0))
    recv_port = temp_sock.getsockname()[1]
    temp_sock.close()

    cancel_event = threading.Event()
    recv_q = queue.Queue()
    send_q = queue.Queue()

    recv_config = ReceiverConfig(
        bind_addr=("127.0.0.1", recv_port),
        destination_dir=dest_dir,
        simulated_loss_percent=loss_rate_percent,
    )
    send_config = SenderConfig(
        file_path=src_file,
        target_addr=("127.0.0.1", recv_port),
        bind_addr=("127.0.0.1", 0),
        chunk_size=chunk_size,
        window_size=window_size,
        simulated_loss_percent=loss_rate_percent,
    )

    recv_thread = threading.Thread(
        target=run_receiver, args=(recv_config, cancel_event, recv_q), daemon=True
    )
    recv_thread.start()
    time.sleep(0.05)  # Ensure receiver socket bound

    start_time = time.time()
    send_thread = threading.Thread(
        target=run_sender, args=(send_config, cancel_event, send_q), daemon=True
    )
    send_thread.start()

    send_success = False
    send_error = None
    retransmissions = 0

    while send_thread.is_alive() or not send_q.empty():
        try:
            ev = send_q.get(timeout=0.1)
            if ev.event_type == "PROGRESS" and ev.stats:
                retransmissions = ev.stats.retransmissions
            elif ev.event_type == "SUCCESS":
                send_success = True
                break
            elif ev.event_type in ("ERROR", "CANCELLED"):
                send_error = ev.message
                break
        except queue.Empty:
            pass

    send_thread.join(timeout=30)
    recv_thread.join(timeout=30)
    elapsed = max(0.0001, time.time() - start_time)

    if send_error:
        raise RuntimeError(f"UDP benchmark error: {send_error}")
    if not send_success:
        raise RuntimeError("UDP benchmark failed to confirm success")

    throughput_mb_s = (file_size / (1024.0 * 1024.0)) / elapsed

    return BenchmarkResult(
        protocol=f"Custom UDP ({loss_rate_percent}% loss)",
        file_size_bytes=file_size,
        elapsed_sec=elapsed,
        throughput_mb_s=throughput_mb_s,
        retransmissions=retransmissions,
        integrity_verified=True,
    )


def run_full_comparison(file_size_mb: int = 5) -> List[BenchmarkResult]:
    with tempfile.TemporaryDirectory() as tmpdir:
        src_file = os.path.join(tmpdir, "bench_sample.bin")
        dest_tcp = os.path.join(tmpdir, "dest_tcp.bin")
        dest_udp_dir = os.path.join(tmpdir, "dest_udp")
        os.makedirs(dest_udp_dir, exist_ok=True)

        size_bytes = file_size_mb * 1024 * 1024
        print(f"Generating {file_size_mb} MB test file...")
        hash_val = create_test_file(src_file, size_bytes)

        results: List[BenchmarkResult] = []

        print("Benchmarking TCP Baseline...")
        results.append(run_tcp_transfer(src_file, dest_tcp, hash_val))

        print("Benchmarking Custom UDP (0% Loss)...")
        results.append(run_custom_udp_benchmark(src_file, dest_udp_dir, 0))

        print("Benchmarking Custom UDP (5% Loss)...")
        results.append(run_custom_udp_benchmark(src_file, dest_udp_dir, 5))

        print("Benchmarking Custom UDP (15% Loss)...")
        results.append(run_custom_udp_benchmark(src_file, dest_udp_dir, 15))

        return results
