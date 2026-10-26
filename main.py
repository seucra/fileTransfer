#!/usr/bin/env python3
"""
RFTP - Reliable File Transfer Protocol over UDP
Desktop GUI, Headless CLI, and Benchmark Runner
"""

import argparse
import queue
import sys
import threading
import time

from rftp.benchmark.runner import run_full_comparison
from rftp.network.receiver import ReceiverConfig, run_receiver
from rftp.network.sender import SenderConfig, TransferEvent, run_sender


def parse_addr(addr_str: str, default_port: int = 9999):
    if ":" in addr_str:
        host, port = addr_str.rsplit(":", 1)
        return host, int(port)
    return addr_str, default_port


def main():
    parser = argparse.ArgumentParser(
        description="Reliable File Transfer Protocol over UDP with GUI, CLI, and Benchmarks"
    )
    parser.add_argument("--gui", action="store_true", help="Launch desktop GUI interface")

    subparsers = parser.add_subparsers(dest="command")

    # Send command
    send_parser = subparsers.add_parser("send", help="Send a file over UDP to a receiver")
    send_parser.add_argument("-f", "--file", required=True, help="Path to file to send")
    send_parser.add_argument(
        "-t", "--target", required=True, help="Target receiver address (e.g. 192.168.43.50:9999)"
    )
    send_parser.add_argument(
        "-b", "--bind", default="0.0.0.0:0", help="Local bind address [default: 0.0.0.0:0]"
    )
    send_parser.add_argument(
        "-c", "--chunk-size", type=int, default=1400, help="Payload chunk size [default: 1400]"
    )
    send_parser.add_argument(
        "-w", "--window", type=int, default=32, help="Sliding window size in packets [default: 32]"
    )
    send_parser.add_argument(
        "-l", "--loss", type=int, default=0, help="Simulated packet drop rate (0-100) [default: 0]"
    )

    # Recv command
    recv_parser = subparsers.add_parser(
        "recv", help="Run as receiver listening for incoming file transfers"
    )
    recv_parser.add_argument(
        "-d", "--dest", default="./downloads", help="Destination save directory [default: ./downloads]"
    )
    recv_parser.add_argument(
        "-b", "--bind", default="0.0.0.0:9999", help="Bind address [default: 0.0.0.0:9999]"
    )
    recv_parser.add_argument(
        "-l", "--loss", type=int, default=0, help="Simulated packet drop rate (0-100) [default: 0]"
    )

    # Benchmark command
    bench_parser = subparsers.add_parser(
        "benchmark", help="Run empirical performance benchmarks comparing TCP vs Custom UDP"
    )
    bench_parser.add_argument(
        "-s", "--size-mb", type=int, default=5, help="Test file size in megabytes [default: 5]"
    )

    args = parser.parse_args()

    if args.command == "send":
        print("=== Reliable UDP Sender (CLI Mode) ===")
        target_host, target_port = parse_addr(args.target, 9999)
        bind_host, bind_port = parse_addr(args.bind, 0)

        config = SenderConfig(
            file_path=args.file,
            target_addr=(target_host, target_port),
            bind_addr=(bind_host, bind_port),
            chunk_size=args.chunk_size,
            window_size=args.window,
            simulated_loss_percent=args.loss,
        )

        cancel_event = threading.Event()
        event_q = queue.Queue()

        t = threading.Thread(
            target=run_sender, args=(config, cancel_event, event_q), daemon=True
        )
        t.start()

        try:
            while t.is_alive() or not event_q.empty():
                try:
                    ev: TransferEvent = event_q.get(timeout=0.1)
                    if ev.event_type == "LOG":
                        print(f"[LOG] {ev.message}")
                    elif ev.event_type == "PROGRESS" and ev.stats:
                        st = ev.stats
                        sys.stdout.write(
                            f"\r[PROGRESS] {st.progress_percent():.1f}% | "
                            f"Speed: {st.formatted_speed()} | "
                            f"Chunks: {st.chunks_transferred}/{st.total_chunks} | "
                            f"Retransmissions: {st.retransmissions}"
                        )
                        sys.stdout.flush()
                    elif ev.event_type == "SUCCESS":
                        print(f"\n[SUCCESS] {ev.message}")
                        break
                    elif ev.event_type == "ERROR":
                        print(f"\n[ERROR] {ev.message}", file=sys.stderr)
                        break
                    elif ev.event_type == "CANCELLED":
                        print(f"\n[CANCELLED] {ev.message}")
                        break
                except queue.Empty:
                    pass
        except KeyboardInterrupt:
            print("\nCancelling transfer...")
            cancel_event.set()
            t.join(timeout=2)

    elif args.command == "recv":
        print("=== Reliable UDP Receiver (CLI Mode) ===")
        bind_host, bind_port = parse_addr(args.bind, 9999)

        config = ReceiverConfig(
            bind_addr=(bind_host, bind_port),
            destination_dir=args.dest,
            simulated_loss_percent=args.loss,
        )

        cancel_event = threading.Event()
        event_q = queue.Queue()

        t = threading.Thread(
            target=run_receiver, args=(config, cancel_event, event_q), daemon=True
        )
        t.start()

        try:
            while t.is_alive() or not event_q.empty():
                try:
                    ev: TransferEvent = event_q.get(timeout=0.1)
                    if ev.event_type == "LOG":
                        print(f"[LOG] {ev.message}")
                    elif ev.event_type == "PROGRESS" and ev.stats:
                        st = ev.stats
                        sys.stdout.write(
                            f"\r[PROGRESS] {st.progress_percent():.1f}% | "
                            f"Speed: {st.formatted_speed()} | "
                            f"Chunks: {st.chunks_transferred}/{st.total_chunks} | "
                            f"Drops: {st.corrupt_packets_dropped + st.duplicate_packets_dropped}"
                        )
                        sys.stdout.flush()
                    elif ev.event_type == "SUCCESS":
                        print(f"\n[SUCCESS] {ev.message}")
                        break
                    elif ev.event_type == "ERROR":
                        print(f"\n[ERROR] {ev.message}", file=sys.stderr)
                        break
                    elif ev.event_type == "CANCELLED":
                        print(f"\n[CANCELLED] {ev.message}")
                        break
                except queue.Empty:
                    pass
        except KeyboardInterrupt:
            print("\nStopping receiver...")
            cancel_event.set()
            t.join(timeout=2)

    elif args.command == "benchmark":
        print("============================================================")
        print("  COMPUTER NETWORKS PROJECT: UDP vs TCP BENCHMARK EVALUATION")
        print("============================================================")
        print(f"Running empirical file transfer benchmarks for {args.size_mb} MB file...\n")

        try:
            results = run_full_comparison(args.size_mb)
            print("\n+--------------------------------+------------+--------------+-----------------+-------------------+")
            print("| Protocol                       | Time (sec) | Speed (MB/s) | Retransmissions | SHA-256 Integrity |")
            print("+--------------------------------+------------+--------------+-----------------+-------------------+")
            for r in results:
                status_str = "VERIFIED OK" if r.integrity_verified else "FAILED"
                print(
                    f"| {r.protocol:<30} | {r.elapsed_sec:>10.2f} | {r.throughput_mb_s:>12.2f} | "
                    f"{r.retransmissions:>15} | {status_str:>17} |"
                )
            print("+--------------------------------+------------+--------------+-----------------+-------------------+\n")
        except Exception as e:
            print(f"Benchmark failed: {e}", file=sys.stderr)

    else:
        # Launch GUI by default
        from rftp.ui.app import launch_gui

        launch_gui()


if __name__ == "__main__":
    main()
