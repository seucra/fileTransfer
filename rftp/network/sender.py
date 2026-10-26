import queue
import socket
import threading
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from rftp.file.chunker import FileChunker
from rftp.network.socket import ReliableUdpSocket
from rftp.protocol.packet import Packet, PacketType, StartAckPayload, StartPayload
from rftp.protocol.stats import TransferStats, format_file_size
from rftp.protocol.window import SenderWindow


@dataclass
class TransferEvent:
    event_type: str  # "LOG", "PROGRESS", "SUCCESS", "ERROR", "CANCELLED"
    message: str = ""
    stats: Optional[TransferStats] = None


@dataclass
class SenderConfig:
    file_path: str
    target_addr: Tuple[str, int]
    bind_addr: Tuple[str, int] = ("0.0.0.0", 0)
    chunk_size: int = 1400
    window_size: int = 32
    simulated_loss_percent: int = 0


def run_sender(
    config: SenderConfig,
    cancel_event: threading.Event,
    event_queue: queue.Queue,
) -> None:
    def log(msg: str) -> None:
        event_queue.put(TransferEvent("LOG", message=msg))

    try:
        log(f"Opening file: {config.file_path}")
        chunker = FileChunker(config.file_path, config.chunk_size)
    except Exception as e:
        event_queue.put(TransferEvent("ERROR", message=f"Failed to open file: {e}"))
        return

    try:
        log(
            f"File: '{chunker.file_name}', Size: {chunker.file_size} bytes, "
            f"Chunks: {chunker.total_chunks}, Hash: {chunker.sha256_hash[:4].hex()}..."
        )

        try:
            sock = ReliableUdpSocket(config.bind_addr, config.simulated_loss_percent)
        except Exception as e:
            event_queue.put(
                TransferEvent("ERROR", message=f"Failed to bind UDP socket on {config.bind_addr}: {e}")
            )
            return

        log(f"UDP socket bound to {sock.local_addr}, targeting {config.target_addr}")
        if config.simulated_loss_percent > 0:
            log(f"Simulating {config.simulated_loss_percent}% packet loss on sender socket")

        # 1. Handshake Phase
        log("Initiating handshake (START)...")
        start_payload = StartPayload(
            file_size=chunker.file_size,
            chunk_size=chunker.chunk_size,
            total_chunks=chunker.total_chunks,
            sha256_hash=chunker.sha256_hash,
            file_name=chunker.file_name,
        )
        start_packet = Packet.new_start(start_payload).serialize()

        handshake_attempts = 0
        max_handshake_attempts = 20
        receiver_ready = False

        while handshake_attempts < max_handshake_attempts:
            if cancel_event.is_set():
                event_queue.put(TransferEvent("CANCELLED", message="Cancelled by user"))
                return

            try:
                sock.send_to(start_packet, config.target_addr)
            except Exception as e:
                event_queue.put(TransferEvent("ERROR", message=f"Failed to send START: {e}"))
                return

            wait_start = time.time()
            while time.time() - wait_start < 0.3:
                try:
                    data, from_addr = sock.recv_from()
                    if from_addr[0] == config.target_addr[0]:
                        pkt = Packet.deserialize(data)
                        if pkt.packet_type == PacketType.START_ACK:
                            ack_payload = StartAckPayload.from_bytes(pkt.payload)
                            if ack_payload.accepted:
                                receiver_ready = True
                                log("Handshake confirmed! Receiver is ready.")
                                break
                            else:
                                event_queue.put(
                                    TransferEvent(
                                        "ERROR",
                                        message=f"Receiver rejected transfer: {ack_payload.message}",
                                    )
                                )
                                return
                except (socket.timeout, BlockingIOError):
                    pass
                except Exception:
                    pass

            if receiver_ready:
                break

            handshake_attempts += 1
            log(f"Waiting for receiver response... (attempt {handshake_attempts}/{max_handshake_attempts})")

        if not receiver_ready:
            event_queue.put(
                TransferEvent(
                    "ERROR",
                    message=f"Handshake timed out after {max_handshake_attempts} attempts.",
                )
            )
            return

        # 2. Data Transmission Phase (Sliding Window + SACK)
        window = SenderWindow(config.window_size, chunker.total_chunks)
        stats = TransferStats(chunker.file_size, chunker.total_chunks)
        rto = 0.250
        last_progress_time = time.time()
        last_progress_emit = time.time()

        sock.set_nonblocking(True)
        log(f"Starting transfer with window size {config.window_size}")

        while not window.is_complete():
            if cancel_event.is_set():
                sock.set_nonblocking(False)
                cancel_pkt = Packet.new_cancel("User cancelled transfer").serialize()
                try:
                    sock.send_to(cancel_pkt, config.target_addr)
                except Exception:
                    pass
                event_queue.put(TransferEvent("CANCELLED", message="Transfer cancelled"))
                return

            now = time.time()

            # Transmit chunks within sliding window
            while window.can_send():
                seq = window.next_seq
                try:
                    chunk_data = chunker.read_chunk(seq)
                    pkt = Packet.new_data(seq, chunk_data, is_retransmission=False).serialize()
                    sock.send_to(pkt, config.target_addr)
                    window.on_packet_sent(seq, chunk_data, now)
                except Exception as e:
                    event_queue.put(TransferEvent("ERROR", message=f"Send chunk error: {e}"))
                    return

            # Drain incoming ACKs
            processed_acks = 0
            while processed_acks < 64:
                try:
                    data, from_addr = sock.recv_from()
                    if from_addr[0] == config.target_addr[0]:
                        try:
                            pkt = Packet.deserialize(data)
                            if pkt.packet_type == PacketType.ACK:
                                newly_acked, rtt_sample = window.on_ack_received(
                                    pkt.ack_num, pkt.sack_bitmask, time.time()
                                )
                                if newly_acked > 0:
                                    last_progress_time = time.time()
                                    for _ in range(newly_acked):
                                        stats.record_chunk(chunker.chunk_size, time.time())
                                if rtt_sample is not None:
                                    stats.update_rtt(rtt_sample)
                                    rto = min(0.8, max(0.08, rtt_sample * 2))
                                processed_acks += 1
                            elif pkt.packet_type == PacketType.CANCEL:
                                reason = pkt.payload.decode("utf-8", errors="replace")
                                event_queue.put(
                                    TransferEvent(
                                        "ERROR", message=f"Receiver cancelled: {reason}"
                                    )
                                )
                                return
                        except Exception:
                            stats.record_corrupt_packet()
                except (BlockingIOError, socket.timeout):
                    break

            # Timeout-based retransmissions
            timed_out = window.get_timed_out_chunks(time.time(), rto)
            for seq in timed_out:
                entry = window.in_flight.get(seq)
                if entry:
                    if entry.retransmit_count > 35:
                        event_queue.put(
                            TransferEvent(
                                "ERROR",
                                message=f"Chunk {seq} exceeded max retransmissions (35). Connection lost.",
                            )
                        )
                        return
                    pkt = Packet.new_data(seq, entry.payload, is_retransmission=True).serialize()
                    try:
                        sock.send_to(pkt, config.target_addr)
                    except Exception:
                        pass
                    entry.sent_at = time.time()
                    entry.retransmit_count += 1
                    stats.record_retransmission()

            # Emit periodic progress updates to UI/CLI
            if time.time() - last_progress_emit >= 0.1:
                event_queue.put(TransferEvent("PROGRESS", stats=stats))
                last_progress_emit = time.time()

            # Stall detection: 25 seconds of no progress
            if time.time() - last_progress_time > 25.0:
                event_queue.put(
                    TransferEvent(
                        "ERROR",
                        message="Transfer stalled: no acknowledgements received for 25 seconds.",
                    )
                )
                return

            time.sleep(0.0005)

        # 3. Termination & Integrity Verification Phase
        sock.set_nonblocking(False)
        sock.set_timeout(0.3)
        log("All data chunks acknowledged! Sending FIN and requesting verification...")

        fin_pkt = Packet.new_fin(chunker.sha256_hash).serialize()
        fin_attempts = 0
        verified = False

        while fin_attempts < 20:
            if cancel_event.is_set():
                event_queue.put(TransferEvent("CANCELLED", message="Cancelled"))
                return

            try:
                sock.send_to(fin_pkt, config.target_addr)
            except Exception:
                pass

            wait_start = time.time()
            while time.time() - wait_start < 0.3:
                try:
                    data, _ = sock.recv_from()
                    pkt = Packet.deserialize(data)
                    if pkt.packet_type == PacketType.FIN_ACK:
                        fin_ack = PacketType.FIN_ACK
                        from rftp.protocol.packet import FinAckPayload

                        fin_payload = FinAckPayload.from_bytes(pkt.payload)
                        if fin_payload.success:
                            verified = True
                            break
                        else:
                            event_queue.put(
                                TransferEvent(
                                    "ERROR",
                                    message=f"Receiver verification failed: {fin_payload.message}",
                                )
                            )
                            return
                except (socket.timeout, BlockingIOError):
                    pass
                except Exception:
                    pass

            if verified:
                break
            fin_attempts += 1

        if verified:
            stats.bytes_transferred = stats.total_bytes
            stats.chunks_transferred = stats.total_chunks
            elapsed = stats.elapsed_duration()
            avg_speed_mb = (stats.total_bytes / (1024.0 * 1024.0)) / max(0.001, elapsed)
            msg = (
                f"Transfer successful and verified! Transferred {format_file_size(stats.total_bytes)} "
                f"in {elapsed:.2f}s ({avg_speed_mb:.2f} MB/s) with {stats.retransmissions} retransmissions."
            )
            event_queue.put(TransferEvent("PROGRESS", stats=stats))
            event_queue.put(TransferEvent("SUCCESS", message=msg))
        else:
            event_queue.put(
                TransferEvent("ERROR", message="FIN_ACK timeout: receiver confirmation not received.")
            )

    finally:
        chunker.close()
        sock.close()
