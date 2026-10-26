import queue
import socket
import threading
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from rftp.file.writer import FileWriter
from rftp.network.sender import TransferEvent
from rftp.network.socket import ReliableUdpSocket
from rftp.protocol.packet import Packet, PacketType, StartPayload
from rftp.protocol.stats import TransferStats, format_file_size
from rftp.protocol.window import ReceiverWindow


@dataclass
class ReceiverConfig:
    bind_addr: Tuple[str, int] = ("0.0.0.0", 9999)
    destination_dir: str = "./downloads"
    simulated_loss_percent: int = 0


def run_receiver(
    config: ReceiverConfig,
    cancel_event: threading.Event,
    event_queue: queue.Queue,
) -> None:
    def log(msg: str) -> None:
        event_queue.put(TransferEvent("LOG", message=msg))

    try:
        sock = ReliableUdpSocket(config.bind_addr, config.simulated_loss_percent)
    except Exception as e:
        event_queue.put(
            TransferEvent("ERROR", message=f"Failed to bind UDP socket on {config.bind_addr}: {e}")
        )
        return

    writer: Optional[FileWriter] = None
    try:
        log(f"Receiver listening on UDP {sock.local_addr}")
        log(f"Saving received files to: {config.destination_dir}")
        if config.simulated_loss_percent > 0:
            log(f"Simulating {config.simulated_loss_percent}% packet loss on receiver socket")

        sender_addr: Optional[Tuple[str, int]] = None
        window: Optional[ReceiverWindow] = None
        stats: Optional[TransferStats] = None
        expected_hash = b""
        last_progress_emit = time.time()
        last_activity_time = time.time()

        while not cancel_event.is_set():
            try:
                data, from_addr = sock.recv_from()
                last_activity_time = time.time()

                try:
                    pkt = Packet.deserialize(data)
                except Exception as e:
                    if stats:
                        stats.record_corrupt_packet()
                    log(f"Corrupt packet dropped from {from_addr}: {e}")
                    continue

                if pkt.packet_type == PacketType.START:
                    # Idempotent response if already in progress with this sender
                    if writer is not None and sender_addr == from_addr:
                        ack = Packet.new_start_ack(True, "Ready to receive").serialize()
                        sock.send_to(ack, from_addr)
                        continue

                    try:
                        start_payload = StartPayload.from_bytes(pkt.payload)
                        sender_addr = from_addr
                        expected_hash = start_payload.sha256_hash
                        file_name = start_payload.file_name
                        total_chunks = start_payload.total_chunks
                        chunk_size = start_payload.chunk_size

                        log(
                            f"Incoming file request: '{file_name}' ({start_payload.file_size} bytes, "
                            f"{total_chunks} chunks) from {from_addr}"
                        )

                        writer = FileWriter(
                            config.destination_dir,
                            file_name,
                            start_payload.file_size,
                            chunk_size,
                        )
                        window = ReceiverWindow(total_chunks)
                        stats = TransferStats(start_payload.file_size, total_chunks)

                        ack = Packet.new_start_ack(True, "Ready to receive").serialize()
                        sock.send_to(ack, from_addr)
                        log("Handshake accepted. Waiting for data chunks...")

                    except Exception as e:
                        ack = Packet.new_start_ack(
                            False, f"Failed to initialize file: {e}"
                        ).serialize()
                        sock.send_to(ack, from_addr)
                        event_queue.put(
                            TransferEvent("ERROR", message=f"File creation error: {e}")
                        )
                        return

                elif pkt.packet_type == PacketType.DATA:
                    if sender_addr and from_addr[0] == sender_addr[0]:
                        if window and writer and stats:
                            seq = pkt.seq_num
                            is_new = window.on_chunk_received(seq)
                            if is_new:
                                try:
                                    writer.write_chunk(seq, pkt.payload)
                                    stats.record_chunk(len(pkt.payload), time.time())
                                except Exception as e:
                                    event_queue.put(
                                        TransferEvent("ERROR", message=f"Write chunk error: {e}")
                                    )
                                    return
                            else:
                                stats.record_duplicate_packet()

                            ack_num, sack_bitmask = window.create_ack_state()
                            ack_pkt = Packet.new_ack(ack_num, sack_bitmask).serialize()
                            sock.send_to(ack_pkt, from_addr)

                            if time.time() - last_progress_emit >= 0.1:
                                event_queue.put(TransferEvent("PROGRESS", stats=stats))
                                last_progress_emit = time.time()

                elif pkt.packet_type == PacketType.FIN:
                    if sender_addr and from_addr[0] == sender_addr[0]:
                        if window and writer and stats:
                            if window.is_complete():
                                log("All chunks received! Verifying SHA-256 checksum...")
                                ok, verify_msg = writer.finalize_and_verify(expected_hash)
                                if ok:
                                    fin_ack = Packet.new_fin_ack(True, "File verified").serialize()
                                    for _ in range(5):
                                        try:
                                            sock.send_to(fin_ack, from_addr)
                                            time.sleep(0.01)
                                        except Exception:
                                            pass

                                    dest_path = writer.destination_path
                                    elapsed = stats.elapsed_duration()
                                    avg_speed_mb = (stats.total_bytes / (1024.0 * 1024.0)) / max(
                                        0.001, elapsed
                                    )
                                    stats.bytes_transferred = stats.total_bytes
                                    stats.chunks_transferred = stats.total_chunks

                                    msg = (
                                        f"File transfer complete & verified!\n"
                                        f"Saved to: {dest_path}\n"
                                        f"Size: {format_file_size(stats.total_bytes)} | "
                                        f"Time: {elapsed:.2f}s ({avg_speed_mb:.2f} MB/s)"
                                    )
                                    event_queue.put(TransferEvent("PROGRESS", stats=stats))
                                    event_queue.put(TransferEvent("SUCCESS", message=msg))
                                    return
                                else:
                                    fin_ack = Packet.new_fin_ack(False, verify_msg).serialize()
                                    sock.send_to(fin_ack, from_addr)
                                    event_queue.put(
                                        TransferEvent(
                                            "ERROR",
                                            message=f"Integrity verification failed: {verify_msg}",
                                        )
                                    )
                                    return
                            else:
                                ack_num, sack_bitmask = window.create_ack_state()
                                ack_pkt = Packet.new_ack(ack_num, sack_bitmask).serialize()
                                sock.send_to(ack_pkt, from_addr)

                elif pkt.packet_type == PacketType.CANCEL:
                    reason = pkt.payload.decode("utf-8", errors="replace")
                    event_queue.put(
                        TransferEvent("ERROR", message=f"Sender cancelled: {reason}")
                    )
                    return

            except (socket.timeout, BlockingIOError):
                if writer is not None and (time.time() - last_activity_time) > 30.0:
                    event_queue.put(
                        TransferEvent(
                            "ERROR",
                            message="Receiver timed out: no packets received for 30 seconds.",
                        )
                    )
                    return

        event_queue.put(TransferEvent("CANCELLED", message="Receiver stopped"))

    finally:
        if writer:
            writer.close()
        sock.close()
