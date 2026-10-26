import time
from typing import Optional


def format_file_size(bytes_count: int) -> str:
    if bytes_count >= 1024 * 1024 * 1024:
        return f"{bytes_count / (1024.0 ** 3):.2f} GB"
    elif bytes_count >= 1024 * 1024:
        return f"{bytes_count / (1024.0 ** 2):.2f} MB"
    elif bytes_count >= 1024:
        return f"{bytes_count / 1024.0:.1f} KB"
    else:
        return f"{bytes_count} B"


class TransferStats:
    def __init__(self, total_bytes: int, total_chunks: int):
        now = time.time()
        self.bytes_transferred = 0
        self.total_bytes = total_bytes
        self.chunks_transferred = 0
        self.total_chunks = total_chunks
        self.retransmissions = 0
        self.corrupt_packets_dropped = 0
        self.duplicate_packets_dropped = 0
        self.start_time = now
        self.last_update_time = now
        self.bytes_since_last_sample = 0
        self.speed_bytes_per_sec = 0.0
        self.current_rtt: Optional[float] = None

    def record_chunk(self, chunk_bytes: int, now: float) -> None:
        self.chunks_transferred = min(self.total_chunks, self.chunks_transferred + 1)
        self.bytes_transferred = min(self.total_bytes, self.bytes_transferred + chunk_bytes)
        self.bytes_since_last_sample += chunk_bytes

        elapsed_since_sample = now - self.last_update_time
        if elapsed_since_sample >= 0.2:
            instant_speed = self.bytes_since_last_sample / max(0.001, elapsed_since_sample)
            if self.speed_bytes_per_sec == 0.0:
                self.speed_bytes_per_sec = instant_speed
            else:
                self.speed_bytes_per_sec = self.speed_bytes_per_sec * 0.7 + instant_speed * 0.3
            self.bytes_since_last_sample = 0
            self.last_update_time = now

    def record_retransmission(self) -> None:
        self.retransmissions += 1

    def record_corrupt_packet(self) -> None:
        self.corrupt_packets_dropped += 1

    def record_duplicate_packet(self) -> None:
        self.duplicate_packets_dropped += 1

    def update_rtt(self, rtt: float) -> None:
        self.current_rtt = rtt

    def progress_ratio(self) -> float:
        if self.total_bytes == 0:
            if self.total_chunks == 0:
                return 1.0
            return max(0.0, min(1.0, self.chunks_transferred / float(self.total_chunks)))
        return max(0.0, min(1.0, self.bytes_transferred / float(self.total_bytes)))

    def progress_percent(self) -> float:
        return self.progress_ratio() * 100.0

    def elapsed_duration(self) -> float:
        return max(0.0, time.time() - self.start_time)

    def estimated_remaining(self) -> Optional[float]:
        if self.speed_bytes_per_sec <= 0.0 or self.bytes_transferred >= self.total_bytes:
            return None
        remaining = self.total_bytes - self.bytes_transferred
        return remaining / self.speed_bytes_per_sec

    def formatted_speed(self) -> str:
        speed = self.speed_bytes_per_sec
        if speed >= 1024.0 * 1024.0:
            return f"{speed / (1024.0 * 1024.0):.2f} MB/s"
        elif speed >= 1024.0:
            return f"{speed / 1024.0:.1f} KB/s"
        else:
            return f"{speed:.0f} B/s"

    def formatted_bytes(self) -> str:
        return f"{format_file_size(self.bytes_transferred)} / {format_file_size(self.total_bytes)}"
