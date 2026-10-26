import time
from typing import Dict, List, Optional, Set, Tuple


class InFlightChunk:
    __slots__ = ("seq_num", "payload", "sent_at", "retransmit_count", "acked")

    def __init__(self, seq_num: int, payload: bytes, sent_at: float):
        self.seq_num = seq_num
        self.payload = payload
        self.sent_at = sent_at
        self.retransmit_count = 0
        self.acked = False


class SenderWindow:
    def __init__(self, window_size: int, total_chunks: int):
        self.window_size = max(1, window_size)
        self.total_chunks = total_chunks
        self.base = 0
        self.next_seq = 0
        self.in_flight: Dict[int, InFlightChunk] = {}

    def can_send(self) -> bool:
        return self.next_seq < self.total_chunks and (self.next_seq - self.base) < self.window_size

    def on_packet_sent(self, seq_num: int, payload: bytes, now: float) -> None:
        if seq_num in self.in_flight:
            entry = self.in_flight[seq_num]
            entry.sent_at = now
            entry.retransmit_count += 1
        else:
            self.in_flight[seq_num] = InFlightChunk(seq_num, payload, now)
            if seq_num >= self.next_seq:
                self.next_seq = seq_num + 1

    def on_ack_received(
        self, ack_num: int, sack_bitmask: int, now: float
    ) -> Tuple[int, Optional[float]]:
        newly_acked = 0
        rtt_sample: Optional[float] = None

        # Cumulative ACK: all chunks strictly < ack_num are fully acknowledged
        unacked_before = [seq for seq in self.in_flight if seq < ack_num]
        for seq in unacked_before:
            entry = self.in_flight.pop(seq)
            newly_acked += 1
            if entry.retransmit_count == 0 and rtt_sample is None:
                rtt_sample = max(0.0001, now - entry.sent_at)

        if ack_num > self.base:
            self.base = ack_num

        # Selective ACK (SACK): bit i corresponds to chunk (ack_num + 1 + i)
        for i in range(64):
            if (sack_bitmask & (1 << i)) != 0:
                sack_seq = ack_num + 1 + i
                if sack_seq in self.in_flight:
                    entry = self.in_flight[sack_seq]
                    if not entry.acked:
                        entry.acked = True
                        newly_acked += 1
                        if entry.retransmit_count == 0 and rtt_sample is None:
                            rtt_sample = max(0.0001, now - entry.sent_at)

        # Advance base over contiguous acked chunks
        while self.base in self.in_flight and self.in_flight[self.base].acked:
            del self.in_flight[self.base]
            self.base += 1

        return newly_acked, rtt_sample

    def get_timed_out_chunks(self, now: float, rto: float) -> List[int]:
        return [
            seq
            for seq, entry in self.in_flight.items()
            if not entry.acked and (now - entry.sent_at) >= rto
        ]

    def is_complete(self) -> bool:
        return self.base >= self.total_chunks and len(self.in_flight) == 0

    def in_flight_count(self) -> int:
        return sum(1 for entry in self.in_flight.values() if not entry.acked)


class ReceiverWindow:
    def __init__(self, total_chunks: int):
        self.total_chunks = total_chunks
        self.base = 0
        self.out_of_order: Set[int] = set()

    def on_chunk_received(self, seq_num: int) -> bool:
        if seq_num < self.base or seq_num >= self.total_chunks:
            return False  # Duplicate or out of bounds

        if seq_num == self.base:
            self.base += 1
            while self.base in self.out_of_order:
                self.out_of_order.remove(self.base)
                self.base += 1
            return True
        else:
            if seq_num not in self.out_of_order:
                self.out_of_order.add(seq_num)
                return True
            return False

    def create_ack_state(self) -> Tuple[int, int]:
        ack_num = self.base
        sack_bitmask = 0
        for i in range(64):
            check_seq = ack_num + 1 + i
            if check_seq in self.out_of_order:
                sack_bitmask |= 1 << i
        return ack_num, sack_bitmask

    def is_complete(self) -> bool:
        return self.base >= self.total_chunks and len(self.out_of_order) == 0

    def received_count(self) -> int:
        return self.base + len(self.out_of_order)
