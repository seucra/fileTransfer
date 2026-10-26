import unittest
from rftp.protocol.packet import (
    HEADER_SIZE,
    Packet,
    PacketType,
    StartPayload,
    StartAckPayload,
    FinAckPayload,
)
from rftp.protocol.window import ReceiverWindow, SenderWindow


class TestPacketProtocol(unittest.TestCase):
    def test_packet_serialization_roundtrip(self):
        payload = b"Hello, Reliable UDP File Transfer in Python!"
        pkt = Packet.new_data(seq_num=42, payload=payload, is_retransmission=False)
        raw = pkt.serialize()

        self.assertEqual(len(raw), HEADER_SIZE + len(payload))
        decoded = Packet.deserialize(raw)
        self.assertEqual(decoded.packet_type, PacketType.DATA)
        self.assertEqual(decoded.seq_num, 42)
        self.assertFalse(decoded.is_retransmission)
        self.assertEqual(decoded.payload, payload)

    def test_packet_crc_corruption_detection(self):
        payload = b"Critical network data chunk"
        pkt = Packet.new_data(seq_num=10, payload=payload)
        raw = bytearray(pkt.serialize())

        # Corrupt one bit in payload
        raw[-1] ^= 0xFF

        with self.assertRaises(ValueError) as ctx:
            Packet.deserialize(bytes(raw))
        self.assertIn("CRC32 mismatch", str(ctx.exception))

    def test_start_payload_roundtrip(self):
        sp = StartPayload(
            file_size=1048576,
            chunk_size=1400,
            total_chunks=750,
            sha256_hash=b"\xab" * 32,
            file_name="assignment_networks.pdf",
        )
        raw = sp.to_bytes()
        decoded = StartPayload.from_bytes(raw)
        self.assertEqual(decoded, sp)


class TestWindowMechanisms(unittest.TestCase):
    def test_sender_window_flow(self):
        win = SenderWindow(window_size=4, total_chunks=10)
        now = 1000.0

        self.assertTrue(win.can_send())
        win.on_packet_sent(0, b"data0", now)
        win.on_packet_sent(1, b"data1", now)
        win.on_packet_sent(2, b"data2", now)
        win.on_packet_sent(3, b"data3", now)

        # Window is full (base 0, next_seq 4, limit 4)
        self.assertFalse(win.can_send())

        # Cumulative ACK for 2 (chunks 0 and 1 acked)
        acked, _ = win.on_ack_received(ack_num=2, sack_bitmask=0, now=now + 0.05)
        self.assertEqual(acked, 2)
        self.assertEqual(win.base, 2)
        self.assertTrue(win.can_send())

    def test_receiver_window_in_order(self):
        r = ReceiverWindow(total_chunks=5)
        self.assertTrue(r.on_chunk_received(0))
        self.assertEqual(r.base, 1)
        self.assertTrue(r.on_chunk_received(1))
        self.assertEqual(r.base, 2)
        self.assertFalse(r.on_chunk_received(0))  # Duplicate
        self.assertEqual(r.base, 2)

    def test_receiver_window_out_of_order_sack(self):
        r = ReceiverWindow(total_chunks=10)

        # Chunk 0 arrives
        r.on_chunk_received(0)
        self.assertEqual(r.base, 1)

        # Chunk 2 arrives (chunk 1 is missing)
        r.on_chunk_received(2)
        self.assertEqual(r.base, 1)
        self.assertIn(2, r.out_of_order)

        ack_num, sack = r.create_ack_state()
        self.assertEqual(ack_num, 1)
        self.assertEqual(sack & 1, 1)  # Bit 0 set for chunk (1 + 1 + 0) = 2

        # Missing chunk 1 arrives
        r.on_chunk_received(1)
        # Base advances past 1 and 2 to 3!
        self.assertEqual(r.base, 3)
        self.assertEqual(len(r.out_of_order), 0)

    def test_sender_window_timeouts(self):
        win = SenderWindow(window_size=4, total_chunks=10)
        t0 = 100.0
        win.on_packet_sent(0, b"p0", t0)
        win.on_packet_sent(1, b"p1", t0)

        # No timeouts at t0 + 0.1s with RTO = 0.25s
        timed_out = win.get_timed_out_chunks(t0 + 0.1, rto=0.25)
        self.assertEqual(timed_out, [])

        # Timeouts at t0 + 0.3s
        timed_out = win.get_timed_out_chunks(t0 + 0.3, rto=0.25)
        self.assertEqual(timed_out, [0, 1])


if __name__ == "__main__":
    unittest.main()
