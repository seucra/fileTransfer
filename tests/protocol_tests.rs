use file_transfer::protocol::packet::{Packet, PacketType, HEADER_SIZE};
use file_transfer::protocol::window::{ReceiverWindow, SenderWindow};
use std::time::{Duration, Instant};

#[test]
fn test_packet_empty_payload() {
    let pkt = Packet::new_ack(100, 0b1010);
    let bytes = pkt.serialize();
    assert_eq!(bytes.len(), HEADER_SIZE);

    let decoded = Packet::deserialize(&bytes).expect("Deserialize ACK");
    assert_eq!(decoded.packet_type, PacketType::Ack);
    assert_eq!(decoded.ack_num, 100);
    assert_eq!(decoded.sack_bitmask, 0b1010);
    assert!(decoded.payload.is_empty());
}

#[test]
fn test_packet_large_payload() {
    let payload = vec![0xAB; 4096];
    let pkt = Packet::new_data(77, payload.clone(), true);
    let bytes = pkt.serialize();
    assert_eq!(bytes.len(), HEADER_SIZE + 4096);

    let decoded = Packet::deserialize(&bytes).expect("Deserialize large data");
    assert_eq!(decoded.packet_type, PacketType::Data);
    assert_eq!(decoded.seq_num, 77);
    assert!(decoded.is_retransmission());
    assert_eq!(decoded.payload, payload);
}

#[test]
fn test_sender_window_retransmissions() {
    let mut win = SenderWindow::new(4, 10);
    let now = Instant::now();

    win.on_packet_sent(0, vec![1, 2, 3], now);
    win.on_packet_sent(1, vec![4, 5, 6], now);

    // No timeout yet with 200ms RTO
    let timed_out = win.get_timed_out_chunks(now, Duration::from_millis(200));
    assert!(timed_out.is_empty());

    // Advance time past RTO
    let later = now + Duration::from_millis(250);
    let timed_out = win.get_timed_out_chunks(later, Duration::from_millis(200));
    assert_eq!(timed_out, vec![0, 1]);

    // Chunk 0 gets ACKed
    win.on_ack_received(1, 0, later);
    let timed_out = win.get_timed_out_chunks(later, Duration::from_millis(200));
    assert_eq!(timed_out, vec![1]);
}

#[test]
fn test_receiver_window_complex_loss_and_recovery() {
    let mut r = ReceiverWindow::new(10);

    // Chunks 0, 1 arrive
    assert!(r.on_chunk_received(0));
    assert!(r.on_chunk_received(1));
    assert_eq!(r.base, 2);

    // Chunk 2 is lost! Chunks 3, 4, 5 arrive out of order
    assert!(r.on_chunk_received(3));
    assert!(r.on_chunk_received(4));
    assert!(r.on_chunk_received(5));
    assert_eq!(r.base, 2); // Base stays 2 waiting for chunk 2

    // SACK state: base=2.
    // Chunk 3 is at base + 1 + 0 -> bit 0
    // Chunk 4 is at base + 1 + 1 -> bit 1
    // Chunk 5 is at base + 1 + 2 -> bit 2
    let (ack_num, sack) = r.create_ack_state();
    assert_eq!(ack_num, 2);
    assert_eq!(sack & 0b111, 0b111);

    // Now chunk 2 arrives
    assert!(r.on_chunk_received(2));
    // Window should slide all the way to 6!
    assert_eq!(r.base, 6);
    assert!(r.out_of_order.is_empty());

    // Receive remaining chunks 6, 7, 8, 9
    for i in 6..10 {
        assert!(r.on_chunk_received(i));
    }
    assert!(r.is_complete());
}
