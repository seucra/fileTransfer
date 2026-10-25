use file_transfer::protocol::packet::{Packet, PacketType, HEADER_SIZE};
use file_transfer::protocol::stats::{
    chunk_byte_range, AckKind, BoundedEventCollector, ChunkState, ProtocolEventKind,
    ProtocolTelemetryEvent, RetransmitReason, DEFAULT_VISIBLE_CHUNK_LIMIT,
};
use file_transfer::protocol::window::{ReceiverWindow, SenderWindow, MAX_WINDOW_HISTORY};
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

#[test]
fn test_pending_to_inflight_transition_only_when_sent() {
    let start = Instant::now();
    let mut win = SenderWindow::new(4, 8);

    // Initially all chunks must be Pending
    let (_, initial_snaps) = win.build_visible_snapshots(start, 8);
    assert_eq!(initial_snaps.len(), 8);
    for snap in &initial_snaps {
        assert_eq!(snap.state, ChunkState::Pending);
        assert_eq!(snap.ack_kind, AckKind::None);
        assert!(snap.first_event_elapsed.is_none());
    }

    // Send only chunks 0 and 1 (even though window_size is 4)
    let send_t = start + Duration::from_millis(5);
    win.on_packet_sent(0, vec![0xAA], send_t);
    win.on_packet_sent(1, vec![0xBB], send_t);

    let (_, snaps) = win.build_visible_snapshots(start, 8);
    assert_eq!(snaps[0].state, ChunkState::InFlight);
    assert_eq!(snaps[1].state, ChunkState::InFlight);
    assert!(snaps[0].first_event_elapsed.is_some());
    // Chunks 2..8 were NOT sent yet, so they must remain Pending
    for (seq, snap) in snaps.iter().enumerate().take(8).skip(2) {
        assert_eq!(
            snap.state,
            ChunkState::Pending,
            "Chunk {} should remain Pending until actually sent",
            seq
        );
    }
}

#[test]
fn test_sack_vs_cumulative_ack_distinction_and_precedence() {
    let start = Instant::now();
    let mut win = SenderWindow::new(8, 8);

    for seq in 0..5 {
        win.on_packet_sent(seq, vec![seq as u8], start);
    }

    // Cumulative ACK for chunk 0..2 (`ack_num = 2`), plus SACK for chunks 3 and 4 (bits 0 and 1 -> seq 3 and 4)
    // while chunk 2 is still unacknowledged at base.
    let ack_t = start + Duration::from_millis(20);
    let (newly_acked, _) = win.on_ack_received(2, 0b11, ack_t);
    assert_eq!(newly_acked, 4); // chunks 0, 1 (cumulative) + chunks 3, 4 (SACK)
    assert_eq!(win.base, 2);

    let (_, snaps) = win.build_visible_snapshots(start, 8);
    // Chunks 0 and 1 are cumulatively Acked
    assert_eq!(snaps[0].state, ChunkState::Acked);
    assert_eq!(snaps[0].ack_kind, AckKind::Cumulative);
    assert_eq!(snaps[1].state, ChunkState::Acked);
    assert_eq!(snaps[1].ack_kind, AckKind::Cumulative);

    // Chunk 2 is still InFlight (never falsely labeled lost merely because ACK is missing!)
    assert_eq!(snaps[2].state, ChunkState::InFlight);
    assert_eq!(snaps[2].ack_kind, AckKind::None);

    // Chunks 3 and 4 are Sacked (Selective ACK), NOT cumulatively Acked yet
    assert_eq!(snaps[3].state, ChunkState::Sacked);
    assert_eq!(snaps[3].ack_kind, AckKind::Selective);
    assert_eq!(snaps[4].state, ChunkState::Sacked);
    assert_eq!(snaps[4].ack_kind, AckKind::Selective);

    // Now chunk 2 is finally acknowledged (`ack_num = 5`):
    // Base advances to 5, and chunks 3 & 4 transition to Acked with SelectiveThenCumulative history!
    let ack_t2 = start + Duration::from_millis(50);
    let (newly_acked_2, _) = win.on_ack_received(5, 0, ack_t2);
    assert_eq!(newly_acked_2, 1); // Only chunk 2 was newly acknowledged
    assert_eq!(win.base, 5);

    let (_, snaps_after) = win.build_visible_snapshots(start, 8);
    assert_eq!(snaps_after[2].state, ChunkState::Acked);
    assert_eq!(snaps_after[2].ack_kind, AckKind::Cumulative);
    assert_eq!(snaps_after[3].state, ChunkState::Acked);
    assert_eq!(snaps_after[3].ack_kind, AckKind::SelectiveThenCumulative);
    assert_eq!(snaps_after[4].state, ChunkState::Acked);
    assert_eq!(snaps_after[4].ack_kind, AckKind::SelectiveThenCumulative);

    // Verify precedence ordering
    assert!(ChunkState::Acked.precedence() > ChunkState::Sacked.precedence());
    assert!(ChunkState::Sacked.precedence() > ChunkState::Retransmitting.precedence());
    assert!(ChunkState::Retransmitting.precedence() > ChunkState::InFlight.precedence());
    assert!(ChunkState::InFlight.precedence() > ChunkState::Pending.precedence());
}

#[test]
fn test_timeout_retransmission_updates_matrix_and_preserves_history() {
    let start = Instant::now();
    let mut win = SenderWindow::new(4, 6);

    win.on_packet_sent(0, vec![10], start);
    win.on_packet_sent(1, vec![20], start);

    // Simulate RTO timeout on chunk 0 and retransmission
    let retry_t = start + Duration::from_millis(300);
    let timed_out = win.get_timed_out_chunks(retry_t, Duration::from_millis(250));
    assert_eq!(timed_out, vec![0, 1]);

    // Retransmit chunk 0
    win.on_packet_sent(0, vec![10], retry_t);
    let snap_retrans = win.snapshot_for_chunk(0, start);
    assert_eq!(snap_retrans.state, ChunkState::Retransmitting);
    assert_eq!(snap_retrans.retransmit_count, 1);
    assert!(snap_retrans.had_retransmission);
    assert_eq!(
        snap_retrans.last_retransmit_reason,
        Some(RetransmitReason::RtoTimeout)
    );

    // Now chunk 0 is cumulatively acknowledged:
    // Its transient state becomes Acked, while its retransmission history (count = 1) is preserved!
    let ack_t = retry_t + Duration::from_millis(15);
    win.on_ack_received(1, 0, ack_t);

    let snap_acked = win.snapshot_for_chunk(0, start);
    assert_eq!(snap_acked.state, ChunkState::Acked);
    assert_eq!(snap_acked.retransmit_count, 1);
    assert!(snap_acked.had_retransmission);
    assert_eq!(
        snap_acked.last_retransmit_reason,
        Some(RetransmitReason::RtoTimeout)
    );
}

#[test]
fn test_receiver_window_snapshots_and_wire_retransmission_flag() {
    let start = Instant::now();
    let mut r = ReceiverWindow::new(6);

    // Chunk 0 arrives in order
    r.on_chunk_received_with_meta(0, false, start + Duration::from_millis(5));
    // Chunk 2 arrives out of order (chunk 1 is an unreceived gap)
    r.on_chunk_received_with_meta(2, false, start + Duration::from_millis(10));

    let (_, snaps) = r.build_visible_snapshots(start, 6);
    assert_eq!(snaps[0].state, ChunkState::Acked);
    // Chunk 1 is an unreceived gap below highest_observed_seq (2), so it is InFlight (awaiting), never falsely labeled Retransmitting/Lost
    assert_eq!(snaps[1].state, ChunkState::InFlight);
    assert!(!snaps[1].had_retransmission);
    // Chunk 2 is Sacked (buffered out-of-order)
    assert_eq!(snaps[2].state, ChunkState::Sacked);
    assert_eq!(snaps[2].ack_kind, AckKind::Selective);
    // Chunks 3..6 are Pending
    assert_eq!(snaps[3].state, ChunkState::Pending);

    // Now chunk 1 arrives with FLAG_RETRANSMISSION = true
    r.on_chunk_received_with_meta(1, true, start + Duration::from_millis(40));
    assert_eq!(r.base, 3);

    let (_, snaps_after) = r.build_visible_snapshots(start, 6);
    assert_eq!(snaps_after[1].state, ChunkState::Acked);
    assert!(snaps_after[1].had_retransmission);
    assert_eq!(
        snaps_after[1].last_retransmit_reason,
        Some(RetransmitReason::WireRetransmitFlag)
    );
    assert_eq!(snaps_after[2].state, ChunkState::Acked);
    assert_eq!(snaps_after[2].ack_kind, AckKind::SelectiveThenCumulative);
}

#[test]
fn test_chunk_byte_range_final_partial_chunk_and_empty_file() {
    // 0-byte file
    assert_eq!(chunk_byte_range(0, 1400, 0), (0, 0));

    // File of 3000 bytes with 1400-byte chunks -> 3 chunks:
    // Chunk 0: 0..1400 (1400 B)
    // Chunk 1: 1400..2800 (1400 B)
    // Chunk 2 (final partial): 2800..3000 (200 B)
    // Chunk 3 (out of bounds): (3000, 0)
    assert_eq!(chunk_byte_range(0, 1400, 3000), (0, 1400));
    assert_eq!(chunk_byte_range(1, 1400, 3000), (1400, 1400));
    assert_eq!(chunk_byte_range(2, 1400, 3000), (2800, 200));
    assert_eq!(chunk_byte_range(3, 1400, 3000), (3000, 0));
}

#[test]
fn test_snapshots_and_event_collector_remain_bounded() {
    let start = Instant::now();
    let mut win = SenderWindow::new(32, 5000);

    // Send and acknowledge 500 chunks
    for seq in 0..500 {
        win.on_packet_sent(seq, vec![1], start);
        win.on_ack_received(seq + 1, 0, start);
    }

    // Completed history must never exceed MAX_WINDOW_HISTORY
    assert!(win.completed_history.len() <= MAX_WINDOW_HISTORY);

    // Visible snapshots must be bounded by DEFAULT_VISIBLE_CHUNK_LIMIT (80)
    let (_, vis) = win.build_visible_snapshots(start, DEFAULT_VISIBLE_CHUNK_LIMIT);
    assert_eq!(vis.len(), DEFAULT_VISIBLE_CHUNK_LIMIT);

    // BoundedEventCollector must never exceed its configured capacity
    let mut collector = BoundedEventCollector::new(16);
    for i in 0..100 {
        collector.push(ProtocolTelemetryEvent {
            elapsed: Duration::from_millis(i),
            kind: if i % 10 == 0 {
                ProtocolEventKind::TimeoutDetected
            } else {
                ProtocolEventKind::DataSent
            },
            seq_num: Some(i as u32),
            ack_num: None,
            sack_bitmask: None,
            message: format!("Event {}", i),
        });
    }
    let drained = collector.drain_to_vec();
    assert_eq!(drained.len(), 16);
    assert!(collector.dropped_high_rate_events > 0);
}
