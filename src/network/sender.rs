use crate::file::FileChunker;
use crate::network::simulated_socket::ReliableUdpSocket;
use crate::protocol::packet::{Packet, PacketType, StartPayload};
use crate::protocol::stats::{
    BoundedEventCollector, ProtocolEventKind, ProtocolTelemetryEvent, TransferStats,
    WindowPerspective, DEFAULT_VISIBLE_CHUNK_LIMIT, MAX_EVENTS_PER_SNAPSHOT,
};
use crate::protocol::window::SenderWindow;
use crossbeam_channel::Sender as ChannelSender;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum TransferEvent {
    Log(String),
    Progress(TransferStats),
    Success(String),
    Error(String),
    Cancelled,
}

pub struct SenderConfig {
    pub file_path: PathBuf,
    pub target_addr: SocketAddr,
    pub bind_addr: SocketAddr,
    pub chunk_size: u32,
    pub window_size: usize,
    pub simulated_loss_percent: u8,
}

fn sync_sender_snapshot(
    stats: &mut TransferStats,
    window: &SenderWindow,
    rto: Duration,
    collector: &mut BoundedEventCollector,
) {
    stats.window_base = window.base;
    stats.next_seq = window.next_seq;
    stats.window_size = window.window_size;
    stats.in_flight_count = window.in_flight_count();
    stats.sacked_count = window.sacked_count();
    stats.update_rto(rto);
    let (vis_start, vis_chunks) =
        window.build_visible_snapshots(stats.start_time, DEFAULT_VISIBLE_CHUNK_LIMIT);
    stats.visible_chunk_start = vis_start;
    stats.visible_chunks = vis_chunks;
    stats.recent_events = collector.drain_to_vec();
}

pub fn run_sender(
    config: SenderConfig,
    cancel_flag: Arc<AtomicBool>,
    event_tx: ChannelSender<TransferEvent>,
) {
    let _ = event_tx.send(TransferEvent::Log(format!(
        "Opening file: {}",
        config.file_path.display()
    )));

    let mut chunker = match FileChunker::open(&config.file_path, config.chunk_size) {
        Ok(c) => c,
        Err(e) => {
            let _ = event_tx.send(TransferEvent::Error(format!("Failed to open file: {}", e)));
            return;
        }
    };

    let _ = event_tx.send(TransferEvent::Log(format!(
        "File: '{}', Size: {} bytes, Chunks: {}, Hash: {:02x?}...",
        chunker.file_name,
        chunker.file_size,
        chunker.total_chunks,
        &chunker.sha256_hash[0..4]
    )));

    let socket = match ReliableUdpSocket::bind(config.bind_addr, config.simulated_loss_percent) {
        Ok(s) => s,
        Err(e) => {
            let _ = event_tx.send(TransferEvent::Error(format!(
                "Failed to bind UDP socket on {}: {}",
                config.bind_addr, e
            )));
            return;
        }
    };

    let _ = event_tx.send(TransferEvent::Log(format!(
        "UDP socket bound to {}, targeting {}",
        socket
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_default(),
        config.target_addr
    )));

    if config.simulated_loss_percent > 0 {
        let _ = event_tx.send(TransferEvent::Log(format!(
            "Simulating {}% packet loss on sender socket",
            config.simulated_loss_percent
        )));
    }

    // 1. Handshake Phase
    let _ = event_tx.send(TransferEvent::Log(
        "Initiating handshake (START)...".to_string(),
    ));

    let start_payload = StartPayload {
        file_size: chunker.file_size,
        chunk_size: chunker.chunk_size,
        total_chunks: chunker.total_chunks,
        sha256_hash: chunker.sha256_hash,
        file_name: chunker.file_name.clone(),
    };
    let start_packet = Packet::new_start(start_payload).serialize();

    let mut handshake_attempts = 0;
    let max_handshake_attempts = 20;
    let mut recv_buf = vec![0u8; 65535];
    let mut receiver_ready = false;

    while handshake_attempts < max_handshake_attempts {
        if cancel_flag.load(Ordering::SeqCst) {
            let _ = event_tx.send(TransferEvent::Cancelled);
            return;
        }

        if let Err(e) = socket.send_to(&start_packet, config.target_addr) {
            let _ = event_tx.send(TransferEvent::Error(format!("Failed to send START: {}", e)));
            return;
        }

        let start_time = Instant::now();
        while start_time.elapsed() < Duration::from_millis(300) {
            match socket.recv_from(&mut recv_buf) {
                Ok((len, from_addr)) => {
                    if from_addr == config.target_addr || from_addr.ip() == config.target_addr.ip()
                    {
                        if let Ok(pkt) = Packet::deserialize(&recv_buf[..len]) {
                            if pkt.packet_type == PacketType::StartAck {
                                if let Ok(ack_payload) =
                                    crate::protocol::packet::StartAckPayload::from_bytes(
                                        &pkt.payload,
                                    )
                                {
                                    if ack_payload.accepted {
                                        receiver_ready = true;
                                        let _ = event_tx.send(TransferEvent::Log(
                                            "Handshake confirmed! Receiver is ready.".to_string(),
                                        ));
                                        break;
                                    } else {
                                        let _ = event_tx.send(TransferEvent::Error(format!(
                                            "Receiver rejected transfer: {}",
                                            ack_payload.message
                                        )));
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    // Socket timeout, continue waiting
                }
            }
        }

        if receiver_ready {
            break;
        }

        handshake_attempts += 1;
        let _ = event_tx.send(TransferEvent::Log(format!(
            "Waiting for receiver response... (attempt {}/{})",
            handshake_attempts, max_handshake_attempts
        )));
    }

    if !receiver_ready {
        let _ = event_tx.send(TransferEvent::Error(format!(
            "Handshake timed out after {} attempts. Is receiver running on {}?",
            max_handshake_attempts, config.target_addr
        )));
        return;
    }

    // 2. Data Transmission Phase (Sliding Window + Selective Repeat)
    let mut window = SenderWindow::new(config.window_size, chunker.total_chunks);
    let mut stats = TransferStats::new(chunker.file_size, chunker.total_chunks);
    let mut rto = Duration::from_millis(250);
    stats.chunk_size = chunker.chunk_size;
    stats.perspective = WindowPerspective::Sender;
    stats.window_size = window.window_size;
    stats.configured_loss_percent = config.simulated_loss_percent;
    stats.update_rto(rto);

    let mut event_collector = BoundedEventCollector::new(MAX_EVENTS_PER_SNAPSHOT);
    event_collector.push(ProtocolTelemetryEvent {
        elapsed: Duration::ZERO,
        kind: ProtocolEventKind::Handshake,
        seq_num: None,
        ack_num: Some(0),
        sack_bitmask: None,
        message: format!(
            "START_ACK accepted: {} chunks ({}B each), window={}",
            chunker.total_chunks, chunker.chunk_size, window.window_size
        ),
    });

    let mut last_progress_log = Instant::now();

    let _ = event_tx.send(TransferEvent::Log(format!(
        "Starting transfer with window size {}",
        config.window_size
    )));

    // Emit initial snapshot at start of data phase
    sync_sender_snapshot(&mut stats, &window, rto, &mut event_collector);
    let _ = event_tx.try_send(TransferEvent::Progress(stats.clone()));

    let _ = socket.set_nonblocking(true);
    let mut last_progress_time = Instant::now();

    while !window.is_complete() {
        if cancel_flag.load(Ordering::SeqCst) {
            let _ = socket.set_nonblocking(false);
            let cancel_pkt = Packet::new_cancel("User cancelled transfer").serialize();
            let _ = socket.send_to(&cancel_pkt, config.target_addr);
            let _ = event_tx.send(TransferEvent::Cancelled);
            return;
        }

        let now = Instant::now();

        // Send packets within sliding window
        let mut batch_first_seq: Option<u32> = None;
        let mut batch_last_seq: Option<u32> = None;
        while window.can_send() {
            let seq = window.next_seq;
            match chunker.read_chunk(seq) {
                Ok(chunk_data) => {
                    let pkt = Packet::new_data(seq, chunk_data.clone(), false).serialize();
                    if let Err(e) = socket.send_to(&pkt, config.target_addr) {
                        let _ = event_tx
                            .send(TransferEvent::Error(format!("Socket send error: {}", e)));
                        return;
                    }
                    window.on_packet_sent(seq, chunk_data, now);
                    stats.packets_sent += 1;
                    if batch_first_seq.is_none() {
                        batch_first_seq = Some(seq);
                    }
                    batch_last_seq = Some(seq);
                }
                Err(e) => {
                    let _ = event_tx.send(TransferEvent::Error(format!(
                        "Error reading chunk {}: {}",
                        seq, e
                    )));
                    return;
                }
            }
        }

        if let (Some(first_s), Some(last_s)) = (batch_first_seq, batch_last_seq) {
            let elapsed = now.saturating_duration_since(stats.start_time);
            let msg = if first_s == last_s {
                format!(
                    "DATA sent: seq #{} (window [{}..{}))",
                    first_s, window.base, window.next_seq
                )
            } else {
                format!(
                    "DATA sent: seq #{}..=#{} ({} pkts, next_seq=#{})",
                    first_s,
                    last_s,
                    last_s - first_s + 1,
                    window.next_seq
                )
            };
            event_collector.push(ProtocolTelemetryEvent {
                elapsed,
                kind: ProtocolEventKind::DataSent,
                seq_num: Some(last_s),
                ack_num: Some(window.base),
                sack_bitmask: None,
                message: msg,
            });
        }

        // Process incoming ACKs
        let mut processed_acks = 0;
        loop {
            match socket.recv_from(&mut recv_buf) {
                Ok((len, from_addr)) => {
                    if from_addr == config.target_addr || from_addr.ip() == config.target_addr.ip()
                    {
                        match Packet::deserialize(&recv_buf[..len]) {
                            Ok(pkt) => {
                                match pkt.packet_type {
                                    PacketType::Ack => {
                                        let ack_now = Instant::now();
                                        let prev_base = window.base;
                                        let prev_sack = stats.last_sack_bitmask;
                                        stats.ack_packets_processed += 1;
                                        stats.last_sack_bitmask = pkt.sack_bitmask;

                                        let (newly_acked, rtt_sample) = window.on_ack_received(
                                            pkt.ack_num,
                                            pkt.sack_bitmask,
                                            ack_now,
                                        );
                                        if newly_acked > 0 {
                                            last_progress_time = ack_now;
                                            for _ in 0..newly_acked {
                                                stats.record_chunk(
                                                    chunker.chunk_size as usize,
                                                    ack_now,
                                                );
                                            }
                                        }
                                        if let Some(rtt) = rtt_sample {
                                            stats.update_rtt(rtt);
                                            // Smoothed RTO estimation: clamped between 80ms and 800ms
                                            rto = (rtt * 2).clamp(
                                                Duration::from_millis(80),
                                                Duration::from_millis(800),
                                            );
                                            stats.update_rto(rto);
                                        }

                                        let elapsed =
                                            ack_now.saturating_duration_since(stats.start_time);
                                        if pkt.sack_bitmask != 0 && pkt.sack_bitmask != prev_sack {
                                            let sack_bits = pkt.sack_bitmask.count_ones();
                                            event_collector.push(ProtocolTelemetryEvent {
                                                elapsed,
                                                kind: ProtocolEventKind::SackUpdate,
                                                seq_num: None,
                                                ack_num: Some(pkt.ack_num),
                                                sack_bitmask: Some(pkt.sack_bitmask),
                                                message: format!(
                                                    "SACK update: ack_num=#{}, mask=0x{:016X} ({} out-of-order chunks)",
                                                    pkt.ack_num, pkt.sack_bitmask, sack_bits
                                                ),
                                            });
                                        }

                                        if window.base > prev_base {
                                            event_collector.push(ProtocolTelemetryEvent {
                                                elapsed,
                                                kind: ProtocolEventKind::WindowAdvanced,
                                                seq_num: None,
                                                ack_num: Some(window.base),
                                                sack_bitmask: Some(pkt.sack_bitmask),
                                                message: format!(
                                                    "Window base advanced #{} -> #{} (+{} cumulative)",
                                                    prev_base,
                                                    window.base,
                                                    window.base - prev_base
                                                ),
                                            });
                                        } else if newly_acked > 0 && pkt.sack_bitmask == 0 {
                                            event_collector.push(ProtocolTelemetryEvent {
                                                elapsed,
                                                kind: ProtocolEventKind::AckReceived,
                                                seq_num: None,
                                                ack_num: Some(pkt.ack_num),
                                                sack_bitmask: Some(pkt.sack_bitmask),
                                                message: format!(
                                                    "ACK received: ack_num=#{}",
                                                    pkt.ack_num
                                                ),
                                            });
                                        }

                                        processed_acks += 1;
                                    }
                                    PacketType::Cancel => {
                                        let reason = String::from_utf8_lossy(&pkt.payload);
                                        let _ = event_tx.send(TransferEvent::Error(format!(
                                            "Receiver cancelled transfer: {}",
                                            reason
                                        )));
                                        return;
                                    }
                                    _ => {}
                                }
                            }
                            Err(_) => {
                                stats.record_corrupt_packet();
                            }
                        }
                    }
                }
                Err(_) => {
                    // No more packets available in this non-blocking poll cycle
                    break;
                }
            }
            if processed_acks > 64 {
                break;
            }
        }

        // Check for timeouts and retransmit missing chunks
        let check_now = Instant::now();
        let timed_out = window.get_timed_out_chunks(check_now, rto);
        for seq in timed_out {
            if let Some(entry) = window.in_flight.get_mut(&seq) {
                if entry.retransmit_count > 35 {
                    let _ = event_tx.send(TransferEvent::Error(format!(
                        "Chunk {} exceeded max retransmissions (35). Connection lost.",
                        seq
                    )));
                    return;
                }
                let elapsed = check_now.saturating_duration_since(stats.start_time);
                event_collector.push(ProtocolTelemetryEvent {
                    elapsed,
                    kind: ProtocolEventKind::TimeoutDetected,
                    seq_num: Some(seq),
                    ack_num: Some(window.base),
                    sack_bitmask: None,
                    message: format!(
                        "RTO timeout ({:.0}ms) expired for chunk #{}",
                        rto.as_secs_f64() * 1000.0,
                        seq
                    ),
                });

                let pkt = Packet::new_data(seq, entry.payload.clone(), true).serialize();
                let _ = socket.send_to(&pkt, config.target_addr);
                entry.sent_at = Instant::now();
                entry.retransmit_count += 1;
                stats.packets_sent += 1;
                stats.record_retransmission();

                event_collector.push(ProtocolTelemetryEvent {
                    elapsed: entry.sent_at.saturating_duration_since(stats.start_time),
                    kind: ProtocolEventKind::Retransmission,
                    seq_num: Some(seq),
                    ack_num: Some(window.base),
                    sack_bitmask: None,
                    message: format!(
                        "Retransmitted chunk #{} (attempt #{})",
                        seq, entry.retransmit_count
                    ),
                });
            }
        }

        // Periodic event updates to UI/CLI (~100 ms)
        if last_progress_log.elapsed() >= Duration::from_millis(100) {
            sync_sender_snapshot(&mut stats, &window, rto, &mut event_collector);
            let _ = event_tx.try_send(TransferEvent::Progress(stats.clone()));
            last_progress_log = Instant::now();
        }

        if last_progress_time.elapsed() > Duration::from_secs(25) {
            let _ = event_tx.send(TransferEvent::Error(
                "Transfer stalled: no acknowledgements received for 25 seconds.".to_string(),
            ));
            return;
        }

        // Short pause to allow receiver to process and avoid pegging CPU
        std::thread::sleep(Duration::from_micros(500));
    }

    let _ = socket.set_nonblocking(false);
    let _ = socket.set_read_timeout(Some(Duration::from_millis(50)));

    // 3. Termination & Integrity Verification Phase
    let _ = event_tx.send(TransferEvent::Log(
        "All data chunks acknowledged! Sending FIN and requesting verification...".to_string(),
    ));
    event_collector.push(ProtocolTelemetryEvent {
        elapsed: Instant::now().saturating_duration_since(stats.start_time),
        kind: ProtocolEventKind::Verification,
        seq_num: None,
        ack_num: Some(window.base),
        sack_bitmask: Some(0),
        message: "All chunks cumulatively ACKed; sent FIN with SHA-256 digest".to_string(),
    });

    let fin_packet = Packet::new_fin(chunker.sha256_hash).serialize();
    let mut fin_attempts = 0;
    let mut verified = false;

    while fin_attempts < 20 {
        if cancel_flag.load(Ordering::SeqCst) {
            let _ = event_tx.send(TransferEvent::Cancelled);
            return;
        }

        let _ = socket.send_to(&fin_packet, config.target_addr);

        let wait_start = Instant::now();
        while wait_start.elapsed() < Duration::from_millis(300) {
            match socket.recv_from(&mut recv_buf) {
                Ok((len, _)) => {
                    if let Ok(pkt) = Packet::deserialize(&recv_buf[..len]) {
                        if pkt.packet_type == PacketType::FinAck {
                            if let Ok(fin_ack) =
                                crate::protocol::packet::FinAckPayload::from_bytes(&pkt.payload)
                            {
                                if fin_ack.success {
                                    verified = true;
                                    break;
                                } else {
                                    let _ = event_tx.send(TransferEvent::Error(format!(
                                        "Receiver integrity verification failed: {}",
                                        fin_ack.message
                                    )));
                                    return;
                                }
                            }
                        }
                    }
                }
                Err(_) => {}
            }
        }

        if verified {
            break;
        }
        fin_attempts += 1;
    }

    if verified {
        stats.bytes_transferred = stats.total_bytes;
        stats.chunks_transferred = stats.total_chunks;
        let elapsed = stats.elapsed_duration();
        let avg_speed_mb =
            (stats.total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.001);
        let msg = format!(
            "Transfer successful and verified! Transferred {} in {:.2}s ({:.2} MB/s) with {} retransmissions.",
            crate::protocol::stats::format_file_size(stats.total_bytes),
            elapsed.as_secs_f64(),
            avg_speed_mb,
            stats.retransmissions
        );
        event_collector.push(ProtocolTelemetryEvent {
            elapsed,
            kind: ProtocolEventKind::Completed,
            seq_num: None,
            ack_num: Some(window.base),
            sack_bitmask: Some(0),
            message: "FIN_ACK received: Receiver verified whole-file SHA-256 OK".to_string(),
        });
        sync_sender_snapshot(&mut stats, &window, rto, &mut event_collector);
        let _ = event_tx.send(TransferEvent::Progress(stats));
        let _ = event_tx.send(TransferEvent::Success(msg));
    } else {
        let _ = event_tx.send(TransferEvent::Error(
            "Transfer ended without confirmation of receiver verification (FIN_ACK timeout)."
                .to_string(),
        ));
    }
}
