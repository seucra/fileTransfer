use crate::file::FileChunker;
use crate::network::simulated_socket::ReliableUdpSocket;
use crate::protocol::packet::{Packet, PacketType, StartPayload};
use crate::protocol::stats::TransferStats;
use crate::protocol::window::SenderWindow;
use crossbeam_channel::Sender as ChannelSender;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
        socket.local_addr().map(|a| a.to_string()).unwrap_or_default(),
        config.target_addr
    )));

    if config.simulated_loss_percent > 0 {
        let _ = event_tx.send(TransferEvent::Log(format!(
            "Simulating {}% packet loss on sender socket",
            config.simulated_loss_percent
        )));
    }

    // 1. Handshake Phase
    let _ = event_tx.send(TransferEvent::Log("Initiating handshake (START)...".to_string()));

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
                    if from_addr == config.target_addr || from_addr.ip() == config.target_addr.ip() {
                        if let Ok(pkt) = Packet::deserialize(&recv_buf[..len]) {
                            if pkt.packet_type == PacketType::StartAck {
                                if let Ok(ack_payload) = crate::protocol::packet::StartAckPayload::from_bytes(&pkt.payload) {
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
    let mut last_progress_log = Instant::now();

    let _ = event_tx.send(TransferEvent::Log(format!(
        "Starting transfer with window size {}",
        config.window_size
    )));

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
        while window.can_send() {
            let seq = window.next_seq;
            match chunker.read_chunk(seq) {
                Ok(chunk_data) => {
                    let pkt = Packet::new_data(seq, chunk_data.clone(), false).serialize();
                    if let Err(e) = socket.send_to(&pkt, config.target_addr) {
                        let _ = event_tx.send(TransferEvent::Error(format!("Socket send error: {}", e)));
                        return;
                    }
                    window.on_packet_sent(seq, chunk_data, now);
                }
                Err(e) => {
                    let _ = event_tx.send(TransferEvent::Error(format!("Error reading chunk {}: {}", seq, e)));
                    return;
                }
            }
        }

        // Process incoming ACKs
        let mut processed_acks = 0;
        loop {
            match socket.recv_from(&mut recv_buf) {
                Ok((len, from_addr)) => {
                    if from_addr == config.target_addr || from_addr.ip() == config.target_addr.ip() {
                        match Packet::deserialize(&recv_buf[..len]) {
                            Ok(pkt) => {
                                match pkt.packet_type {
                                    PacketType::Ack => {
                                        let (newly_acked, rtt_sample) =
                                            window.on_ack_received(pkt.ack_num, pkt.sack_bitmask, Instant::now());
                                        if newly_acked > 0 {
                                            last_progress_time = Instant::now();
                                            for _ in 0..newly_acked {
                                                stats.record_chunk(chunker.chunk_size as usize, Instant::now());
                                            }
                                        }
                                        if let Some(rtt) = rtt_sample {
                                            stats.update_rtt(rtt);
                                            // Smoothed RTO estimation: clamped between 80ms and 800ms
                                            rto = (rtt * 2).clamp(Duration::from_millis(80), Duration::from_millis(800));
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
        let timed_out = window.get_timed_out_chunks(Instant::now(), rto);
        for seq in timed_out {
            if let Some(entry) = window.in_flight.get_mut(&seq) {
                if entry.retransmit_count > 35 {
                    let _ = event_tx.send(TransferEvent::Error(format!(
                        "Chunk {} exceeded max retransmissions (35). Connection lost.",
                        seq
                    )));
                    return;
                }
                let pkt = Packet::new_data(seq, entry.payload.clone(), true).serialize();
                let _ = socket.send_to(&pkt, config.target_addr);
                entry.sent_at = Instant::now();
                entry.retransmit_count += 1;
                stats.record_retransmission();
            }
        }

        // Periodic event updates to UI/CLI
        if last_progress_log.elapsed() >= Duration::from_millis(100) {
            let _ = event_tx.send(TransferEvent::Progress(stats.clone()));
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
                            if let Ok(fin_ack) = crate::protocol::packet::FinAckPayload::from_bytes(&pkt.payload) {
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
        let avg_speed_mb = (stats.total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.001);
        let msg = format!(
            "Transfer successful and verified! Transferred {} in {:.2}s ({:.2} MB/s) with {} retransmissions.",
            crate::protocol::stats::format_file_size(stats.total_bytes),
            elapsed.as_secs_f64(),
            avg_speed_mb,
            stats.retransmissions
        );
        let _ = event_tx.send(TransferEvent::Progress(stats));
        let _ = event_tx.send(TransferEvent::Success(msg));
    } else {
        let _ = event_tx.send(TransferEvent::Error(
            "Transfer ended without confirmation of receiver verification (FIN_ACK timeout).".to_string(),
        ));
    }
}
