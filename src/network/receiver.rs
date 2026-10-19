use crate::file::FileWriter;
use crate::network::sender::TransferEvent;
use crate::network::simulated_socket::ReliableUdpSocket;
use crate::protocol::packet::{Packet, PacketType, StartPayload};
use crate::protocol::stats::TransferStats;
use crate::protocol::window::ReceiverWindow;
use crossbeam_channel::Sender as ChannelSender;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct ReceiverConfig {
    pub bind_addr: SocketAddr,
    pub destination_dir: PathBuf,
    pub simulated_loss_percent: u8,
}

pub fn run_receiver(
    config: ReceiverConfig,
    cancel_flag: Arc<AtomicBool>,
    event_tx: ChannelSender<TransferEvent>,
) {
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

    let bound_addr = socket.local_addr().map(|a| a.to_string()).unwrap_or_default();
    let _ = event_tx.send(TransferEvent::Log(format!(
        "Receiver listening on UDP {}",
        bound_addr
    )));
    let _ = event_tx.send(TransferEvent::Log(format!(
        "Saving received files to: {}",
        config.destination_dir.display()
    )));

    if config.simulated_loss_percent > 0 {
        let _ = event_tx.send(TransferEvent::Log(format!(
            "Simulating {}% packet loss on receiver socket",
            config.simulated_loss_percent
        )));
    }

    let mut recv_buf = vec![0u8; 65535];
    let mut sender_addr: Option<SocketAddr> = None;
    let mut writer: Option<FileWriter> = None;
    let mut window: Option<ReceiverWindow> = None;
    let mut stats: Option<TransferStats> = None;
    let mut expected_hash = [0u8; 32];
    let mut last_progress_time = Instant::now();
    let mut last_activity_time = Instant::now();

    // Loop until file received or cancelled
    while !cancel_flag.load(Ordering::SeqCst) {
        match socket.recv_from(&mut recv_buf) {
            Ok((len, from_addr)) => {
                last_activity_time = Instant::now();
                let pkt = match Packet::deserialize(&recv_buf[..len]) {
                    Ok(p) => p,
                    Err(e) => {
                        if let Some(s) = &mut stats {
                            s.record_corrupt_packet();
                        }
                        let _ = event_tx.send(TransferEvent::Log(format!(
                            "Corrupt packet dropped from {}: {}",
                            from_addr, e
                        )));
                        continue;
                    }
                };

                match pkt.packet_type {
                    PacketType::Start => {
                        // Handshake initiation from sender
                        if writer.is_some() && sender_addr == Some(from_addr) {
                            let ack = Packet::new_start_ack(true, "Ready to receive").serialize();
                            let _ = socket.send_to(&ack, from_addr);
                            continue;
                        }

                        match StartPayload::from_bytes(&pkt.payload) {
                            Ok(start_payload) => {
                                sender_addr = Some(from_addr);
                                expected_hash = start_payload.sha256_hash;
                                let file_name = start_payload.file_name.clone();
                                let total_chunks = start_payload.total_chunks;
                                let chunk_size = start_payload.chunk_size;

                                let _ = event_tx.send(TransferEvent::Log(format!(
                                    "Incoming file request: '{}' ({} bytes, {} chunks) from {}",
                                    file_name, start_payload.file_size, total_chunks, from_addr
                                )));

                                match FileWriter::new(
                                    &config.destination_dir,
                                    &file_name,
                                    start_payload.file_size,
                                    chunk_size,
                                ) {
                                    Ok(w) => {
                                        writer = Some(w);
                                        window = Some(ReceiverWindow::new(total_chunks));
                                        stats = Some(TransferStats::new(start_payload.file_size, total_chunks));

                                        let ack = Packet::new_start_ack(true, "Ready to receive").serialize();
                                        let _ = socket.send_to(&ack, from_addr);
                                        let _ = event_tx.send(TransferEvent::Log(
                                            "Handshake accepted. Waiting for data chunks...".to_string(),
                                        ));
                                    }
                                    Err(e) => {
                                        let ack = Packet::new_start_ack(
                                            false,
                                            &format!("Failed to create destination file: {}", e),
                                        )
                                        .serialize();
                                        let _ = socket.send_to(&ack, from_addr);
                                        let _ = event_tx.send(TransferEvent::Error(format!(
                                            "File creation error: {}",
                                            e
                                        )));
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = event_tx.send(TransferEvent::Log(format!(
                                    "Invalid Start payload: {}",
                                    e
                                )));
                            }
                        }
                    }

                    PacketType::Data => {
                        if sender_addr.map_or(false, |s| s.ip() == from_addr.ip()) {
                            if let (Some(win), Some(w), Some(st)) = (&mut window, &mut writer, &mut stats) {
                                let seq = pkt.seq_num;
                                let is_new = win.on_chunk_received(seq);

                                if is_new {
                                    if let Err(e) = w.write_chunk(seq, &pkt.payload) {
                                        let _ = event_tx.send(TransferEvent::Error(format!(
                                            "Error writing chunk {}: {}",
                                            seq, e
                                        )));
                                        return;
                                    }
                                    st.record_chunk(pkt.payload.len(), Instant::now());
                                } else {
                                    st.record_duplicate_packet();
                                }

                                // Send ACK back to sender
                                let (ack_num, sack_bitmask) = win.create_ack_state();
                                let ack_pkt = Packet::new_ack(ack_num, sack_bitmask).serialize();
                                let _ = socket.send_to(&ack_pkt, from_addr);

                                if last_progress_time.elapsed() >= Duration::from_millis(100) {
                                    let _ = event_tx.send(TransferEvent::Progress(st.clone()));
                                    last_progress_time = Instant::now();
                                }
                            }
                        }
                    }

                    PacketType::Fin => {
                        if sender_addr.map_or(false, |s| s.ip() == from_addr.ip()) {
                            if let (Some(win), Some(w), Some(st)) = (&mut window, &mut writer, &mut stats) {
                                if win.is_complete() {
                                    let _ = event_tx.send(TransferEvent::Log(
                                        "All chunks received! Verifying SHA-256 checksum...".to_string(),
                                    ));

                                    match w.finalize_and_verify(&expected_hash) {
                                        Ok(()) => {
                                            // Send confirmation to sender multiple times to ensure delivery
                                            let fin_ack = Packet::new_fin_ack(true, "File verified successfully").serialize();
                                            for _ in 0..5 {
                                                let _ = socket.send_to(&fin_ack, from_addr);
                                                std::thread::sleep(Duration::from_millis(10));
                                            }

                                            let dest_path = w.destination_path.display().to_string();
                                            let elapsed = st.elapsed_duration();
                                            let avg_speed_mb = (st.total_bytes as f64 / (1024.0 * 1024.0))
                                                / elapsed.as_secs_f64().max(0.001);

                                            let msg = format!(
                                                "File transfer complete & verified!\nSaved to: {}\nSize: {} | Time: {:.2}s ({:.2} MB/s)",
                                                dest_path,
                                                crate::protocol::stats::format_file_size(st.total_bytes),
                                                elapsed.as_secs_f64(),
                                                avg_speed_mb
                                            );

                                            st.bytes_transferred = st.total_bytes;
                                            st.chunks_transferred = st.total_chunks;
                                            let _ = event_tx.send(TransferEvent::Progress(st.clone()));
                                            let _ = event_tx.send(TransferEvent::Success(msg));
                                            return;
                                        }
                                        Err(e) => {
                                            let fin_ack = Packet::new_fin_ack(false, &e).serialize();
                                            let _ = socket.send_to(&fin_ack, from_addr);
                                            let _ = event_tx.send(TransferEvent::Error(format!(
                                                "Integrity verification failed: {}",
                                                e
                                            )));
                                            return;
                                        }
                                    }
                                } else {
                                    // Chunks missing: send current ACK state so sender retransmits
                                    let (ack_num, sack_bitmask) = win.create_ack_state();
                                    let ack_pkt = Packet::new_ack(ack_num, sack_bitmask).serialize();
                                    let _ = socket.send_to(&ack_pkt, from_addr);
                                }
                            }
                        }
                    }

                    PacketType::Cancel => {
                        let reason = String::from_utf8_lossy(&pkt.payload);
                        let _ = event_tx.send(TransferEvent::Error(format!(
                            "Sender cancelled transfer: {}",
                            reason
                        )));
                        return;
                    }

                    _ => {}
                }
            }
            Err(_) => {
                // Timeout on socket read: check idle timeout if transfer was in progress
                if writer.is_some() && last_activity_time.elapsed() > Duration::from_secs(30) {
                    let _ = event_tx.send(TransferEvent::Error(
                        "Receiver timed out: no data received from sender for 30 seconds.".to_string(),
                    ));
                    return;
                }
            }
        }
    }

    let _ = event_tx.send(TransferEvent::Cancelled);
}
