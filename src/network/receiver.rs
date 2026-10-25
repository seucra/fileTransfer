use crate::file::FileWriter;
use crate::network::sender::TransferEvent;
use crate::network::simulated_socket::ReliableUdpSocket;
use crate::protocol::packet::{Packet, PacketType, StartPayload};
use crate::protocol::stats::{
    BoundedEventCollector, ProtocolEventKind, ProtocolTelemetryEvent, TransferStats,
    WindowPerspective, DEFAULT_VISIBLE_CHUNK_LIMIT, MAX_EVENTS_PER_SNAPSHOT,
};
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

fn sync_receiver_snapshot(
    stats: &mut TransferStats,
    window: &ReceiverWindow,
    sack_bitmask: u64,
    collector: &mut BoundedEventCollector,
) {
    stats.window_base = window.base;
    stats.next_seq = window.highest_observed_seq.map_or(0, |h| h + 1);
    stats.sacked_count = window.out_of_order.len();
    stats.in_flight_count = window.highest_observed_seq.map_or(0, |hi| {
        if hi >= window.base {
            ((hi - window.base + 1) as usize).saturating_sub(window.out_of_order.len())
        } else {
            0
        }
    });
    stats.last_sack_bitmask = sack_bitmask;
    let (vis_start, vis_chunks) =
        window.build_visible_snapshots(stats.start_time, DEFAULT_VISIBLE_CHUNK_LIMIT);
    stats.visible_chunk_start = vis_start;
    stats.visible_chunks = vis_chunks;
    stats.recent_events = collector.drain_to_vec();
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

    let bound_addr = socket
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
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
    let mut event_collector = BoundedEventCollector::new(MAX_EVENTS_PER_SNAPSHOT);
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
                                        let win = ReceiverWindow::new(total_chunks);
                                        let mut st = TransferStats::new(
                                            start_payload.file_size,
                                            total_chunks,
                                        );
                                        st.chunk_size = chunk_size;
                                        st.perspective = WindowPerspective::Receiver;
                                        st.window_size = 64;
                                        st.configured_loss_percent = config.simulated_loss_percent;

                                        event_collector.push(ProtocolTelemetryEvent {
                                            elapsed: Duration::ZERO,
                                            kind: ProtocolEventKind::Handshake,
                                            seq_num: None,
                                            ack_num: Some(0),
                                            sack_bitmask: Some(0),
                                            message: format!(
                                                "START accepted for '{}' ({} chunks, {}B each)",
                                                file_name, total_chunks, chunk_size
                                            ),
                                        });

                                        sync_receiver_snapshot(
                                            &mut st,
                                            &win,
                                            0,
                                            &mut event_collector,
                                        );
                                        let _ =
                                            event_tx.try_send(TransferEvent::Progress(st.clone()));

                                        writer = Some(w);
                                        window = Some(win);
                                        stats = Some(st);

                                        let ack = Packet::new_start_ack(true, "Ready to receive")
                                            .serialize();
                                        let _ = socket.send_to(&ack, from_addr);
                                        let _ = event_tx.send(TransferEvent::Log(
                                            "Handshake accepted. Waiting for data chunks..."
                                                .to_string(),
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
                        if sender_addr.is_some_and(|s| s.ip() == from_addr.ip()) {
                            if let (Some(win), Some(w), Some(st)) =
                                (&mut window, &mut writer, &mut stats)
                            {
                                let now = Instant::now();
                                let seq = pkt.seq_num;
                                let is_retrans = pkt.is_retransmission();
                                let prev_base = win.base;
                                let prev_sack = st.last_sack_bitmask;

                                let is_new = win.on_chunk_received_with_meta(seq, is_retrans, now);
                                st.ack_packets_processed += 1;

                                if is_retrans {
                                    st.record_retransmission();
                                }

                                if is_new {
                                    if let Err(e) = w.write_chunk(seq, &pkt.payload) {
                                        let _ = event_tx.send(TransferEvent::Error(format!(
                                            "Error writing chunk {}: {}",
                                            seq, e
                                        )));
                                        return;
                                    }
                                    st.record_chunk(pkt.payload.len(), now);
                                } else {
                                    st.record_duplicate_packet();
                                }

                                // Send ACK back to sender
                                let (ack_num, sack_bitmask) = win.create_ack_state();
                                let ack_pkt = Packet::new_ack(ack_num, sack_bitmask).serialize();
                                let _ = socket.send_to(&ack_pkt, from_addr);
                                st.packets_sent += 1;

                                let elapsed = now.saturating_duration_since(st.start_time);
                                if is_retrans {
                                    event_collector.push(ProtocolTelemetryEvent {
                                        elapsed,
                                        kind: ProtocolEventKind::Retransmission,
                                        seq_num: Some(seq),
                                        ack_num: Some(ack_num),
                                        sack_bitmask: Some(sack_bitmask),
                                        message: format!(
                                            "Received retransmitted DATA chunk #{} (FLAG_RETRANSMISSION=1)",
                                            seq
                                        ),
                                    });
                                }

                                if win.base > prev_base {
                                    event_collector.push(ProtocolTelemetryEvent {
                                        elapsed,
                                        kind: ProtocolEventKind::WindowAdvanced,
                                        seq_num: Some(seq),
                                        ack_num: Some(win.base),
                                        sack_bitmask: Some(sack_bitmask),
                                        message: format!(
                                            "Cumulative base advanced #{} -> #{} (received chunk #{})",
                                            prev_base, win.base, seq
                                        ),
                                    });
                                } else if is_new && seq > win.base && sack_bitmask != prev_sack {
                                    event_collector.push(ProtocolTelemetryEvent {
                                        elapsed,
                                        kind: ProtocolEventKind::SackUpdate,
                                        seq_num: Some(seq),
                                        ack_num: Some(ack_num),
                                        sack_bitmask: Some(sack_bitmask),
                                        message: format!(
                                            "Out-of-order chunk #{} buffered (awaiting #{}, SACK=0x{:016X})",
                                            seq, ack_num, sack_bitmask
                                        ),
                                    });
                                }

                                if last_progress_time.elapsed() >= Duration::from_millis(100) {
                                    sync_receiver_snapshot(
                                        st,
                                        win,
                                        sack_bitmask,
                                        &mut event_collector,
                                    );
                                    let _ = event_tx.try_send(TransferEvent::Progress(st.clone()));
                                    last_progress_time = Instant::now();
                                }
                            }
                        }
                    }

                    PacketType::Fin => {
                        if sender_addr.is_some_and(|s| s.ip() == from_addr.ip()) {
                            if let (Some(win), Some(w), Some(st)) =
                                (&mut window, &mut writer, &mut stats)
                            {
                                if win.is_complete() {
                                    let _ = event_tx.send(TransferEvent::Log(
                                        "All chunks received! Verifying SHA-256 checksum..."
                                            .to_string(),
                                    ));
                                    event_collector.push(ProtocolTelemetryEvent {
                                        elapsed: Instant::now()
                                            .saturating_duration_since(st.start_time),
                                        kind: ProtocolEventKind::Verification,
                                        seq_num: None,
                                        ack_num: Some(win.base),
                                        sack_bitmask: Some(0),
                                        message:
                                            "FIN received: verifying whole-file SHA-256 on disk"
                                                .to_string(),
                                    });

                                    match w.finalize_and_verify(&expected_hash) {
                                        Ok(()) => {
                                            // Send confirmation to sender multiple times to ensure delivery
                                            let fin_ack = Packet::new_fin_ack(
                                                true,
                                                "File verified successfully",
                                            )
                                            .serialize();
                                            for _ in 0..5 {
                                                let _ = socket.send_to(&fin_ack, from_addr);
                                                st.packets_sent += 1;
                                                std::thread::sleep(Duration::from_millis(10));
                                            }

                                            let dest_path =
                                                w.destination_path.display().to_string();
                                            let elapsed = st.elapsed_duration();
                                            let avg_speed_mb = (st.total_bytes as f64
                                                / (1024.0 * 1024.0))
                                                / elapsed.as_secs_f64().max(0.001);

                                            let msg = format!(
                                                "File transfer complete & verified!\nSaved to: {}\nSize: {} | Time: {:.2}s ({:.2} MB/s)",
                                                dest_path,
                                                crate::protocol::stats::format_file_size(
                                                    st.total_bytes
                                                ),
                                                elapsed.as_secs_f64(),
                                                avg_speed_mb
                                            );

                                            st.bytes_transferred = st.total_bytes;
                                            st.chunks_transferred = st.total_chunks;
                                            event_collector.push(ProtocolTelemetryEvent {
                                                elapsed,
                                                kind: ProtocolEventKind::Completed,
                                                seq_num: None,
                                                ack_num: Some(win.base),
                                                sack_bitmask: Some(0),
                                                message: "SHA-256 verified OK; sent FIN_ACK"
                                                    .to_string(),
                                            });
                                            sync_receiver_snapshot(
                                                st,
                                                win,
                                                0,
                                                &mut event_collector,
                                            );
                                            let _ =
                                                event_tx.send(TransferEvent::Progress(st.clone()));
                                            let _ = event_tx.send(TransferEvent::Success(msg));
                                            return;
                                        }
                                        Err(e) => {
                                            let fin_ack =
                                                Packet::new_fin_ack(false, &e).serialize();
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
                                    let ack_pkt =
                                        Packet::new_ack(ack_num, sack_bitmask).serialize();
                                    let _ = socket.send_to(&ack_pkt, from_addr);
                                    st.packets_sent += 1;
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
                        "Receiver timed out: no data received from sender for 30 seconds."
                            .to_string(),
                    ));
                    return;
                }
            }
        }
    }

    let _ = event_tx.send(TransferEvent::Cancelled);
}
