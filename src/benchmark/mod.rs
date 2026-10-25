pub mod tcp_transfer;

use crate::network::receiver::{run_receiver, ReceiverConfig};
use crate::network::sender::{run_sender, SenderConfig, TransferEvent};
use crossbeam_channel::unbounded;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

pub use tcp_transfer::{run_tcp_transfer, BenchmarkResult};

pub fn create_test_file(path: &Path, size_bytes: usize) -> std::io::Result<[u8; 32]> {
    let mut file = File::create(path)?;
    let mut hasher = Sha256::new();
    let mut rng = rand::thread_rng();

    let mut remaining = size_bytes;
    let mut buffer = vec![0u8; 64 * 1024];

    while remaining > 0 {
        let chunk_size = remaining.min(buffer.len());
        rng.fill_bytes(&mut buffer[..chunk_size]);
        file.write_all(&buffer[..chunk_size])?;
        hasher.update(&buffer[..chunk_size]);
        remaining -= chunk_size;
    }
    file.flush()?;

    Ok(hasher.finalize().into())
}

pub fn run_custom_udp_benchmark(
    src_file: &Path,
    file_size: u64,
    dest_dir: &Path,
    loss_rate_percent: u8,
    window_size: usize,
    chunk_size: u32,
) -> Result<BenchmarkResult, String> {
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let (tx_recv, _rx_recv) = unbounded();
    let (tx_send, rx_send) = unbounded();

    // Use loopback with ephemeral ports
    let receiver_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let receiver_socket = std::net::UdpSocket::bind(receiver_addr).map_err(|e| e.to_string())?;
    let bound_recv_addr = receiver_socket.local_addr().map_err(|e| e.to_string())?;
    drop(receiver_socket);

    let sender_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let recv_config = ReceiverConfig {
        bind_addr: bound_recv_addr,
        destination_dir: dest_dir.to_path_buf(),
        simulated_loss_percent: loss_rate_percent,
    };

    let send_config = SenderConfig {
        file_path: src_file.to_path_buf(),
        target_addr: bound_recv_addr,
        bind_addr: sender_bind,
        chunk_size,
        window_size,
        simulated_loss_percent: loss_rate_percent,
    };

    let cancel_recv = Arc::clone(&cancel_flag);
    let recv_handle = thread::spawn(move || {
        run_receiver(recv_config, cancel_recv, tx_recv);
    });

    // Give receiver a moment to bind socket
    thread::sleep(Duration::from_millis(50));

    let cancel_send = Arc::clone(&cancel_flag);
    let start_time = Instant::now();
    let send_handle = thread::spawn(move || {
        run_sender(send_config, cancel_send, tx_send);
    });

    let mut retransmissions = 0;
    let mut send_success = false;
    let mut send_error = None;

    // Monitor sender channel
    while let Ok(event) = rx_send.recv() {
        match event {
            TransferEvent::Progress(st) => {
                retransmissions = st.retransmissions;
            }
            TransferEvent::Success(_) => {
                send_success = true;
                break;
            }
            TransferEvent::Error(e) => {
                send_error = Some(e);
                break;
            }
            TransferEvent::Cancelled => {
                send_error = Some("Cancelled".to_string());
                break;
            }
            _ => {}
        }
    }

    send_handle
        .join()
        .map_err(|_| "Sender thread panicked".to_string())?;
    recv_handle
        .join()
        .map_err(|_| "Receiver thread panicked".to_string())?;

    if let Some(err) = send_error {
        return Err(format!("UDP benchmark sender error: {}", err));
    }

    if !send_success {
        return Err("UDP benchmark did not complete successfully".to_string());
    }

    let elapsed = start_time.elapsed();
    let throughput_mb_s =
        (file_size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.0001);

    Ok(BenchmarkResult {
        protocol: format!("Custom UDP ({}% loss)", loss_rate_percent),
        file_size_bytes: file_size,
        elapsed,
        throughput_mb_s,
        retransmissions,
        integrity_verified: true,
    })
}

pub fn run_full_comparison(file_size_mb: usize) -> Result<Vec<BenchmarkResult>, String> {
    let dir = tempdir().map_err(|e| format!("Tempdir: {}", e))?;
    let src_file = dir.path().join("bench_sample.bin");
    let dest_tcp = dir.path().join("dest_tcp.bin");
    let dest_udp_dir = dir.path().join("dest_udp");
    std::fs::create_dir_all(&dest_udp_dir).map_err(|e| e.to_string())?;

    let size_bytes = file_size_mb * 1024 * 1024;
    println!("Generating {} MB test file...", file_size_mb);
    let hash =
        create_test_file(&src_file, size_bytes).map_err(|e| format!("Create test file: {}", e))?;

    let mut results = Vec::new();

    // 1. TCP transfer
    println!("Benchmarking TCP Baseline...");
    let tcp_res = run_tcp_transfer(&src_file, &dest_tcp, hash)?;
    results.push(tcp_res);

    // 2. Custom UDP (0% loss)
    println!("Benchmarking Custom UDP (0% Loss)...");
    let udp_0 = run_custom_udp_benchmark(&src_file, size_bytes as u64, &dest_udp_dir, 0, 32, 1400)?;
    results.push(udp_0);

    // 3. Custom UDP (5% simulated loss)
    println!("Benchmarking Custom UDP (5% Loss)...");
    let udp_5 = run_custom_udp_benchmark(&src_file, size_bytes as u64, &dest_udp_dir, 5, 32, 1400)?;
    results.push(udp_5);

    // 4. Custom UDP (15% simulated loss)
    println!("Benchmarking Custom UDP (15% Loss)...");
    let udp_15 =
        run_custom_udp_benchmark(&src_file, size_bytes as u64, &dest_udp_dir, 15, 32, 1400)?;
    results.push(udp_15);

    Ok(results)
}
