use crossbeam_channel::bounded;
use file_transfer::benchmark::{create_test_file, run_custom_udp_benchmark};
use file_transfer::network::receiver::{run_receiver, ReceiverConfig};
use file_transfer::network::sender::{run_sender, SenderConfig, TransferEvent};
use file_transfer::protocol::stats::{ChunkState, DEFAULT_VISIBLE_CHUNK_LIMIT};
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_e2e_udp_transfer_lossless() {
    let dir = tempdir().expect("tempdir");
    let src_file = dir.path().join("source_lossless.dat");
    let dest_dir = dir.path().join("received_lossless");
    std::fs::create_dir_all(&dest_dir).unwrap();

    let file_size: usize = 512 * 1024; // 512 KB
    let original_hash = create_test_file(&src_file, file_size).expect("create test file");

    let result = run_custom_udp_benchmark(
        &src_file,
        file_size as u64,
        &dest_dir,
        0,    // 0% loss
        16,   // window size 16
        1400, // chunk size 1400
    )
    .expect("UDP transfer must succeed");

    assert!(result.integrity_verified);
    assert_eq!(result.file_size_bytes, file_size as u64);

    let received_path = dest_dir.join("source_lossless.dat");
    assert!(received_path.exists());
    let received_bytes = std::fs::read(&received_path).expect("read received file");
    assert_eq!(received_bytes.len(), file_size);

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(&received_bytes);
    let received_hash: [u8; 32] = hasher.finalize().into();
    assert_eq!(original_hash, received_hash, "File hash must match exactly");
}

#[test]
fn test_e2e_udp_transfer_with_packet_loss() {
    let dir = tempdir().expect("tempdir");
    let src_file = dir.path().join("source_lossy.dat");
    let dest_dir = dir.path().join("received_lossy");
    std::fs::create_dir_all(&dest_dir).unwrap();

    let file_size: usize = 256 * 1024; // 256 KB
    let original_hash = create_test_file(&src_file, file_size).expect("create test file");

    // 10% simulated packet loss
    let result = run_custom_udp_benchmark(
        &src_file,
        file_size as u64,
        &dest_dir,
        10,   // 10% packet loss
        16,   // window size 16
        1400, // chunk size 1400
    )
    .expect("UDP transfer with simulated loss must recover and succeed");

    assert!(result.integrity_verified);
    // At 10% loss, retransmissions should be triggered
    assert!(
        result.retransmissions > 0,
        "Retransmissions should have occurred under loss"
    );

    let received_path = dest_dir.join("source_lossy.dat");
    assert!(received_path.exists());
    let received_bytes = std::fs::read(&received_path).expect("read received file");
    assert_eq!(received_bytes.len(), file_size);

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(&received_bytes);
    let received_hash: [u8; 32] = hasher.finalize().into();
    assert_eq!(
        original_hash, received_hash,
        "File hash under packet loss must match exactly"
    );
}

#[test]
fn test_e2e_observability_snapshots_agree_with_protocol_state() {
    let dir = tempdir().expect("tempdir");
    let src_file = dir.path().join("obs_test.dat");
    let dest_dir = dir.path().join("obs_dest");
    std::fs::create_dir_all(&dest_dir).unwrap();

    // 180 KB (~132 chunks at 1400B) so visible window slides across > 80 chunks
    let file_size: usize = 180 * 1024;
    let _hash = create_test_file(&src_file, file_size).expect("create test file");

    let cancel_flag = Arc::new(AtomicBool::new(false));
    let (tx_recv, rx_recv) = bounded(128);
    let (tx_send, rx_send) = bounded(128);

    let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let recv_addr: SocketAddr = probe.local_addr().unwrap();
    drop(probe);

    let recv_config = ReceiverConfig {
        bind_addr: recv_addr,
        destination_dir: dest_dir.clone(),
        simulated_loss_percent: 8,
    };

    let send_config = SenderConfig {
        file_path: src_file.clone(),
        target_addr: recv_addr,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        chunk_size: 1400,
        window_size: 16,
        simulated_loss_percent: 8,
    };

    let c_recv = Arc::clone(&cancel_flag);
    let recv_handle = thread::spawn(move || {
        run_receiver(recv_config, c_recv, tx_recv);
    });

    thread::sleep(Duration::from_millis(40));

    let c_send = Arc::clone(&cancel_flag);
    let send_handle = thread::spawn(move || {
        run_sender(send_config, c_send, tx_send);
    });

    let mut sender_snapshots = Vec::new();
    let mut sender_succeeded = false;
    while let Ok(ev) = rx_send.recv() {
        match ev {
            TransferEvent::Progress(st) => {
                assert!(st.visible_chunks.len() <= DEFAULT_VISIBLE_CHUNK_LIMIT);
                assert_eq!(st.configured_loss_percent, 8);
                sender_snapshots.push(st);
            }
            TransferEvent::Success(_) => {
                sender_succeeded = true;
                break;
            }
            TransferEvent::Error(e) => panic!("Sender error: {}", e),
            _ => {}
        }
    }

    let mut receiver_snapshots = Vec::new();
    while let Ok(ev) = rx_recv.recv() {
        match ev {
            TransferEvent::Progress(st) => {
                assert!(st.visible_chunks.len() <= DEFAULT_VISIBLE_CHUNK_LIMIT);
                receiver_snapshots.push(st);
            }
            TransferEvent::Success(_) => break,
            TransferEvent::Error(e) => panic!("Receiver error: {}", e),
            _ => {}
        }
    }

    send_handle.join().unwrap();
    recv_handle.join().unwrap();

    assert!(sender_succeeded);
    assert!(!sender_snapshots.is_empty());
    assert!(!receiver_snapshots.is_empty());

    let final_send = sender_snapshots.last().unwrap();
    assert_eq!(final_send.window_base, final_send.total_chunks);
    assert_eq!(final_send.in_flight_count, 0);
    assert!(final_send
        .visible_chunks
        .iter()
        .all(|c| c.state == ChunkState::Acked));

    let final_recv = receiver_snapshots.last().unwrap();
    assert_eq!(final_recv.window_base, final_recv.total_chunks);
    assert!(final_recv
        .visible_chunks
        .iter()
        .all(|c| c.state == ChunkState::Acked));
}
