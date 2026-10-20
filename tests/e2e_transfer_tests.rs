use file_transfer::benchmark::{create_test_file, run_custom_udp_benchmark};
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
        0, // 0% loss
        16, // window size 16
        1400, // chunk size 1400
    ).expect("UDP transfer must succeed");

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
        10, // 10% packet loss
        16, // window size 16
        1400, // chunk size 1400
    ).expect("UDP transfer with simulated loss must recover and succeed");

    assert!(result.integrity_verified);
    // At 10% loss, retransmissions should be triggered
    assert!(result.retransmissions > 0, "Retransmissions should have occurred under loss");

    let received_path = dest_dir.join("source_lossy.dat");
    assert!(received_path.exists());
    let received_bytes = std::fs::read(&received_path).expect("read received file");
    assert_eq!(received_bytes.len(), file_size);

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(&received_bytes);
    let received_hash: [u8; 32] = hasher.finalize().into();
    assert_eq!(original_hash, received_hash, "File hash under packet loss must match exactly");
}
