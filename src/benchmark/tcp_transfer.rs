use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub protocol: String,
    pub file_size_bytes: u64,
    pub elapsed: Duration,
    pub throughput_mb_s: f64,
    pub retransmissions: u64,
    pub integrity_verified: bool,
}

pub fn run_tcp_transfer(
    file_path: &Path,
    dest_path: &Path,
    expected_hash: [u8; 32],
) -> Result<BenchmarkResult, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| format!("TCP bind: {}", e))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("TCP addr: {}", e))?
        .port();
    let file_path = file_path.to_path_buf();
    let dest_path = dest_path.to_path_buf();

    let start_time = Instant::now();

    // Receiver thread
    let recv_handle = thread::spawn(move || -> Result<(), String> {
        let (mut stream, _) = listener
            .accept()
            .map_err(|e| format!("TCP accept: {}", e))?;
        let mut outfile = File::create(&dest_path).map_err(|e| format!("Create dest: {}", e))?;
        let mut buf = vec![0u8; 64 * 1024];
        let mut hasher = Sha256::new();

        loop {
            let n = stream
                .read(&mut buf)
                .map_err(|e| format!("TCP read: {}", e))?;
            if n == 0 {
                break;
            }
            outfile
                .write_all(&buf[..n])
                .map_err(|e| format!("Write: {}", e))?;
            hasher.update(&buf[..n]);
        }

        let actual_hash: [u8; 32] = hasher.finalize().into();
        if actual_hash != expected_hash {
            return Err("TCP transfer hash mismatch".to_string());
        }
        Ok(())
    });

    // Sender thread
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .map_err(|e| format!("TCP connect: {}", e))?;
    let mut infile = File::open(&file_path).map_err(|e| format!("Open src: {}", e))?;
    let file_size = infile
        .metadata()
        .map_err(|e| format!("Metadata: {}", e))?
        .len();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let n = infile
            .read(&mut buf)
            .map_err(|e| format!("TCP src read: {}", e))?;
        if n == 0 {
            break;
        }
        stream
            .write_all(&buf[..n])
            .map_err(|e| format!("TCP write: {}", e))?;
    }
    stream.flush().map_err(|e| format!("TCP flush: {}", e))?;
    drop(stream); // Send TCP FIN

    recv_handle
        .join()
        .map_err(|_| "Receiver thread panicked".to_string())??;

    let elapsed = start_time.elapsed();
    let throughput_mb_s =
        (file_size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.0001);

    Ok(BenchmarkResult {
        protocol: "TCP (Baseline)".to_string(),
        file_size_bytes: file_size,
        elapsed,
        throughput_mb_s,
        retransmissions: 0,
        integrity_verified: true,
    })
}
