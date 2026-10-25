use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct FileWriter {
    pub destination_path: PathBuf,
    pub expected_size: u64,
    pub chunk_size: u32,
    file: File,
}

impl FileWriter {
    pub fn new<P: AsRef<Path>>(
        dest_dir: P,
        file_name: &str,
        expected_size: u64,
        chunk_size: u32,
    ) -> io::Result<Self> {
        let dir = dest_dir.as_ref();
        if !dir.exists() {
            fs::create_dir_all(dir)?;
        }

        // Sanitize file name to avoid directory traversal
        let clean_name = Path::new(file_name)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("received_file.bin");

        let destination_path = dir.join(clean_name);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&destination_path)?;

        // Preallocate or set initial file length
        if expected_size > 0 {
            file.set_len(expected_size)?;
        }

        Ok(Self {
            destination_path,
            expected_size,
            chunk_size,
            file,
        })
    }

    /// Write chunk at given index to disk
    pub fn write_chunk(&mut self, chunk_index: u32, data: &[u8]) -> io::Result<()> {
        let offset = (chunk_index as u64) * (self.chunk_size as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }

    /// Flush to disk, truncate to expected size, and compute SHA-256 to verify integrity
    pub fn finalize_and_verify(&mut self, expected_hash: &[u8; 32]) -> Result<(), String> {
        self.file
            .flush()
            .map_err(|e| format!("Flush error: {}", e))?;
        self.file
            .set_len(self.expected_size)
            .map_err(|e| format!("Truncate error: {}", e))?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| format!("Seek error: {}", e))?;

        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        let mut total_read = 0u64;

        loop {
            let n = self
                .file
                .read(&mut buf)
                .map_err(|e| format!("Read error during verification: {}", e))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            total_read += n as u64;
        }

        if total_read != self.expected_size {
            return Err(format!(
                "File size mismatch: received {} bytes, expected {}",
                total_read, self.expected_size
            ));
        }

        let actual_hash: [u8; 32] = hasher.finalize().into();
        if &actual_hash != expected_hash {
            return Err(format!(
                "File hash mismatch! Expected: {:02x?}, Received: {:02x?}",
                &expected_hash[0..8],
                &actual_hash[0..8]
            ));
        }

        Ok(())
    }
}
