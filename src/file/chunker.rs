use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub struct FileChunker {
    pub path: PathBuf,
    pub file_name: String,
    pub file_size: u64,
    pub chunk_size: u32,
    pub total_chunks: u32,
    pub sha256_hash: [u8; 32],
    file: File,
}

impl FileChunker {
    pub fn open<P: AsRef<Path>>(path: P, chunk_size: u32) -> io::Result<Self> {
        let path_ref = path.as_ref();
        let mut file = File::open(path_ref)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();

        let file_name = path_ref
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("transfer.bin")
            .to_string();

        let chunk_size = chunk_size.max(256);
        let total_chunks = if file_size == 0 {
            1
        } else {
            ((file_size + (chunk_size as u64) - 1) / (chunk_size as u64)) as u32
        };

        // Compute SHA-256 of entire file
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let sha256_hash: [u8; 32] = hasher.finalize().into();

        // Rewind file
        file.seek(SeekFrom::Start(0))?;

        Ok(Self {
            path: path_ref.to_path_buf(),
            file_name,
            file_size,
            chunk_size,
            total_chunks,
            sha256_hash,
            file,
        })
    }

    /// Read chunk at specific index
    pub fn read_chunk(&mut self, chunk_index: u32) -> io::Result<Vec<u8>> {
        if chunk_index >= self.total_chunks {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Chunk index {} out of range (total: {})", chunk_index, self.total_chunks),
            ));
        }

        if self.file_size == 0 {
            return Ok(Vec::new());
        }

        let offset = (chunk_index as u64) * (self.chunk_size as u64);
        self.file.seek(SeekFrom::Start(offset))?;

        let bytes_to_read = if offset + (self.chunk_size as u64) > self.file_size {
            (self.file_size - offset) as usize
        } else {
            self.chunk_size as usize
        };

        let mut buf = vec![0u8; bytes_to_read];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }
}
