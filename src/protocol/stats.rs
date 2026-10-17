use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TransferStats {
    pub bytes_transferred: u64,
    pub total_bytes: u64,
    pub chunks_transferred: u32,
    pub total_chunks: u32,
    pub retransmissions: u64,
    pub corrupt_packets_dropped: u64,
    pub duplicate_packets_dropped: u64,
    pub start_time: Instant,
    pub last_update_time: Instant,
    pub bytes_since_last_sample: u64,
    pub speed_bytes_per_sec: f64,
    pub current_rtt: Option<Duration>,
}

impl TransferStats {
    pub fn new(total_bytes: u64, total_chunks: u32) -> Self {
        let now = Instant::now();
        Self {
            bytes_transferred: 0,
            total_bytes,
            chunks_transferred: 0,
            total_chunks,
            retransmissions: 0,
            corrupt_packets_dropped: 0,
            duplicate_packets_dropped: 0,
            start_time: now,
            last_update_time: now,
            bytes_since_last_sample: 0,
            speed_bytes_per_sec: 0.0,
            current_rtt: None,
        }
    }

    pub fn record_chunk(&mut self, chunk_bytes: usize, now: Instant) {
        self.chunks_transferred = (self.chunks_transferred + 1).min(self.total_chunks);
        self.bytes_transferred = (self.bytes_transferred + chunk_bytes as u64).min(self.total_bytes);
        self.bytes_since_last_sample += chunk_bytes as u64;

        let elapsed_since_sample = now.saturating_duration_since(self.last_update_time);
        if elapsed_since_sample >= Duration::from_millis(200) {
            let instant_speed = (self.bytes_since_last_sample as f64) / elapsed_since_sample.as_secs_f64();
            // Exponential moving average for smooth display
            if self.speed_bytes_per_sec == 0.0 {
                self.speed_bytes_per_sec = instant_speed;
            } else {
                self.speed_bytes_per_sec = self.speed_bytes_per_sec * 0.7 + instant_speed * 0.3;
            }
            self.bytes_since_last_sample = 0;
            self.last_update_time = now;
        }
    }

    pub fn record_retransmission(&mut self) {
        self.retransmissions += 1;
    }

    pub fn record_corrupt_packet(&mut self) {
        self.corrupt_packets_dropped += 1;
    }

    pub fn record_duplicate_packet(&mut self) {
        self.duplicate_packets_dropped += 1;
    }

    pub fn update_rtt(&mut self, rtt: Duration) {
        self.current_rtt = Some(rtt);
    }

    pub fn progress_ratio(&self) -> f32 {
        if self.total_bytes == 0 {
            if self.total_chunks == 0 { 1.0 } else { (self.chunks_transferred as f32 / self.total_chunks as f32).clamp(0.0, 1.0) }
        } else {
            (self.bytes_transferred as f64 / self.total_bytes as f64).clamp(0.0, 1.0) as f32
        }
    }

    pub fn progress_percent(&self) -> f32 {
        self.progress_ratio() * 100.0
    }

    pub fn elapsed_duration(&self) -> Duration {
        Instant::now().saturating_duration_since(self.start_time)
    }

    pub fn estimated_remaining(&self) -> Option<Duration> {
        if self.speed_bytes_per_sec <= 0.0 || self.bytes_transferred >= self.total_bytes {
            return None;
        }
        let remaining_bytes = self.total_bytes.saturating_sub(self.bytes_transferred);
        let secs = (remaining_bytes as f64) / self.speed_bytes_per_sec;
        Some(Duration::from_secs_f64(secs))
    }

    pub fn formatted_speed(&self) -> String {
        let speed = self.speed_bytes_per_sec;
        if speed >= 1024.0 * 1024.0 {
            format!("{:.2} MB/s", speed / (1024.0 * 1024.0))
        } else if speed >= 1024.0 {
            format!("{:.1} KB/s", speed / 1024.0)
        } else {
            format!("{:.0} B/s", speed)
        }
    }

    pub fn formatted_bytes(&self) -> String {
        format!("{}/{}", format_file_size(self.bytes_transferred), format_file_size(self.total_bytes))
    }
}

pub fn format_file_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}
