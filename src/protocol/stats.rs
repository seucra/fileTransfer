use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Default number of visible chunks in a single matrix snapshot (bounded within 64..=96).
pub const DEFAULT_VISIBLE_CHUNK_LIMIT: usize = 80;

/// Maximum number of protocol telemetry events carried in a single snapshot tick.
pub const MAX_EVENTS_PER_SNAPSHOT: usize = 48;

/// Explicit packet/chunk state with well-defined precedence:
/// `Acked (4) > Sacked (3) > Retransmitting (2) > InFlight (1) > Pending (0)`
///
/// Transition rules (Sender):
/// - `Pending` -> `InFlight`: when `on_packet_sent` transmits a chunk for the first time (`seq < next_seq`).
/// - `InFlight` -> `Retransmitting`: when the chunk's RTO timer expires and a retransmission is sent.
/// - `InFlight` | `Retransmitting` -> `Sacked`: when an ACK arrives with the SACK bit set for `seq > base`.
/// - `InFlight` | `Retransmitting` | `Sacked` -> `Acked`: when cumulative `ack_num` advances past `seq` (`seq < base`).
///
/// Transition rules (Receiver):
/// - `Pending` -> `InFlight`: when a higher-numbered chunk arrives, making `seq` an unreceived gap (`base <= seq < highest_observed_seq`).
/// - `Pending` | `InFlight` -> `Sacked`: when `seq > base` arrives out-of-order and is written to disk.
/// - `Pending` | `InFlight` | `Sacked` -> `Acked`: when `seq == base` arrives and advances cumulative `base` across any contiguous buffered chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    Pending,
    InFlight,
    Retransmitting,
    Sacked,
    Acked,
}

impl ChunkState {
    /// Numeric state precedence (`Acked` highest, `Pending` lowest).
    pub fn precedence(self) -> u8 {
        match self {
            ChunkState::Pending => 0,
            ChunkState::InFlight => 1,
            ChunkState::Retransmitting => 2,
            ChunkState::Sacked => 3,
            ChunkState::Acked => 4,
        }
    }

    /// Human-readable state label.
    pub fn label(self) -> &'static str {
        match self {
            ChunkState::Pending => "Pending",
            ChunkState::InFlight => "In-Flight",
            ChunkState::Retransmitting => "Retransmitting",
            ChunkState::Sacked => "SACKed (Out-of-Order)",
            ChunkState::Acked => "ACKed (Cumulative)",
        }
    }

    /// Short non-color glyph prefix for matrix cells.
    pub fn symbol(self) -> &'static str {
        match self {
            ChunkState::Pending => "·",
            ChunkState::InFlight => "→",
            ChunkState::Retransmitting => "↻",
            ChunkState::Sacked => "S",
            ChunkState::Acked => "✓",
        }
    }
}

/// Distinguishes cumulative acknowledgment from selective acknowledgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckKind {
    None,
    /// Selectively acknowledged via 64-bit SACK bitmask (`seq > base`).
    Selective,
    /// Cumulatively acknowledged directly via `ack_num` (`seq < base`).
    Cumulative,
    /// First selectively acknowledged out-of-order, then cumulatively acknowledged when `base` advanced.
    SelectiveThenCumulative,
}

impl AckKind {
    pub fn label(self) -> &'static str {
        match self {
            AckKind::None => "Unacknowledged",
            AckKind::Selective => "Selective ACK (SACK bitmask)",
            AckKind::Cumulative => "Cumulative ACK (in-order base)",
            AckKind::SelectiveThenCumulative => "SACKed out-of-order, then Cumulative ACK",
        }
    }
}

/// Authoritative reason for a retransmission event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetransmitReason {
    /// Sender RTO timer expired before chunk was acknowledged.
    RtoTimeout,
    /// Receiver observed `FLAG_RETRANSMISSION` (`0x01`) in incoming packet header.
    WireRetransmitFlag,
}

impl RetransmitReason {
    pub fn label(self) -> &'static str {
        match self {
            RetransmitReason::RtoTimeout => "RTO timeout expired",
            RetransmitReason::WireRetransmitFlag => "Observed FLAG_RETRANSMISSION on wire",
        }
    }
}

/// Identifies whether the snapshot originates from the Sender window or Receiver buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowPerspective {
    Sender,
    Receiver,
}

/// Per-chunk observability snapshot derived from authoritative window state.
/// Tracks transient `state` separately from persistent retransmission history (`retransmit_count`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSnapshot {
    pub seq_num: u32,
    pub state: ChunkState,
    pub retransmit_count: u32,
    pub had_retransmission: bool,
    pub last_retransmit_reason: Option<RetransmitReason>,
    pub ack_kind: AckKind,
    /// Elapsed time since transfer start when chunk was first sent (Sender) or received (Receiver).
    pub first_event_elapsed: Option<Duration>,
    /// Elapsed time since transfer start when chunk was most recently sent (Sender only).
    pub last_sent_elapsed: Option<Duration>,
    /// Elapsed time since transfer start when chunk was acknowledged (Sender) or written (Receiver).
    pub acked_elapsed: Option<Duration>,
}

/// Compute exact `(byte_offset, byte_length)` for `seq_num`, handling the final partial chunk and 0-byte files.
pub fn chunk_byte_range(seq_num: u32, chunk_size: u32, total_bytes: u64) -> (u64, u64) {
    if total_bytes == 0 || chunk_size == 0 {
        return (0, 0);
    }
    let offset = (seq_num as u64).saturating_mul(chunk_size as u64);
    if offset >= total_bytes {
        return (total_bytes, 0);
    }
    let remaining = total_bytes - offset;
    let length = remaining.min(chunk_size as u64);
    (offset, length)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolEventKind {
    Handshake,
    DataSent,
    AckReceived,
    SackUpdate,
    TimeoutDetected,
    Retransmission,
    WindowAdvanced,
    Verification,
    Completed,
    Cancelled,
}

impl ProtocolEventKind {
    pub fn tag(self) -> &'static str {
        match self {
            ProtocolEventKind::Handshake => "HANDSHAKE",
            ProtocolEventKind::DataSent => "DATA_TX",
            ProtocolEventKind::AckReceived => "ACK_RX",
            ProtocolEventKind::SackUpdate => "SACK",
            ProtocolEventKind::TimeoutDetected => "TIMEOUT",
            ProtocolEventKind::Retransmission => "RETRANS",
            ProtocolEventKind::WindowAdvanced => "WIN_SLIDE",
            ProtocolEventKind::Verification => "VERIFY",
            ProtocolEventKind::Completed => "COMPLETE",
            ProtocolEventKind::Cancelled => "CANCEL",
        }
    }
}

/// Structured, timestamped protocol event recorded from real worker operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolTelemetryEvent {
    pub elapsed: Duration,
    pub kind: ProtocolEventKind,
    pub seq_num: Option<u32>,
    pub ack_num: Option<u32>,
    pub sack_bitmask: Option<u64>,
    pub message: String,
}

/// Bounded collector used by sender/receiver workers to batch telemetry events without unbounded growth.
#[derive(Debug, Clone)]
pub struct BoundedEventCollector {
    capacity: usize,
    events: VecDeque<ProtocolTelemetryEvent>,
    pub dropped_high_rate_events: u64,
}

impl BoundedEventCollector {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(8);
        Self {
            capacity: cap,
            events: VecDeque::with_capacity(cap),
            dropped_high_rate_events: 0,
        }
    }

    pub fn push(&mut self, event: ProtocolTelemetryEvent) {
        if self.events.len() >= self.capacity {
            // Prefer dropping routine high-rate DataSent/AckReceived entries before critical events
            if let Some(pos) = self.events.iter().position(|e| {
                matches!(
                    e.kind,
                    ProtocolEventKind::DataSent | ProtocolEventKind::AckReceived
                )
            }) {
                self.events.remove(pos);
                self.dropped_high_rate_events += 1;
            } else {
                self.events.pop_front();
                self.dropped_high_rate_events += 1;
            }
        }
        self.events.push_back(event);
    }

    pub fn drain_to_vec(&mut self) -> Vec<ProtocolTelemetryEvent> {
        self.events.drain(..).collect()
    }
}

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

    // Extended protocol observability fields
    pub chunk_size: u32,
    pub perspective: WindowPerspective,
    pub window_base: u32,
    pub next_seq: u32,
    pub window_size: usize,
    pub in_flight_count: usize,
    pub sacked_count: usize,
    pub packets_sent: u64,
    pub ack_packets_processed: u64,
    pub current_rto: Option<Duration>,
    pub last_sack_bitmask: u64,
    pub configured_loss_percent: u8,
    pub visible_chunk_start: u32,
    pub visible_chunks: Vec<ChunkSnapshot>,
    pub recent_events: Vec<ProtocolTelemetryEvent>,
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
            chunk_size: 1400,
            perspective: WindowPerspective::Sender,
            window_base: 0,
            next_seq: 0,
            window_size: 32,
            in_flight_count: 0,
            sacked_count: 0,
            packets_sent: 0,
            ack_packets_processed: 0,
            current_rto: None,
            last_sack_bitmask: 0,
            configured_loss_percent: 0,
            visible_chunk_start: 0,
            visible_chunks: Vec::new(),
            recent_events: Vec::new(),
        }
    }

    pub fn record_chunk(&mut self, chunk_bytes: usize, now: Instant) {
        self.chunks_transferred = (self.chunks_transferred + 1).min(self.total_chunks);
        self.bytes_transferred =
            (self.bytes_transferred + chunk_bytes as u64).min(self.total_bytes);
        self.bytes_since_last_sample += chunk_bytes as u64;

        let elapsed_since_sample = now.saturating_duration_since(self.last_update_time);
        if elapsed_since_sample >= Duration::from_millis(200) {
            let instant_speed =
                (self.bytes_since_last_sample as f64) / elapsed_since_sample.as_secs_f64();
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

    pub fn update_rto(&mut self, rto: Duration) {
        self.current_rto = Some(rto);
    }

    pub fn progress_ratio(&self) -> f32 {
        if self.total_bytes == 0 {
            if self.total_chunks == 0 {
                1.0
            } else {
                (self.chunks_transferred as f32 / self.total_chunks as f32).clamp(0.0, 1.0)
            }
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
        format!(
            "{}/{}",
            format_file_size(self.bytes_transferred),
            format_file_size(self.total_bytes)
        )
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
