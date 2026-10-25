use crate::protocol::stats::{
    AckKind, ChunkSnapshot, ChunkState, RetransmitReason, DEFAULT_VISIBLE_CHUNK_LIMIT,
};
use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

/// Maximum number of completed chunk metadata records retained in window history.
pub const MAX_WINDOW_HISTORY: usize = 192;

#[derive(Debug, Clone)]
pub struct InFlightChunk {
    pub seq_num: u32,
    pub payload: Vec<u8>,
    pub first_sent_at: Instant,
    pub sent_at: Instant,
    pub retransmit_count: u32,
    pub acked: bool,
    pub acked_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct CompletedChunkMeta {
    pub seq_num: u32,
    pub retransmit_count: u32,
    pub was_sacked: bool,
    pub first_sent_at: Instant,
    pub last_sent_at: Instant,
    pub acked_at: Instant,
}

#[derive(Debug)]
pub struct SenderWindow {
    pub window_size: usize,
    pub total_chunks: u32,
    pub base: u32,
    pub next_seq: u32,
    pub in_flight: BTreeMap<u32, InFlightChunk>,
    pub completed_history: BTreeMap<u32, CompletedChunkMeta>,
}

impl SenderWindow {
    pub fn new(window_size: usize, total_chunks: u32) -> Self {
        Self {
            window_size: window_size.max(1),
            total_chunks,
            base: 0,
            next_seq: 0,
            in_flight: BTreeMap::new(),
            completed_history: BTreeMap::new(),
        }
    }

    /// Can the sender send another new packet within the current sliding window?
    pub fn can_send(&self) -> bool {
        self.next_seq < self.total_chunks && (self.next_seq - self.base) < self.window_size as u32
    }

    /// Record that chunk `seq_num` was transmitted
    pub fn on_packet_sent(&mut self, seq_num: u32, payload: Vec<u8>, now: Instant) {
        if let Some(entry) = self.in_flight.get_mut(&seq_num) {
            entry.sent_at = now;
            entry.retransmit_count += 1;
        } else {
            self.in_flight.insert(
                seq_num,
                InFlightChunk {
                    seq_num,
                    payload,
                    first_sent_at: now,
                    sent_at: now,
                    retransmit_count: 0,
                    acked: false,
                    acked_at: None,
                },
            );
            if seq_num >= self.next_seq {
                self.next_seq = seq_num + 1;
            }
        }
    }

    fn record_completed(&mut self, entry: InFlightChunk, now: Instant) {
        if self.completed_history.len() >= MAX_WINDOW_HISTORY {
            if let Some(&oldest_key) = self.completed_history.keys().next() {
                self.completed_history.remove(&oldest_key);
            }
        }
        self.completed_history.insert(
            entry.seq_num,
            CompletedChunkMeta {
                seq_num: entry.seq_num,
                retransmit_count: entry.retransmit_count,
                was_sacked: entry.acked,
                first_sent_at: entry.first_sent_at,
                last_sent_at: entry.sent_at,
                acked_at: entry.acked_at.unwrap_or(now),
            },
        );
    }

    /// Process incoming ACK with cumulative ack_num and 64-bit SACK bitmask.
    /// Returns (newly_acknowledged_count, optional_rtt_sample).
    pub fn on_ack_received(
        &mut self,
        ack_num: u32,
        sack_bitmask: u64,
        now: Instant,
    ) -> (usize, Option<Duration>) {
        let mut newly_acked = 0;
        let mut rtt_sample = None;

        // Cumulative ACK: all chunks strictly less than ack_num are acknowledged
        let unacked_before_base: Vec<u32> =
            self.in_flight.range(..ack_num).map(|(&k, _)| k).collect();

        for seq in unacked_before_base {
            if let Some(entry) = self.in_flight.remove(&seq) {
                if !entry.acked {
                    newly_acked += 1;
                    // Only use RTT sample if chunk was not retransmitted (Karn's algorithm)
                    if entry.retransmit_count == 0 && rtt_sample.is_none() {
                        rtt_sample = Some(now.saturating_duration_since(entry.sent_at));
                    }
                }
                self.record_completed(entry, now);
            }
        }

        // Advance base to ack_num if greater
        if ack_num > self.base {
            self.base = ack_num;
        }

        // Selective ACK (SACK): bit i corresponds to chunk (ack_num + 1 + i)
        for i in 0..64 {
            if (sack_bitmask & (1u64 << i)) != 0 {
                let sack_seq = ack_num + 1 + (i as u32);
                if let Some(entry) = self.in_flight.get_mut(&sack_seq) {
                    if !entry.acked {
                        entry.acked = true;
                        entry.acked_at = Some(now);
                        newly_acked += 1;
                        if entry.retransmit_count == 0 && rtt_sample.is_none() {
                            rtt_sample = Some(now.saturating_duration_since(entry.sent_at));
                        }
                    }
                }
            }
        }

        // Clean up any contiguous acked chunks at base
        while let Some(first_key) = self.in_flight.keys().next().copied() {
            if first_key == self.base && self.in_flight.get(&first_key).is_some_and(|e| e.acked) {
                if let Some(entry) = self.in_flight.remove(&first_key) {
                    self.record_completed(entry, now);
                }
                self.base += 1;
            } else {
                break;
            }
        }

        (newly_acked, rtt_sample)
    }

    /// Get list of chunks that have timed out and require retransmission.
    pub fn get_timed_out_chunks(&self, now: Instant, rto: Duration) -> Vec<u32> {
        let mut timed_out = Vec::new();
        for (&seq, entry) in &self.in_flight {
            if !entry.acked && now.saturating_duration_since(entry.sent_at) >= rto {
                timed_out.push(seq);
            }
        }
        timed_out
    }

    /// Check if all chunks have been acknowledged.
    pub fn is_complete(&self) -> bool {
        self.base >= self.total_chunks && self.in_flight.is_empty()
    }

    /// Number of unacknowledged packets currently in flight.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.values().filter(|e| !e.acked).count()
    }

    /// Number of selectively acknowledged packets currently above `base`.
    pub fn sacked_count(&self) -> usize {
        self.in_flight.values().filter(|e| e.acked).count()
    }

    /// Derive authoritative snapshot for a single chunk sequence number.
    pub fn snapshot_for_chunk(&self, seq: u32, start_time: Instant) -> ChunkSnapshot {
        if seq < self.base {
            if let Some(meta) = self.completed_history.get(&seq) {
                let ack_kind = if meta.was_sacked {
                    AckKind::SelectiveThenCumulative
                } else {
                    AckKind::Cumulative
                };
                let had_retrans = meta.retransmit_count > 0;
                ChunkSnapshot {
                    seq_num: seq,
                    state: ChunkState::Acked,
                    retransmit_count: meta.retransmit_count,
                    had_retransmission: had_retrans,
                    last_retransmit_reason: if had_retrans {
                        Some(RetransmitReason::RtoTimeout)
                    } else {
                        None
                    },
                    ack_kind,
                    first_event_elapsed: Some(
                        meta.first_sent_at.saturating_duration_since(start_time),
                    ),
                    last_sent_elapsed: Some(
                        meta.last_sent_at.saturating_duration_since(start_time),
                    ),
                    acked_elapsed: Some(meta.acked_at.saturating_duration_since(start_time)),
                }
            } else {
                ChunkSnapshot {
                    seq_num: seq,
                    state: ChunkState::Acked,
                    retransmit_count: 0,
                    had_retransmission: false,
                    last_retransmit_reason: None,
                    ack_kind: AckKind::Cumulative,
                    first_event_elapsed: None,
                    last_sent_elapsed: None,
                    acked_elapsed: None,
                }
            }
        } else if let Some(entry) = self.in_flight.get(&seq) {
            let had_retrans = entry.retransmit_count > 0;
            let state = if entry.acked {
                ChunkState::Sacked
            } else if had_retrans {
                ChunkState::Retransmitting
            } else {
                ChunkState::InFlight
            };
            let ack_kind = if entry.acked {
                AckKind::Selective
            } else {
                AckKind::None
            };
            ChunkSnapshot {
                seq_num: seq,
                state,
                retransmit_count: entry.retransmit_count,
                had_retransmission: had_retrans,
                last_retransmit_reason: if had_retrans {
                    Some(RetransmitReason::RtoTimeout)
                } else {
                    None
                },
                ack_kind,
                first_event_elapsed: Some(
                    entry.first_sent_at.saturating_duration_since(start_time),
                ),
                last_sent_elapsed: Some(entry.sent_at.saturating_duration_since(start_time)),
                acked_elapsed: entry
                    .acked_at
                    .map(|t| t.saturating_duration_since(start_time)),
            }
        } else {
            ChunkSnapshot {
                seq_num: seq,
                state: ChunkState::Pending,
                retransmit_count: 0,
                had_retransmission: false,
                last_retransmit_reason: None,
                ack_kind: AckKind::None,
                first_event_elapsed: None,
                last_sent_elapsed: None,
                acked_elapsed: None,
            }
        }
    }

    /// Generate a bounded visible chunk snapshot centered around the active sliding window.
    /// Returns `(visible_chunk_start, visible_chunks)`.
    pub fn build_visible_snapshots(
        &self,
        start_time: Instant,
        max_visible: usize,
    ) -> (u32, Vec<ChunkSnapshot>) {
        let limit = max_visible.clamp(1, 96);
        let total = self.total_chunks as usize;
        let count = total.min(limit);
        if count == 0 {
            return (0, Vec::new());
        }

        let start_seq = if total <= limit {
            0
        } else {
            let lead_in = (limit / 5) as u32; // ~16 chunks of recently ACKed history
            let raw_start = self.base.saturating_sub(lead_in);
            let max_start = self.total_chunks.saturating_sub(count as u32);
            raw_start.min(max_start)
        };

        let mut snapshots = Vec::with_capacity(count);
        for offset in 0..(count as u32) {
            let seq = start_seq + offset;
            snapshots.push(self.snapshot_for_chunk(seq, start_time));
        }

        (start_seq, snapshots)
    }
}

#[derive(Debug, Clone)]
pub struct ReceiverChunkMeta {
    pub received_at: Instant,
    pub arrived_out_of_order: bool,
    pub wire_retransmit_flag: bool,
    pub duplicate_count: u32,
}

#[derive(Debug)]
pub struct ReceiverWindow {
    pub total_chunks: u32,
    pub base: u32,
    pub out_of_order: HashSet<u32>,
    pub highest_observed_seq: Option<u32>,
    pub recent_meta: BTreeMap<u32, ReceiverChunkMeta>,
}

impl ReceiverWindow {
    pub fn new(total_chunks: u32) -> Self {
        Self {
            total_chunks,
            base: 0,
            out_of_order: HashSet::new(),
            highest_observed_seq: None,
            recent_meta: BTreeMap::new(),
        }
    }

    /// Process a received chunk sequence number.
    /// Returns true if this is a newly received chunk (needs to be written to disk),
    /// or false if it was already received or out of range.
    pub fn on_chunk_received(&mut self, seq_num: u32) -> bool {
        self.on_chunk_received_with_meta(seq_num, false, Instant::now())
    }

    /// Process a received chunk along with wire-level metadata (`FLAG_RETRANSMISSION` and arrival timestamp).
    pub fn on_chunk_received_with_meta(
        &mut self,
        seq_num: u32,
        is_retransmission: bool,
        now: Instant,
    ) -> bool {
        if seq_num >= self.total_chunks {
            return false;
        }

        self.highest_observed_seq = Some(
            self.highest_observed_seq
                .map_or(seq_num, |prev| prev.max(seq_num)),
        );

        if seq_num < self.base {
            // Duplicate of an already cumulatively acknowledged chunk
            if let Some(meta) = self.recent_meta.get_mut(&seq_num) {
                meta.duplicate_count += 1;
                if is_retransmission {
                    meta.wire_retransmit_flag = true;
                }
            }
            return false;
        }

        if seq_num == self.base {
            // In-order chunk received: record meta and advance base
            self.insert_meta(seq_num, false, is_retransmission, now);
            self.base += 1;
            // Advance base across any previously received out-of-order chunks
            while self.out_of_order.remove(&self.base) {
                self.base += 1;
            }
            true
        } else {
            // Out-of-order chunk received
            if self.out_of_order.insert(seq_num) {
                self.insert_meta(seq_num, true, is_retransmission, now);
                true
            } else {
                if let Some(meta) = self.recent_meta.get_mut(&seq_num) {
                    meta.duplicate_count += 1;
                    if is_retransmission {
                        meta.wire_retransmit_flag = true;
                    }
                }
                false // Already buffered out of order
            }
        }
    }

    fn insert_meta(
        &mut self,
        seq_num: u32,
        arrived_out_of_order: bool,
        wire_retransmit_flag: bool,
        now: Instant,
    ) {
        if self.recent_meta.len() >= MAX_WINDOW_HISTORY {
            if let Some(&oldest_key) = self.recent_meta.keys().next() {
                if oldest_key < self.base {
                    self.recent_meta.remove(&oldest_key);
                }
            }
        }
        self.recent_meta.insert(
            seq_num,
            ReceiverChunkMeta {
                received_at: now,
                arrived_out_of_order,
                wire_retransmit_flag,
                duplicate_count: 0,
            },
        );
    }

    /// Generate an ACK packet payload: (ack_num, sack_bitmask).
    /// `ack_num` is the lowest missing chunk sequence number (`base`).
    /// `sack_bitmask` marks up to 64 chunks after `base`: bit i = 1 if chunk (base + 1 + i) received.
    pub fn create_ack_state(&self) -> (u32, u64) {
        let ack_num = self.base;
        let mut sack_bitmask: u64 = 0;

        for i in 0..64 {
            let check_seq = ack_num + 1 + (i as u32);
            if self.out_of_order.contains(&check_seq) {
                sack_bitmask |= 1u64 << i;
            }
        }

        (ack_num, sack_bitmask)
    }

    /// Check if all chunks from 0 to total_chunks - 1 have been received.
    pub fn is_complete(&self) -> bool {
        self.base >= self.total_chunks && self.out_of_order.is_empty()
    }

    /// Number of unique chunks received so far.
    pub fn received_count(&self) -> u32 {
        self.base + (self.out_of_order.len() as u32)
    }

    /// Derive authoritative receiver-perspective snapshot for a single chunk sequence number.
    pub fn snapshot_for_chunk(&self, seq: u32, start_time: Instant) -> ChunkSnapshot {
        let meta = self.recent_meta.get(&seq);
        let wire_retrans = meta.is_some_and(|m| m.wire_retransmit_flag);
        let dup_count = meta.map_or(0, |m| m.duplicate_count);
        let retrans_count = u32::from(wire_retrans) + dup_count;
        let recv_elapsed = meta.map(|m| m.received_at.saturating_duration_since(start_time));

        if seq < self.base {
            let ack_kind = if meta.is_some_and(|m| m.arrived_out_of_order) {
                AckKind::SelectiveThenCumulative
            } else {
                AckKind::Cumulative
            };
            ChunkSnapshot {
                seq_num: seq,
                state: ChunkState::Acked,
                retransmit_count: retrans_count,
                had_retransmission: wire_retrans || dup_count > 0,
                last_retransmit_reason: if wire_retrans {
                    Some(RetransmitReason::WireRetransmitFlag)
                } else {
                    None
                },
                ack_kind,
                first_event_elapsed: recv_elapsed,
                last_sent_elapsed: None,
                acked_elapsed: recv_elapsed,
            }
        } else if self.out_of_order.contains(&seq) {
            ChunkSnapshot {
                seq_num: seq,
                state: ChunkState::Sacked,
                retransmit_count: retrans_count,
                had_retransmission: wire_retrans || dup_count > 0,
                last_retransmit_reason: if wire_retrans {
                    Some(RetransmitReason::WireRetransmitFlag)
                } else {
                    None
                },
                ack_kind: AckKind::Selective,
                first_event_elapsed: recv_elapsed,
                last_sent_elapsed: None,
                acked_elapsed: recv_elapsed,
            }
        } else if self.highest_observed_seq.is_some_and(|hi| seq < hi) {
            // Unreceived gap below highest observed sequence number: awaiting arrival (never assumed physically lost)
            ChunkSnapshot {
                seq_num: seq,
                state: ChunkState::InFlight,
                retransmit_count: 0,
                had_retransmission: false,
                last_retransmit_reason: None,
                ack_kind: AckKind::None,
                first_event_elapsed: None,
                last_sent_elapsed: None,
                acked_elapsed: None,
            }
        } else {
            ChunkSnapshot {
                seq_num: seq,
                state: ChunkState::Pending,
                retransmit_count: 0,
                had_retransmission: false,
                last_retransmit_reason: None,
                ack_kind: AckKind::None,
                first_event_elapsed: None,
                last_sent_elapsed: None,
                acked_elapsed: None,
            }
        }
    }

    /// Generate a bounded visible chunk snapshot around the receiver's `base`.
    pub fn build_visible_snapshots(
        &self,
        start_time: Instant,
        max_visible: usize,
    ) -> (u32, Vec<ChunkSnapshot>) {
        let limit = max_visible.clamp(1, DEFAULT_VISIBLE_CHUNK_LIMIT.max(96));
        let total = self.total_chunks as usize;
        let count = total.min(limit);
        if count == 0 {
            return (0, Vec::new());
        }

        let start_seq = if total <= limit {
            0
        } else {
            let lead_in = (limit / 5) as u32;
            let raw_start = self.base.saturating_sub(lead_in);
            let max_start = self.total_chunks.saturating_sub(count as u32);
            raw_start.min(max_start)
        };

        let mut snapshots = Vec::with_capacity(count);
        for offset in 0..(count as u32) {
            let seq = start_seq + offset;
            snapshots.push(self.snapshot_for_chunk(seq, start_time));
        }

        (start_seq, snapshots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sender_window_flow() {
        let mut win = SenderWindow::new(4, 10);
        let now = Instant::now();

        assert!(win.can_send());
        win.on_packet_sent(0, vec![1], now);
        win.on_packet_sent(1, vec![2], now);
        win.on_packet_sent(2, vec![3], now);
        win.on_packet_sent(3, vec![4], now);

        // Window of 4 is full (base 0, next_seq 4)
        assert!(!win.can_send());

        // ACK arrives for cumulative 2 (chunks 0 and 1 acked)
        let (acked, _) = win.on_ack_received(2, 0, now);
        assert_eq!(acked, 2);
        assert_eq!(win.base, 2);
        assert!(win.can_send()); // Now can send chunk 4 and 5
    }

    #[test]
    fn test_receiver_window_in_order() {
        let mut r = ReceiverWindow::new(5);
        assert!(r.on_chunk_received(0));
        assert_eq!(r.base, 1);
        assert!(r.on_chunk_received(1));
        assert_eq!(r.base, 2);
        assert!(!r.on_chunk_received(0)); // Duplicate
        assert_eq!(r.base, 2);
    }

    #[test]
    fn test_receiver_window_out_of_order_sack() {
        let mut r = ReceiverWindow::new(10);
        // Chunk 0 arrives
        r.on_chunk_received(0);
        assert_eq!(r.base, 1);

        // Chunk 2 arrives (chunk 1 missing!)
        r.on_chunk_received(2);
        assert_eq!(r.base, 1);
        assert!(r.out_of_order.contains(&2));

        // Generate ACK state: base=1, bit 0 corresponds to base+1=2
        let (ack_num, sack) = r.create_ack_state();
        assert_eq!(ack_num, 1);
        assert_eq!(sack & 1, 1); // Bit 0 is set for chunk 2

        // Now missing chunk 1 arrives
        r.on_chunk_received(1);
        // Base should jump past 1 and 2 to 3!
        assert_eq!(r.base, 3);
        assert!(r.out_of_order.is_empty());
    }
}
