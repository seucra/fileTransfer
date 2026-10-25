pub mod benchmark;
pub mod file;
pub mod network;
pub mod protocol;
pub mod ui;

pub use benchmark::{run_full_comparison, BenchmarkResult};
pub use file::{FileChunker, FileWriter};
pub use network::{run_receiver, run_sender, ReceiverConfig, SenderConfig, TransferEvent};
pub use protocol::{
    chunk_byte_range, AckKind, BoundedEventCollector, ChunkSnapshot, ChunkState, Packet,
    PacketType, ProtocolEventKind, ProtocolTelemetryEvent, ReceiverWindow, RetransmitReason,
    SenderWindow, TransferStats, WindowPerspective, DEFAULT_VISIBLE_CHUNK_LIMIT,
    MAX_EVENTS_PER_SNAPSHOT,
};
pub use ui::TransferApp;
