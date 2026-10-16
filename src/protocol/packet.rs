use crc32fast::Hasher;
use std::fmt;

pub const PROTOCOL_MAGIC: [u8; 4] = [b'R', b'F', b'T', b'P'];
pub const PROTOCOL_VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 30;

// Flag bitmasks
pub const FLAG_RETRANSMISSION: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Start = 1,
    StartAck = 2,
    Data = 3,
    Ack = 4,
    Fin = 5,
    FinAck = 6,
    Cancel = 7,
    Heartbeat = 8,
}

impl PacketType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            1 => Some(PacketType::Start),
            2 => Some(PacketType::StartAck),
            3 => Some(PacketType::Data),
            4 => Some(PacketType::Ack),
            5 => Some(PacketType::Fin),
            6 => Some(PacketType::FinAck),
            7 => Some(PacketType::Cancel),
            8 => Some(PacketType::Heartbeat),
            _ => None,
        }
    }
}

impl fmt::Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketType::Start => write!(f, "START"),
            PacketType::StartAck => write!(f, "START_ACK"),
            PacketType::Data => write!(f, "DATA"),
            PacketType::Ack => write!(f, "ACK"),
            PacketType::Fin => write!(f, "FIN"),
            PacketType::FinAck => write!(f, "FIN_ACK"),
            PacketType::Cancel => write!(f, "CANCEL"),
            PacketType::Heartbeat => write!(f, "HEARTBEAT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub version: u8,
    pub packet_type: PacketType,
    pub flags: u8,
    pub seq_num: u32,
    pub ack_num: u32,
    pub sack_bitmask: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartPayload {
    pub file_size: u64,
    pub chunk_size: u32,
    pub total_chunks: u32,
    pub sha256_hash: [u8; 32],
    pub file_name: String,
}

impl StartPayload {
    pub fn to_bytes(&self) -> Vec<u8> {
        let name_bytes = self.file_name.as_bytes();
        let mut buf = Vec::with_capacity(8 + 4 + 4 + 32 + 2 + name_bytes.len());
        buf.extend_from_slice(&self.file_size.to_be_bytes());
        buf.extend_from_slice(&self.chunk_size.to_be_bytes());
        buf.extend_from_slice(&self.total_chunks.to_be_bytes());
        buf.extend_from_slice(&self.sha256_hash);
        let name_len = (name_bytes.len().min(u16::MAX as usize)) as u16;
        buf.extend_from_slice(&name_len.to_be_bytes());
        buf.extend_from_slice(&name_bytes[..name_len as usize]);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 + 4 + 4 + 32 + 2 {
            return Err("Start payload too short".to_string());
        }
        let file_size = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let chunk_size = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let total_chunks = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        let mut sha256_hash = [0u8; 32];
        sha256_hash.copy_from_slice(&bytes[16..48]);
        let name_len = u16::from_be_bytes(bytes[48..50].try_into().unwrap()) as usize;
        if bytes.len() < 50 + name_len {
            return Err("Start payload missing filename bytes".to_string());
        }
        let file_name = String::from_utf8_lossy(&bytes[50..50 + name_len]).to_string();

        Ok(Self {
            file_size,
            chunk_size,
            total_chunks,
            sha256_hash,
            file_name,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartAckPayload {
    pub accepted: bool,
    pub message: String,
}

impl StartAckPayload {
    pub fn to_bytes(&self) -> Vec<u8> {
        let msg_bytes = self.message.as_bytes();
        let mut buf = Vec::with_capacity(1 + 2 + msg_bytes.len());
        buf.push(if self.accepted { 1 } else { 0 });
        let len = (msg_bytes.len().min(u16::MAX as usize)) as u16;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&msg_bytes[..len as usize]);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 3 {
            return Err("StartAck payload too short".to_string());
        }
        let accepted = bytes[0] == 1;
        let len = u16::from_be_bytes(bytes[1..3].try_into().unwrap()) as usize;
        let message = if bytes.len() >= 3 + len {
            String::from_utf8_lossy(&bytes[3..3 + len]).to_string()
        } else {
            String::new()
        };
        Ok(Self { accepted, message })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinAckPayload {
    pub success: bool,
    pub message: String,
}

impl FinAckPayload {
    pub fn to_bytes(&self) -> Vec<u8> {
        let msg_bytes = self.message.as_bytes();
        let mut buf = Vec::with_capacity(1 + 2 + msg_bytes.len());
        buf.push(if self.success { 1 } else { 0 });
        let len = (msg_bytes.len().min(u16::MAX as usize)) as u16;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&msg_bytes[..len as usize]);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 3 {
            return Err("FinAck payload too short".to_string());
        }
        let success = bytes[0] == 1;
        let len = u16::from_be_bytes(bytes[1..3].try_into().unwrap()) as usize;
        let message = if bytes.len() >= 3 + len {
            String::from_utf8_lossy(&bytes[3..3 + len]).to_string()
        } else {
            String::new()
        };
        Ok(Self { success, message })
    }
}

impl Packet {
    pub fn new(packet_type: PacketType, seq_num: u32, ack_num: u32, sack_bitmask: u64, payload: Vec<u8>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            packet_type,
            flags: 0,
            seq_num,
            ack_num,
            sack_bitmask,
            payload,
        }
    }

    pub fn new_start(payload: StartPayload) -> Self {
        Self::new(PacketType::Start, 0, 0, 0, payload.to_bytes())
    }

    pub fn new_start_ack(accepted: bool, message: &str) -> Self {
        let payload = StartAckPayload {
            accepted,
            message: message.to_string(),
        };
        Self::new(PacketType::StartAck, 0, 0, 0, payload.to_bytes())
    }

    pub fn new_data(seq_num: u32, payload: Vec<u8>, is_retransmission: bool) -> Self {
        let flags = if is_retransmission { FLAG_RETRANSMISSION } else { 0 };
        Self {
            version: PROTOCOL_VERSION,
            packet_type: PacketType::Data,
            flags,
            seq_num,
            ack_num: 0,
            sack_bitmask: 0,
            payload,
        }
    }

    pub fn new_ack(ack_num: u32, sack_bitmask: u64) -> Self {
        Self::new(PacketType::Ack, 0, ack_num, sack_bitmask, Vec::new())
    }

    pub fn new_fin(sha256_hash: [u8; 32]) -> Self {
        Self::new(PacketType::Fin, 0, 0, 0, sha256_hash.to_vec())
    }

    pub fn new_fin_ack(success: bool, message: &str) -> Self {
        let payload = FinAckPayload {
            success,
            message: message.to_string(),
        };
        Self::new(PacketType::FinAck, 0, 0, 0, payload.to_bytes())
    }

    pub fn new_cancel(reason: &str) -> Self {
        Self::new(PacketType::Cancel, 0, 0, 0, reason.as_bytes().to_vec())
    }

    pub fn new_heartbeat() -> Self {
        Self::new(PacketType::Heartbeat, 0, 0, 0, Vec::new())
    }

    pub fn is_retransmission(&self) -> bool {
        (self.flags & FLAG_RETRANSMISSION) != 0
    }

    /// Serialize packet into binary bytes including CRC32 checksum.
    pub fn serialize(&self) -> Vec<u8> {
        let payload_len = self.payload.len().min(u16::MAX as usize) as u16;
        let total_size = HEADER_SIZE + payload_len as usize;
        let mut buf = vec![0u8; total_size];

        // 0..4 Magic
        buf[0..4].copy_from_slice(&PROTOCOL_MAGIC);
        // 4 Version
        buf[4] = self.version;
        // 5 Type
        buf[5] = self.packet_type as u8;
        // 6 Flags
        buf[6] = self.flags;
        // 7 Reserved
        buf[7] = 0;
        // 8..12 Seq Num
        buf[8..12].copy_from_slice(&self.seq_num.to_be_bytes());
        // 12..16 Ack Num
        buf[12..16].copy_from_slice(&self.ack_num.to_be_bytes());
        // 16..24 SACK bitmask
        buf[16..24].copy_from_slice(&self.sack_bitmask.to_be_bytes());
        // 24..26 Payload length
        buf[24..26].copy_from_slice(&payload_len.to_be_bytes());
        // 30.. Payload
        buf[HEADER_SIZE..total_size].copy_from_slice(&self.payload[..payload_len as usize]);

        // Calculate CRC32 over bytes 0..26 and payload bytes
        let mut hasher = Hasher::new();
        hasher.update(&buf[0..26]);
        hasher.update(&buf[HEADER_SIZE..total_size]);
        let crc = hasher.finalize();

        // 26..30 CRC32
        buf[26..30].copy_from_slice(&crc.to_be_bytes());

        buf
    }

    /// Deserialize binary bytes into Packet, validating magic, length, and CRC32 checksum.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < HEADER_SIZE {
            return Err(format!("Datagram too short: {} bytes < {}", bytes.len(), HEADER_SIZE));
        }

        // Verify magic
        if &bytes[0..4] != &PROTOCOL_MAGIC {
            return Err("Invalid protocol magic bytes".to_string());
        }

        let version = bytes[4];
        if version != PROTOCOL_VERSION {
            return Err(format!("Unsupported protocol version: {}", version));
        }

        let packet_type = PacketType::from_u8(bytes[5])
            .ok_or_else(|| format!("Unknown packet type: {}", bytes[5]))?;

        let flags = bytes[6];
        let seq_num = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let ack_num = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        let sack_bitmask = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        let payload_len = u16::from_be_bytes(bytes[24..26].try_into().unwrap()) as usize;
        let expected_crc = u32::from_be_bytes(bytes[26..30].try_into().unwrap());

        if bytes.len() < HEADER_SIZE + payload_len {
            return Err(format!(
                "Datagram payload truncated: got {} bytes, expected {}",
                bytes.len() - HEADER_SIZE,
                payload_len
            ));
        }

        let payload_bytes = &bytes[HEADER_SIZE..HEADER_SIZE + payload_len];

        // Verify CRC32
        let mut hasher = Hasher::new();
        hasher.update(&bytes[0..26]);
        hasher.update(payload_bytes);
        let actual_crc = hasher.finalize();

        if actual_crc != expected_crc {
            return Err(format!(
                "CRC32 checksum mismatch: expected 0x{:08X}, calculated 0x{:08X}",
                expected_crc, actual_crc
            ));
        }

        Ok(Self {
            version,
            packet_type,
            flags,
            seq_num,
            ack_num,
            sack_bitmask,
            payload: payload_bytes.to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_serialization_roundtrip() {
        let payload = b"Hello, Computer Networks UDP Transfer!".to_vec();
        let pkt = Packet::new_data(42, payload.clone(), false);
        let bytes = pkt.serialize();

        let decoded = Packet::deserialize(&bytes).expect("Deserialization failed");
        assert_eq!(decoded.packet_type, PacketType::Data);
        assert_eq!(decoded.seq_num, 42);
        assert_eq!(decoded.flags, 0);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_packet_crc_corruption_detection() {
        let payload = b"Sensitive network data".to_vec();
        let pkt = Packet::new_data(10, payload, false);
        let mut bytes = pkt.serialize();

        // Corrupt a byte in the payload
        let last_idx = bytes.len() - 1;
        bytes[last_idx] ^= 0xFF;

        let result = Packet::deserialize(&bytes);
        assert!(result.is_err(), "Corrupted packet should fail CRC check");
        assert!(result.unwrap_err().contains("CRC32 checksum mismatch"));
    }

    #[test]
    fn test_start_payload_roundtrip() {
        let sp = StartPayload {
            file_size: 1048576,
            chunk_size: 1400,
            total_chunks: 750,
            sha256_hash: [7u8; 32],
            file_name: "lecture_video.mp4".to_string(),
        };
        let bytes = sp.to_bytes();
        let decoded = StartPayload::from_bytes(&bytes).expect("StartPayload decode");
        assert_eq!(decoded, sp);
    }
}
