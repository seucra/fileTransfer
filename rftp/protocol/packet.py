import struct
import zlib
from enum import IntEnum
from typing import Optional, Tuple

PROTOCOL_MAGIC = b"RFTP"
PROTOCOL_VERSION = 1
HEADER_SIZE = 30

FLAG_RETRANSMISSION = 0x01


class PacketType(IntEnum):
    START = 1
    START_ACK = 2
    DATA = 3
    ACK = 4
    FIN = 5
    FIN_ACK = 6
    CANCEL = 7
    HEARTBEAT = 8

    def __str__(self) -> str:
        return self.name


class StartPayload:
    __slots__ = ("file_size", "chunk_size", "total_chunks", "sha256_hash", "file_name")

    def __init__(
        self,
        file_size: int,
        chunk_size: int,
        total_chunks: int,
        sha256_hash: bytes,
        file_name: str,
    ):
        self.file_size = file_size
        self.chunk_size = chunk_size
        self.total_chunks = total_chunks
        self.sha256_hash = sha256_hash if isinstance(sha256_hash, bytes) else bytes(sha256_hash)
        self.file_name = file_name

    def to_bytes(self) -> bytes:
        name_bytes = self.file_name.encode("utf-8")[:65535]
        return struct.pack(
            ">QII32sH",
            self.file_size,
            self.chunk_size,
            self.total_chunks,
            self.sha256_hash,
            len(name_bytes),
        ) + name_bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> "StartPayload":
        min_len = struct.calcsize(">QII32sH")
        if len(data) < min_len:
            raise ValueError(f"StartPayload too short: {len(data)} < {min_len}")
        file_size, chunk_size, total_chunks, sha256_hash, name_len = struct.unpack_from(
            ">QII32sH", data, 0
        )
        if len(data) < min_len + name_len:
            raise ValueError("StartPayload missing filename bytes")
        file_name = data[min_len : min_len + name_len].decode("utf-8", errors="replace")
        return cls(file_size, chunk_size, total_chunks, sha256_hash, file_name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StartPayload):
            return False
        return (
            self.file_size == other.file_size
            and self.chunk_size == other.chunk_size
            and self.total_chunks == other.total_chunks
            and self.sha256_hash == other.sha256_hash
            and self.file_name == other.file_name
        )


class StartAckPayload:
    __slots__ = ("accepted", "message")

    def __init__(self, accepted: bool, message: str = ""):
        self.accepted = accepted
        self.message = message

    def to_bytes(self) -> bytes:
        msg_bytes = self.message.encode("utf-8")[:65535]
        return struct.pack(">BH", 1 if self.accepted else 0, len(msg_bytes)) + msg_bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> "StartAckPayload":
        if len(data) < 3:
            raise ValueError("StartAckPayload too short")
        accepted_int, msg_len = struct.unpack_from(">BH", data, 0)
        message = data[3 : 3 + msg_len].decode("utf-8", errors="replace")
        return cls(accepted_int == 1, message)


class FinAckPayload:
    __slots__ = ("success", "message")

    def __init__(self, success: bool, message: str = ""):
        self.success = success
        self.message = message

    def to_bytes(self) -> bytes:
        msg_bytes = self.message.encode("utf-8")[:65535]
        return struct.pack(">BH", 1 if self.success else 0, len(msg_bytes)) + msg_bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> "FinAckPayload":
        if len(data) < 3:
            raise ValueError("FinAckPayload too short")
        success_int, msg_len = struct.unpack_from(">BH", data, 0)
        message = data[3 : 3 + msg_len].decode("utf-8", errors="replace")
        return cls(success_int == 1, message)


class Packet:
    __slots__ = (
        "version",
        "packet_type",
        "flags",
        "seq_num",
        "ack_num",
        "sack_bitmask",
        "payload",
    )

    def __init__(
        self,
        packet_type: PacketType,
        seq_num: int = 0,
        ack_num: int = 0,
        sack_bitmask: int = 0,
        payload: bytes = b"",
        flags: int = 0,
        version: int = PROTOCOL_VERSION,
    ):
        self.version = version
        self.packet_type = packet_type
        self.flags = flags
        self.seq_num = seq_num
        self.ack_num = ack_num
        self.sack_bitmask = sack_bitmask
        self.payload = payload if isinstance(payload, bytes) else bytes(payload)

    @property
    def is_retransmission(self) -> bool:
        return bool(self.flags & FLAG_RETRANSMISSION)

    @classmethod
    def new_start(cls, payload: StartPayload) -> "Packet":
        return cls(PacketType.START, payload=payload.to_bytes())

    @classmethod
    def new_start_ack(cls, accepted: bool, message: str = "") -> "Packet":
        return cls(PacketType.START_ACK, payload=StartAckPayload(accepted, message).to_bytes())

    @classmethod
    def new_data(cls, seq_num: int, payload: bytes, is_retransmission: bool = False) -> "Packet":
        flags = FLAG_RETRANSMISSION if is_retransmission else 0
        return cls(PacketType.DATA, seq_num=seq_num, payload=payload, flags=flags)

    @classmethod
    def new_ack(cls, ack_num: int, sack_bitmask: int) -> "Packet":
        return cls(PacketType.ACK, ack_num=ack_num, sack_bitmask=sack_bitmask)

    @classmethod
    def new_fin(cls, sha256_hash: bytes) -> "Packet":
        return cls(PacketType.FIN, payload=sha256_hash)

    @classmethod
    def new_fin_ack(cls, success: bool, message: str = "") -> "Packet":
        return cls(PacketType.FIN_ACK, payload=FinAckPayload(success, message).to_bytes())

    @classmethod
    def new_cancel(cls, reason: str) -> "Packet":
        return cls(PacketType.CANCEL, payload=reason.encode("utf-8"))

    @classmethod
    def new_heartbeat(cls) -> "Packet":
        return cls(PacketType.HEARTBEAT)

    def serialize(self) -> bytes:
        payload_len = min(len(self.payload), 65535)
        # First 26 bytes: Magic, Version, Type, Flags, Reserved(0), Seq, Ack, Sack, PayloadLen
        header_26 = struct.pack(
            ">4sBBBBIIQH",
            PROTOCOL_MAGIC,
            self.version,
            int(self.packet_type),
            self.flags,
            0,  # reserved
            self.seq_num,
            self.ack_num,
            self.sack_bitmask,
            payload_len,
        )
        # Compute CRC32 over header_26 and payload
        crc = zlib.crc32(header_26)
        crc = zlib.crc32(self.payload[:payload_len], crc) & 0xFFFFFFFF

        return header_26 + struct.pack(">I", crc) + self.payload[:payload_len]

    @classmethod
    def deserialize(cls, data: bytes) -> "Packet":
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Datagram too short: {len(data)} < {HEADER_SIZE}")

        magic, version, pkt_type_int, flags, _reserved, seq_num, ack_num, sack_bitmask, payload_len = (
            struct.unpack_from(">4sBBBBIIQH", data, 0)
        )

        if magic != PROTOCOL_MAGIC:
            raise ValueError(f"Invalid protocol magic: {magic!r}")
        if version != PROTOCOL_VERSION:
            raise ValueError(f"Unsupported protocol version: {version}")

        try:
            packet_type = PacketType(pkt_type_int)
        except ValueError:
            raise ValueError(f"Unknown packet type: {pkt_type_int}")

        expected_crc = struct.unpack_from(">I", data, 26)[0]

        if len(data) < HEADER_SIZE + payload_len:
            raise ValueError(
                f"Datagram payload truncated: got {len(data) - HEADER_SIZE}, expected {payload_len}"
            )

        payload = data[HEADER_SIZE : HEADER_SIZE + payload_len]

        # Verify CRC32
        computed_crc = zlib.crc32(data[:26])
        computed_crc = zlib.crc32(payload, computed_crc) & 0xFFFFFFFF

        if computed_crc != expected_crc:
            raise ValueError(
                f"CRC32 mismatch: expected 0x{expected_crc:08X}, calculated 0x{computed_crc:08X}"
            )

        return cls(
            packet_type=packet_type,
            seq_num=seq_num,
            ack_num=ack_num,
            sack_bitmask=sack_bitmask,
            payload=payload,
            flags=flags,
            version=version,
        )
