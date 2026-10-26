import hashlib
import os
from typing import BinaryIO


class FileChunker:
    def __init__(self, path: str, chunk_size: int = 1400):
        self.path = os.path.abspath(path)
        if not os.path.isfile(self.path):
            raise FileNotFoundError(f"File not found: {self.path}")

        self.file_size = os.path.getsize(self.path)
        self.file_name = os.path.basename(self.path) or "transfer.bin"
        self.chunk_size = max(256, chunk_size)

        if self.file_size == 0:
            self.total_chunks = 1
        else:
            self.total_chunks = (self.file_size + self.chunk_size - 1) // self.chunk_size

        # Compute SHA-256 of entire file
        hasher = hashlib.sha256()
        with open(self.path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        self.sha256_hash = hasher.digest()

        self._file: BinaryIO = open(self.path, "rb")

    def read_chunk(self, chunk_index: int) -> bytes:
        if chunk_index >= self.total_chunks:
            raise IndexError(
                f"Chunk index {chunk_index} out of range (total {self.total_chunks})"
            )

        if self.file_size == 0:
            return b""

        offset = chunk_index * self.chunk_size
        self._file.seek(offset)
        bytes_to_read = min(self.chunk_size, self.file_size - offset)
        return self._file.read(bytes_to_read)

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()

    def __enter__(self) -> "FileChunker":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
