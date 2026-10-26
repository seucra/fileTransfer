import hashlib
import os
from typing import BinaryIO, Tuple


class FileWriter:
    def __init__(self, dest_dir: str, file_name: str, expected_size: int, chunk_size: int):
        self.dest_dir = os.path.abspath(dest_dir)
        os.makedirs(self.dest_dir, exist_ok=True)

        clean_name = os.path.basename(file_name) or "received_file.bin"
        self.destination_path = os.path.join(self.dest_dir, clean_name)
        self.expected_size = expected_size
        self.chunk_size = chunk_size

        self._file: BinaryIO = open(self.destination_path, "wb+")
        if expected_size > 0:
            self._file.truncate(expected_size)

    def write_chunk(self, chunk_index: int, data: bytes) -> None:
        offset = chunk_index * self.chunk_size
        self._file.seek(offset)
        self._file.write(data)

    def finalize_and_verify(self, expected_hash: bytes) -> Tuple[bool, str]:
        self._file.flush()
        self._file.truncate(self.expected_size)
        self._file.seek(0)

        hasher = hashlib.sha256()
        total_read = 0
        while chunk := self._file.read(65536):
            hasher.update(chunk)
            total_read += len(chunk)

        if total_read != self.expected_size:
            return (
                False,
                f"File size mismatch: received {total_read} bytes, expected {self.expected_size}",
            )

        actual_hash = hasher.digest()
        if actual_hash != expected_hash:
            return (
                False,
                f"Hash mismatch! Expected {expected_hash[:8].hex()}..., got {actual_hash[:8].hex()}...",
            )

        return True, "Integrity verified successfully"

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()

    def __enter__(self) -> "FileWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
