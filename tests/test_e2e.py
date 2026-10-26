import hashlib
import os
import tempfile
import unittest

from rftp.benchmark.runner import create_test_file, run_custom_udp_benchmark


class TestEndToEndTransfer(unittest.TestCase):
    def test_e2e_udp_transfer_lossless(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src_file = os.path.join(tmpdir, "source_lossless.bin")
            dest_dir = os.path.join(tmpdir, "dest_lossless")
            os.makedirs(dest_dir, exist_ok=True)

            size_bytes = 256 * 1024  # 256 KB
            original_hash = create_test_file(src_file, size_bytes)

            result = run_custom_udp_benchmark(
                src_file=src_file,
                dest_dir=dest_dir,
                loss_rate_percent=0,
                window_size=16,
                chunk_size=1400,
            )

            self.assertTrue(result.integrity_verified)
            self.assertEqual(result.file_size_bytes, size_bytes)

            received_file = os.path.join(dest_dir, "source_lossless.bin")
            self.assertTrue(os.path.isfile(received_file))
            self.assertEqual(os.path.getsize(received_file), size_bytes)

            hasher = hashlib.sha256()
            with open(received_file, "rb") as f:
                while chunk := f.read(65536):
                    hasher.update(chunk)
            self.assertEqual(original_hash, hasher.digest())

    def test_e2e_udp_transfer_with_simulated_loss(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src_file = os.path.join(tmpdir, "source_lossy.bin")
            dest_dir = os.path.join(tmpdir, "dest_lossy")
            os.makedirs(dest_dir, exist_ok=True)

            size_bytes = 128 * 1024  # 128 KB
            original_hash = create_test_file(src_file, size_bytes)

            result = run_custom_udp_benchmark(
                src_file=src_file,
                dest_dir=dest_dir,
                loss_rate_percent=10,  # 10% simulated packet loss
                window_size=16,
                chunk_size=1400,
            )

            self.assertTrue(result.integrity_verified)
            self.assertGreater(
                result.retransmissions, 0, "Lossy transfer should trigger retransmissions"
            )

            received_file = os.path.join(dest_dir, "source_lossy.bin")
            self.assertTrue(os.path.isfile(received_file))

            hasher = hashlib.sha256()
            with open(received_file, "rb") as f:
                while chunk := f.read(65536):
                    hasher.update(chunk)
            self.assertEqual(original_hash, hasher.digest())


if __name__ == "__main__":
    unittest.main()
