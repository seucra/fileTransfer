import random
import socket
from typing import Optional, Tuple


class ReliableUdpSocket:
    def __init__(self, bind_addr: Tuple[str, int], loss_rate_percent: int = 0):
        self.loss_rate_percent = max(0, min(100, loss_rate_percent))
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Allow reusing port for fast rebinding during testing
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(bind_addr)
        self._sock.settimeout(0.01)

    @property
    def local_addr(self) -> Tuple[str, int]:
        return self._sock.getsockname()

    def set_timeout(self, timeout: Optional[float]) -> None:
        self._sock.settimeout(timeout)

    def set_nonblocking(self, nonblocking: bool) -> None:
        self._sock.setblocking(not nonblocking)

    def send_to(self, data: bytes, target: Tuple[str, int]) -> int:
        if self.loss_rate_percent > 0:
            if random.randint(0, 99) < self.loss_rate_percent:
                # Simulated drop: silently pretend sent
                return len(data)
        return self._sock.sendto(data, target)

    def recv_from(self, bufsize: int = 65535) -> Tuple[bytes, Tuple[str, int]]:
        while True:
            data, addr = self._sock.recvfrom(bufsize)
            if self.loss_rate_percent > 0:
                if random.randint(0, 99) < self.loss_rate_percent:
                    # Simulated drop on receive
                    continue
            return data, addr

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass

    def __enter__(self) -> "ReliableUdpSocket":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
