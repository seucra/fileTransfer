use rand::Rng;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

pub struct ReliableUdpSocket {
    socket: UdpSocket,
    pub loss_rate_percent: u8,
}

impl ReliableUdpSocket {
    pub fn bind<A: std::net::ToSocketAddrs>(addr: A, loss_rate_percent: u8) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        socket.set_read_timeout(Some(Duration::from_millis(10)))?;
        Ok(Self {
            socket,
            loss_rate_percent,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.socket.set_read_timeout(timeout)
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.socket.set_nonblocking(nonblocking)
    }

    /// Send datagram to target, subject to simulated packet loss
    pub fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        if self.loss_rate_percent > 0 {
            let mut rng = rand::thread_rng();
            let roll: u8 = rng.gen_range(0..100);
            if roll < self.loss_rate_percent {
                // Simulate packet drop by silently pretending we sent it!
                return Ok(buf.len());
            }
        }
        self.socket.send_to(buf, target)
    }

    /// Receive datagram from socket, subject to simulated packet loss
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            let (len, addr) = self.socket.recv_from(buf)?;
            if self.loss_rate_percent > 0 {
                let mut rng = rand::thread_rng();
                let roll: u8 = rng.gen_range(0..100);
                if roll < self.loss_rate_percent {
                    // Simulate drop on receive by ignoring and trying next or timing out
                    continue;
                }
            }
            return Ok((len, addr));
        }
    }
}
