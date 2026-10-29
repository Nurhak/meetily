//! Connection limits for the hub's listener: at most N connections overall and per client address, and connections
//! that stay silent too long are closed. Any device on the Wi-Fi can connect before authenticating, so these keep a
//! misbehaving one from exhausting the desktop app's file descriptors.

use std::collections::HashMap;
use std::future::{ready, Ready};
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use axum_server::accept::Accept;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep};

pub struct ConnectionLimits {
    global: Arc<Semaphore>,
    per_address: Arc<Mutex<HashMap<IpAddr, usize>>>,
    max_per_address: usize,
    idle: Duration,
}

impl ConnectionLimits {
    pub fn new(max_total: usize, max_per_address: usize, idle: Duration) -> Arc<Self> {
        Arc::new(Self {
            global: Arc::new(Semaphore::new(max_total)),
            per_address: Arc::new(Mutex::new(HashMap::new())),
            max_per_address,
            idle,
        })
    }

    fn admit(&self, stream: TcpStream) -> io::Result<Guarded> {
        let ip = stream.peer_addr()?.ip();
        let permit = self
            .global
            .clone()
            .try_acquire_owned()
            .map_err(|_| io::Error::other("too many connections"))?;
        {
            let mut map = self.per_address.lock().unwrap();
            let count = map.entry(ip).or_default();
            if *count >= self.max_per_address {
                return Err(io::Error::other("too many connections from one address"));
            }
            *count += 1;
        }
        Ok(Guarded {
            stream,
            _permit: permit,
            _address: AddressSlot {
                map: self.per_address.clone(),
                ip,
            },
            idle: self.idle,
            deadline: Box::pin(tokio::time::sleep(self.idle)),
        })
    }
}

#[derive(Clone)]
pub struct LimitAcceptor {
    limits: Arc<ConnectionLimits>,
}

impl LimitAcceptor {
    pub fn new(limits: Arc<ConnectionLimits>) -> Self {
        Self { limits }
    }
}

impl<S> Accept<TcpStream, S> for LimitAcceptor {
    type Stream = Guarded;
    type Service = S;
    type Future = Ready<io::Result<(Guarded, S)>>;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        // An error drops the connection right away.
        ready(self.limits.admit(stream).map(|s| (s, service)))
    }
}

struct AddressSlot {
    map: Arc<Mutex<HashMap<IpAddr, usize>>>,
    ip: IpAddr,
}

impl Drop for AddressSlot {
    fn drop(&mut self) {
        let mut map = self.map.lock().unwrap();
        if let Some(count) = map.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                map.remove(&self.ip);
            }
        }
    }
}

/// A connection that counts against the limits until it closes, and fails once it has been idle too long.
pub struct Guarded {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    _address: AddressSlot,
    idle: Duration,
    deadline: Pin<Box<Sleep>>,
}

impl Guarded {
    fn touch(&mut self) {
        let next = Instant::now() + self.idle;
        self.deadline.as_mut().reset(next);
    }

    fn check_idle(&mut self, cx: &mut Context<'_>) -> Poll<io::Error> {
        match self.deadline.as_mut().poll(cx) {
            Poll::Ready(()) => {
                Poll::Ready(io::Error::new(io::ErrorKind::TimedOut, "connection idle"))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

use std::future::Future as _;

impl AsyncRead for Guarded {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.stream).poll_read(cx, buf) {
            Poll::Ready(result) => {
                this.touch();
                Poll::Ready(result)
            }
            Poll::Pending => match this.check_idle(cx) {
                Poll::Ready(e) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl AsyncWrite for Guarded {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.stream).poll_write(cx, buf) {
            Poll::Ready(result) => {
                this.touch();
                Poll::Ready(result)
            }
            Poll::Pending => match this.check_idle(cx) {
                Poll::Ready(e) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.stream).poll_write_vectored(cx, bufs) {
            Poll::Ready(result) => {
                this.touch();
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}
