//! Backend connection pool.
//!
//! The pool keeps warm TCP connections to a single backend address. It is
//! intentionally unaware of HTTP semantics: any connection that is returned
//! to it is assumed to be at a clean message boundary. Only the thin-l7 proxy
//! ever returns connections, because it is the only mode that can prove a
//! response was fully consumed.
//!
//! Idle timeout is independent from stream duration: a connection that is
//! checked out for a 10 minute stream is never considered idle.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::timeout;

struct Idle {
    stream: TcpStream,
    since: Instant,
}

/// A pool of idle connections to one backend.
pub struct ConnPool {
    addr: SocketAddr,
    max_idle: usize,
    idle_timeout: Duration,
    idle: Mutex<VecDeque<Idle>>,
}

impl ConnPool {
    /// Create an empty pool.
    pub fn new(addr: SocketAddr, max_idle: usize, idle_timeout: Duration) -> Self {
        Self {
            addr,
            max_idle,
            idle_timeout,
            idle: Mutex::new(VecDeque::new()),
        }
    }

    /// Number of connections currently sitting idle.
    pub fn idle_len(&self) -> usize {
        self.idle.lock().map(|g| g.len()).unwrap_or(0)
    }

    /// Acquire a connection.
    ///
    /// Returns `(stream, reused)`. Idle connections are checked out in LIFO
    /// order (most recently returned first) so the warmest socket is reused
    /// while cold connections at the front age out. Each candidate is validated
    /// with a non-blocking read: a closed socket or a socket with unexpected
    /// buffered data is discarded. When no healthy idle connection exists a
    /// new one is dialed, bounded by `connect_timeout`.
    pub async fn get(&self, connect_timeout: Duration) -> io::Result<(TcpStream, bool)> {
        loop {
            let candidate = {
                let mut guard = self.idle.lock().expect("pool mutex poisoned");
                while guard
                    .front()
                    .is_some_and(|i| i.since.elapsed() >= self.idle_timeout)
                {
                    guard.pop_front();
                }
                guard.pop_back()
            };
            let Some(idle) = candidate else { break };
            let stream = idle.stream;
            let mut probe = [0u8; 1];
            match stream.try_read(&mut probe) {
                // Cleanly closed by the backend.
                Ok(0) => continue,
                // Unexpected bytes: the previous exchange was not clean.
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok((stream, true));
                }
                Err(_) => continue,
            }
        }

        let stream = timeout(connect_timeout, TcpStream::connect(self.addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "backend connect timeout"))??;
        stream.set_nodelay(true)?;
        Ok((stream, false))
    }

    /// Return a healthy connection to the pool.
    ///
    /// Expired connections at the front are reaped first; if the pool is still
    /// at `max_idle`, the oldest idle connection at the front is evicted so the
    /// freshly active connection is retained.
    pub fn put(&self, stream: TcpStream) {
        if self.max_idle == 0 {
            return;
        }
        let mut probe = [0u8; 1];
        match stream.try_read(&mut probe) {
            Ok(0) => return,
            Ok(_) => return,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => return,
        }
        if let Ok(mut guard) = self.idle.lock() {
            while guard
                .front()
                .is_some_and(|i| i.since.elapsed() >= self.idle_timeout)
            {
                guard.pop_front();
            }
            if guard.len() >= self.max_idle {
                guard.pop_front();
            }
            guard.push_back(Idle {
                stream,
                since: Instant::now(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn reuses_idle_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Keep the connection open; just drain.
            let mut buf = [0u8; 64];
            let _ = socket.read(&mut buf).await;
        });

        let pool = ConnPool::new(addr, 4, Duration::from_secs(30));
        let (stream, reused) = pool.get(Duration::from_secs(1)).await.unwrap();
        assert!(!reused);
        pool.put(stream);
        let (_stream, reused) = pool.get(Duration::from_secs(1)).await.unwrap();
        assert!(reused);
    }

    #[tokio::test]
    async fn prefers_most_recently_returned_connection_lifo() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    let _ = socket.read(&mut buf).await;
                });
            }
        });

        let pool = ConnPool::new(addr, 4, Duration::from_secs(30));
        let (c1, _) = pool.get(Duration::from_secs(1)).await.unwrap();
        let (c2, _) = pool.get(Duration::from_secs(1)).await.unwrap();
        let port1 = c1.local_addr().unwrap().port();
        let port2 = c2.local_addr().unwrap().port();
        pool.put(c1);
        pool.put(c2);

        // LIFO: c2 was returned last and must be checked out first.
        let (first_out, reused) = pool.get(Duration::from_secs(1)).await.unwrap();
        assert!(reused);
        assert_eq!(first_out.local_addr().unwrap().port(), port2);

        let (second_out, reused) = pool.get(Duration::from_secs(1)).await.unwrap();
        assert!(reused);
        assert_eq!(second_out.local_addr().unwrap().port(), port1);
    }

    #[tokio::test]
    async fn drops_expired_idle_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    let _ = socket.read(&mut buf).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        let pool = ConnPool::new(addr, 4, Duration::from_millis(10));
        let (stream, _) = pool.get(Duration::from_secs(1)).await.unwrap();
        pool.put(stream);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let (_stream, reused) = pool.get(Duration::from_secs(1)).await.unwrap();
        assert!(!reused);
    }
}
