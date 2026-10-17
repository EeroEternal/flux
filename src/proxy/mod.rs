//! The forwarding engines.

pub mod http1;
pub mod l4;
pub mod l7;

use crate::config::Mode;
use crate::server::AppState;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

/// Dispatch an accepted client connection to the configured forwarding mode.
///
/// The client may be a plain TCP stream or a TLS stream, hence the generic
/// bound. Rely on type inference at the call site instead of naming the type.
pub async fn handle<S>(client: S, peer: SocketAddr, state: Arc<AppState>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match state.config.server.mode {
        Mode::L4 => l4::handle(client, peer, state).await,
        Mode::ThinL7 => l7::handle(client, peer, state).await,
    }
}

/// Read with both an idle timeout (per operation) and a total deadline.
pub(crate) async fn read_with_idle<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    idle: Duration,
    deadline: Instant,
) -> io::Result<usize> {
    let now = Instant::now();
    if now >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "total stream timeout",
        ));
    }
    let budget = idle.min(deadline - now);
    match timeout(budget, reader.read(buf)).await {
        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "idle timeout")),
        Ok(result) => result,
    }
}

/// Write with both an idle timeout (per operation) and a total deadline.
pub(crate) async fn write_all_with_idle<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
    idle: Duration,
    deadline: Instant,
) -> io::Result<()> {
    let now = Instant::now();
    if now >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "total stream timeout",
        ));
    }
    let budget = idle.min(deadline - now);
    match timeout(budget, writer.write_all(data)).await {
        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "idle timeout")),
        Ok(result) => result,
    }
}

/// Write two buffers (`head` and `body`) using vectored I/O (`writev`) when
/// possible so TCP_NODELAY sockets emit a single packet and a single syscall.
pub(crate) async fn write_vectored_all_with_idle<W: AsyncWrite + Unpin>(
    writer: &mut W,
    mut a: &[u8],
    mut b: &[u8],
    idle: Duration,
    deadline: Instant,
) -> io::Result<()> {
    if b.is_empty() {
        return write_all_with_idle(writer, a, idle, deadline).await;
    }
    while !a.is_empty() || !b.is_empty() {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "total stream timeout",
            ));
        }
        let budget = idle.min(deadline - now);
        let written = if !a.is_empty() {
            let slices = [io::IoSlice::new(a), io::IoSlice::new(b)];
            match timeout(budget, writer.write_vectored(&slices)).await {
                Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "idle timeout")),
                Ok(res) => res?,
            }
        } else {
            return write_all_with_idle(writer, b, idle, deadline).await;
        };
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        if written <= a.len() {
            a = &a[written..];
        } else {
            let rem = written - a.len();
            a = &[];
            b = &b[rem..];
        }
    }
    Ok(())
}
