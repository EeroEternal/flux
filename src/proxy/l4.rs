//! Pure L4 forwarding.
//!
//! One client connection is mapped to one backend connection and spliced
//! bidirectionally with zero application-level buffering. The splice ends when
//! either direction finishes \`fin\` after the first half-close, when the total
//! deadline fires, or when either side goes silent for longer than the idle
//! timeout.
//!
//! L4 cannot prove where an HTTP response ends, so a backend connection is
//! never returned to the pool (it has been half-closed by the splice anyway).
//! Request-level connection reuse is provided by the thin-l7 mode.
//!
//! This module is also used as the fallback path when thin-l7 sees traffic it
//! does not understand (HTTP/2, WebSocket upgrades, TLS, arbitrary protocols).

use crate::backend::{Backend, Lease};
use crate::server::AppState;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Notify;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Client,
    Backend,
}

/// Shared liveness bookkeeping for both directions of a splice.
struct Activity {
    start: Instant,
    last_millis: AtomicU64,
    backend_responded: std::sync::atomic::AtomicBool,
    first_backend_read: Mutex<Option<Instant>>,
    first_client_write: Mutex<Option<Instant>>,
}

impl Activity {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            last_millis: AtomicU64::new(0),
            backend_responded: std::sync::atomic::AtomicBool::new(false),
            first_backend_read: Mutex::new(None),
            first_client_write: Mutex::new(None),
        }
    }

    fn touch(&self) {
        let now = self.start.elapsed().as_millis() as u64;
        self.last_millis.store(now, Ordering::Relaxed);
    }

    fn idle_for(&self) -> Duration {
        let now = self.start.elapsed().as_millis() as u64;
        let last = self.last_millis.load(Ordering::Relaxed);
        Duration::from_millis(now.saturating_sub(last))
    }

    fn has_backend_read(&self) -> bool {
        self.backend_responded.load(Ordering::Relaxed)
    }

    fn on_read(&self, role: Role) {
        self.touch();
        if role == Role::Backend && !self.backend_responded.swap(true, Ordering::Relaxed) {
            if let Ok(mut slot) = self.first_backend_read.lock() {
                if slot.is_none() {
                    *slot = Some(Instant::now());
                }
            }
        }
    }

    fn on_write(&self, role: Role) {
        self.touch();
        if role == Role::Client {
            if let Ok(mut slot) = self.first_client_write.lock() {
                if slot.is_none() {
                    *slot = Some(Instant::now());
                }
            }
        }
    }

    fn ttft_delta(&self) -> Option<f64> {
        let start = (*self.first_backend_read.lock().ok()?)?;
        let end = (*self.first_client_write.lock().ok()?)?;
        Some((end - start).as_secs_f64() * 1000.0)
    }
}

/// An \`AsyncRead\`/\`AsyncWrite\` wrapper that records activity.
struct Tracked<T> {
    inner: T,
    activity: Arc<Activity>,
    role: Role,
}

impl<T> Tracked<T> {
    fn new(inner: T, activity: Arc<Activity>, role: Role) -> Self {
        Self {
            inner,
            activity,
            role,
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Tracked<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = &mut *self;
        let before = buf.filled().len();
        let result = Pin::new(&mut me.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            if buf.filled().len() > before {
                me.activity.on_read(me.role);
            }
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Tracked<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = &mut *self;
        let result = Pin::new(&mut me.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &result {
            if *n > 0 {
                me.activity.on_write(me.role);
            }
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

enum SpliceOutcome {
    Done(io::Result<(u64, u64)>),
    Idle,
    Total,
}

/// Handle one L4 client connection.
pub async fn handle<S>(client: S, peer: SocketAddr, state: Arc<AppState>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (lease, conn) = connect(&state).await?;
    let backend = lease.backend().clone();
    tracing::debug!(%peer, backend = %backend.label, "l4 stream open");
    let result = splice(client, conn, state, backend, Vec::new()).await;
    drop(lease);
    result
}

/// Splice an already-connected client and backend connection.
///
/// \`initial_client_bytes\` are bytes that were read from the client before the
/// decision to use L4 was made (for example by the thin-l7 protocol sniffer).
/// They are flushed to the backend before the bidirectional copy starts.
pub(crate) async fn splice<S>(
    client: S,
    mut conn: TcpStream,
    state: Arc<AppState>,
    backend: Arc<Backend>,
    initial_client_bytes: Vec<u8>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let started = Instant::now();
    let first_byte = state.config.timeouts.effective_first_byte();
    let idle = state.config.timeouts.idle;
    let total = state.config.timeouts.total;

    if !initial_client_bytes.is_empty() {
        let deadline = started + total;
        super::write_all_with_idle(&mut conn, &initial_client_bytes, idle, deadline).await?;
    }

    let activity = Arc::new(Activity::new());
    let mut client = Tracked::new(client, activity.clone(), Role::Client);
    let mut upstream = Tracked::new(conn, activity.clone(), Role::Backend);

    let notify = Arc::new(Notify::new());
    let watchdog = {
        let activity = activity.clone();
        let notify = notify.clone();
        let min_budget = idle.min(first_byte);
        let tick = Duration::from_millis(250)
            .min(min_budget / 2)
            .max(Duration::from_millis(10));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tick).await;
                let budget = if activity.has_backend_read() {
                    idle
                } else {
                    first_byte
                };
                if activity.idle_for() >= budget {
                    notify.notify_one();
                    return;
                }
            }
        })
    };

    let outcome = tokio::select! {
        result = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {
            SpliceOutcome::Done(result)
        }
        _ = tokio::time::sleep(total) => SpliceOutcome::Total,
        _ = notify.notified() => SpliceOutcome::Idle,
    };
    watchdog.abort();

    match outcome {
        SpliceOutcome::Done(Ok((client_to_backend, backend_to_client))) => {
            state
                .metrics
                .bytes_total
                .with_label_values(&[&backend.label, "request"])
                .inc_by(client_to_backend);
            state
                .metrics
                .bytes_total
                .with_label_values(&[&backend.label, "response"])
                .inc_by(backend_to_client);
        }
        SpliceOutcome::Done(Err(e)) => {
            backend.record_upstream_error();
            tracing::debug!(backend = %backend.label, error = %e, "l4 stream ended with error");
        }
        SpliceOutcome::Idle => {
            state.metrics.rejected("timeout");
            tracing::debug!(backend = %backend.label, "l4 stream idle timeout");
        }
        SpliceOutcome::Total => {
            state.metrics.rejected("timeout");
            tracing::debug!(backend = %backend.label, "l4 stream total timeout");
        }
    }

    if let Some(delta) = activity.ttft_delta() {
        state.metrics.ttft_delta_ms.observe(delta);
    }
    state
        .metrics
        .stream_duration_seconds
        .observe(started.elapsed().as_secs_f64());
    Ok(())
}

/// Select a backend and establish a connection, honoring the retry policy.
///
/// Retries apply only to connection establishment (never after a response
/// byte). Each failed attempt redispatches to a different healthy backend
/// when one is available; if every backend has been tried, further attempts
/// may reuse the original set so a single backend can recover from a
/// transient refuse.
pub(crate) async fn connect(state: &AppState) -> anyhow::Result<(Lease, TcpStream)> {
    let mut last_error: Option<io::Error> = None;
    let mut skip = Vec::new();
    for attempt in 0..=state.config.retry.max_retries {
        let lease = match state.scheduler.select_skipping(&skip) {
            Some(lease) => lease,
            None => match state.scheduler.select() {
                Some(lease) => lease,
                None => {
                    state.metrics.rejected("unhealthy");
                    anyhow::bail!("no healthy backend available");
                }
            },
        };
        let addr = lease.backend().addr;
        match lease.backend().acquire().await {
            Ok(conn) => return Ok((lease, conn)),
            Err(e) => {
                if !skip.contains(&addr) {
                    skip.push(addr);
                }
                last_error = Some(e);
                if attempt < state.config.retry.max_retries {
                    tokio::time::sleep(Duration::from_millis(20 * (attempt as u64 + 1))).await;
                }
            }
        }
    }
    Err(anyhow::anyhow!(
        "backend connect failed after retries: {}",
        last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
}
