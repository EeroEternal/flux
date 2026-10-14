//! Thin L7 forwarding.
//!
//! Flux parses only the request/response heads, then streams bodies verbatim.
//! Because it knows exactly where a response body ends, it can safely return a
//! backend connection to the pool and serve subsequent requests on a client
//! keep-alive connection from any backend.
//!
//! When the first bytes look like something other than HTTP/1.1 — TLS, HTTP/2,
//! WebSocket/CONNECT upgrades, or an arbitrary binary protocol — Flux stops
//! parsing and transparently switches to the L4 splice, forwarding whatever it
//! already read and then shuttling raw bytes. This keeps the pooling benefits
//! for the common case without breaking protocol transparency.

use super::http1::{self, BodyKind, ConnBuffer, RequestHead};
use super::{read_with_idle, write_all_with_idle};
use crate::metrics::Metrics;
use crate::server::AppState;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

static REQUEST_SEQ: AtomicU64 = AtomicU64::new(1);

fn next_request_id() -> u64 {
    REQUEST_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Handle one thin-l7 client connection.
pub async fn handle<S>(mut client: S, peer: SocketAddr, state: Arc<AppState>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let timeouts = &state.config.timeouts;
    let idle = timeouts.idle;
    let max_head = state.config.server.max_header_bytes;
    let mut client_buf = ConnBuffer::new();

    loop {
        let request_start = Instant::now();
        let deadline = request_start + timeouts.total;

        // Make sure we have at least one byte so the protocol can be classified
        // before we insist on reading a full HTTP head.
        if client_buf.is_empty() {
            let mut first = [0u8; 1];
            match read_with_idle(&mut client, &mut first, idle, deadline).await {
                Ok(0) => break,
                Ok(_) => client_buf.push(&first[..1]),
                Err(_) => break,
            }
        }

        // HTTP request lines always begin with an uppercase method character.
        // TLS record headers (0x16), HTTP/2 frames and other binary protocols
        // do not. Hand those straight to the raw splice.
        if !client_buf.available()[0].is_ascii_uppercase() {
            return fallback_to_l4(client, client_buf.into_available(), state).await;
        }

        let head =
            match http1::read_head(&mut client, &mut client_buf, max_head, idle, deadline).await {
                Ok(Some(head)) => head,
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!(%peer, error = %e, "failed to read request head");
                    // An oversized or malformed head that starts like a method
                    // is more likely a foreign protocol than a client error.
                    if client_buf.is_empty() {
                        state.metrics.client_errors_total.inc();
                        break;
                    }
                    let initial = client_buf.into_available();
                    return fallback_to_l4(client, initial, state).await;
                }
            };

        // HTTP/2 connection preface: `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`.
        if head.starts_with(b"PRI ") {
            return fallback_with_head(client, head, client_buf, state).await;
        }

        let request = match http1::parse_request(&head) {
            Ok(request) => request,
            Err(e) => {
                tracing::debug!(%peer, error = %e, "unrecognized request; falling back to l4");
                return fallback_with_head(client, head, client_buf, state).await;
            }
        };

        // CONNECT tunnels and Upgrade handshakes must be handled at byte level.
        if request.method.eq_ignore_ascii_case("CONNECT") || request.has_upgrade {
            tracing::debug!(
                %peer,
                method = %request.method,
                "upgrade/connect request; falling back to l4"
            );
            return fallback_with_head(client, head, client_buf, state).await;
        }

        let request_id = next_request_id();
        let client_keep_alive = request.keep_alive;

        let (lease, mut conn) = match super::l4::connect(&state).await {
            Ok(pair) => pair,
            Err(e) => {
                state.metrics.rejected("unhealthy");
                tracing::debug!(%peer, error = %e, "no backend available");
                write_error(&mut client, 503, idle, deadline).await;
                break;
            }
        };
        let backend = lease.backend().clone();

        // Forward the request head verbatim, then stream the body.
        if let Err(e) = write_all_with_idle(&mut conn, &head, idle, deadline).await {
            backend.record_upstream_error();
            tracing::debug!(%peer, backend = %backend.label, error = %e, "failed to write request head");
            break;
        }
        if let Err(e) = http1::forward_body(
            &mut client,
            &mut conn,
            &mut client_buf,
            request.body,
            idle,
            deadline,
        )
        .await
        {
            state.metrics.client_errors_total.inc();
            tracing::debug!(%peer, backend = %backend.label, error = %e, "failed to forward request body");
            break;
        }

        let request_done = Instant::now();
        let mut response_buf = ConnBuffer::new();
        match forward_response(
            &mut client,
            &mut conn,
            &mut response_buf,
            &request,
            max_head,
            idle,
            deadline,
            request_done,
            &state.metrics,
        )
        .await
        {
            Ok(outcome) => {
                if outcome.reusable {
                    backend.release_conn(conn);
                }
                state
                    .metrics
                    .stream_duration_seconds
                    .observe(request_start.elapsed().as_secs_f64());
                tracing::debug!(
                    request_id,
                    %peer,
                    backend = %backend.label,
                    method = %request.method,
                    path = %request.path,
                    reusable = outcome.reusable,
                    "stream complete"
                );
                if !client_keep_alive || !outcome.client_persistent {
                    break;
                }
            }
            Err(e) => {
                backend.record_upstream_error();
                state
                    .metrics
                    .stream_duration_seconds
                    .observe(request_start.elapsed().as_secs_f64());
                tracing::debug!(request_id, %peer, backend = %backend.label, error = %e, "response failed");
                break;
            }
        }
    }

    Ok(())
}

/// Fall back to L4, prepending the already-consumed request head.
async fn fallback_with_head<S>(
    client: S,
    head: Vec<u8>,
    client_buf: ConnBuffer,
    state: Arc<AppState>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut initial = head;
    initial.extend_from_slice(client_buf.available());
    fallback_to_l4(client, initial, state).await
}

/// Connect to a backend and splice raw bytes for the rest of the connection.
async fn fallback_to_l4<S>(client: S, initial: Vec<u8>, state: Arc<AppState>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (lease, conn) = super::l4::connect(&state).await?;
    let backend = lease.backend().clone();
    tracing::info!(
        backend = %backend.label,
        buffered = initial.len(),
        "thin-l7: unrecognized protocol, falling back to l4 passthrough"
    );
    let result = super::l4::splice(client, conn, state, backend, initial).await;
    drop(lease);
    result
}

struct ResponseOutcome {
    /// The backend connection may be returned to the pool.
    reusable: bool,
    /// The client connection may be used for another request.
    client_persistent: bool,
}

#[allow(clippy::too_many_arguments)]
async fn forward_response<S>(
    client: &mut S,
    conn: &mut TcpStream,
    response_buf: &mut ConnBuffer,
    request: &RequestHead,
    max_head: usize,
    idle: Duration,
    deadline: Instant,
    request_done: Instant,
    metrics: &Metrics,
) -> io::Result<ResponseOutcome>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut backend_first_byte: Option<Instant> = None;
    let response;
    loop {
        let head = http1::read_head(conn, response_buf, max_head, idle, deadline)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "backend closed before sending a final response",
                )
            })?;
        if backend_first_byte.is_none() {
            backend_first_byte = Some(Instant::now());
        }
        let parsed = http1::parse_response(&head)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let interim = (100..200).contains(&parsed.status) && parsed.status != 101;
        let client_first_byte = Instant::now();
        write_all_with_idle(client, &head, idle, deadline).await?;
        if let Some(backend_ts) = backend_first_byte {
            metrics
                .ttft_delta_ms
                .observe((client_first_byte - backend_ts).as_secs_f64() * 1000.0);
            metrics
                .upstream_ttft_ms
                .observe((backend_ts - request_done).as_secs_f64() * 1000.0);
            metrics
                .client_ttft_ms
                .observe((client_first_byte - request_done).as_secs_f64() * 1000.0);
        }
        if interim {
            continue;
        }
        response = parsed;
        break;
    }

    let body = if request.method.eq_ignore_ascii_case("HEAD") {
        BodyKind::None
    } else {
        response.body
    };
    http1::forward_body(conn, client, response_buf, body, idle, deadline).await?;

    let self_delimiting = !matches!(body, BodyKind::UntilClose);
    Ok(ResponseOutcome {
        reusable: response.keep_alive
            && request.keep_alive
            && self_delimiting
            && response_buf.is_empty(),
        client_persistent: response.keep_alive && request.keep_alive && self_delimiting,
    })
}

async fn write_error<S>(client: &mut S, status: u16, idle: Duration, deadline: Instant)
where
    S: AsyncWrite + Unpin,
{
    let reason = match status {
        400 => "Bad Request",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let response =
        format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    let _ = write_all_with_idle(client, response.as_bytes(), idle, deadline).await;
}
