//! Thin L7 forwarding.
//!
//! xLB parses only the request/response heads, then streams bodies verbatim.
//! Bodies are opaque: no SSE, NDJSON or JSON inspection. Because it knows
//! exactly where a response body ends (HTTP framing, not `[DONE]`), it can
//! safely return a backend connection to the pool and serve subsequent requests
//! on a client keep-alive connection from any backend.
//!
//! When the first bytes look like something other than HTTP/1.1 — TLS, HTTP/2,
//! WebSocket/CONNECT upgrades, or an arbitrary binary protocol — xLB stops
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

        let mut request = match http1::parse_request(&head) {
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
        let backend_head = http1::rewrite_request_for_backend_pool(&head, &mut request);

        let tagged = match super::l4::connect_tagged(&state).await {
            Ok(tagged) => tagged,
            Err(e) => {
                tracing::debug!(%peer, error = %e, "no backend available");
                write_error(&mut client, 503, idle).await;
                break;
            }
        };
        let mut backend = tagged.lease.backend().clone();
        let mut lease = Some(tagged.lease);
        let mut conn = tagged.conn;
        let mut reused = tagged.reused;
        let mut failed_backends = tagged.failed_backends;
        let mut retries_used = tagged.retries_used;

        // Buffer fixed-length request bodies up to `MAX_REROUTE_BODY_BYTES`
        // whenever connection reuse or cross-backend retry/rerouting is active,
        // so stale keep-alive sockets or retryable HTTP status codes (e.g. 404
        // model-not-found on an asymmetric node, 402, 429, 5xx) can be replayed
        // onto another healthy backend before any response byte is sent.
        let max_retries = state.config.effective_max_retries();
        let inline_body: Option<Vec<u8>> = if reused || max_retries > 0 {
            match request.body {
                BodyKind::None => Some(Vec::new()),
                BodyKind::Length(n) if n <= max_head as u64 => {
                    let len = n as usize;
                    let mut bytes = Vec::with_capacity(len);
                    let avail = client_buf.available().len().min(len);
                    if avail > 0 {
                        bytes.extend_from_slice(&client_buf.available()[..avail]);
                        client_buf.consume(avail);
                    }
                    let mut scratch = [0u8; 16 * 1024];
                    while bytes.len() < len {
                        let want = (len - bytes.len()).min(scratch.len());
                        match read_with_idle(&mut client, &mut scratch[..want], idle, deadline)
                            .await
                        {
                            Ok(0) | Err(_) => {
                                state.metrics.client_errors_total.inc();
                                return Ok(());
                            }
                            Ok(read_n) => bytes.extend_from_slice(&scratch[..read_n]),
                        }
                    }
                    Some(bytes)
                }
                _ => None,
            }
        } else {
            None
        };

        let mut response_buf = ConnBuffer::new();
        let mut wrote_to_client = false;
        let resp_result = if let Some(ref body_bytes) = inline_body {
            loop {
                if let Err(e) =
                    send_inline_request(&mut conn, &backend_head, body_bytes, idle, deadline).await
                {
                    if reused && is_stale_reuse_error(&e) {
                        match redial_or_failover(
                            &state,
                            &mut lease,
                            &mut backend,
                            &mut failed_backends,
                            &mut retries_used,
                        )
                        .await
                        {
                            Ok((fresh, fresh_reused)) => {
                                conn = fresh;
                                reused = fresh_reused;
                                continue;
                            }
                            Err(e2) => {
                                tracing::debug!(%peer, backend = %backend.label, error = %e2, "failed to redial after stale pooled socket write error");
                                write_error(&mut client, 503, idle).await;
                                return Ok(());
                            }
                        }
                    }
                    backend.record_upstream_error();
                    if e.kind() == io::ErrorKind::TimedOut {
                        state.metrics.rejected("timeout");
                        write_error(&mut client, 504, idle).await;
                    }
                    tracing::debug!(%peer, backend = %backend.label, error = %e, "failed to write request to backend");
                    return Ok(());
                }

                let can_reroute_status = retries_used < max_retries && {
                    let mut next_skip = failed_backends.clone();
                    if !next_skip.contains(&backend.addr) {
                        next_skip.push(backend.addr);
                    }
                    state.scheduler.has_healthy_skipping(&next_skip)
                };
                let reroute_statuses =
                    can_reroute_status.then_some(state.config.retry.retry_statuses.as_slice());

                let request_done = Instant::now();
                let res = forward_response(
                    &mut client,
                    &mut conn,
                    &mut response_buf,
                    &mut client_buf,
                    &request,
                    max_head,
                    timeouts,
                    deadline,
                    request_done,
                    &state.metrics,
                    &mut wrote_to_client,
                    reroute_statuses,
                )
                .await;

                if let Ok(ref outcome) = res {
                    if let Some(status) = outcome.reroute_status {
                        if outcome.reusable {
                            if reused {
                                backend.record_conn_reuse();
                            }
                            backend.release_conn(conn);
                        }
                        state
                            .metrics
                            .reroutes_total
                            .with_label_values(&[&backend.label, &status.to_string()])
                            .inc();
                        if !failed_backends.contains(&backend.addr) {
                            failed_backends.push(backend.addr);
                        }
                        drop(lease.take());
                        retries_used += 1;
                        match super::l4::connect_tagged_with_budget(
                            &state,
                            &mut failed_backends,
                            &mut retries_used,
                        )
                        .await
                        {
                            Ok((new_lease, next_conn, next_reused)) => {
                                backend = new_lease.backend().clone();
                                lease = Some(new_lease);
                                conn = next_conn;
                                reused = next_reused;
                                response_buf = ConnBuffer::new();
                                continue;
                            }
                            Err(e2) => {
                                tracing::debug!(%peer, status, error = %e2, "status reroute failed to connect to next backend");
                                write_error(&mut client, 503, idle).await;
                                return Ok(());
                            }
                        }
                    }
                }

                if let Err(ref e) = res {
                    if reused
                        && !wrote_to_client
                        && response_buf.is_empty()
                        && is_stale_reuse_error(e)
                    {
                        match redial_or_failover(
                            &state,
                            &mut lease,
                            &mut backend,
                            &mut failed_backends,
                            &mut retries_used,
                        )
                        .await
                        {
                            Ok((fresh, fresh_reused)) => {
                                conn = fresh;
                                reused = fresh_reused;
                                continue;
                            }
                            Err(e2) => {
                                tracing::debug!(%peer, backend = %backend.label, error = %e2, "failed to redial after stale pooled socket EOF");
                                write_error(&mut client, 503, idle).await;
                                return Ok(());
                            }
                        }
                    }
                }
                if res.is_ok() && reused {
                    backend.record_conn_reuse();
                }
                break res;
            }
        } else {
            loop {
                match write_all_with_idle(&mut conn, &backend_head, idle, deadline).await {
                    Ok(()) => break,
                    Err(e) if reused && is_stale_reuse_error(&e) => {
                        match redial_or_failover(
                            &state,
                            &mut lease,
                            &mut backend,
                            &mut failed_backends,
                            &mut retries_used,
                        )
                        .await
                        {
                            Ok((fresh, fresh_reused)) => {
                                conn = fresh;
                                reused = fresh_reused;
                            }
                            Err(e2) => {
                                tracing::debug!(%peer, backend = %backend.label, error = %e2, "failed to redial after stale pooled socket write error");
                                write_error(&mut client, 503, idle).await;
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        backend.record_upstream_error();
                        if e.kind() == io::ErrorKind::TimedOut {
                            state.metrics.rejected("timeout");
                            write_error(&mut client, 504, idle).await;
                        }
                        tracing::debug!(%peer, backend = %backend.label, error = %e, "failed to write request head");
                        return Ok(());
                    }
                }
            }
            if reused {
                backend.record_conn_reuse();
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
            forward_response(
                &mut client,
                &mut conn,
                &mut response_buf,
                &mut client_buf,
                &request,
                max_head,
                timeouts,
                deadline,
                request_done,
                &state.metrics,
                &mut wrote_to_client,
                None,
            )
            .await
        };

        match resp_result {
            Ok(outcome) => {
                if outcome.reusable {
                    backend.release_conn(conn);
                }
                drop(lease);
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
                if e.kind() == io::ErrorKind::ConnectionAborted {
                    state.metrics.client_errors_total.inc();
                } else {
                    backend.record_upstream_error();
                }
                if e.kind() == io::ErrorKind::TimedOut {
                    state.metrics.rejected("timeout");
                    if !wrote_to_client {
                        write_error(&mut client, 504, idle).await;
                    }
                }
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

async fn redial_or_failover(
    state: &AppState,
    lease: &mut Option<crate::backend::Lease>,
    backend: &mut Arc<crate::backend::Backend>,
    failed_backends: &mut Vec<SocketAddr>,
    retries_used: &mut u32,
) -> anyhow::Result<(TcpStream, bool)> {
    let fresh_res = lease
        .as_mut()
        .expect("active backend lease")
        .acquire_fresh()
        .await;
    match fresh_res {
        Ok(fresh) => Ok((fresh, false)),
        Err(first_err) => {
            backend.record_upstream_error();
            let dead_addr = backend.addr;
            if !failed_backends.contains(&dead_addr) {
                failed_backends.push(dead_addr);
            }
            // Release the dead backend's active stream slot and concurrency
            // permit before sleeping or selecting a failover backend.
            *lease = None;
            if *retries_used >= state.config.effective_max_retries() {
                let reason = if first_err.kind() == io::ErrorKind::WouldBlock {
                    "queue_full"
                } else {
                    "unhealthy"
                };
                state.metrics.rejected(reason);
                return Err(first_err.into());
            }
            *retries_used += 1;
            let backoff_ms = 20 * (*retries_used as u64);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            let (new_lease, conn, reused) =
                super::l4::connect_tagged_with_budget(state, failed_backends, retries_used).await?;
            *backend = new_lease.backend().clone();
            *lease = Some(new_lease);
            Ok((conn, reused))
        }
    }
}

async fn send_inline_request(
    conn: &mut TcpStream,
    head: &[u8],
    body: &[u8],
    idle: Duration,
    deadline: Instant,
) -> io::Result<()> {
    super::write_vectored_all_with_idle(conn, head, body, idle, deadline).await
}

fn is_stale_reuse_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
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
    /// When `Some(status)`, the response body was drained without writing to
    /// `client` so the caller can reroute the request to another backend.
    reroute_status: Option<u16>,
}

#[allow(clippy::too_many_arguments)] // reason: response path needs timeouts, buffers, metrics without a context struct
async fn forward_response<S>(
    client: &mut S,
    conn: &mut TcpStream,
    response_buf: &mut ConnBuffer,
    client_buf: &mut ConnBuffer,
    request: &RequestHead,
    max_head: usize,
    timeouts: &crate::config::TimeoutConfig,
    deadline: Instant,
    request_done: Instant,
    metrics: &Metrics,
    wrote_to_client: &mut bool,
    reroute_statuses: Option<&[u16]>,
) -> io::Result<ResponseOutcome>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let first_byte = timeouts.effective_first_byte();
    let idle = timeouts.idle;
    let mut backend_first_byte: Option<Instant> = None;
    let response;
    loop {
        let head_budget = if backend_first_byte.is_none() {
            first_byte
        } else {
            idle
        };
        let head = http1::read_response_head_or_client_abort(
            conn,
            response_buf,
            client,
            client_buf,
            max_head,
            head_budget,
            deadline,
        )
        .await?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "backend closed before sending a final response",
            )
        })?;
        let is_first_head = backend_first_byte.is_none();
        if is_first_head {
            backend_first_byte = Some(Instant::now());
        }
        let parsed = http1::parse_response(&head)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let interim = (100..200).contains(&parsed.status) && parsed.status != 101;
        if !interim
            && !*wrote_to_client
            && reroute_statuses.is_some_and(|codes| codes.contains(&parsed.status))
        {
            let body = if request.method.eq_ignore_ascii_case("HEAD") {
                BodyKind::None
            } else {
                parsed.body
            };
            let drained = http1::drain_response_body(conn, response_buf, body, idle, deadline)
                .await
                .is_ok();
            let self_delimiting = !matches!(body, BodyKind::UntilClose);
            return Ok(ResponseOutcome {
                reusable: drained
                    && parsed.keep_alive
                    && request.keep_alive
                    && self_delimiting
                    && response_buf.is_empty(),
                client_persistent: true,
                reroute_status: Some(parsed.status),
            });
        }
        let client_first_byte = Instant::now();
        *wrote_to_client = true;
        write_all_with_idle(client, &head, idle, deadline).await?;
        // First-byte histograms measure TTFB, not first token. Record once,
        // including when the first head is a 1xx informational response.
        if is_first_head {
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
    http1::forward_response_body_or_client_abort(
        conn,
        client,
        response_buf,
        client_buf,
        body,
        idle,
        deadline,
    )
    .await?;

    let self_delimiting = !matches!(body, BodyKind::UntilClose);
    Ok(ResponseOutcome {
        reusable: response.keep_alive
            && request.keep_alive
            && self_delimiting
            && response_buf.is_empty(),
        client_persistent: response.keep_alive && request.keep_alive && self_delimiting,
        reroute_status: None,
    })
}

async fn write_error<S>(client: &mut S, status: u16, idle: Duration)
where
    S: AsyncWrite + Unpin,
{
    let reason = match status {
        400 => "Bad Request",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Error",
    };
    let response =
        format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    let error_deadline = Instant::now() + idle;
    let _ = write_all_with_idle(client, response.as_bytes(), idle, error_deadline).await;
}
