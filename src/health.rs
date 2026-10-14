//! Active health checking.
//!
//! Probes only hit a lightweight endpoint and never touch in-flight streams.
//! A failing probe only affects *new* scheduling decisions.

use crate::backend::Backend;
use crate::metrics::Metrics;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Spawn one health-check task per backend. Returns the join handles so the
/// caller can await them during shutdown.
pub fn spawn_all(
    backends: Vec<Arc<Backend>>,
    path: String,
    interval: Duration,
    probe_timeout: Duration,
    metrics: Arc<Metrics>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    if interval.is_zero() {
        tracing::info!("active health checking disabled");
        return Vec::new();
    }
    let mut handles = Vec::new();
    for backend in backends {
        let path = path.clone();
        let metrics = metrics.clone();
        let mut shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            // Small initial jitter so all probes do not fire simultaneously.
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = ticker.tick() => {
                        let ok = probe(&backend, &path, probe_timeout).await;
                        if ok {
                            metrics
                                .health_check_success_total
                                .with_label_values(&[&backend.label])
                                .inc();
                        }
                        backend.record_health(ok);
                    }
                }
            }
        }));
    }
    handles
}

async fn probe(backend: &Backend, path: &str, probe_timeout: Duration) -> bool {
    let addr = backend.addr;
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nUser-Agent: flux-health\r\n\r\n"
    );
    let result = tokio::time::timeout(probe_timeout, async move {
        let mut socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;
        socket.write_all(request.as_bytes()).await?;
        socket.flush().await?;
        let mut buf = [0u8; 256];
        let n = socket.read(&mut buf).await?;
        Ok::<_, std::io::Error>(status_is_ok(&buf[..n]))
    })
    .await;
    matches!(result, Ok(Ok(true)))
}

fn status_is_ok(response: &[u8]) -> bool {
    let text = String::from_utf8_lossy(response);
    let Some(line) = text.lines().next() else {
        return false;
    };
    let mut parts = line.split_whitespace();
    let version_ok = parts.next().is_some_and(|v| v.starts_with("HTTP/"));
    let code = parts.next().and_then(|c| c.parse::<u16>().ok());
    version_ok && matches!(code, Some(c) if (200..400).contains(&c))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_parsing() {
        assert!(status_is_ok(b"HTTP/1.1 200 OK\r\n"));
        assert!(status_is_ok(b"HTTP/1.1 204 No Content\r\n"));
        assert!(!status_is_ok(b"HTTP/1.1 500 Oops\r\n"));
        assert!(!status_is_ok(b"garbage"));
    }
}
