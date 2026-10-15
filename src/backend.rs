//! Backends, connection pools and the scheduler.

use crate::config::{Algorithm, Config};
use crate::metrics::Metrics;
use crate::pool::ConnPool;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;

/// A single gateway instance plus its private connection pool.
pub struct Backend {
    /// Backend address.
    pub addr: SocketAddr,
    /// Scheduling weight.
    pub weight: u32,
    /// Human readable label used for metrics.
    pub label: String,
    pool: ConnPool,
    connect_timeout: Duration,
    active: AtomicUsize,
    healthy: AtomicBool,
    success_streak: AtomicU32,
    failure_streak: AtomicU32,
    unhealthy_threshold: u32,
    healthy_threshold: u32,
    metrics: Arc<Metrics>,
}

impl Backend {
    fn new(addr: SocketAddr, weight: u32, config: &Config, metrics: Arc<Metrics>) -> Arc<Self> {
        let label = addr.to_string();
        let backend = Arc::new(Self {
            addr,
            weight,
            label,
            pool: ConnPool::new(
                addr,
                config.pool.max_idle_per_backend,
                config.pool.idle_timeout,
            ),
            connect_timeout: config.timeouts.connect,
            active: AtomicUsize::new(0),
            healthy: AtomicBool::new(true),
            success_streak: AtomicU32::new(0),
            failure_streak: AtomicU32::new(0),
            unhealthy_threshold: config.backends.unhealthy_threshold,
            healthy_threshold: config.backends.healthy_threshold,
            metrics,
        });
        backend
            .metrics
            .backend_healthy
            .with_label_values(&[&backend.label])
            .set(1);
        backend
    }

    /// Current in-flight stream count.
    pub fn active_conns(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// Whether the scheduler may currently pick this backend.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Acquire a connection, preferring a warm pooled one.
    pub async fn acquire(&self) -> io::Result<TcpStream> {
        match self.pool.get(self.connect_timeout).await {
            Ok((stream, reused)) => {
                if reused {
                    self.metrics
                        .conn_reuse_total
                        .with_label_values(&[&self.label])
                        .inc();
                } else {
                    self.metrics
                        .conn_new_total
                        .with_label_values(&[&self.label])
                        .inc();
                }
                Ok(stream)
            }
            Err(e) => {
                self.metrics
                    .conn_dial_errors_total
                    .with_label_values(&[&self.label])
                    .inc();
                Err(e)
            }
        }
    }

    /// Return a connection to the pool.
    pub fn release_conn(&self, conn: TcpStream) {
        self.pool.put(conn);
    }

    fn on_select(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .active_streams
            .with_label_values(&[&self.label])
            .inc();
    }

    fn on_release(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .active_streams
            .with_label_values(&[&self.label])
            .dec();
    }

    /// Record the outcome of a health probe and apply the hysteresis thresholds.
    pub fn record_health(&self, ok: bool) {
        if ok {
            self.failure_streak.store(0, Ordering::Relaxed);
            let successes = self.success_streak.fetch_add(1, Ordering::Relaxed) + 1;
            if !self.is_healthy() && successes >= self.healthy_threshold {
                self.healthy.store(true, Ordering::Relaxed);
                self.metrics
                    .backend_healthy
                    .with_label_values(&[&self.label])
                    .set(1);
                tracing::info!(backend = %self.label, "backend marked healthy");
            }
        } else {
            self.success_streak.store(0, Ordering::Relaxed);
            let failures = self.failure_streak.fetch_add(1, Ordering::Relaxed) + 1;
            self.metrics
                .health_check_failures
                .with_label_values(&[&self.label])
                .inc();
            if self.is_healthy() && failures >= self.unhealthy_threshold {
                self.healthy.store(false, Ordering::Relaxed);
                self.metrics
                    .backend_healthy
                    .with_label_values(&[&self.label])
                    .set(0);
                tracing::warn!(backend = %self.label, "backend marked unhealthy");
            }
        }
    }

    /// Record an upstream transfer failure.
    pub fn record_upstream_error(&self) {
        self.metrics
            .upstream_errors_total
            .with_label_values(&[&self.label])
            .inc();
    }
}

/// A scheduling lease. Dropping the lease releases the in-flight slot.
pub struct Lease {
    backend: Arc<Backend>,
}

impl Lease {
    /// The selected backend.
    pub fn backend(&self) -> &Arc<Backend> {
        &self.backend
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.backend.on_release();
    }
}

/// Picks backends for new streams.
pub struct Scheduler {
    backends: Vec<Arc<Backend>>,
    algorithm: Algorithm,
    round_robin: AtomicUsize,
}

impl Scheduler {
    /// Build the backend set from configuration.
    pub fn new(config: &Config, metrics: Arc<Metrics>) -> anyhow::Result<Self> {
        let mut backends = Vec::new();
        for server in &config.backends.servers {
            let addr = crate::config::resolve_addr(&server.addr).ok_or_else(|| {
                anyhow::anyhow!("cannot resolve backend address {:?}", server.addr)
            })?;
            backends.push(Backend::new(addr, server.weight, config, metrics.clone()));
        }
        Ok(Self {
            backends,
            algorithm: config.backends.algorithm,
            round_robin: AtomicUsize::new(0),
        })
    }

    /// All backends, healthy or not. Used by the health checker.
    pub fn backends(&self) -> &[Arc<Backend>] {
        &self.backends
    }

    /// Select a backend and reserve an in-flight slot.
    ///
    /// Returns `None` when no backend is healthy.
    pub fn select(&self) -> Option<Lease> {
        self.select_skipping(&[])
    }

    /// Select a healthy backend that is not in `skip`.
    ///
    /// Used by connect retries so a refused backend is not immediately
    /// re-picked by leastconn. Returns `None` when every healthy backend is
    /// skipped; the caller may then fall back to [`Self::select`].
    pub(crate) fn select_skipping(&self, skip: &[SocketAddr]) -> Option<Lease> {
        let backend = match self.algorithm {
            Algorithm::Leastconn => self.select_leastconn(skip),
            Algorithm::Roundrobin => self.select_round_robin(skip),
        }?;
        backend.on_select();
        Some(Lease { backend })
    }

    fn select_leastconn(&self, skip: &[SocketAddr]) -> Option<Arc<Backend>> {
        self.backends
            .iter()
            .filter(|b| b.is_healthy() && !skip.contains(&b.addr))
            .min_by(|a, b| {
                // Weighted least-connections: compare active/weight without
                // floating point by cross multiplying. Lower wins.
                let lhs = a.active_conns() as u128 * b.weight as u128;
                let rhs = b.active_conns() as u128 * a.weight as u128;
                lhs.cmp(&rhs)
            })
            .cloned()
    }

    fn select_round_robin(&self, skip: &[SocketAddr]) -> Option<Arc<Backend>> {
        let n = self.backends.len();
        if n == 0 {
            return None;
        }
        let start = self.round_robin.fetch_add(1, Ordering::Relaxed);
        for offset in 0..n {
            let idx = (start + offset) % n;
            let backend = &self.backends[idx];
            if backend.is_healthy() && !skip.contains(&backend.addr) {
                return Some(backend.clone());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leastconn_prefers_idle_backend() {
        // Build a scheduler directly with two fake backends.
        let config = crate::config::Config::from_toml(
            r#"
[server]
bind = "127.0.0.1:0"
[backends]
[[backends.servers]]
addr = "127.0.0.1:1"
[[backends.servers]]
addr = "127.0.0.1:2"
"#,
        )
        .unwrap();
        let metrics = Metrics::new().unwrap();
        let scheduler = Scheduler::new(&config, metrics).unwrap();
        let first = scheduler.select().unwrap();
        let first_addr = first.backend().addr;
        // Hold the first lease, then select again: it must pick the other one.
        let second = scheduler.select().unwrap();
        assert_ne!(first_addr, second.backend().addr);
    }

    #[test]
    fn leastconn_skips_failed_backend_on_retry() {
        let config = crate::config::Config::from_toml(
            r#"
[server]
bind = "127.0.0.1:0"
[backends]
[[backends.servers]]
addr = "127.0.0.1:1"
[[backends.servers]]
addr = "127.0.0.1:2"
"#,
        )
        .unwrap();
        let metrics = Metrics::new().unwrap();
        let scheduler = Scheduler::new(&config, metrics).unwrap();
        let first = scheduler.select().unwrap();
        let first_addr = first.backend().addr;
        drop(first);
        let second = scheduler.select_skipping(&[first_addr]).unwrap();
        assert_ne!(first_addr, second.backend().addr);
        assert!(scheduler
            .select_skipping(&[first_addr, second.backend().addr])
            .is_none());
    }
}
