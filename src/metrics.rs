//! Prometheus metrics for xLB.

use anyhow::Result;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use std::sync::Arc;

const TTFT_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0,
];

const DURATION_BUCKETS: &[f64] = &[
    0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
];

/// Every metric xLB exposes.
pub struct Metrics {
    /// Registry used to render the Prometheus text exposition.
    pub registry: Registry,
    /// Proxy-added first-byte latency for L7 (milliseconds). TTFB, not tokens.
    pub ttft_delta_ms: Histogram,
    /// Time from the request being fully written to the backend until the
    /// backend's first response byte (TTFB, not first token).
    pub upstream_ttft_ms: Histogram,
    /// Time from the request being fully written to the backend until the
    /// client receives its first response byte (TTFB, not first token).
    pub client_ttft_ms: Histogram,
    /// Whole request/stream duration.
    pub stream_duration_seconds: Histogram,
    /// In-flight streams per backend.
    pub active_streams: IntGaugeVec,
    /// Backend connections taken from the idle pool.
    pub conn_reuse_total: IntCounterVec,
    /// Backend connections freshly dialed.
    pub conn_new_total: IntCounterVec,
    /// Backend dial failures.
    pub conn_dial_errors_total: IntCounterVec,
    /// Rejected client requests, labelled by reason.
    pub rejected_total: IntCounterVec,
    /// Failed health probes per backend.
    pub health_check_failures: IntCounterVec,
    /// Successful health probes per backend.
    pub health_check_success_total: IntCounterVec,
    /// Upstream transfer failures per backend.
    pub upstream_errors_total: IntCounterVec,
    /// Client-side transfer failures.
    pub client_errors_total: IntCounter,
    /// Bytes proxied, labelled by `(backend, direction)`.
    pub bytes_total: IntCounterVec,
    /// Total client connections accepted.
    pub connections_total: IntCounter,
    /// Currently open client connections.
    pub active_connections: IntGauge,
    /// Backend health state (1 healthy, 0 unhealthy).
    pub backend_healthy: IntGaugeVec,
    /// Pre-first-byte HTTP status reroutes per `(backend, status)`.
    pub reroutes_total: IntCounterVec,
}

impl Metrics {
    /// Build the registry and every metric.
    pub fn new() -> Result<Arc<Self>> {
        let registry = Registry::new();

        let ttft_delta_ms = Histogram::with_opts(
            HistogramOpts::new(
                "xlb_ttft_delta_ms",
                "Proxy-added first-byte latency in ms (TTFB; not token-aware)",
            )
            .buckets(TTFT_BUCKETS.to_vec()),
        )?;
        let upstream_ttft_ms = Histogram::with_opts(
            HistogramOpts::new(
                "xlb_upstream_ttft_ms",
                "Time from request completion to backend first byte in ms (TTFB; not token-aware)",
            )
            .buckets(TTFT_BUCKETS.to_vec()),
        )?;
        let client_ttft_ms = Histogram::with_opts(
            HistogramOpts::new(
                "xlb_client_ttft_ms",
                "Time from request completion to client first byte in ms (TTFB; not token-aware)",
            )
            .buckets(TTFT_BUCKETS.to_vec()),
        )?;
        let stream_duration_seconds = Histogram::with_opts(
            HistogramOpts::new("xlb_stream_duration_seconds", "Whole stream duration (s)")
                .buckets(DURATION_BUCKETS.to_vec()),
        )?;
        let active_streams = IntGaugeVec::new(
            Opts::new("xlb_active_streams", "In-flight streams"),
            &["backend"],
        )?;
        let conn_reuse_total = IntCounterVec::new(
            Opts::new(
                "xlb_conn_reuse_total",
                "Backend connections reused from the pool",
            ),
            &["backend"],
        )?;
        let conn_new_total = IntCounterVec::new(
            Opts::new("xlb_conn_new_total", "Backend connections freshly dialed"),
            &["backend"],
        )?;
        let conn_dial_errors_total = IntCounterVec::new(
            Opts::new("xlb_conn_dial_errors_total", "Backend dial failures"),
            &["backend"],
        )?;
        let rejected_total = IntCounterVec::new(
            Opts::new("xlb_rejected_total", "Rejected client requests"),
            &["reason"],
        )?;
        let health_check_failures = IntCounterVec::new(
            Opts::new("xlb_health_check_failures", "Failed health probes"),
            &["backend"],
        )?;
        let health_check_success_total = IntCounterVec::new(
            Opts::new("xlb_health_check_success_total", "Successful health probes"),
            &["backend"],
        )?;
        let upstream_errors_total = IntCounterVec::new(
            Opts::new("xlb_upstream_errors_total", "Upstream transfer failures"),
            &["backend"],
        )?;
        let client_errors_total =
            IntCounter::new("xlb_client_errors_total", "Client-side transfer failures")?;
        let bytes_total = IntCounterVec::new(
            Opts::new("xlb_bytes_total", "Bytes proxied"),
            &["backend", "direction"],
        )?;
        let connections_total =
            IntCounter::new("xlb_connections_total", "Client connections accepted")?;
        let active_connections = IntGauge::new(
            "xlb_active_connections",
            "Currently open client connections",
        )?;
        let backend_healthy = IntGaugeVec::new(
            Opts::new(
                "xlb_backend_healthy",
                "Backend health (1 healthy, 0 unhealthy)",
            ),
            &["backend"],
        )?;
        let reroutes_total = IntCounterVec::new(
            Opts::new(
                "xlb_reroutes_total",
                "Pre-first-byte HTTP status reroutes to another healthy backend",
            ),
            &["backend", "status"],
        )?;

        registry.register(Box::new(ttft_delta_ms.clone()))?;
        registry.register(Box::new(upstream_ttft_ms.clone()))?;
        registry.register(Box::new(client_ttft_ms.clone()))?;
        registry.register(Box::new(stream_duration_seconds.clone()))?;
        registry.register(Box::new(active_streams.clone()))?;
        registry.register(Box::new(conn_reuse_total.clone()))?;
        registry.register(Box::new(conn_new_total.clone()))?;
        registry.register(Box::new(conn_dial_errors_total.clone()))?;
        registry.register(Box::new(rejected_total.clone()))?;
        registry.register(Box::new(health_check_failures.clone()))?;
        registry.register(Box::new(health_check_success_total.clone()))?;
        registry.register(Box::new(upstream_errors_total.clone()))?;
        registry.register(Box::new(client_errors_total.clone()))?;
        registry.register(Box::new(bytes_total.clone()))?;
        registry.register(Box::new(connections_total.clone()))?;
        registry.register(Box::new(active_connections.clone()))?;
        registry.register(Box::new(backend_healthy.clone()))?;
        registry.register(Box::new(reroutes_total.clone()))?;

        Ok(Arc::new(Self {
            registry,
            ttft_delta_ms,
            upstream_ttft_ms,
            client_ttft_ms,
            stream_duration_seconds,
            active_streams,
            conn_reuse_total,
            conn_new_total,
            conn_dial_errors_total,
            rejected_total,
            health_check_failures,
            health_check_success_total,
            upstream_errors_total,
            client_errors_total,
            bytes_total,
            connections_total,
            active_connections,
            backend_healthy,
            reroutes_total,
        }))
    }

    /// Count a rejected request.
    pub fn rejected(&self, reason: &str) {
        self.rejected_total.with_label_values(&[reason]).inc();
    }

    /// Render the registry in the Prometheus text format.
    pub fn encode(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        if encoder
            .encode(&self.registry.gather(), &mut buffer)
            .is_err()
        {
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }
}
