//! Configuration model, parsed from TOML.

use crate::duration;
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;

/// Top level xLB configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub timeouts: TimeoutConfig,
    #[serde(default)]
    pub pool: PoolConfig,
    pub backends: BackendsConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

/// Listener configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Address to bind, e.g. `0.0.0.0:8006`.
    pub bind: String,
    /// Forwarding mode: `l4` or `thin-l7`.
    #[serde(default = "default_mode")]
    pub mode: Mode,
    /// Hard cap on concurrent client connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Set `TCP_NODELAY` on accepted client sockets.
    #[serde(default = "default_true")]
    pub tcp_nodelay: bool,
    /// Enable `SO_REUSEPORT` so several xLB processes can share a port.
    #[serde(default)]
    pub reuseport: bool,
    /// Maximum size of a single HTTP message head in thin-l7 mode.
    #[serde(default = "default_max_header_bytes")]
    pub max_header_bytes: usize,
    /// Optional TLS termination for client connections.
    #[serde(default)]
    pub tls: TlsConfig,
}

/// Client-facing TLS termination.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TlsConfig {
    /// Terminate TLS on the client side.
    #[serde(default)]
    pub enabled: bool,
    /// PEM certificate chain.
    #[serde(default)]
    pub cert: Option<String>,
    /// PEM private key.
    #[serde(default)]
    pub key: Option<String>,
}

/// Forwarding mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// Pure TCP splice.
    L4,
    /// Parse request/response heads, then stream bodies verbatim.
    ThinL7,
}

/// Layered timeout model. Every value can be overridden per deployment.
#[derive(Debug, Clone, Deserialize)]
pub struct TimeoutConfig {
    /// Backend connection establishment.
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_connect_timeout"
    )]
    pub connect: Duration,
    /// Optional timeout waiting for the backend's first response byte (TTFB).
    /// When omitted, defaults to `idle`.
    #[serde(default, deserialize_with = "duration::deserialize_opt")]
    pub first_byte: Option<Duration>,
    /// Maximum silence on any single direction once response bytes are flowing.
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_idle_timeout"
    )]
    pub idle: Duration,
    /// Hard ceiling for a whole stream, covering long model thinking.
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_total_timeout"
    )]
    pub total: Duration,
    /// How long to wait for the opposite direction after one side half-closes.
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_fin_timeout"
    )]
    pub fin: Duration,
}

impl TimeoutConfig {
    /// Effective timeout waiting for the backend's first response byte.
    /// Falls back to [`Self::idle`] when [`Self::first_byte`] is not set.
    pub fn effective_first_byte(&self) -> Duration {
        self.first_byte.unwrap_or(self.idle)
    }
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect: default_connect_timeout(),
            first_byte: None,
            idle: default_idle_timeout(),
            total: default_total_timeout(),
            fin: default_fin_timeout(),
        }
    }
}

/// Backend connection pool settings.
#[derive(Debug, Clone, Deserialize)]
pub struct PoolConfig {
    /// Idle connections kept warm per backend.
    #[serde(default = "default_max_idle")]
    pub max_idle_per_backend: usize,
    /// Hard cap on concurrent in-flight connections per backend. `0` (default) means unlimited.
    #[serde(default)]
    pub max_conns_per_backend: usize,
    /// Maximum wait for a backend concurrency slot when `max_conns_per_backend > 0`.
    /// When omitted, defaults to `timeouts.connect`.
    #[serde(default, deserialize_with = "duration::deserialize_opt")]
    pub acquire_timeout: Option<Duration>,
    /// How long an idle connection may sit in the pool before it is discarded.
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_pool_idle_timeout"
    )]
    pub idle_timeout: Duration,
}

impl PoolConfig {
    /// Effective timeout waiting for a backend concurrency slot when
    /// [`Self::max_conns_per_backend`] is positive. Falls back to `connect`.
    pub fn effective_acquire_timeout(&self, connect: Duration) -> Duration {
        self.acquire_timeout.unwrap_or(connect)
    }
}

/// Backend selection and health checking.
#[derive(Debug, Clone, Deserialize)]
pub struct BackendsConfig {
    /// Scheduling algorithm.
    #[serde(default)]
    pub algorithm: Algorithm,
    /// Lightweight health endpoint probed by xLB.
    #[serde(default = "default_health_path")]
    pub health_check_path: String,
    /// Extra `Name: value` header lines sent on every health probe. Needed for
    /// gateways whose `/health` requires auth (e.g. LiteLLM with a master key).
    #[serde(default)]
    pub health_check_headers: Vec<String>,
    /// Probe interval. Set to `"0s"` to disable active health checking.
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_health_interval"
    )]
    pub health_interval: Duration,
    /// Timeout applied to a single probe (connect + response).
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_health_timeout"
    )]
    pub health_check_timeout: Duration,
    /// Consecutive failures before a backend is marked unhealthy.
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    /// Consecutive successes before a backend is marked healthy again.
    #[serde(default = "default_healthy_threshold")]
    pub healthy_threshold: u32,
    /// Consecutive data-plane TCP dial failures before passively ejecting a
    /// backend without waiting for the next active probe. `0` (default) disables
    /// passive ejection.
    #[serde(default)]
    pub passive_unhealthy_threshold: u32,
    /// Cooldown before a passively ejected backend is re-admitted when active
    /// health probes have not already recovered it.
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_eject_cooldown"
    )]
    pub eject_cooldown: Duration,
    /// The gateway instances.
    #[serde(default)]
    pub servers: Vec<ServerEntry>,
}

/// A single gateway instance.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerEntry {
    /// `host:port` of the gateway.
    pub addr: String,
    /// Relative weight used by the weighted schedulers.
    #[serde(default = "default_weight")]
    pub weight: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_backend: default_max_idle(),
            max_conns_per_backend: 0,
            acquire_timeout: None,
            idle_timeout: default_pool_idle_timeout(),
        }
    }
}

/// Scheduling algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Algorithm {
    /// Pick the healthy backend with the fewest in-flight streams.
    #[default]
    Leastconn,
    /// Pick healthy backends round-robin.
    Roundrobin,
}

/// Retry and pre-first-byte status rerouting policy.
#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    /// Maximum retries across backends. `-1` (the default) retries across all
    /// remaining healthy backends (`backends.servers.len() - 1`); `0` disables
    /// retries entirely.
    #[serde(default = "default_max_retries")]
    pub max_retries: i32,
    /// Optional wall-clock ceiling across all retry/reroute attempts before the
    /// first response byte is written to the client. Must be `> 0s` and
    /// `<= timeouts.total`.
    #[serde(default, deserialize_with = "duration::deserialize_opt")]
    pub timeout: Option<Duration>,
    /// HTTP status codes that trigger pre-first-byte rerouting to another
    /// healthy backend in `thin-l7` mode (default: `[402, 404, 429, 500, 502, 503, 504]`).
    #[serde(default = "default_retry_statuses")]
    pub retry_statuses: Vec<u16>,
    /// Optional narrower status list for non-idempotent requests (`POST`,
    /// `PUT`, `PATCH`, `DELETE`) when no idempotency key is accepted. Use e.g.
    /// `[402, 404, 429, 503]` to reroute only unambiguous pre-execution
    /// refusals while avoiding automatic cross-node replay on in-doubt `500`,
    /// `502`, or `504` responses that may occur after upstream acceptance.
    #[serde(default)]
    pub non_idempotent_retry_statuses: Option<Vec<u16>>,
    /// When `true`, non-idempotent requests carrying an `Idempotency-Key` or
    /// `X-Idempotency-Key` header may reroute on the full `retry_statuses` list
    /// even when `non_idempotent_retry_statuses` is configured. Defaults to
    /// `false` because heterogeneous upstream gateways do not necessarily share
    /// an idempotency ledger.
    #[serde(default)]
    pub respect_idempotency_key: bool,
    /// Maximum fixed-length request body size buffered in `thin-l7` mode so
    /// long-context prompts (up to 64 MiB by default, matching `xlite`) can be
    /// replayed across backends on retryable HTTP status codes or stale pooled
    /// keep-alive sockets.
    #[serde(default = "default_retry_max_body_bytes")]
    pub max_body_bytes: usize,
}

impl RetryConfig {
    /// Status codes eligible for pre-first-byte rerouting given request method
    /// idempotency and presence of an idempotency key header.
    pub fn effective_retry_statuses(
        &self,
        is_idempotent_method: bool,
        has_idempotency_key: bool,
    ) -> &[u16] {
        if is_idempotent_method || (self.respect_idempotency_key && has_idempotency_key) {
            &self.retry_statuses
        } else if let Some(ref statuses) = self.non_idempotent_retry_statuses {
            statuses.as_slice()
        } else {
            &self.retry_statuses
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            timeout: None,
            retry_statuses: default_retry_statuses(),
            non_idempotent_retry_statuses: None,
            respect_idempotency_key: false,
            max_body_bytes: default_retry_max_body_bytes(),
        }
    }
}

/// Metrics endpoint settings.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Expose a Prometheus `/metrics` endpoint.
    #[serde(default)]
    pub enabled: bool,
    /// Address to bind the metrics endpoint on.
    #[serde(default = "default_metrics_bind")]
    pub bind: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_metrics_bind(),
        }
    }
}

impl Config {
    /// Load and validate a configuration file.
    pub fn load(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", path.as_ref().display()))?;
        Self::from_toml(&text)
    }

    /// Parse and validate configuration from a TOML string.
    pub fn from_toml(text: &str) -> anyhow::Result<Self> {
        let config: Config = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate cross-field constraints.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.backends.servers.is_empty() {
            anyhow::bail!("at least one [[backends.servers]] entry is required");
        }
        for server in &self.backends.servers {
            if parse_addr(&server.addr).is_none() {
                anyhow::bail!("invalid backend address: {:?}", server.addr);
            }
            if server.weight == 0 {
                anyhow::bail!("backend {} has weight 0", server.addr);
            }
        }
        if self.server.bind.parse::<SocketAddr>().is_err()
            && resolve_addr(&self.server.bind).is_none()
        {
            anyhow::bail!("invalid server.bind: {:?}", self.server.bind);
        }
        if self.timeouts.connect.is_zero() {
            anyhow::bail!("timeouts.connect must be greater than zero");
        }
        if self.timeouts.idle.is_zero() {
            anyhow::bail!("timeouts.idle must be greater than zero");
        }
        if self.timeouts.fin.is_zero() {
            anyhow::bail!("timeouts.fin must be greater than zero");
        }
        if self.timeouts.total < self.timeouts.connect {
            anyhow::bail!("timeouts.total must be >= timeouts.connect");
        }
        if self.timeouts.total < self.timeouts.idle {
            anyhow::bail!("timeouts.total must be >= timeouts.idle");
        }
        if let Some(first_byte) = self.timeouts.first_byte {
            if first_byte.is_zero() {
                anyhow::bail!("timeouts.first_byte must be greater than zero");
            }
            if self.timeouts.total < first_byte {
                anyhow::bail!("timeouts.total must be >= timeouts.first_byte");
            }
        }
        if let Some(retry_timeout) = self.retry.timeout {
            if retry_timeout.is_zero() {
                anyhow::bail!("retry.timeout must be greater than zero");
            }
            if self.timeouts.total < retry_timeout {
                anyhow::bail!("timeouts.total must be >= retry.timeout");
            }
        }
        if let Some(acquire_timeout) = self.pool.acquire_timeout {
            if acquire_timeout.is_zero() {
                anyhow::bail!("pool.acquire_timeout must be greater than zero");
            }
        }
        if self.backends.passive_unhealthy_threshold > 0 && self.backends.eject_cooldown.is_zero() {
            anyhow::bail!(
                "backends.eject_cooldown must be greater than zero when passive_unhealthy_threshold > 0"
            );
        }
        for header in &self.backends.health_check_headers {
            if header.contains(['\r', '\n']) || !header.contains(':') {
                anyhow::bail!(
                    "invalid backends.health_check_headers entry (expected `Name: value`, no CR/LF): {:?}",
                    header
                );
            }
        }
        if self.server.tls.enabled
            && (self.server.tls.cert.is_none() || self.server.tls.key.is_none())
        {
            anyhow::bail!("server.tls.enabled requires both server.tls.cert and server.tls.key");
        }
        Ok(())
    }

    /// Resolve the configured bind address.
    pub fn bind_addr(&self) -> anyhow::Result<SocketAddr> {
        resolve_addr(&self.server.bind)
            .ok_or_else(|| anyhow::anyhow!("cannot resolve server.bind {:?}", self.server.bind))
    }

    /// Effective retry budget for a request. `-1` expands to
    /// `backends.servers.len().saturating_sub(1)` so every healthy backend can
    /// be tried at most once; `0` disables retries; a positive integer caps
    /// retries at that count.
    pub fn effective_max_retries(&self) -> u32 {
        if self.retry.max_retries < 0 {
            self.backends.servers.len().saturating_sub(1) as u32
        } else {
            self.retry.max_retries as u32
        }
    }
}

/// Parse a literal socket address.
pub fn parse_addr(raw: &str) -> Option<SocketAddr> {
    raw.parse::<SocketAddr>().ok()
}

/// Resolve a socket address, falling back to DNS for host names.
pub fn resolve_addr(raw: &str) -> Option<SocketAddr> {
    use std::net::ToSocketAddrs;
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Some(addr);
    }
    raw.to_socket_addrs().ok().and_then(|mut it| it.next())
}

fn default_mode() -> Mode {
    Mode::ThinL7
}
fn default_max_retries() -> i32 {
    -1
}
fn default_retry_statuses() -> Vec<u16> {
    vec![402, 404, 429, 500, 502, 503, 504]
}
fn default_retry_max_body_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_max_connections() -> usize {
    100_000
}
fn default_true() -> bool {
    true
}
fn default_max_header_bytes() -> usize {
    64 * 1024
}
fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(600)
}
fn default_total_timeout() -> Duration {
    Duration::from_secs(3600)
}
fn default_fin_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_max_idle() -> usize {
    64
}
fn default_pool_idle_timeout() -> Duration {
    Duration::from_secs(60)
}
fn default_health_path() -> String {
    "/health".to_string()
}
fn default_health_interval() -> Duration {
    Duration::from_secs(2)
}
fn default_health_timeout() -> Duration {
    Duration::from_secs(2)
}
fn default_unhealthy_threshold() -> u32 {
    2
}
fn default_healthy_threshold() -> u32 {
    2
}
fn default_eject_cooldown() -> Duration {
    Duration::from_secs(5)
}
fn default_weight() -> u32 {
    1
}
fn default_metrics_bind() -> String {
    "0.0.0.0:9106".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[server]
bind = "0.0.0.0:8006"
mode = "l4"

[timeouts]
connect = "5s"
idle = "600s"
total = "3600s"
fin = "30s"

[pool]
max_idle_per_backend = 64
idle_timeout = "60s"

[backends]
algorithm = "leastconn"
health_check_path = "/health"
health_interval = "2s"

[[backends.servers]]
addr = "127.0.0.1:8001"
weight = 1

[[backends.servers]]
addr = "127.0.0.1:8002"
weight = 2

[retry]
max_retries = 0
"#;

    #[test]
    fn parses_sample() {
        let config = Config::from_toml(SAMPLE).unwrap();
        assert_eq!(config.server.mode, Mode::L4);
        assert_eq!(config.timeouts.first_byte, None);
        assert_eq!(
            config.timeouts.effective_first_byte(),
            Duration::from_secs(600)
        );
        assert_eq!(config.timeouts.idle, Duration::from_secs(600));
        assert_eq!(config.backends.servers.len(), 2);
        assert_eq!(config.backends.algorithm, Algorithm::Leastconn);
        assert_eq!(config.backends.servers[1].weight, 2);
        assert_eq!(config.pool.max_idle_per_backend, 64);
        assert_eq!(config.pool.max_conns_per_backend, 0);
        assert_eq!(
            config
                .pool
                .effective_acquire_timeout(config.timeouts.connect),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn validates_pool_acquire_timeout() {
        let valid = r#"
[server]
bind = "0.0.0.0:8006"
[pool]
max_conns_per_backend = 128
acquire_timeout = "2s"
[backends]
[[backends.servers]]
addr = "127.0.0.1:8001"
"#;
        let cfg = Config::from_toml(valid).unwrap();
        assert_eq!(cfg.pool.max_conns_per_backend, 128);
        assert_eq!(
            cfg.pool.effective_acquire_timeout(cfg.timeouts.connect),
            Duration::from_secs(2)
        );

        let zero = r#"
[server]
bind = "0.0.0.0:8006"
[pool]
max_conns_per_backend = 128
acquire_timeout = "0s"
[backends]
[[backends.servers]]
addr = "127.0.0.1:8001"
"#;
        assert!(Config::from_toml(zero).is_err());
    }

    #[test]
    fn validates_health_check_headers() {
        let cfg = |headers: &str| {
            format!(
                r#"
[server]
bind = "0.0.0.0:8006"
[backends]
health_check_headers = [{headers}]
[[backends.servers]]
addr = "127.0.0.1:8001"
"#
            )
        };
        assert!(Config::from_toml(&cfg(r#""Authorization: Bearer sk-x""#)).is_ok());
        assert_eq!(
            Config::from_toml(&cfg(r#""Authorization: Bearer sk-x""#))
                .unwrap()
                .backends
                .health_check_headers,
            vec!["Authorization: Bearer sk-x".to_string()]
        );
        assert!(Config::from_toml(&cfg(r#""X-Evil: y\r\nInjected: z""#)).is_err());
        assert!(Config::from_toml(&cfg(r#""NoColonHeader""#)).is_err());
    }

    #[test]
    fn validates_first_byte_timeout() {
        let valid = r#"
[server]
bind = "0.0.0.0:8006"
[timeouts]
first_byte = "30s"
idle = "15s"
total = "600s"
[backends]
[[backends.servers]]
addr = "127.0.0.1:8001"
"#;
        let cfg = Config::from_toml(valid).unwrap();
        assert_eq!(cfg.timeouts.effective_first_byte(), Duration::from_secs(30));
        assert_eq!(cfg.timeouts.idle, Duration::from_secs(15));

        let zero = r#"
[server]
bind = "0.0.0.0:8006"
[timeouts]
first_byte = "0s"
idle = "15s"
total = "600s"
[backends]
[[backends.servers]]
addr = "127.0.0.1:8001"
"#;
        assert!(Config::from_toml(zero).is_err());

        let exceeds_total = r#"
[server]
bind = "0.0.0.0:8006"
[timeouts]
first_byte = "700s"
idle = "15s"
total = "600s"
[backends]
[[backends.servers]]
addr = "127.0.0.1:8001"
"#;
        assert!(Config::from_toml(exceeds_total).is_err());
    }

    #[test]
    fn validates_retry_timeout_and_idempotency_policy() {
        let valid = r#"
[server]
bind = "0.0.0.0:8006"
[timeouts]
idle = "30s"
total = "60s"
[backends]
[[backends.servers]]
addr = "127.0.0.1:8001"
[retry]
timeout = "10s"
retry_statuses = [402, 404, 429, 500, 502, 503, 504]
non_idempotent_retry_statuses = [402, 404, 429, 503]
respect_idempotency_key = true
"#;
        let cfg = Config::from_toml(valid).unwrap();
        assert_eq!(cfg.retry.timeout, Some(Duration::from_secs(10)));
        assert_eq!(
            cfg.retry.effective_retry_statuses(false, false),
            &[402, 404, 429, 503]
        );
        assert_eq!(
            cfg.retry.effective_retry_statuses(false, true),
            &[402, 404, 429, 500, 502, 503, 504]
        );
        assert_eq!(
            cfg.retry.effective_retry_statuses(true, false),
            &[402, 404, 429, 500, 502, 503, 504]
        );

        let zero = r#"
[server]
bind = "0.0.0.0:8006"
[backends]
[[backends.servers]]
addr = "127.0.0.1:8001"
[retry]
timeout = "0s"
"#;
        assert!(Config::from_toml(zero).is_err());

        let exceeds_total = r#"
[server]
bind = "0.0.0.0:8006"
[timeouts]
connect = "1s"
idle = "5s"
total = "10s"
[backends]
[[backends.servers]]
addr = "127.0.0.1:8001"
[retry]
timeout = "20s"
"#;
        assert!(Config::from_toml(exceeds_total).is_err());
    }

    #[test]
    fn rejects_empty_backends() {
        let text = r#"
[server]
bind = "0.0.0.0:8006"
[backends]
"#;
        assert!(Config::from_toml(text).is_err());
    }
}
