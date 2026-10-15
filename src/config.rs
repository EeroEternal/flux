//! Configuration model, parsed from TOML.

use crate::duration;
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;

/// Top level Flux configuration.
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
    /// Enable `SO_REUSEPORT` so several Flux processes can share a port.
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
    /// Maximum silence on any single direction before the stream is aborted.
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

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect: default_connect_timeout(),
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
    /// How long an idle connection may sit in the pool before it is discarded.
    #[serde(
        deserialize_with = "duration::deserialize",
        default = "default_pool_idle_timeout"
    )]
    pub idle_timeout: Duration,
}

/// Backend selection and health checking.
#[derive(Debug, Clone, Deserialize)]
pub struct BackendsConfig {
    /// Scheduling algorithm.
    #[serde(default)]
    pub algorithm: Algorithm,
    /// Lightweight health endpoint probed by Flux.
    #[serde(default = "default_health_path")]
    pub health_check_path: String,
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

/// Retry policy. Retries only ever apply to connection establishment, before
/// any response byte. Each attempt redispatches to a different healthy backend
/// when one is available.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RetryConfig {
    /// Maximum connection-establishment retries. `0` disables retries entirely.
    #[serde(default)]
    pub max_retries: u32,
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
        if self.timeouts.idle.is_zero() {
            anyhow::bail!("timeouts.idle must be greater than zero");
        }
        if self.timeouts.total < self.timeouts.idle {
            anyhow::bail!("timeouts.total must be >= timeouts.idle");
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
    Mode::L4
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
        assert_eq!(config.timeouts.idle, Duration::from_secs(600));
        assert_eq!(config.backends.servers.len(), 2);
        assert_eq!(config.backends.algorithm, Algorithm::Leastconn);
        assert_eq!(config.backends.servers[1].weight, 2);
        assert_eq!(config.pool.max_idle_per_backend, 64);
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
