# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Write bullets that a human can ship from: **what changed**, **who it affects**,
**breaking or not**. Do not paste commit titles, `misc`, or empty sections.

## [Unreleased]

### Changed

- **Renamed project to xFlux (`xflux`)**: crate, binary (`xflux`), server type
  (`XFluxServer`), health probe `User-Agent` (`xflux-health`), JSON log env var
  (`XFLUX_LOG_JSON`), default log filter (`xflux=info`), and all Prometheus
  metric names (`xflux_*`) are unified under the `xflux` namespace.
- Documented the HAProxy-like LB boundary: xFlux inspects HTTP/TCP framing only
  and never payload semantics (SSE, JSON, tokens). Metric help strings now say
  TTFB, not token-aware TTFT.

### Fixed

- First-byte histograms are recorded only on the first response head, not again
  after a 1xx informational response.
- Connect retries redispatch to a different healthy backend when one is
  available, instead of leastconn repeatedly picking the same refused server.

## [0.0.1] - 2026-10-14

### Added

- **L4 forwarding** (`mode = "l4"`, default): pure TCP splice built on
  `copy_bidirectional` with an activity watchdog. Zero HTTP parsing, zero body
  buffering, half-close aware. One client connection maps to one backend
  connection. Applies to any protocol (HTTP/1.1, HTTP/2, WebSocket, gRPC, TLS).
- **Thin-L7 forwarding** (`mode = "thin-l7"`): parses only the request/response
  heads with `httparse` and streams bodies verbatim. Because response
  boundaries are known, completed responses return their backend connection to
  the idle pool, enabling request-level connection reuse for POST as well.
- **Automatic L4 fallback** (thin-l7): TLS/binary first bytes, the HTTP/2
  preface, `Upgrade`/`CONNECT` requests and unparseable heads switch the
  connection to the raw splice, forwarding already-buffered bytes first. The
  client observes no protocol change.
- **Backend connection pool** (`src/pool.rs`): per-backend idle pool, idle
  timeout decoupled from stream duration, non-blocking liveness probing on both
  acquire and release. Applies to all HTTP methods.
- **Layered timeouts**: `connect`, per-direction `idle`, whole-stream `total`,
  and half-close `fin`, each independently configurable.
- **Weighted least-connections scheduler** plus round-robin, with health-aware
  filtering and an RAII in-flight `Lease`.
- **Active health checking** on a lightweight path with unhealthy/healthy
  hysteresis; never touches in-flight streams.
- **Optional client-facing TLS termination** via rustls (ring provider), PEM
  cert/key, `http/1.1` ALPN. Orthogonal to the forwarding mode.
- **Prometheus metrics**: `flux_ttft_delta_ms`, `flux_upstream_ttft_ms`,
  `flux_client_ttft_ms`, `flux_stream_duration_seconds`,
  `flux_active_streams`, `flux_conn_reuse_total`, `flux_conn_new_total`,
  `flux_conn_dial_errors_total`, `flux_rejected_total`,
  `flux_health_check_failures`, `flux_health_check_success_total`,
  `flux_backend_healthy`, `flux_upstream_errors_total`, `flux_bytes_total`,
  `flux_connections_total`, `flux_active_connections`. Served on `GET /metrics`.
- **Operations**: single binary, TOML config, `--check` validation, graceful
  shutdown on SIGINT/SIGTERM, `SO_REUSEPORT`.
- **Agent governance**: `AGENTS.md`, `docs/ai/agents/`, and
  `.agents/skills/` imported from console-kit conventions, plus a CI workflow
  mirroring the local gates.
