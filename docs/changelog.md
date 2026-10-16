# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Write bullets that a human can ship from: **what changed**, **who it affects**,
**breaking or not**. Do not paste commit titles, `misc`, or empty sections.

## [Unreleased]

## [0.0.3] - 2026-10-16

### Added

- **Prebuilt release binaries & `Dockerfile`**: added GitHub Actions release
  pipeline ([`.github/workflows/release.yml`](../.github/workflows/release.yml))
  publishing prebuilt tarballs and SHA-256 checksums for Linux (`x86_64` /
  `aarch64` musl) and macOS (`aarch64` / `x86_64`) on `v*.*.*` tags, plus a
  multi-stage [`Dockerfile`](../Dockerfile). Breaking: No.
- **MIT `LICENSE`, vector topology diagram & observability guide**: added root
  [`LICENSE`](../LICENSE), SVG architecture diagram
  ([`docs/assets/architecture.svg`](assets/architecture.svg)), and dedicated
  observability/verification reference
  ([`docs/observability.md`](observability.md)), while streamlining
  [`README.md`](../README.md) with HAProxy comparison and minimal quick-start
  configuration. Breaking: No.

### Changed

- **Renamed project to xLB (`xlb`)**: crate, binary (`xlb`), server type
  (`XlbServer`), health probe `User-Agent` (`xlb-health`), JSON log env var
  (`XLB_LOG_JSON`), default log filter (`xlb=info`), and all Prometheus metric
  names (`xlb_*`) are unified under the `xlb` namespace, with all documentation
  and agent specifications translated to English. Breaking: Yes
  (replaces `xflux` / `XFLUX_LOG_JSON` / `xflux_*`).

## [0.0.2] - 2026-10-16

### Added

- **Passive dial-failure ejection (`backends.passive_unhealthy_threshold` & `backends.eject_cooldown`)**:
  optional data-plane circuit breaking (default `0` = disabled) that marks a
  backend unhealthy after N consecutive TCP dial failures without waiting for
  the next active `/health` tick, purges its idle connection pool, and
  automatically re-admits it after `eject_cooldown` (default `5s`) unless
  active health probes have marked it unhealthy (`src/backend.rs`,
  `src/config.rs`). Breaking: No.
- **Stale pooled connection redial (`thin-l7`)**: when a pooled keep-alive
  connection (`reused = true`) races with a backend keep-alive close (failing on
  request write or returning an immediate 0-byte EOF/RST before any response
  byte), `thin-l7` transparently dials a fresh connection once on the same lease
  instead of surfacing a broken stream to the client (`src/proxy/l7.rs`).
  Breaking: No.
- **Per-backend concurrency cap (`pool.max_conns_per_backend` & `pool.acquire_timeout`)**:
  optional per-backend in-flight connection limit (default `0` = unlimited) with
  bounded queue wait (`pool.acquire_timeout`, defaulting to `timeouts.connect`)
  and capacity-aware scheduling spillover across healthy backends. When slot
  acquisition times out, xFlux increments
  `xflux_rejected_total{reason="queue_full"}` and returns `503 Service Unavailable`
  in `thin-l7` mode (`src/backend.rs`, `src/proxy/l4.rs`, `src/proxy/l7.rs`).
  Breaking: No.
- **Phased stream timeouts (`timeouts.first_byte` vs `timeouts.idle`)**: optional
  `timeouts.first_byte` decouples the pre-response TTFB budget from the in-stream
  chunk silence timeout (`timeouts.idle`, which remains the fallback when
  `first_byte` is omitted). In `thin-l7`, a timeout before any response byte is
  written to the client returns `504 Gateway Timeout` and increments
  `xflux_rejected_total{reason="timeout"}` (`src/config.rs`, `src/proxy/l4.rs`,
  `src/proxy/l7.rs`). Breaking: No.
- **Local pre-push CI hook & console-kit skills**: added `.githooks/pre-push`
  and `scripts/install-hooks.sh` to run `scripts/local-gates.sh` automatically
  before `git push`, plus path-filtered CI in `.github/workflows/ci.yml` and
  `git-stash-safe`, `promote-lesson`, and `ci-concurrency-optimization` skills
  under `.agents/skills/`. Breaking: No.

### Changed

- **LIFO idle connection pooling**: `ConnPool` now checks out the most recently
  returned connection first (`pop_back`) to maximize socket warmth and reduce
  peer keep-alive close races, while evicting expired/oldest connections from
  the front (`pop_front`) (`src/pool.rs`). Breaking: No.
- **Renamed project to xFlux (`xflux`)**: crate, binary (`xflux`), server type
  (`XFluxServer`), health probe `User-Agent` (`xflux-health`), JSON log env var
  (`XFLUX_LOG_JSON`), default log filter (`xflux=info`), and all Prometheus
  metric names (`xflux_*`) are unified under the `xflux` namespace.
  Breaking: Yes (binary name, `XFLUX_LOG_JSON` env var, and `xflux_*` Prometheus
  metric prefix replace `flux` / `FLUX_LOG_JSON` / `flux_*`).
- **Documented HAProxy-like LB boundary**: xFlux inspects HTTP/TCP framing only
  and never payload semantics (SSE, JSON, tokens). Metric help strings now state
  TTFB rather than token-aware TTFT (`AGENTS.md`, `docs/`). Breaking: No.

### Fixed

- First-byte histograms are recorded only on the first response head, not again
  after a 1xx informational response (`src/proxy/l7.rs`). Breaking: No.
- Connect retries redispatch to a different healthy backend when one is
  available, instead of leastconn repeatedly picking the same refused server
  (`src/backend.rs`, `src/proxy/l4.rs`). Breaking: No.
- `504 Gateway Timeout` error responses in `thin-l7` use a fresh write deadline
  so the status line is still delivered when `first_byte` reaches `total`
  (`src/proxy/l7.rs`). Breaking: No.

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
