# xFlux

**xFlux** is a streaming-first load balancer for AI inference gateways. It sits
in front of one or more gateway instances and
forwards long-lived, non-idempotent streaming POSTs with as little added latency
as possible.

xFlux occupies the HAProxy role: it forwards bytes, not tokens. Model selection,
token accounting, prompt rewriting and protocol translation stay in the
backends. xFlux only does connection management, scheduling, health checking,
timeouts and transparent forwarding.

> Why *xFlux*? A short name for the thing that matters here: a long-lived,
> continuously flowing stream in the X inference stack.

## What is implemented

| Area | Status |
|------|--------|
| L4 TCP splice (zero parsing) | ✅ default |
| Thin-L7 (head parsing, body streaming) | ✅ request-level connection reuse |
| Zero response/request buffering | ✅ bodies are copied in bounded chunks |
| Backend connection pool for all methods | ✅ LIFO checkout; stale-socket redial; optional per-backend concurrency cap (`max_conns_per_backend`) |
| Layered timeouts (connect / first_byte / idle / total / fin) | ✅ |
| Least-connections scheduling (weighted) | ✅ plus round-robin |
| Health checks on `/health` + passive dial ejection | ✅ hysteresis, never touches live streams |
| Retry only before response bytes | ✅ default 0 |
| Prometheus metrics + TTFT delta | ✅ |
| Graceful shutdown, `SO_REUSEPORT` | ✅ |
| Auto L4 fallback for non-HTTP/1.1 traffic | ✅ thin-l7 sniffs HTTP/2, TLS, upgrades, binary |
| Optional client-facing TLS termination | ✅ rustls, HTTP/1.1 ALPN |

## Modes and the connection-reuse tradeoff

xFlux has two forwarding modes:

- **`l4`** (default) — a pure TCP splice. No HTTP parsing overhead, so TTFT
  noise is minimal. A client connection is mapped to one backend connection and
  spliced with `copy_bidirectional`. Because a plain L4 proxy cannot tell where
  an HTTP response ends, it cannot safely return a backend connection to the
  pool, so L4 provides connection reuse only for the lifetime of a single client
  connection.
- **`thin-l7`** — parses only the request/response *heads* with `httparse`,
  then streams bodies verbatim. Knowing where a response ends is what makes
  request-level pooling correct: a completed response returns its backend
  connection to the idle pool, and the next request on a client keep-alive
  connection can be served by any backend. This is the mode that delivers the
  "reuse backend connections for POST" constraint *across* client connections.

Both modes are zero-buffering: bytes are moved in 16 KiB chunks and written
straight through; nothing is aggregated, transformed or decoded.

### Automatic L4 fallback (thin-l7 only)

thin-l7 only understands HTTP/1.1. If the first bytes of a connection look like
something else, xFlux stops parsing and transparently switches to the raw L4
splice, forwarding the bytes it already buffered and then shuttling the rest:

- first byte is not an uppercase HTTP method character (TLS records, binary);
- the HTTP/2 connection preface (`PRI * HTTP/2.0`);
- a request that fails to parse as HTTP/1.1;
- `CONNECT` or any request carrying an `Upgrade` header (WebSocket, h2c).

The fallback decision is logged and the stream continues byte-for-byte, so a
client never notices. In l4 mode all of these are passed through natively.

### TLS termination

xFlux can optionally terminate TLS for clients (`[server.tls]`). Client streams
are then decrypted and handed to the normal forwarding path, so HTTPS clients
reach plaintext gateways without a sidecar. Only HTTP/1.1 is advertised via
ALPN. TLS is terminated in Rust with rustls (ring provider); the certificate and
key are read from PEM files at startup.

Note that TLS termination and `l4` mode are independent: enabling TLS only
affects the client→xFlux leg. With `mode = "l4"` xFlux still splices the
decrypted bytes to the backend; with `mode = "thin-l7"` it additionally gets
request-level connection reuse.

## Architecture

```
src/
├── main.rs          CLI, tracing, shutdown signals
├── config.rs        TOML model, duration parsing, validation
├── server.rs        listener, accept loop, limits, TLS acceptor, metrics endpoint
├── backend.rs       Backend, connection pool, weighted leastconn scheduler
├── pool.rs          idle connection pool with liveness probing
├── health.rs        active health probes + hysteresis
├── metrics.rs       Prometheus registry
└── proxy/
    ├── mod.rs       dispatch + idle/deadline read/write helpers
    ├── l4.rs        bidirectional splice with activity watchdog (also the fallback)
    ├── l7.rs        HTTP/1.1 framing + protocol sniff + auto L4 fallback
    └── http1.rs     head parsing, chunked scanner, body streaming
```

## Quick start

```bash
cargo build --release
./target/release/xflux --config config.toml
# or validate only
./target/release/xflux --check --config config.toml
```

Example `config.toml`:

```toml
[server]
bind = "0.0.0.0:8006"
mode = "thin-l7"          # l4 | thin-l7
max_connections = 100000
reuseport = false

# [server.tls]              # optional client-facing TLS termination
# enabled = true
# cert = "/etc/xflux/tls/fullchain.pem"
# key = "/etc/xflux/tls/privkey.pem"

[timeouts]
connect = "5s"            # backend connection establishment
# first_byte = "60s"      # optional TTFB limit before first response byte (defaults to idle)
idle = "600s"             # max silence per direction once streaming
total = "3600s"           # hard ceiling for a whole stream
fin = "30s"               # wait after a half-close in l4

[pool]
max_idle_per_backend = 64
# max_conns_per_backend = 0   # optional per-backend in-flight cap (0 = unlimited)
# acquire_timeout = "5s"      # max wait for a backend slot (defaults to timeouts.connect)
idle_timeout = "60s"

[backends]
algorithm = "leastconn"   # leastconn | roundrobin
health_check_path = "/health"
health_interval = "2s"    # "0s" disables active checking
health_check_timeout = "2s"
unhealthy_threshold = 2
healthy_threshold = 2
# passive_unhealthy_threshold = 0  # optional consecutive dial failures before immediate ejection
# eject_cooldown = "5s"            # cooldown before passively ejected backend is re-admitted

[[backends.servers]]
addr = "10.56.88.32:8001"
weight = 1

[[backends.servers]]
addr = "10.56.88.33:8002"
weight = 1

[retry]
max_retries = 0           # only ever applies before any response byte

[metrics]
enabled = true
bind = "0.0.0.0:9106"
```

## Metrics

Every metric is exposed on `GET /metrics`.

| Metric | Type | Meaning |
|--------|------|---------|
| `xflux_ttft_delta_ms` | histogram | Proxy-added first-byte latency (TTFB, not first token) |
| `xflux_upstream_ttft_ms` | histogram | Request fully sent → backend first byte (TTFB) |
| `xflux_client_ttft_ms` | histogram | Request fully sent → client first byte (TTFB) |
| `xflux_stream_duration_seconds` | histogram | Whole stream duration |
| `xflux_active_streams{backend}` | gauge | In-flight streams |
| `xflux_conn_reuse_total{backend}` | counter | Connections taken from the idle pool |
| `xflux_conn_new_total{backend}` | counter | Connections freshly dialed |
| `xflux_conn_dial_errors_total{backend}` | counter | Failed dials |
| `xflux_rejected_total{reason}` | counter | `unhealthy`, `queue_full`, `timeout`, `bad_request` |
| `xflux_health_check_failures{backend}` | counter | Failed probes |
| `xflux_health_check_success_total{backend}` | counter | Successful probes |
| `xflux_backend_healthy{backend}` | gauge | 1 healthy / 0 unhealthy |
| `xflux_upstream_errors_total{backend}` | counter | Upstream transfer failures |
| `xflux_bytes_total{backend,direction}` | counter | Bytes proxied |
| `xflux_connections_total` | counter | Client connections accepted |
| `xflux_active_connections` | gauge | Open client connections |

Logs are structured via `tracing`; set `XFLUX_LOG_JSON=1` for JSON and
`RUST_LOG` for the filter.

## Design rules enforced by the code

1. **Never buffer a body.** No intermediate aggregation; `forward_body` copies
   one bounded read at a time.
2. **Reuse is only claimed when it is safe.** A backend connection is pooled
   only after a self-delimiting response (Content-Length or a complete chunked
   body) has been fully streamed and no leftover bytes remain.
3. **Non-idempotent requests are never retried after bytes flow.** Retries apply
   to connection establishment only.
4. **Timeouts are layered** and per-direction for L4; the idle timeout covers
   model thinking without killing long streams.
5. **Health failures only affect new scheduling decisions.** Existing streams
   continue to completion.

## Tests

```bash
cargo test
```

Integration tests start real mock gateways and a real xFlux server and cover
pass-through, half-close, chunked streaming, backend connection reuse,
pipelined requests, abrupt backend death, least-connections scheduling,
automatic L4 fallback (HTTP/2 preface, binary protocols, upgrades) and TLS
termination end-to-end (including backend reuse over HTTPS).

## Minimal verification

```bash
# direct
curl -o /dev/null -w 'direct ttft=%{time_starttransfer}s\n' \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock","stream":true}' \
  http://10.56.88.32:8001/v1/chat/completions

# through xflux (thin-l7 so the pool is exercised)
curl -o /dev/null -w 'xflux  ttft=%{time_starttransfer}s\n' \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock","stream":true}' \
  http://10.56.88.31:8006/v1/chat/completions
```

See [docs/design.md](docs/design.md) for the full design rationale,
[docs/architecture.md](docs/architecture.md) for the code-level architecture,
[docs/changelog.md](docs/changelog.md) for release notes, and [AGENTS.md](AGENTS.md)
for the development conventions.
