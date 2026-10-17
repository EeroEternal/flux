# xLB: AI Streaming Load Balancer Design Document

> Name: **xLB** (X Load Balancer). Short, self-explanatory, and aligned with
> the X inference stack (`xLB`, `xRouter`, `xlite`, `Xinference`). At the heart
> of an AI gateway is not a short request/response transaction, but a
> long-lived stream emitted chunk by chunk.

## 1. Overview

xLB is a minimal, streaming-first load balancer built for AI inference
gateways. Its sole responsibility is to forward long-lived, non-idempotent
streaming POST requests to backend gateway instances with minimal latency and
exact framing semantics.

It is neither a general-purpose L7 proxy nor a model-routing/billing gateway.
Model selection, token accounting, prompt rewriting, and protocol conversion
stay in the backend gateways. xLB focuses exclusively on connection
management, scheduling, health checking, and transparent byte forwarding.

## 2. Goals and Non-Goals

### Goals
- Near-zero first-byte overhead (direct vs. via-xLB TTFB delta within a few milliseconds)
- Backend connection reuse across all HTTP methods, including `POST`
- Zero buffering of request or response bodies
- Timeouts tailored for long-running streams (default `idle >= 600s`)
- Never automatically retry a non-idempotent request once response bytes have flowed
- Schedule by in-flight connection load (`leastconn`)
- First-class observability: per-hop TTFB delta, connection reuse rate, and stream lifecycle

### Non-Goals
- No model-aware routing; no billing, authentication, or content rewriting; no request body aggregation or response body reassembly
- Never parse SSE / NDJSON / JSON payloads; never treat `[DONE]` or token boundaries as forwarding, pooling, or scheduling conditions
- No automatic replay of generative requests; not a full-featured API gateway
- xLB occupies the HAProxy position: it forwards bytes, not tokens

## 3. Core Constraints

1. **Zero response body buffering**: response bytes read from a backend are written immediately to the client; in `thin-l7` mode, fixed-length request bodies up to `retry.max_body_bytes` (default `64 MiB`) are buffered only until final response headers arrive (and freed immediately before response body streaming) so pre-first-byte status rerouting and stale pooled socket recovery can replay the request.
2. **All-method connection reuse**: including `POST`. Connection pools are maintained per backend address with idle timeouts decoupled from stream duration.
3. **Layered timeouts**: short connection establishment (`5s`), optional pre-response `first_byte` budget, long in-stream silence (`600s`), longer total stream ceiling (`3600s`), and independent half-close (`fin`, `30s`).
4. **Failure semantics**: retries and status reroutes apply strictly **before any response byte is written to the client** (`retry.max_retries` default `-1`, trying all remaining healthy backends; `0` disables retries). In `thin-l7` mode, responses matching `retry.retry_statuses` (default `[402, 404, 429, 500, 502, 503, 504]`) drain the upstream error body and replay onto another healthy backend. Once response bytes begin flowing to the client, failures close both ends immediately without replay.
5. **Scheduling**: weighted `leastconn` by default.
6. **Health checking**: probes a lightweight endpoint (`GET /health`) out-of-band without disturbing in-flight streams.

## 4. Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Client    │────▶│      xLB       │────▶│  AI Gateway(s)  │
│  (k6 / app) │◀────│  (L4 or thin L7) │◀────│  backends       │
└─────────────┘     └──────────────────┘     └─────────────────┘
                           │
                    ┌──────┴──────┐
                    │  Conn Pool  │
                    │  Scheduler  │
                    │  Health     │
                    │  Metrics    │
                    └─────────────┘
```

### 4.1 Forwarding Modes

- **Default Thin-L7 (`mode = "thin-l7"`)**: parses only the HTTP/1.1 request/response heads and streams response bodies verbatim. Because message boundaries (`Content-Length` or `chunked`) are known, completed responses safely return the backend connection to the idle pool for request-level reuse, and retryable pre-first-byte HTTP status codes (`retry.retry_statuses`) can be rerouted across heterogeneous backends.
- **L4 (`mode = "l4"`)**: pure TCP splice with zero HTTP parsing overhead and minimal first-byte noise. Tradeoff: no header-level inspection, no status-code rerouting, and no cross-client backend connection reuse.

### 4.1.1 Protocol Sniffing and Automatic L4 Fallback

`thin-l7` speaks HTTP/1.1. When the first byte is not an uppercase ASCII method character (e.g., TLS record or binary protocol), when an HTTP/2 connection preface (`PRI * HTTP/2.0`) is detected, when headers cannot be parsed as HTTP/1.1, or when `CONNECT` / `Upgrade` (WebSocket, h2c) is requested, xLB immediately stops parsing and hands the buffered bytes plus the live sockets to the raw L4 splice. The client observes no protocol interruption.

### 4.1.2 Client-Side TLS Termination (Optional)

When `[server.tls]` is enabled, xLB terminates client TLS via `rustls` and passes the plaintext stream into the configured forwarding mode (`l4` or `thin-l7`), allowing HTTPS clients to reach plaintext backend gateways. ALPN advertises `http/1.1`. TLS termination is orthogonal to the forwarding mode: `l4` performs raw byte splicing, while `thin-l7` additionally provides request-level backend connection pooling.

### 4.2 Key Components

| Component | Responsibility | Key Details |
|-----------|----------------|-------------|
| Listener | Accept client sockets | `TCP_NODELAY`, backlog, `SO_REUSEPORT` |
| Scheduler | Pick backend | Weighted `leastconn` + health + capacity awareness; optional `roundrobin` |
| ConnPool | Reuse backend sockets | Per-backend LIFO idle pool; idle timeout decoupled from active stream duration |
| Proxy | Bidirectional byte transfer | Zero response buffering, pre-first-byte status rerouting (`thin-l7`), half-close handling, layered timeouts |
| Health | Liveness probing | Out-of-band `/health` task + optional passive dial-failure circuit breaking |
| Metrics | Observability | TTFB delta, connection reuse rate, status reroutes (`xlb_reroutes_total`), stream duration, rejection reasons |

## 5. Key Mechanisms

### 5.1 Connection Pool and Reuse
- Connections are checked out in **LIFO (last-in, first-out)** order so the warmest recently returned socket is reused first, validated with a non-blocking read probe (discarding sockets with EOF or unexpected leftover bytes) while reaping expired connections from the front of the queue.
- **Stale pooled socket redial and failover (`thin-l7`)**: when a pooled connection (`reused = true`) fails while writing the request head, or when a bodiless/inline-body request encounters a peer keep-alive close race or backend crash (0-byte EOF or RST before any response byte), xLB transparently dials a fresh connection on the same backend and, if that dial fails, fails over to another healthy backend within `retry.max_retries`.
- Non-upgrade HTTP/1.1 requests carrying `Connection: close` from short-lived clients have `close` rewritten to `keep-alive` on the backend leg so the backend socket can still be returned to the idle pool after a self-delimiting response while closing the client socket as requested.
- After a stream finishes at a proven HTTP message boundary with no leftover bytes (`thin-l7` only), the backend socket is returned to the pool; if the pool is at `max_idle_per_backend`, the oldest idle socket at the front is evicted.
- Optional per-backend concurrency cap `pool.max_conns_per_backend` (default `0` = unlimited): bounds concurrent in-flight streams per backend via an RAII semaphore permit. When waiting for a slot exceeds `pool.acquire_timeout` (defaults to `timeouts.connect`), xLB increments `xlb_rejected_total{reason="queue_full"}` and returns `503 Service Unavailable` in `thin-l7`.
- `POST` and `GET` are treated identically for connection reuse.

### 5.2 Zero-Buffer Forwarding
- **L4**: `tokio::io::copy_bidirectional` paired with an activity watchdog enforcing `first_byte` and per-direction `idle` timeouts.
- **Thin-L7**: after parsing the HTTP head, the response body is forwarded chunk-by-chunk through a bounded 16 KiB read buffer without aggregation; fixed-length request bodies buffered for pre-first-byte replay are freed immediately upon receiving the final response head before response body streaming begins.

### 5.3 Timeout Model
`connect` (`5s`), optional `first_byte` (pre-response TTFB budget, defaulting to `idle` when omitted; returns `504 Gateway Timeout` in `thin-l7` if exceeded before any response byte is written to the client), `idle` (`600s` in-stream silence), `total` (`3600s`), and `fin` (`30s`).

### 5.4 Scheduling
Weighted `leastconn` (and `roundrobin`): prefers healthy backends that have not reached `max_conns_per_backend`, picking the backend with the lowest `active / weight` ratio (or next round-robin index) so long-running streams do not pile new requests onto busy nodes.

### 5.5 Failure Handling
- **Pre-first-byte connection & HTTP status rerouting**: retryable up to `retry.max_retries` (default `-1`, trying all remaining healthy backends; `0` disables retries). In `thin-l7` mode, fixed-length request bodies up to `retry.max_body_bytes` (default `64 MiB`, matching `xlite`) are buffered before checking out a backend socket (and freed as soon as final response headers are written to the client) so responses matching `retry.retry_statuses` (default `[402, 404, 429, 500, 502, 503, 504]`) before any client byte is written have their error body drained (returning keep-alive sockets to the pool) and are transparently rerouted to another healthy backend.
- **Passive dial-failure circuit breaking** (optional `backends.passive_unhealthy_threshold`): marks a backend unhealthy immediately after N consecutive TCP dial failures without waiting for the next active `/health` tick, preventing `leastconn` from routing traffic to a dead `active = 0` backend between probes. Re-admits the backend after `backends.eject_cooldown` (default `5s`) unless active health checks have marked it unhealthy.
- **Mid-stream failure**: closes both client and backend sockets immediately, records metrics, and never replays the request.
- **Active health check failure**: marks the backend unhealthy and clears its idle pool; in-flight streams continue undisturbed while new requests are routed elsewhere.

## 6. Technology Stack

| Layer | Choice |
|-------|--------|
| Runtime | `tokio` |
| L4 | `tokio::net` |
| L7 | `httparse` (head-only) + manual streaming body framer |
| Config | `serde` + `toml` |
| Metrics | `prometheus` crate |
| Logging | `tracing` |

Full HTTP frameworks (`axum` / `actix`) are intentionally avoided to eliminate implicit body buffering and middleware overhead.

## 7. Observability

See the metrics table in `README.md` and [`observability.md`](observability.md). Key signals are `xlb_ttft_delta_ms` (proxy-added first-byte latency), `xlb_conn_reuse_total` / `xlb_conn_new_total` (backend connection reuse rate), `xlb_reroutes_total{backend,status}` (pre-first-byte HTTP status reroutes), and `xlb_stream_duration_seconds` (stream lifetime distribution).

## 8. Deployment and Verification

- Single static-friendly binary, configuration-driven; deploy in the same region/LAN as backend gateways for sub-millisecond RTT.
- Acceptance criteria: direct vs. via-xLB TTFB delta < 10 ms; identical total stream duration, QPS, and inter-chunk cadence; controlled A/B verification of connection pooling, layered timeouts, and scheduling.

## 9. Implementation Milestones

1. **MVP (L4)**: bidirectional TCP splice + `leastconn` + connection pool + layered timeouts + `/health` — Completed
2. **Observability**: TTFB delta histograms, connection reuse counters, stream lifecycle metrics — Completed
3. **Thin-L7**: HTTP/1.1 head parsing + request-level connection reuse — Completed
4. **Protocol sniffing + automatic L4 fallback** — Completed
5. **Client-side TLS termination (`rustls`)** — Completed
6. **Extended resilience**: phased `first_byte` timeout, LIFO bounded pool, passive dial ejection, and stale-socket redial — Completed
7. **Extended scheduling**: out-of-band agent load signals (queue depth / in-flight), without payload inspection — Planned

## 10. Risks and Boundaries

- Custom proxy connection management, half-close handling, and timeout boundaries require rigorous testing; `tests/integration.rs` covers long streams, half-close, abrupt backend disconnects, fallback protocols, and health transitions.
- Do not add model routing, prompt inspection, or token billing inside xLB; those belong in the backend AI gateways.
