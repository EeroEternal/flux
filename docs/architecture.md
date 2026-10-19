# xLB Architecture

This document describes how the design in [`design.md`](design.md) is realised in
code. It is the reference for anyone touching `src/proxy/`, timeouts, pooling or
TLS.

## Module map

```
src/
├── main.rs          CLI, tracing, shutdown signals
├── config.rs        TOML model, duration parsing, validation
├── server.rs        listener, accept loop, limits, TLS acceptor, metrics endpoint
├── backend.rs       Backend, weighted leastconn / roundrobin scheduler, Lease
├── pool.rs          idle connection pool with non-blocking liveness probing
├── health.rs        active /health probes with hysteresis
├── metrics.rs       Prometheus registry + text exposition
└── proxy/
    ├── mod.rs       mode dispatch + idle/deadline read/write helpers
    ├── l4.rs        bidirectional splice + activity watchdog (fallback path)
    ├── l7.rs        HTTP/1.1 framing, protocol sniffing, auto L4 fallback
    └── http1.rs     head parsing, chunked scanner, verbatim body streaming
```

## Listener and connection lifecycle

1. `server::bind` creates the socket (with `SO_REUSEPORT` when configured).
2. The accept loop enforces `server.max_connections` with a semaphore; over-limit
   connections are counted as `xlb_rejected_total{reason="queue_full"}`.
3. If `server.tls.enabled`, the raw TCP stream is wrapped with `TlsAcceptor`
   before any parsing happens. The decrypted stream and a plain TCP stream are
   indistinguishable to the proxy.
4. `proxy::handle` dispatches on `server.mode` to `l4` or `l7`. Both are
   generic over `AsyncRead + AsyncWrite + Unpin + Send`.

## The connection-reuse boundary (core)

A TCP connection is a byte stream, not a message stream. On a shared
backend connection the bytes look like:

```
[response 1 head][response 1 body][response 2 head][response 2 body]...
```

To hand that connection to a *different* client, xLB must know exactly where
response 1 ends. L4 does not parse HTTP, so it cannot know. If it guessed (for
example "idle for N seconds means done"), a backend that is slow to produce the
next chunk — normal during model thinking — would be misclassified and the next
client would read the previous user's leftover bytes. That is a correctness and
confidentiality failure, so **L4 never pools across client connections**.

L7 parses the head and knows the body framing (`Content-Length`, chunked
terminator, or until-close). Only when the response is fully and safely consumed
does it call `Backend::release_conn`, and `ConnPool::put` re-validates with a
non-blocking `try_read` before storing the connection. A connection is reusable
only when all hold:

- response framing is self-delimiting (not until-close);
- `Connection` semantics allow keep-alive on the backend leg and response (for
  non-upgrade HTTP/1.1 requests, `Connection: close` from a short-lived client is
  rewritten to `Connection: keep-alive` on the backend leg while still closing
  the client connection after the response);
- no unread bytes remain in the response buffer.

L4 still reuses a connection for the lifetime of a single client connection
(one client connection maps to one backend connection), which is the natural
reuse for keep-alive clients.

## Automatic L4 fallback (thin-l7)

thin-l7 handles HTTP/1.1. When the first bytes indicate something else it stops
parsing and switches to the raw splice, forwarding everything already buffered
first:

| Signal | Detection |
| --- | --- |
| TLS / binary protocol | first byte is not an uppercase HTTP method char |
| HTTP/2 | head starts with `PRI ` |
| unparseable request | `http1::parse_request` fails |
| WebSocket / h2c | request carries an `Upgrade` header |
| CONNECT tunnel | `method == CONNECT` |
| oversized/malformed head | `read_head` returns `InvalidData` / `UnexpectedEof` |

The fallback is **connection-scoped**: once switched, the whole connection is
spliced. The bytes consumed by `read_head` are re-prepended via
`fallback_with_head`, and `l4::splice` flushes `initial_client_bytes` to the
backend before starting `copy_bidirectional`. The client never observes the
switch.

## TLS termination

`server::load_tls` builds a rustls `ServerConfig` from PEM cert/key files and
advertises only `http/1.1` via ALPN. TLS termination is orthogonal to the
forwarding mode: `mode="l4"` splices the decrypted bytes; `mode="thin-l7"`
additionally gets request-level pooling. xLB rejects config where TLS is enabled
without both `cert` and `key`.

## Timeout model

| Timeout | Scope |
| --- | --- |
| `connect` | backend connection establishment |
| `first_byte` | optional TTFB budget before the backend sends its first response byte (defaults to `idle`); in `thin-l7`, expiry before any response byte is written to the client returns `504 Gateway Timeout` |
| `idle` | maximum silence on a direction once response bytes are flowing; L4 via an activity watchdog, L7 via per-read/write timeout |
| `total` | hard ceiling for a whole stream |
| `fin` | grace period after a half-close in L4 |

The L4 watchdog polls the shared `Activity` (updated on every read/write) and
notifies a `Notify` when a direction has been silent for `first_byte` (before
the backend's first byte) or `idle` (afterwards). The splice is then cancelled.
`total` is a plain sleep raced against the splice.

## Scheduling and health

- `Scheduler::select` reserves an in-flight slot through a `Lease` (RAII:
  dropping it decrements `active` and releases any per-backend concurrency
  permit). This is why a panic in a connection task cannot leak a slot.
- `ConnPool` checks out idle connections in **LIFO** order (`pop_back`) so the
  most recently active socket is reused first, while opportunistically reaping
  expired sockets from the front (`pop_front`).
- When `pool.max_conns_per_backend > 0`, each `Backend` enforces a semaphore cap
  acquired in `Lease::acquire` up to `pool.acquire_timeout` (defaulting to
  `timeouts.connect`). Both `leastconn` and `roundrobin` prefer healthy backends
  below `max_conns_per_backend` before falling back to full backends.
- leastconn compares `active/weight` by cross-multiplying to avoid floats.
- health probes hit `GET {path}` and only flip the `healthy` flag after
  `unhealthy_threshold` / `healthy_threshold` consecutive results. When a
  backend transitions to unhealthy (or fails a TCP dial), its idle `ConnPool` is
  immediately cleared. In-flight streams are never interrupted.
- When `backends.passive_unhealthy_threshold > 0`, consecutive data-plane TCP
  dial failures immediately mark the backend unhealthy so `leastconn` does not
  blackhole traffic onto a dead `active == 0` backend between active probes; the
  backend is re-admitted after `backends.eject_cooldown` (or earlier if an
  active `/health` probe succeeds).
- Retries and status reroutes apply strictly **before any response byte is
  written to the client**, bounded by `retry.max_retries` (default `-1`, which
  expands to `backends.servers.len() - 1` so every healthy backend can be tried
  at most once; `0` disables retries) and optional wall-clock ceiling
  `retry.timeout` (`0 < retry.timeout <= timeouts.total`). The ceiling is
  anchored at request arrival, so time spent reading the request head and the
  request body counts against it; in `l4` mode it bounds the connect-retry
  loop, in `thin-l7` it bounds connect retries, stale-socket failovers and
  status reroutes together.
- In `thin-l7` (the default `server.mode`), fixed-length request bodies up to
  `retry.max_body_bytes` (default `64 MiB`) are buffered before checking out a
  backend connection and freed immediately once final response headers are
  written to the client:
  - If a connection checked out from the idle pool (`reused = true`) fails while
    writing the request or immediately closes with 0 response bytes (HTTP/1.1
    keep-alive timeout race or backend crash), xLB first attempts one fresh
    connection on the same backend and, if that dial fails, fails over across
    other healthy backends within the retry budget and `retry.timeout`.
  - If a backend returns an HTTP status in the effective retry status list
    (`retry.retry_statuses`, default `[402, 404, 429, 500, 502, 503, 504]`, or
    `retry.non_idempotent_retry_statuses` for non-idempotent methods when no
    cross-node `Idempotency-Key` / `X-Idempotency-Key` is accepted via
    `retry.respect_idempotency_key`) before any response byte is written to the
    client and before `retry.timeout` expires, `thin-l7` drains the error body
    (while watching for client disconnect or timeout), returns self-delimiting
    keep-alive sockets to the backend's idle pool, increments
    `xlb_reroutes_total{backend,status}`, and replays the request onto the next
    healthy backend.
  - Structured request logs include a sanitized `trace_id` extracted from
    `X-Request-Id` / `X-Correlation-Id` (redacting secret prefixes such as
    `sk-` or `Bearer ` and falling back to `xlb-{request_id}`) plus the attempt
    counter so operators can correlate multi-attempt executions across nodes.
    These events are emitted at `debug` level: enable them with
    `RUST_LOG=xlb=debug`.

## Invariants (must hold after any change)

1. No response body is ever buffered beyond one bounded 16 KiB read; in `thin-l7`,
   fixed-length request bodies up to `retry.max_body_bytes` are buffered only
   during the pre-first-byte phase for connection/status replay and are dropped
   before response body streaming begins.
2. A backend connection is pooled only at a proven message boundary.
3. No request is retried after the first response byte is written to the client.
4. `connect <= total`, `idle <= total`, `first_byte <= total`, and `retry.timeout <= total`; `fin` is independent.
5. Any protocol thin-l7 cannot parse is forwarded byte-for-byte via L4.
6. Every metric that has a success/increment path has a failure counterpart.
7. Payload bytes are opaque. xLB must not parse SSE, NDJSON, JSON, or any
   application body to make forwarding, pooling, retry, or scheduling decisions.

## Testing strategy

- Unit tests live next to the code (`duration`, `http1` chunk scanner, pool,
  scheduler, health status parsing).
- `tests/integration.rs` starts real mock gateways and a real xLB server. It
  covers pass-through, half-close, chunked streaming, backend reuse, pipelined
  requests, abrupt backend death, leastconn scheduling, automatic L4 fallback
  (HTTP/2, binary, Upgrade), TLS termination, opaque SSE/`[DONE]` bodies,
  connect redispatch, pre-first-byte HTTP status rerouting (including long-context
  SSE requests across asymmetric backends), and no retry after the first response
  byte.
- `scripts/local-gates.sh` must be green before pushing.
