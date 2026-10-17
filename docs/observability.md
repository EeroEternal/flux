# Observability, Metrics & Verification

## Prometheus Metrics

Every metric is exposed on `GET /metrics` (default bind `0.0.0.0:9106`).

| Metric | Type | Meaning |
|--------|------|---------|
| `xlb_ttft_delta_ms` | histogram | Proxy-added first-byte latency (`client_ttft - upstream_ttft`) |
| `xlb_upstream_ttft_ms` | histogram | Request fully sent → backend first byte (TTFB) |
| `xlb_client_ttft_ms` | histogram | Request fully sent → client first byte (TTFB) |
| `xlb_stream_duration_seconds` | histogram | Whole stream duration |
| `xlb_active_streams{backend}` | gauge | In-flight streams per backend |
| `xlb_conn_reuse_total{backend}` | counter | Connections checked out from the idle pool |
| `xlb_conn_new_total{backend}` | counter | Connections freshly dialed |
| `xlb_conn_dial_errors_total{backend}` | counter | Failed backend dials |
| `xlb_reroutes_total{backend,status}` | counter | Pre-first-byte HTTP status reroutes to another healthy backend (`thin-l7`) |
| `xlb_rejected_total{reason}` | counter | Rejected requests (`unhealthy`, `queue_full`, `timeout`, `bad_request`) |
| `xlb_health_check_failures{backend}` | counter | Failed active `/health` probes |
| `xlb_health_check_success_total{backend}` | counter | Successful active `/health` probes |
| `xlb_backend_healthy{backend}` | gauge | `1` healthy / `0` unhealthy |
| `xlb_upstream_errors_total{backend}` | counter | Mid-stream upstream transfer failures |
| `xlb_client_errors_total` | counter | Downstream client disconnects or malformed requests before completion |
| `xlb_bytes_total{backend,direction}` | counter | Total bytes proxied (`tx` / `rx`) |
| `xlb_connections_total` | counter | Cumulative client TCP connections accepted |
| `xlb_active_connections` | gauge | Currently open client TCP connections |

## Structured Logging

Logs are emitted via `tracing`:

- Set `RUST_LOG=info` (or `debug` / `xlb=trace`) to control log verbosity.
- Set `XLB_LOG_JSON=1` to emit JSON lines for log collectors.

## Verifying TTFT & Connection Pool Reuse

With `xlb` running against local backends in `thin-l7` mode, send two separate
`POST` requests and inspect the connection pool and TTFT metrics:

```bash
# 1. Compare direct vs xLB first-byte latency
curl -s -o /dev/null -w 'direct ttft=%{time_starttransfer}s\n' \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock","stream":true}' \
  http://127.0.0.1:8001/v1/chat/completions

curl -s -o /dev/null -w 'xlb    ttft=%{time_starttransfer}s\n' \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock","stream":true}' \
  http://127.0.0.1:8006/v1/chat/completions

# 2. Confirm backend connections are reused from the pool
curl -s http://127.0.0.1:9106/metrics | grep -E 'xlb_conn_(new|reuse)_total|xlb_ttft_delta_ms_count'
```
