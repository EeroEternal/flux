# Verification log

Evidence recorded while validating xLB. Re-run the commands to reproduce.

## Unreleased — 2026-10-15

### Flux in front of local xlite + stream mock

Harness: `bash docs/dev/verify-with-xlite.sh`
(mock SSE 8 chunks × 20ms, mock TTFT 30ms, Flux `thin-l7`).

```
direct  http=200 ttfb=0.0339s total=0.2017s body=2059B
flux1   http=200 ttfb=0.0320s total=0.1991s body=2059B
flux2   http=200 ttfb=0.0324s total=0.1995s body=2059B
ttfb_delta_ms=-1.86
flux_conn_new_total{backend="127.0.0.1:18080"} 1
flux_conn_reuse_total{backend="127.0.0.1:18080"} 2
PASS
```

- TTFB through Flux matched direct xlite (noise, not added latency).
- `total - ttfb ≈ 170ms` matches 8×20ms token interval: Flux did not buffer the SSE body.
- Two client requests plus the proxied `/health` probe reused a single xlite connection.
- SSE framing (`data:` / `[DONE]`) was forwarded as opaque payload.

## v0.0.1 — 2026-10-14

### Local gates

```
$ bash scripts/local-gates.sh
cargo fmt --check                              # clean
cargo clippy --all-targets -- -D warnings      # clean
cargo test --workspace                         # 14 unit + 11 integration, all pass
```

### Real-binary smoke test (thin-l7, plaintext backend)

```
direct ttft=0.000855s status=200
flux   ttfb=0.001310s  status=200
flux_conn_new_total{backend="127.0.0.1:18001"}   1
flux_conn_reuse_total{backend="127.0.0.1:18001"} 1
```

Two separate one-shot clients used a single backend connection, confirming
cross-connection pooling in thin-l7.

### Real-binary smoke test (TLS termination)

```
$ curl -sk https://127.0.0.1:18007/v1/chat/completions
data: {"delta":"Hel"}\n\ndata: {"delta":"lo"}\n\ndata: [DONE]
status=200
```

Flux log: `TLS termination enabled cert=/tmp/fluxsmoke/cert.pem`, then
`flux listening addr=127.0.0.1:18007`.

### Automatic fallback

Covered by `tests/integration.rs`: `thin_l7_falls_back_on_http2_preface`,
`thin_l7_falls_back_on_binary_protocol`, `thin_l7_falls_back_on_upgrade`.
