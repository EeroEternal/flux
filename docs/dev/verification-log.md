# Verification log

Evidence recorded while validating Flux. Re-run the commands to reproduce.

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
