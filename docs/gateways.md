# Gateway Integration Notes (LiteLLM / New API / AISIX / Higress)

xLB probes one lightweight endpoint per backend (`backends.health_check_path`).
The gateways below do **not** all expose an unauthenticated health endpoint at
`/health`, and a wrong path silently ejects healthy backends (clients get `503`
while the gateway is fine). Probes return `2xx`/`3xx` => healthy.

| Gateway | Probe path | Auth | Notes |
| --- | --- | --- | --- |
| LiteLLM (≥ 1.106) | `/health` | `Authorization: Bearer $LITELLM_MASTER_KEY` | Real per-model health. Without the header it answers `401` once a master key is set, so every backend is ejected. Send the header via `health_check_headers` (see below). |
| LiteLLM | `/health/liveliness` | none | `200` whenever the process is up. Cheap, but a gateway whose providers are all down still looks healthy. |
| LiteLLM | `/health/readiness` | none | `200` when the DB/Redis the proxy needs are reachable. |
| New API / one-api | `/api/status` | none | `200` + JSON. This is the real readiness probe. |
| New API / one-api | `/health`, `/healthz` | none | **Not an endpoint**: falls through to the console SPA and returns `200` HTML, so a probe there always "succeeds" even when the relay is broken. |
| AISIX (≥ 1.6) | `/readyz` | none | Served on the proxy listener (`:3000`), not a side port. `/livez` is process-alive only; `/health` is `404`, so the default path ejects a healthy gateway. |
| Higress / Envoy | any path your Ingress routes, e.g. `/v1/models` | none | The gateway only answers routed paths; everything else is `404`/`503`. The all-in-one ships a default-domain route on `/` (Exact) to the console landing page, which answers `200` HTML — a `/health` probe there silently "succeeds". A route on the default domain also keeps the probe working, because the probe's `Host` header is the backend address. |
| xlite | `/health` | none | `200` when up. |

## LiteLLM with a master key

```toml
[backends]
health_check_path = "/health"
health_check_headers = ["Authorization: Bearer sk-..."]   # $LITELLM_MASTER_KEY
```

`health_check_headers` accepts any number of `Name: value` lines; entries with a
missing `:` or embedded CR/LF are rejected at startup. The same trick works for
any gateway that gates its health endpoint behind a token. Keep the key out of
version control (mount `config.toml` as a secret).

## New API

```toml
[backends]
health_check_path = "/api/status"
```

New API answers an unknown model with `503` + `{"error":{"code":"model_not_found"}}`,
and `503` is in the default `retry.retry_statuses`. xLB therefore walks every
healthy backend before returning that `503` — wanted when gateways serve
disjoint model subsets, pure latency when the model name is simply wrong. Narrow
`retry.retry_statuses` or set `retry.max_retries = 0` if you prefer failing fast.

## AISIX

```toml
[backends]
health_check_path = "/readyz"
```

Unknown models get `404` + `model_not_found` (also in the default retry list),
so the same caveat as New API applies.

## Higress

```toml
[backends]
health_check_path = "/v1/models"   # or any path your Ingress routes
```

Envoy answers `503` with an empty body and `connection: close` when a route has
no healthy upstream — that is also how a mis-typed `higress.io/destination`
shows up, and it is in the default retry list, so xLB reroutes it as expected.

## Warm-connection latency (Envoy, and any upstream with Nagle on)

A pooled connection can carry a ~40 ms penalty per small response when the peer
writes the response head and body as separate segments and leaves `TCP_NODELAY`
off: the second write waits for the delayed ACK. Measured with Higress
all-in-one (Envoy 1.36.10): `GET /v1/models` took 3–8 ms on a fresh connection and
43–48 ms from the second request on, reproduced with a plain keep-alive Python
client and no xLB in the path. A bare `python -m http.server` shows the same
thing. xLB pools backend connections, so it pays this on every request after the
first; `mode = "l4"` hides it only because one-shot client connections get a
fresh upstream socket. It is not an xLB defect — with `TCP_NODELAY` enabled on
the upstream, xLB and direct latency matched (~6 ms). Before blaming the proxy,
check the gateway's downstream `TCP_NODELAY` setting.

## Verifying a gateway pairing

All four pairings were exercised end-to-end (streaming SSE integrity, `POST`
pooling, client aborts, 50 concurrent streams, pre-first-byte `429` rerouting,
latency) against LiteLLM `main-stable`, New API `latest`, AISIX `latest` (1.6)
and Higress all-in-one, each with a mock OpenAI upstream: `xlb_ttft_delta_ms`
stayed at ~0 ms and a 20-request sequential streaming loop cost a single backend
dial in every case.
