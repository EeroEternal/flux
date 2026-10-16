#!/usr/bin/env bash
# Closed-loop smoke: xLB in front of a local xlite + stream mock.
# Not part of the public product surface; working-notes harness only.
set -euo pipefail

XLB_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
XLITE_ROOT="${XLITE_ROOT:-$HOME/github/xlite}"
XLB_BIN="${XLB_BIN:-$XLB_ROOT/target/debug/xlb}"
XLITE_BIN="${XLITE_BIN:-$XLITE_ROOT/target/release/xlite}"
MOCK_JS="${MOCK_JS:-$XLITE_ROOT/scripts/stream_mock_server.mjs}"

MOCK_PORT="${MOCK_PORT:-14101}"
XLITE_PORT="${XLITE_PORT:-18080}"
XLB_PORT="${XLB_PORT:-18006}"
METRICS_PORT="${METRICS_PORT:-19106}"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/xlb-xlite-XXXXXX")"
PIDS=()

log() { printf '[verify] %s\n' "$*"; }
die() { printf '[verify] ERROR: %s\n' "$*" >&2; exit 1; }

cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

wait_http() {
  local url="$1" tries="${2:-50}"
  local i
  for i in $(seq 1 "$tries"); do
    if curl -fsS -o /dev/null --max-time 1 "$url" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  die "timeout waiting for $url"
}

curl_stream() {
  local url="$1" out="$2"
  curl -sS --no-buffer \
    -D "$out.hdr" \
    -o "$out.body" \
    -H "Authorization: Bearer sk-loadtest" \
    -H "Content-Type: application/json" \
    -w "http=%{http_code} ttfb=%{time_starttransfer} total=%{time_total}\n" \
    --max-time 10 \
    -d '{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}' \
    "$url" | tee "$out.timing"
}

[[ -x "$XLB_BIN" ]] || die "xlb binary missing: $XLB_BIN (cargo build)"
[[ -x "$XLITE_BIN" ]] || die "xlite binary missing: $XLITE_BIN"
[[ -f "$MOCK_JS" ]] || die "mock missing: $MOCK_JS"

log "workdir $WORKDIR"

node "$MOCK_JS" \
  --port "$MOCK_PORT" \
  --listen 127.0.0.1 \
  --model mock \
  --chunks 8 \
  --interval-ms 20 \
  --ttft-ms 30 \
  --batch 1 \
  --label xlb-verify \
  >"$WORKDIR/mock.log" 2>&1 &
PIDS+=("$!")
wait_http "http://127.0.0.1:${MOCK_PORT}/health"

cat >"$WORKDIR/catalog.yaml" <<EOF
providers:
  - name: mock-local
    base_url: http://127.0.0.1:${MOCK_PORT}
    api_key: ""
    enabled: true
models:
  - name: mock
    enabled: true
    channels:
      - provider: mock-local
        upstream_model: mock
        weight: 1
        priority: 0
        enabled: true
api_keys:
  - name: loadtest
    key: sk-loadtest
    models: []
    enabled: true
EOF

export DATABASE_URL="sqlite:${WORKDIR}/xlite.db"
export ADMIN_PASSWORD=admin
export PORT="$XLITE_PORT"
export LISTEN_HOST=127.0.0.1
unset REDIS_URL || true

log "import xlite catalog"
"$XLITE_BIN" import "$WORKDIR/catalog.yaml" --replace

"$XLITE_BIN" >"$WORKDIR/xlite.log" 2>&1 &
PIDS+=("$!")
wait_http "http://127.0.0.1:${XLITE_PORT}/health"

cat >"$WORKDIR/xlb.toml" <<EOF
[server]
bind = "127.0.0.1:${XLB_PORT}"
mode = "thin-l7"
max_connections = 1024

[timeouts]
connect = "2s"
idle = "10s"
total = "30s"
fin = "1s"

[pool]
max_idle_per_backend = 8
idle_timeout = "10s"

[backends]
algorithm = "leastconn"
health_check_path = "/health"
health_interval = "1s"
health_check_timeout = "1s"
unhealthy_threshold = 2
healthy_threshold = 1

[[backends.servers]]
addr = "127.0.0.1:${XLITE_PORT}"
weight = 1

[retry]
max_retries = 0

[metrics]
enabled = true
bind = "127.0.0.1:${METRICS_PORT}"
EOF

log "start xlb"
"$XLB_BIN" --config "$WORKDIR/xlb.toml" >"$WORKDIR/xlb.log" 2>&1 &
PIDS+=("$!")
wait_http "http://127.0.0.1:${XLB_PORT}/health"

DIRECT="http://127.0.0.1:${XLITE_PORT}/v1/chat/completions"
VIA="http://127.0.0.1:${XLB_PORT}/v1/chat/completions"

log "direct xlite"
curl_stream "$DIRECT" "$WORKDIR/direct"
log "via xlb (1)"
curl_stream "$VIA" "$WORKDIR/xlb1"
log "via xlb (2)"
curl_stream "$VIA" "$WORKDIR/xlb2"

METRICS="$(curl -fsS "http://127.0.0.1:${METRICS_PORT}/metrics")"
printf '%s\n' "$METRICS" >"$WORKDIR/metrics.txt"
echo "$METRICS" | rg -n 'xlb_conn_(new|reuse)_total|xlb_ttft_delta_ms_count|xlb_bytes_total|xlb_backend_healthy' || true

NEW="$(echo "$METRICS" | sed -n 's/^xlb_conn_new_total{[^}]*} //p' | awk '{s+=$1} END {print s+0}')"
REUSE="$(echo "$METRICS" | sed -n 's/^xlb_conn_reuse_total{[^}]*} //p' | awk '{s+=$1} END {print s+0}')"
log "backend conns new=$NEW reuse=$REUSE"
if [[ "$NEW" -lt 1 ]]; then
  die "expected at least one newly dialed backend connection"
fi
if [[ "$REUSE" -lt 1 ]]; then
  die "expected thin-l7 to reuse the xlite connection on the second request (new=$NEW reuse=$REUSE)"
fi

python3 - <<PY
import re, sys
from pathlib import Path
w = Path("$WORKDIR")

def timing(p):
    text = p.read_text()
    m = re.search(r"http=(\d+) ttfb=([0-9.]+) total=([0-9.]+)", text)
    return int(m.group(1)), float(m.group(2)), float(m.group(3))

failed = []
for name in ("direct", "xlb1", "xlb2"):
    http, ttfb, total = timing(w / f"{name}.timing")
    raw = (w / f"{name}.body").read_bytes()
    if http != 200:
        failed.append(f"{name} http={http}")
    if b"data:" not in raw:
        failed.append(f"{name} no SSE")
    if b"[DONE]" not in raw:
        failed.append(f"{name} no [DONE]")
    if total - ttfb < 0.05:
        failed.append(f"{name} buffered total-ttfb={total-ttfb:.4f}")
    print(f"{name:7} http={http} ttfb={ttfb:.4f}s total={total:.4f}s body={len(raw)}B")

_, d_ttfb, _ = timing(w / "direct.timing")
_, f_ttfb, _ = timing(w / "xlb1.timing")
delta_ms = (f_ttfb - d_ttfb) * 1000
print(f"ttfb_delta_ms={delta_ms:.2f}")
if delta_ms > 50:
    failed.append(f"ttfb delta {delta_ms:.1f}ms")
if failed:
    print("FAIL:", "; ".join(failed))
    sys.exit(1)
print("PASS")
PY

log "ok"
