# Engineering Hard Rules

This file adapts the console-kit engineering rules to xFlux. Rules whose original
motivation was database/UI specific are omitted; universal rules are preserved
and extended with xFlux's streaming invariants.

## 1. No Piggybacking (Silent Drift)

A single commit or PR must not carry changes unrelated to its stated objective:

1. **Unannounced parameter tuning**: modifying default timeouts, pool sizes,
   thresholds, concurrency caps, or scheduling weights under the guise of
   `refactor` or `format`.
2. **Bulk reformatting**: `cargo fmt` sweeps must be committed separately and
   never mixed with behavioral changes.
3. **Undocumented `#[allow(...)]` suppressions**: never silence a warning with
   `#[allow(clippy::xxx)]` without an inline `// reason:` comment on the same line.
4. **Cross-module opportunistic refactoring**: do not rename or restructure
   module B while fixing a bug in module A.

Violations must be split via `git reset --mixed HEAD~1`.

---

## 2. Safe `git stash` Discipline

1. **Honest naming**: `git stash push -m "..."` must accurately describe what is
   being stashed.
2. **Before stashing**: run `git diff --stat` and confirm no current-feature
   dependency is removed.
3. **After stashing or popping**: running `cargo check --tests` is mandatory.
4. **Never stash** `Cargo.toml`, `Cargo.lock`, or build scripts.

---

## 3. Background Tasks and `tokio::spawn` Discipline

`tokio::spawn` is a high-risk construct:

1. **Explicit lifecycle**: never call `tokio::spawn` inside `new()` or
   `Default::default()`. Provide an explicit `start_xxx()` / `spawn()` entry
   point invoked from `main()` or server startup.
2. **Runtime guard**: ensure spawned tasks run only within a known Tokio runtime
   context or guard with `tokio::runtime::Handle::try_current()`.
3. **Timeout protection**: every async network/IO call inside a long-running
   background loop must be bounded by `tokio::time::timeout(...)` so a daemon
   can never hang indefinitely.
4. **Never swallow errors**: panics or unexpected task exits must log at least
   one `tracing::error!` / `tracing::warn!` event.
5. **Observability**: shared state mutated by background tasks must be observable
   through Prometheus metrics or health state.

---

## 4. Zero Hallucination Code (Ghost Code)

1. **Defined but never called**: functions or types with zero callers across the
   repository (`rg` finds no usage).
2. **Read without write**: state fields that are read/loaded but never updated.
3. **One-sided metrics**: counters/gauges that track increments without
   corresponding decrements or failure paths. Critical paths must instrument both
   success and failure outcomes.
4. **Unlinked `TODO`**: any `// TODO:` comment must reference a tracking issue.

Any ghost code discovered must be wired up or removed immediately in the same
change.

---

## 5. Scripted Bulk Edits

1. **Always verify regex replacements with `rg`** (scripts can silently match
   nothing and exit 0).
2. **Avoid fragile multiline regexes**: prefer structured edit tools over ad-hoc
   multiline `sed` / Python regex scripts.
3. **Inspect escaping**: manually review diffs after any scripted transformation.

---

## 6. Pipeline Exit Code Traps

In shell pipelines, the default exit code is that of the **last** command:
`failing_cmd | tail -3` exits `0`.

- Always enable `set -o pipefail` in scripts, capture output to a file, or run
  the command directly without piping when checking pass/fail status.
- When verifying GitHub Actions or PR status, inspect authoritative fields
  (`gh run view` / `gh pr checks` `conclusion`) rather than wrapper exit codes.
- `bash -n` checks syntax only; always execute scripts to verify behavior.

---

## 7. Streaming Core Invariants (xFlux Specific, Highest Priority)

Every change in xFlux must preserve these four invariants:

1. **Zero body buffering**: `forward_body` holds at most one bounded read chunk
   at a time; L4 uses `copy_bidirectional`. Never introduce an intermediate
   body-aggregating or decoding layer.
2. **Pool reuse only at proven message boundaries**: a backend connection may be
   returned to the pool (`release_conn`) only when the response is
   self-delimiting (`Content-Length` or complete `chunked` terminator) and the
   read buffer has no leftover bytes.
3. **No retry after first response byte**: once any response byte has been
   written to the client, any subsequent error must close both sides without
   replaying the request.
4. **Layered timeouts**: `connect`, `first_byte`, `idle`, `total`, and `fin`
   remain distinct; L4 enforces `first_byte`/`idle` via the activity watchdog,
   and Thin-L7 enforces them via per-read/write timeouts.

Any modification under `src/proxy/` must add or update corresponding behavioral
tests in `tests/integration.rs`.

---

## 8. Violation Handling

1. **Report contradicts code**: `git reset --hard` to the last verified state
   and rewrite the change cleanly.
2. **Claiming completion while the build or tests fail**: stop immediately and
   fix the failure before reporting.
3. **Repeated identical mistakes (3+ times)**: require independent critic review
   before every subsequent commit.
4. **Piggybacked production parameter changes**: revert immediately and submit
   as a dedicated, documented commit.

---

## 9. Load Balancer Boundary (HAProxy Position, Never Drift into Gateway Logic)

xFlux is an L4/Thin-L7 load balancer in front of long-lived streams, not an AI
inference gateway. Apply this rule when reviewing hot paths:

> **Touch only the framing required to decide which backend to pick, whether a
> connection can be returned to the pool, and when to close; never inspect
> payload semantics.**

| Permitted (Framing) | Prohibited (Payload Semantics) |
| --- | --- |
| TCP splice; HTTP headers; `Content-Length` / `chunked` framing | Scanning SSE `data:` / `[DONE]` / NDJSON lines |
| Protocol sniffing and automatic L4 fallback | Extracting `model`, prompt hashes, or KV prefixes from the body |
| `leastconn` / `roundrobin`, health, request headers, out-of-band load | Scheduling by token generation rate |
| Redispatching on connection dial failure | Retrying or replaying after any response byte has flowed |

Pool return is governed by HTTP self-delimiting framing completion, never by a
model emitting `[DONE]`. Any hot-path parsing of body/SSE/JSON payloads is out
of bounds and must be rejected in review.
