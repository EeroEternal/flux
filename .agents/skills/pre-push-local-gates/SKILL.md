---
name: pre-push-local-gates
description: Run full CI-equivalent local quality gates (Rust fmt, clippy, tests) before every git push. Never use CI as a local sandbox. Use before every git push touching src/, tests/, Cargo.toml, or CI.
---

# Pre-Push Local Gates

## Core Rule & Prohibition

Never use GitHub Actions CI as a local sandbox. Pushing code only to discover
rustfmt diffs, clippy warnings, compile errors, or test failures creates a
wasteful "push -> fail -> fix -> push" loop that pollutes commit history and
burns runner quota.

## Local Gate Standard

Before running `git push` or opening/updating a PR, all of the following must
pass locally (100% equivalent to CI):

```bash
# Single command (recommended)
bash scripts/local-gates.sh

# Install the local Git pre-push hook once after cloning (runs local-gates.sh automatically on git push)
bash scripts/install-hooks.sh

# Equivalent manual commands
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

Any change touching forwarding, timeouts, or connection pooling semantics must
also verify that `tests/integration.rs` covers the behavior (passthrough,
half-close, chunked streaming, connection reuse, abrupt disconnect, automatic L4
fallback, TLS termination).

Before cutting a release or tag, switch to skill [`release`](../release/SKILL.md)
and follow the full release promoter process.

## Scope & Discipline

- Intermediate local commits during active development may skip the full suite,
  but **the final state before `git push` must pass all local gates**.
- If CI ever fails unexpectedly: do not guess blindly; reproduce the exact
  failing command locally, verify the fix locally, and only then push.
- Watch out for shell pipeline exit code traps (`cmd | tail` returns `tail`'s
  exit code). `scripts/local-gates.sh` enforces `set -euo pipefail`.
