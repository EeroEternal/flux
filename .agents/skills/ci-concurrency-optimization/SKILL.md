---
name: ci-concurrency-optimization
description: GitHub Actions CI concurrency cancellation, path-based change filtering, and Rust cache anti-thrashing rules. Use when configuring, debugging, or optimizing CI pipelines for xLB.
---

# CI Concurrency & Cache Optimization

Derived from the `console-kit` / `xrouter` production CI architecture, this
specification prevents **runner queue congestion, redundant compilation on
documentation-only commits, runner quota exhaustion, and GitHub Actions cache
eviction thrashing**.

---

## Core Architectural Pillars

### 1. Preemptive Concurrency Cancellation
During rapid iteration on a branch or PR, older CI runs that are still in
flight waste runner capacity and delay feedback. Always configure workflow-level
concurrency so a new push to the same ref automatically cancels any in-progress
run:

```yaml
concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
```

### 2. Path-Based Change Detection
Not every commit requires a full Rust compile and integration test run. Use the
GitHub API in a lightweight `changes` job to inspect modified file paths:
- **Documentation / AI metacode only** (`docs/`, `.agents/`, `*.md`): skip heavy
  Rust compilation and test jobs.
- **Code / build / script / workflow changes** (`src/`, `tests/`, `Cargo.toml`,
  `Cargo.lock`, `scripts/`, `.github/`): run the full Rust quality gate.

### 3. Unified Shared Rust Cache (Cache Quota Anti-Thrashing)
- **Mandatory rule**: when using `Swatinem/rust-cache@v2`, explicitly set a
  shared cache key (`shared-key: ci-rust`):
  ```yaml
  - uses: Swatinem/rust-cache@v2
    with:
      shared-key: ci-rust
  ```
- **Disable incremental compilation in CI**: set `CARGO_INCREMENTAL: "0"` in CI
  environments to avoid cache bloat and slower cold builds.

---

## Verification Checklist

When configuring or updating `.github/workflows/ci.yml`, verify:
- [ ] Top-level `concurrency.cancel-in-progress: true` is set;
- [ ] A `changes` job exports the `code` filter output;
- [ ] All Rust jobs set `CARGO_INCREMENTAL: "0"` and `shared-key: ci-rust`;
- [ ] CI gate steps invoke `bash scripts/local-gates.sh` (100% aligned with
      `pre-push-local-gates`).
