# AGENTS.md — Code Agent Collaboration Specification

High-density entry point for AI coding agents working on **xLB**. Its purpose
is to eliminate hallucinations, prevent piggybacking, and keep commits
reproducible. Detailed rules live in [`docs/ai/agents/`](docs/ai/agents/) and
[`.agents/skills/`](.agents/skills/). Do **not** load every chapter by default.

## Knowledge Tiering & Token Budget

| Tier | Content | Entry point |
| --- | --- | --- |
| **Standing constraints** | Inviolable rules for every task | This file; expanded in [`docs/ai/agents/`](docs/ai/agents/) |
| **Reusable workflows** | Procedures + validation commands | [`.agents/skills/*/SKILL.md`](.agents/skills/) (authoritative) |
| **Domain specs** | Architecture / forwarding semantics | [`docs/architecture.md`](docs/architecture.md), [`docs/design.md`](docs/design.md) |
| **History** | What shipped, when | [`docs/changelog.md`](docs/changelog.md) |

This file has a soft limit of **~90 lines**. When near it, apply the zero-sum
rule (add one, remove one). Never add a global rule from a single mishap: a rule
needs **≥ 2 independent occurrences** and human review.

## Agent Reading Map

| Task signal | Required reading |
| --- | --- |
| Any change under `src/proxy/` (L4/L7, framing, pooling) | [`docs/architecture.md`](docs/architecture.md) + [`docs/design.md`](docs/design.md) |
| Timeouts, connection reuse, retry, scheduling | [`docs/architecture.md`](docs/architecture.md) "Invariants" |
| TLS termination / listener changes | [`docs/dev/`](docs/dev/) + `src/server.rs` |
| Commit message wording | [`docs/ai/agents/commit-style.md`](docs/ai/agents/commit-style.md) |
| `git stash` operations | skill [`git-stash-safe`](.agents/skills/git-stash-safe/SKILL.md) |
| CI pipelines / concurrency / caching | skill [`ci-concurrency-optimization`](.agents/skills/ci-concurrency-optimization/SKILL.md) |
| Push / PR | skill [`pre-push-local-gates`](.agents/skills/pre-push-local-gates/SKILL.md) |
| Tag / release / deploy | skill [`release`](.agents/skills/release/SKILL.md) |
| Code review / acceptance | skill [`review`](.agents/skills/review/SKILL.md) |
| Payload parsing, model routing, "AI-aware" features | Constraint 9 + [`docs/ai/agents/engineering.md`](docs/ai/agents/engineering.md) §9 |

## Always Active (Highest Standing Constraints)

1. **No piggybacking.** A commit/PR must not carry unrelated changes. Repo-wide
   reformatting, unannounced tuning of production defaults, undocumented
   `#[allow]`, and opportunistic cross-module refactors are prohibited. Split via
   `git reset --mixed HEAD~1`.
2. **Zero hallucination code.** Every definition has a caller; every metric has
   success **and** failure paths; every `TODO` references an issue. Design docs
   must never cite skeleton-only features as existing.
3. **Safe stash.** Honest `-m` naming; `git diff --stat` before stashing;
   `cargo check --tests` after popping. Never stash `Cargo.toml` / `Cargo.lock`
   / build scripts.
4. **Release guardrail.** Local closed-loop verification first. Do not run
   `git tag` or merge a release branch without explicit human approval (follow
   skill [`release`](.agents/skills/release/SKILL.md)).
5. **Streaming invariants (xLB core).** Never buffer a request or response
   body; only return a backend connection to the pool at a proven message
   boundary; never retry a request once any response byte has flowed.
6. **Timeout layering is load-bearing.** `connect <= total`, `idle <= total`,
   `first_byte <= total`, and `fin` governs half-close. Do not collapse these
   into a single timeout.
7. **Protocol transparency.** Any traffic thin-l7 cannot parse must fall back to
   the L4 splice byte-for-byte, preserving the bytes already buffered.
8. **Pre-push local quality gate.** Never use CI as a local sandbox. Run
   `scripts/local-gates.sh` (fmt, clippy `-D warnings`, tests) before pushing.
9. **LB boundary (HAProxy position).** xLB forwards bytes, not tokens.
   Inspect only framing needed to pick a backend, reuse a connection, or
   tear one down (TCP, HTTP heads, Content-Length/chunked, protocol sniff).
   Never parse SSE, NDJSON, JSON, or any payload semantic. Scheduling keys
   are in-flight counts, health, request heads, or out-of-band load — never
   the body. Pooling is an HTTP message boundary, not `[DONE]`.

## Skills Index

Authoritative skills live under [`.agents/skills/`](.agents/skills/).

- [`ci-concurrency-optimization`](.agents/skills/ci-concurrency-optimization/SKILL.md)
- [`git-stash-safe`](.agents/skills/git-stash-safe/SKILL.md)
- [`pre-push-local-gates`](.agents/skills/pre-push-local-gates/SKILL.md)
- [`promote-lesson`](.agents/skills/promote-lesson/SKILL.md)
- [`release`](.agents/skills/release/SKILL.md)
- [`review`](.agents/skills/review/SKILL.md)
