# AGENTS.md — Code Agent Collaboration Specification

High-density entry point for AI coding agents working on **Flux**. Its purpose
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
| Push / PR | skill [`pre-push-local-gates`](.agents/skills/pre-push-local-gates/SKILL.md) |
| Tag / release / deploy | skill [`release`](.agents/skills/release/SKILL.md) |
| Code review / acceptance | skill [`review`](.agents/skills/review/SKILL.md) |

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
   `git tag` or merge a release branch without explicit human approval.
5. **Streaming invariants (Flux core).** Never buffer a request or response
   body; only return a backend connection to the pool at a proven message
   boundary; never retry a request once any response byte has flowed.
6. **Timeout layering is load-bearing.** `connect < idle < total`, and `fin`
   governs half-close. Do not collapse these into a single timeout.
7. **Protocol transparency.** Any traffic thin-l7 cannot parse must fall back to
   the L4 splice byte-for-byte, preserving the bytes already buffered.
8. **Pre-push local quality gate.** Never use CI as a local sandbox. Run
   `scripts/local-gates.sh` (fmt, clippy `-D warnings`, tests) before pushing.

## Skills Index

Authoritative skills live under [`.agents/skills/`](.agents/skills/).

- [`pre-push-local-gates`](.agents/skills/pre-push-local-gates/SKILL.md)
- [`release`](.agents/skills/release/SKILL.md)
- [`review`](.agents/skills/review/SKILL.md)
