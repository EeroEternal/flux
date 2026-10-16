# Documentation Layout & Lifecycle

Structure and lifecycle of the `docs/` tree.

## Directory structure

- `docs/design.md`: single source of truth for the xLB design rationale
  (goals, non-goals, constraints, timeout model, roadmap).
- `docs/architecture.md`: how the design is realised in code — module map,
  request lifecycle, invariants, mode tradeoffs, fallback and TLS.
- `docs/dev/`: working notes, verification logs and capacity/benchmark
  procedures that are not part of the stable spec.
- `docs/changelog.md`: Keep a Changelog + SemVer. Notable work lands under
  `[Unreleased]`; a version cut promotes that section (skill `release`).
- `docs/ai/agents/`: engineering guidelines, commit standards, agent
  governance.

Current version and recent work: `Cargo.toml` + `git log` +
`docs/changelog.md`. Do not keep a second project-status snapshot.

## Lifecycle discipline

1. **No phantom capabilities.** Never document skeleton-only or hypothetical
   features as ready.
2. **Deterministic verification.** Code snippets and shell commands inside docs
   must be executable and verified in this repo.
3. **Landed work.** Write the conclusion into `docs/changelog.md`, then delete
   the stale draft. Do not leave "done" proposals as the source of truth.
