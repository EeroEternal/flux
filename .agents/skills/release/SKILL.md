---
name: release
description: Full release promoter workflow for cutting a vX.Y.Z tag: run full local gates, verify version/changelog/docs, stop for explicit human approval, then tag and verify. Use before cutting any vX.Y.Z tag or production deploy.
---

# Release Promoter Process

## When to Use

Follow this specification before running any `git tag v*.*.*`, merging a release
branch, or triggering a production release.

---

## Core Rule

**An AI agent must never execute `git tag` or publish a release without explicit
human approval.** A direct user request to "release vX.Y.Z" counts as approval,
provided the full release summary and changelog are prepared and verified.

---

## Changelog Discipline (Mandatory)

Canonical file: [`docs/changelog.md`](../../../docs/changelog.md) (Keep a
Changelog + SemVer). There is no root `CHANGELOG.md`.

**When landing notable work** (not only at tag time): record notable features,
fixes, or breaking changes under `## [Unreleased]` in the appropriate
`### Added` / `### Changed` / `### Fixed` / `### Removed` section.

**When cutting `vX.Y.Z`**:

1. Rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` (current date).
2. Insert a fresh empty `## [Unreleased]` heading above it.
3. Remove empty subsections. Releases containing only `misc`, raw commit titles,
   or "various improvements" are prohibited.

Every bullet must be self-contained for human readers:

| Required Element | Example |
| --- | --- |
| What changed | Automatically fall back to L4 when `thin-l7` sniffs non-HTTP/1.1 bytes |
| Who / where it affects | All `mode = "thin-l7"` deployments; `src/proxy/l7.rs` |
| Breaking or not | Breaking: No |

Prohibited bullets: `- feat: auth`, `- update docs`, `- fix bug`.

---

## 4-Step Release Workflow

### Step 1: Run Full Local Gates

```bash
git status            # Confirm worktree is clean
bash scripts/local-gates.sh
```

### Step 2: Three Pre-Release Checks

1. **Version consistency**: `version` in `Cargo.toml` (and `Cargo.lock`) is
   bumped to `X.Y.Z`.
2. **Changelog**: `docs/changelog.md` promotes `[Unreleased]` to
   `[X.Y.Z] - <date>` with clear, human-readable entries.
3. **Sensitive files & artifacts check**: no private keys, `.env` files, debug
   dumps, or untracked build artifacts.

### Step 3: Human Approval Gate

Present the complete release summary to the user (proposed tag, commit hash, and
**all changelog entries for this version**) and obtain explicit approval before
tagging. Never paste a raw commit list in place of the changelog.

### Step 4: Tag and Verify

Once approved:

```bash
git tag -a vX.Y.Z -m "release: vX.Y.Z"
git push origin main
git push origin vX.Y.Z
```

Verify that the remote tag points to the exact expected commit hash on `main`.
