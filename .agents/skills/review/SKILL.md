---
name: review
description: "Independent read-only code review (critic): execute in a separate session or read-only subagent, never self-review in the author context; run deterministic local gates before reading diffs, and output file:line findings with a SHIP or REWORK verdict. Use when reviewing a PR, a finished task, or before requesting human approval."
---

# Review (Independent Read-Only Critic)

## Symptom / Misjudgment

An author reviewing their own code in the same context tends to grade
themselves generously: attention is already saturated by implementation details,
making piggybacked changes, hallucinated capabilities, or simpler alternatives
easy to miss. Historical bugs in this repository were caught only during
**independent critic review**.

## Rules

1. **Independent context**: reviews must run in a separate session or a
   read-only subagent (no file write/edit tools); never "quickly self-review"
   inside the authoring context.
2. **Read-only**: the critic never modifies files; it reports `file:line`
   findings and suggested fixes for the author to address and resubmit.
3. **Verification layering**: run deterministic local gates first, and read the
   diff only after they pass:
   ```bash
   bash scripts/local-gates.sh
   ```
   If any gate fails, return **REWORK** immediately with the failing gate.
4. **Check every standing constraint**: piggybacked unrelated edits? Ghost code
   (defined without callers / docs citing non-existent fields)? Streaming
   invariants (zero buffering, proven pool boundary, no retry after first byte,
   layered timeouts, LB boundary) preserved?
5. **Look for unnecessary complexity**: lines that can be deleted and branches
   that can be simplified are first-class review findings.

## Procedure

1. Confirm inputs: diff range and claimed deliverables.
2. Run deterministic local gates (`bash scripts/local-gates.sh`); if red,
   return **REWORK** immediately.
3. Walk the diff read-only: inspect tests first (do assertions test real
   behavior?), then implementation, then documentation.
4. Output findings as `file:line [blocker|major|minor] description`; issue
   **SHIP** only when no blocker or major finding remains.
5. Verdict is strictly binary: **SHIP** or **REWORK** (with blockers listed).
   "Mostly fine" is not a valid verdict.

## Scope

- This skill governs code review discipline and does not replace explicit human
  approval required by the `release` skill.
- After fixing **REWORK** items, the author must request a fresh re-review.
