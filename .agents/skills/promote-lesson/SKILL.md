---
name: promote-lesson
description: Promote a repeated agent/session pitfall into reviewed repository metacode (AGENTS.md, skills, or docs/ai). Use at end of a task when the same mistake would recur, or when a high-cost misjudgment was discovered.
---

# Promote a lesson from session memory into the repo

## When to promote
Promote if any of these hold:
- The same pitfall appeared in a second session or PR.
- Cost is high (false-green build, production tunables, protocol boundary, dependency stripped by stash).
- Corrective action needs a non-obvious command or ordered checklist.

Do **not** promote one-off environment noise (transient port conflict, flaky network).

## What to write
Every promoted entry needs: symptom (what will be misread), root cause, correct action, verification command, scope. Vague advice ("be careful with timeouts") is insufficient.

## Where it lands
| Kind of knowledge | Destination |
| --- | --- |
| Standing constraint for all agents | Thin rule in root `AGENTS.md` + detail in `docs/ai/agents/*.md`; link to a skill for procedures |
| Repeatable procedure / checklist | `.agents/skills/<name>/SKILL.md` (single canonical home) |
| Long-lived architecture decision | `docs/architecture.md` or `docs/design.md` |
| Still exploratory | `docs/dev/`, delete or fold into changelog when landed |

Do not invent parallel roots (`CLAUDE.md` full forks of `AGENTS.md`, etc.). Do not commit local IDE state (plan files, editor scratch).

## Procedure
1. State the lesson in one paragraph with symptom → action → verify.
2. Choose destination from the table; prefer a skill if it is a multi-step procedure.
3. If `AGENTS.md` only needs a hard rule, add the rule plus a pointer to `.agents/skills/...` — do not paste the full checklist twice.
4. Remove or shorten duplicates in `docs/ai/*` so one path is canonical.
5. Human reviews before merge (`docs(ai): …` / `docs(agents): …`). Unreviewed local agent memory is not team truth.
6. When behavior changes later, update metacode in the same PR as the code change; delete stale entries instead of archiving piles.

## Verification
- `rg` shows a single canonical home (or a one-line pointer + skill body).
- A new agent can follow the text without the original chat.
