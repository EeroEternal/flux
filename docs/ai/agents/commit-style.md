# Commit Message Style

## Format

`<type>(<scope>): <subject>`

Common `type` values:

- `feat` — new feature
- `fix` — bug fix
- `refactor` — code restructuring without behavioral change
- `perf` — performance improvement
- `test` — test-only additions or updates
- `docs` — documentation-only changes
- `chore` — build, release, or dependency maintenance
- `ci` — CI / workflow changes

Common xFlux `scope` values: `proxy`, `l4`, `l7`, `pool`, `scheduler`, `health`,
`metrics`, `config`, `server`, `tls`, `docs`, `ci`, `release`.

**The subject line must accurately reflect the diff**. Prohibited patterns:

- Vague messages such as `fix: misc` or `chore: cleanup`;
- Broad milestone claims like `feat: complete phase N` that hide unrelated
  parameter tuning;
- Using the commit message to obscure piggybacked changes.

**Recommended commit body structure**:

- **What** changed
- **Why** it changed
- **Risk / Rollback** plan (how to revert safely)

## Example

```text
feat(proxy): add automatic l4 fallback for non-HTTP/1.1 traffic

What:
- thin-l7 now sniffs the first bytes. HTTP/2 preface, TLS records,
  Upgrade/CONNECT, and unparseable heads switch to the raw splice,
  forwarding buffered bytes first.

Why:
- Preserve protocol transparency on thin-l7 listeners without losing
  HTTP/1.1 backend connection pooling.

Risk / Rollback:
- Fallback is connection-scoped; no per-request escalation back to l7.
  Revert by removing the sniff branch in src/proxy/l7.rs.
```
