# Commit message style

## 格式

`<type>(<scope>): <subject>`

常用 type：

- `feat` — 新功能
- `fix` — bug 修复
- `refactor` — 重构（不影响行为）
- `perf` — 性能优化
- `test` — 只改测试
- `docs` — 只改文档
- `chore` — 构建、发版、依赖升级
- `ci` — CI / 工作流

xFlux 常用 scope：`proxy`、`l4`、`l7`、`pool`、`scheduler`、`health`、
`metrics`、`config`、`server`、`tls`、`docs`、`ci`、`release`。

**subject 必须反映真实内容**。禁止：

- `fix: misc` / `chore: cleanup` 这类无信息 message；
- `feat: complete phase N` 但 commit 里夹带无关调参；
- 用 message 隐瞒夹带变更。

**长 body 推荐**包含：

- 改了什么（what）
- 为什么改（why）
- 风险点 / 回滚方法（how to revert）

## 示例

```
feat(proxy): add automatic l4 fallback for non-HTTP/1.1 traffic

thin-l7 now sniffs the first bytes. HTTP/2 preface, TLS records,
Upgrade/CONNECT and unparseable heads switch to the raw splice,
forwarding buffered bytes first.

Risk: fallback is connection-scoped; no per-request escalation back
to l7. Revert by removing the sniff branch in src/proxy/l7.rs.
```
