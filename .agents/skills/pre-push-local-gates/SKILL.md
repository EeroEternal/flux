---
name: pre-push-local-gates
description: Push 前必须在本地跑满与 CI 等效的门禁（Rust fmt/clippy/tests），禁止把 CI 当本地沙盒。Use before every git push touching src/, tests/, Cargo.toml or CI.
---

# Pre-push local gates（推送前本地门禁）

## 核心痛点与禁止项

严禁把 CI 当本地沙盒：推送后才发现 lint 报错、rustfmt 未对齐、编译报警、测试
失败，形成「推 → 挂 → 本地修 → 再推」的低效循环，污染提交历史并浪费 Runner 配额。

## 本地门禁执行标准

在执行 `git push` 或提 PR 之前，以下命令必须**全部在本地通过**（与 CI 等效）：

```bash
# 一条命令跑满（推荐）
bash scripts/local-gates.sh

# 首次克隆后安装本地 Git pre-push 钩子（自动在 git push 前执行 local-gates.sh）
bash scripts/install-hooks.sh

# 等价手工命令
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

涉及转发/超时/池化语义的改动，还必须确认 `tests/integration.rs` 覆盖了对应行为
（透传、半关闭、chunked 长流、连接复用、突然断开、自动回退、TLS）。

发版与打 Tag 前，转入 skill [`release`](../release/SKILL.md) 执行完整流程。

## 适用范围与纪律

- 开发过程中的中间 commit 允许临时不跑全量，但 **push 前最后一次提交必须全绿**。
- CI 意外挂了：禁止盲猜盲改，必须在本地复现等价的失败命令，本地确认修复后再推送。
- 判断上游成败时注意管道退出码陷阱：`cmd | tail` 的退出码是 tail 的。本 skill 的
  `scripts/local-gates.sh` 使用 `set -o pipefail`。
