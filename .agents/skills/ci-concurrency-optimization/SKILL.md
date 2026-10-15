---
name: ci-concurrency-optimization
description: GitHub Actions CI 并发优化、变更感知过滤与缓存防逐出规范。Use when configuring, debugging, or optimizing CI pipelines for xFlux.
---

# CI 并发优化与缓存调度规范 (CI Concurrency & Cache Optimization)

本规范源自 `console-kit` / `xrouter` 生产级 CI 体系的工程化沉淀，旨在解决 GitHub Actions 中面临的**构建排队、纯文档改动重复编译、Runner 配额耗尽与缓存频繁逐出**等痛点。

---

## 核心设计支柱 (Core Architectural Pillars)

### 1. 抢占式并发取消 (Preemptive Concurrency Cancellation)
在高频提交或 PR 迭代场景下，旧 commit 的 CI 仍在运行会挤占 Runner 队列并造成无谓等待。
必须配置 workflow 级并发控制，使同分支新 push 自动取消前序运行：

```yaml
concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
```

### 2. 变更感知与精准分流 (Path-Based Change Detection)
并非所有提交都需要跑满全量编译与集成测试。通过 GitHub API 比对 PR / push 改动的文件清单，进行条件分流：
- **纯文档/规范/Skill 变更**（如 `docs/`、`.agents/`、`*.md`）：直接跳过重型编译与测试。
- **代码/构建/脚本变更**（`src/`、`tests/`、`Cargo.toml`、`Cargo.lock`、`scripts/`、`.github/`）：触发 Rust 门禁。

### 3. Rust 统一共享缓存 (Cache Quota Anti-Thrashing)
- **强制规则**：使用 `Swatinem/rust-cache@v2` 时显式指定统一的 `shared-key`：
  ```yaml
  - uses: Swatinem/rust-cache@v2
    with:
      shared-key: ci-rust
  ```
- **禁用增量编译**：CI 环境中设置 `CARGO_INCREMENTAL: "0"`，避免未命中缓存时的磁盘与时间膨胀。

---

## 检查清单 (Verification Checklist)

配置或调整 CI 流水线时，核对以下项目：
- [ ] 根部已设置 `concurrency.cancel-in-progress: true`；
- [ ] 包含前置 `changes` job，正确导出 `code` 判断标志；
- [ ] 所有 Rust 构建步骤包含 `CARGO_INCREMENTAL: "0"` 与统一的 `shared-key: ci-rust`；
- [ ] 门禁步骤与本地 `scripts/local-gates.sh`（`pre-push-local-gates`）保持 100% 对齐。
