---
name: release
description: 发版（打 tag）完整 promoter 流程：本地门禁全量重跑、发版三查（版本号/changelog/文档生命周期）、人工批准硬停、tag 与验证。Use before cutting any vX.Y.Z tag or production deploy.
---

# Release Promoter Process（发版与打 Tag 流程规范）

## 适用场景

在执行任何 `git tag v*.*.*`、合并 release 分支或触发生产部署之前，必须遵守本规范。

---

## 核心铁律

**未经人类用户明确书面批准，AI Agent 绝对不得执行 `git tag` 或合并发布分支。**
用户口头/文字要求"发布 vX.Y.Z"即视为批准，但必须先把发版摘要贴全。

---

## Changelog（强制）

Canonical file: [`docs/changelog.md`](../../../docs/changelog.md)（Keep a Changelog + SemVer）。
仓库根目录不存在 `CHANGELOG.md`。

**落地工作时**（不只是打 tag 时）：把 notable feature / fix / breaking change 写到
`## [Unreleased]` 下对应的 `### Added` / `### Changed` / `### Fixed` / `### Removed`。

**切 `vX.Y.Z` 时：**

1. 把 `## [Unreleased]` 改名为 `## [X.Y.Z] - YYYY-MM-DD`（当天日期）。
2. 在其上方插入新的空 `## [Unreleased]`。
3. 删掉空的小节。禁止出现只有 `misc` / commit 标题 / "various improvements" 的版本。

每条 bullet 必须能让用户读懂：

| 必须包含 | 示例 |
| --- | --- |
| 改了什么 | thin-l7 嗅探到非 HTTP/1.1 时自动回退 L4 |
| 影响谁 / 哪里 | 所有 `mode = "thin-l7"` 部署；`src/proxy/l7.rs` |
| 是否破坏性 | Breaking: 无 |

拒绝：`- feat: auth`、`- update docs`、`- fix bug`。

---

## 发版 4 步走

### 第 1 步：本地门禁全量重跑

```bash
git status            # 确认工作区干净
bash scripts/local-gates.sh
```

### 第 2 步：发版三查

1. **版本号一致性**：`Cargo.toml` 的 `version` 已正确自增。
2. **Changelog**：`docs/changelog.md` 已把 `[Unreleased]` 转正为
   `[X.Y.Z] - <date>`，条目可独立阅读。
3. **敏感信息与构建物扫描**：无私有密钥、`.env`、临时调试日志或未编译产物。

### 第 3 步：人工批准硬停

向用户输出完整发版摘要（拟定 Tag、Commit Hash、**本版全部 changelog 条目**），
明确请求批准。不得只贴 commit list 代替 changelog。

### 第 4 步：打 Tag 并验证

获得批准后：

```bash
git tag -a vX.Y.Z -m "release: vX.Y.Z"
git push origin main
git push origin vX.Y.Z
```

推完确认远端 tag 指向的 commit 与本地一致。
