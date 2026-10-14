# Engineering hard rules

This file adapts the console-kit engineering rules to Flux. Rules whose original
motivation was database/UI specific are dropped; the universal ones are kept and
extended with Flux's streaming constraints.

## 一、禁止夹带变更（Silent Drift）

单个 commit / PR 不得夹带与声明目标无关的修改：

1. **调参私货**：以 `refactor` / `format` 名义修改默认超时、池大小、阈值、
   并发上限、调度权重等生产参数。
2. **批量重格式化**：`cargo fmt` 的结果必须单独 commit，不得与逻辑改动混合。
3. **`#[allow(...)]` 压制**：不得用 `#[allow(clippy::xxx)]` 让警告消失而不写
   原因。确需豁免时，`// reason:` 必须同行。
4. **跨模块 refactor**：修 A 模块 bug 时不得顺手重命名 B 模块。

违规一律 `git reset --mixed HEAD~1` 重新拆分。

---

## 二、`git stash` 使用规则

1. **命名诚实**：`git stash push -m "..."` 不得用含糊描述掩盖逻辑改动。
2. **stash 前** `git diff --stat`，确认 feature 依赖不被抽走。
3. **stash 后必须** `cargo check --tests`（不可跳过）。
4. **不得 stash** `Cargo.toml` / `Cargo.lock` / 构建脚本。

---

## 三、后台任务 / `tokio::spawn` 规范

`tokio::spawn` 是高风险构造：

1. **生命周期显式化**：不得在 `new()` / `Default::default()` 里裸写
   `tokio::spawn`。必须提供显式 `start_background_xxx()` 入口，由 `main()`
   调用。
2. **Runtime 守卫**：spawn 代码用 `tokio::runtime::Handle::try_current()` 包装，
   或只在已知 runtime 上下文触发。
3. **超时保护**：长跑任务里的每次异步调用必须有 `tokio::time::timeout(...)`
   兜底，避免 daemon 永久挂死。
4. **错误不得吞掉**：panic / timeout 必须 `tracing::error!` 至少记录一条。
5. **可观测**：daemon 写共享状态必须有 metric / debug endpoint 能观察到，
   否则无法判断存活。

---

## 四、避免幻觉代码（Ghost Code）

1. **有定义无调用**：函数 / 类型全仓库 `rg` 找不到 caller。
2. **有路径无写入**：新增字段有 load 点无 store 点。
3. **有埋点无分类**：metric 只埋上升不埋下降（或反之）。关键路径正反两向都要埋。
4. **有 TODO 无 issue**：`// TODO:` 必须关联 issue。

发现即一次性补齐 caller / store / 下游埋点，不接受"下一轮再做"。

---

## 五、脚本化批量修改规范

1. **regex 替换后必须 grep 验证**（脚本静默失败不报错）。
2. **多行 regex 特别危险**：优先用 `StrReplace`/编辑工具而非自写脚本。
3. **内嵌字符串逃逸**：脚本改完必须人肉 review 生成的代码段。

---

## 六、命令管道的退出码陷阱

管道退出码默认取末位命令：`失败命令 | tail -3` 的退出码是 tail 的 0。

- 判断上游成败用 `set -o pipefail`、临时文件承接输出，或先跑再读输出。
- 交互式判断"完成了吗"直接查权威源，不信代理进程退出码。
- `bash -n` 只验语法；必须实跑并制造一次上游失败确认链路中断。

---

## 七、流式内核不变量（Flux 专属，最高优先级）

以下四条是 Flux 的正确性底线，任何改动都必须保持：

1. **零缓冲**：`forward_body` 每次只持有一个有界 read；L4 用
   `copy_bidirectional`。不得引入任何聚合/解码中间层。
2. **只在可证明的边界复用连接**：只有响应自定界（Content-Length 或完整
   chunked）且无残留字节时，才可 `release_conn`。
3. **首字节后不重试**：一旦有响应字节流向客户端，任何失败只关两端、不重放。
4. **超时分层**：`connect / idle / total / fin` 各司其职；L4 的 idle 由活动
   看门狗实现，L7 由每次读写超时实现。

修改 `src/proxy/` 必须同时在 `tests/integration.rs` 增加或更新对应行为测试。

---

## 八、违规处理

1. **报告与代码不符**：`git reset --hard` 回退到上一个诚实状态，重写提交。
2. **编译失败还报完成**：暂停工作，提交复盘。
3. **重复同类错误 3 次以上**：后续 commit 必须 pair review。
4. **夹带生产参数私货**：立即 revert，重新按单独 commit 提交。
