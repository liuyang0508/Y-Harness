# Y-Harness 交付验收清单

本清单把“可以运行”与“可以发布”分开。所有命令均在仓库根目录执行。

## A. 安装与第一体验

- [ ] `./scripts/install.sh` 成功完成。
- [ ] `./scripts/install-tui.sh` 成功完成，且不改变 `yh` 引擎安装。
- [ ] `yh --version` 输出当前版本。
- [ ] `yh demo "验收 Harness"` 返回终态文本、Thread ID 和 Trace 路径。
- [ ] `yh-tui --demo` 进入 alternate screen，可执行 Turn、查看 Inspector
      并用 `/quit` 完整恢复终端。
- [ ] 不安装 `yh-tui` 时，`yh serve`、嵌入式 Core 和协议行为保持不变。

## B. 项目初始化与诊断

```bash
project="$(mktemp -d)/project"
yh init "$project"
yh doctor "$project/y-harness.json"
```

- [ ] `y-harness.json`、`.gitignore` 和 `.y-harness/` 已创建。
- [ ] 对同一路径再次执行 `yh init` 失败，原配置字节不变。
- [ ] `doctor` 输出 Protocol、schema、模型、数据目录和 `status: ok`。

## C. 持久化服务

```bash
printf '%s\n' \
  '{"id":"init-1","protocol_version":"11","command":{"method":"initialize"}}' \
  | yh serve "$project/y-harness.json"
```

- [ ] 返回一个且仅一个成功 JSON 响应。
- [ ] 能力包含 Thread、Approval 和 Task Worker 表面。
- [ ] `.y-harness/state.db`、`approvals.db`、`tasks.db` 已创建。
- [ ] 服务重启后此前创建的 Thread 和 Task Graph 仍可读取。

自动化证据：

```bash
cargo test --locked --test service_cli
```

## D. Task Worker

```bash
YH_BIN="$(command -v yh)" \
python3 examples/task_worker_client.py "$project/y-harness.json"
```

- [ ] 输出 `tasks: 2 terminal: true`。
- [ ] 第二个 Task 只能在第一个 Task 完成后 claim。
- [ ] Mailbox 消息通过当前未过期租约读取。
- [ ] 最终图 revision 为 6。

## E. Rust 公共接口

```bash
cargo run --locked --example embedded
cargo run --locked --example orchestrated
```

- [ ] 嵌入式 Agent Loop 保留 Model、Tool 与 Policy 来源证据。
- [ ] Orchestrator 完成依赖 DAG、Workspace cleanup 和精确租约结算。

## F. 回归、安全与兼容性

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo clippy --locked -p y-harness --lib --all-features -- \
  -D clippy::unwrap_used \
  -D clippy::expect_used \
  -D clippy::panic \
  -D warnings
cargo clippy --locked -p y-harness-tui --bin yh-tui -- \
  -D clippy::unwrap_used \
  -D clippy::expect_used \
  -D clippy::panic \
  -D warnings
cargo test --locked --workspace --all-targets --no-default-features
cargo test --locked --workspace --all-targets --all-features
python3 scripts/smoke-tui.py
python3 scripts/smoke-tui.py --configured
cargo run --locked -- eval-smoke
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features
cargo audit --deny warnings
```

- [ ] 所有必需门禁通过。
- [ ] 只有文档明确列出的外部 fixture 和最大迁移测试可以 ignored。
- [ ] `docs/protocol.md`、`docs/compatibility.md` 与代码坐标一致。
- [ ] 没有已知 Critical 或 High 缺陷。

## G. 发布

- [ ] `LICENSE-MIT` 与 `LICENSE-APACHE` 存在。
- [ ] `cargo package --locked -p y-harness` 成功。
- [ ] Git 工作区干净。
- [ ] 远程 CI 在精确发布提交上全绿。
- [ ] Release notes 明确 unsupported platform、外部 Gateway 和沙箱边界。

“永久零 Bug”不是可证明属性。最终发布声明应为：在指定提交和日期上，
所有支持范围的可执行证据通过，且没有已知 Critical/High 缺陷。
