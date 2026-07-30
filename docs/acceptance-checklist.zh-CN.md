# Y-Harness 交付验收清单

本清单把“可以运行”与“可以发布”分开。所有命令均在仓库根目录执行。

## A. 安装与第一体验

- [ ] `./scripts/install.sh` 成功完成。
- [ ] `./scripts/install-tui.sh` 成功完成，且不改变 `yh` 引擎安装。
- [ ] `yh --version` 输出当前版本。
- [ ] `yh demo "验收 Harness"` 返回终态文本、Thread ID 和 Trace 路径。
- [ ] `yh-tui --demo` 进入 alternate screen，可执行 Turn、查看 Inspector
      并用 `/quit` 完整恢复终端。
- [ ] 空 Thread 给出首个 Turn 引导；短会话靠近 Composer；非零 State
      压力显示为 `<1%` 而不是 `0%`，并保留已用/上限。
- [ ] Header 将当前 Thread 容量标为 `thread events`；Activity 将事件
      ID 范围标为 `global sequence`，不会把两个计数解释成矛盾。
- [ ] `local/demo` 在 Header 和对应 Assistant 记录中明确标记为
      deterministic/no-network；Header 使用 `LAST MODEL`，不从历史记录
      推断下一 Turn 的 Engine Route。
- [ ] Engine/TUI 协议不匹配时，错误同时报告两个协议坐标，并给出从同一
      Checkout 重装 `yh` 与 `yh-tui` 的命令。
- [ ] 不安装 `yh-tui` 时，`yh serve`、嵌入式 Core 和协议行为保持不变。

## B. 项目初始化与诊断

```bash
project="$(mktemp -d)/project"
yh init "$project"
yh doctor "$project/y-harness.json"
```

- [ ] `y-harness.json`、`.gitignore` 和 `.y-harness/` 已创建。
- [ ] 对同一路径再次执行 `yh init` 失败，原配置字节不变。
- [ ] `doctor` 输出 Protocol、schema、模型、数据目录、六库
      `ready`/`will be created` 状态和 `status: ok`。
- [ ] 把旧版 State 测试库放入数据目录后，`doctor` 与 `serve` 都在构造
      外部 Model/MCP 前返回 `state-migrate` 指引；数据库字节不变且不会
      自动生成备份。
- [ ] 只含部分 Workflow 或 Effect 表的数据库失败关闭，不被当成可初始化空库，
      且 `doctor`/`serve` 在外部 Model 构造前不修改该库。
- [ ] 重启服务后，TUI Sessions 面板仍能列出并恢复此前 Thread，分叉项显示权威直接父级。
- [ ] TUI `/name <标题>` 设置的名称在重启后仍显示，`/name` 可清除。
- [ ] TUI `/fork [已终结-turn-id]` 创建独立子 Thread；父子后续 Turn
      互不影响，重启后 `lineage` 仍可读取。
- [ ] `yh thread export <thread-id> <archive> <config>` 导出终态 Thread，
      重复导出不覆盖文件；`yh thread import <archive> <target-id> <config>`
      原子导入且相同来源/目标可安全重试。
- [ ] 修改归档事件或摘要后导入失败，目标 Thread 不存在。

## C. 持久化服务

```bash
printf '%s\n' \
  '{"id":"init-1","protocol_version":"30","command":{"method":"initialize"}}' \
  | yh serve "$project/y-harness.json"
```

- [ ] 返回一个且仅一个成功 JSON 响应。
- [ ] 能力包含 Thread、Approval、Task Worker、Workflow、Human Handoff
      和 Effect
      表面。
- [ ] `.y-harness/state.db`、`approvals.db`、`tasks.db`、`workflows.db` 和
      `human-handoffs.db`、`effects.db` 已创建。
- [ ] 服务重启后此前创建的 Thread、Task Graph、Workflow Run 和 Human
      Handoff 与 Effect 仍可读取。
- [ ] Human Handoff 只能引用同租户的既有 Thread 或 Workflow Run；
      Claim/续租/释放/结算要求精确 revision、当前 actor 与 claim fence，
      同一 command ID 被其他 actor 或不同内容复用时失败。
- [ ] 嵌入宿主可用同一租户 Authority 与可信时间调用有限 Temporal tick，
      推进到期 Workflow 等待、过期 Claim 和 Effect 租约；Effect 进入
      `unknown` 而非重试，Core 不会自行启动后台轮询。
- [ ] `yh serve` 未配置 `temporal` 时不轮询；显式配置后使用同一固定
      Authority，限制 cadence/scan、跳过漏拍，并在 stdio 关闭时先停止。
- [ ] `recover_thread` 只有 `thread.recover` 权限可调用，并要求精确
      `expected_turn_id`；同一 Host 仍有活跃 Operation、过期 Turn ID
      或新的 running Turn 时均不修改 State。

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

## F. Model 扩展边界

- [ ] `model.type = "json_command"` 可用于兼容单 Model 配置，也可作为
      `models + model_route` 中的一个精确身份，无需修改 Rust。
- [ ] 命令必须是现存绝对路径，并显式选择 Process Broker；工作目录
      规范化、环境清空后逐项映射，输入/输出/时间/并发均有上限。
- [ ] 真实服务 Turn 能从 stdin 发送 `ModelRequest`，从 stdout 接收一个
      `message`、`tool_call` 或 `tool_calls`，并在 State 中保持 External
      Model 来源。
- [ ] Turn 取消传播给 Model 进程；超时、future drop 和 Unix 子进程组
      按现有有界 settlement 规则清理。
- [ ] 未配置 protocol 时严格保持 `output_v1`；不会从裸 `ModelOutput`
      推断 usage、cost、Provider request/model、continuation 或失败类别。
- [ ] 显式 `settlement_v1` 可保真返回 Provider 证据或有界 typed failure；
      Runtime 而非 adapter 决定 retry/failover，未知字段失败关闭。
- [ ] 两种 command Model protocol 都不伪装 provisional stream。

## G. MCP 扩展边界

- [ ] `mcp_servers` 的 stdio 条目必须显式启用和选择 Tool；停用条目不
      启动进程、不发现目录，也不授予 Policy 或 Memory 权限。
- [ ] `https_mcp_servers` 可仅通过配置加入远程 Tool；Endpoint 必须是
      无 userinfo/query/fragment 的 HTTPS，Bearer 只从命名环境变量解析。
- [ ] 远程 Tool 仍按 `namespace + allow` 精确注册，并经过普通
      Policy/Approval/State；缺失任一 Tool 时整批失败。
- [ ] 私有 CA 的真实 TLS `tools/list`、`tools/call` 和 `doctor` 装配测试
      通过；响应、Session ID、请求和超时均有上限。
- [ ] 当前 JSON-response 适配器明确拒绝 SSE，且不会重放失败 Tool；
      OAuth、跳转、环境代理和任意 Header 不会被误报为支持。

## H. Skill 供应链与项目生命周期

```bash
yh skill install \
  examples/skills/concise-assistant.skill.json \
  "$project/y-harness.json"
yh skill list "$project/y-harness.json"
yh skill verify "$project/y-harness.json"
```

- [ ] 本地包按内容摘要写入项目 `skills/`，但不会自动激活或修改配置。
- [ ] 离线第三方签名包只能通过 `install-external` 进入
      `.signed-skill.json` 存储，并保持 `External` 来源。
- [ ] `install-https` 要求精确公共 HTTPS URL、`name@version`、内容
      SHA-256 和预先验证通过的发布者信任配置。
- [ ] 发布者签名、有效期、撤销以及必需/已提供的透明度收据在首次写盘前
      通过验证；`doctor` 输出发布者和日志锁。
- [ ] 安装不等于激活；只有显式列入 `external_package_files` 和
      `activate` 的精确包进入 Context。
- [ ] 发布者或日志被实时撤销后，后续 `list`、`verify` 和 Context 编译
      失败；取消配置引用后，包仍能被移入可恢复垃圾目录。
- [ ] 不会自动更新、递归下载依赖、执行包内代码或把网络包降级为本地信任。

## I. 回归、安全与兼容性

- [ ] `evaluation.graders` 可通过配置增加外部 Grader，不需要修改 Rust。
- [ ] 每个 Grader 的样本、输出、并发、时间和取消均独立有界，来源与
      format-2 baseline 精确匹配。
- [ ] `yh eval` 使用进程内 State，不打开生产 State、Approval 或 Task
      数据库；`yh serve` 不构造未使用的 Grader。
- [ ] Grader 不能修改 Agent Loop、调用 Tool、替代 Verifier 或提交
      Turn。
- [ ] `yh-bench opencode <spec>` 只接受 format 5、绝对程序路径、精确
      CLI 版本与 SHA-256、`provider/model` 和显式环境变量名；`bare`
      profile 独占空 Home/XDG/Auth/内存数据库边界。
- [ ] OpenCode JSONL 跨 Session、重叠 Step、Tool 事件、非法 token/cost
      或错误后的尾随事件失败关闭；错误流不得把未知费用写成零，也不得
      把 requested Model 冒充 observed Model。
- [ ] OpenCode Model、variant 与 system prompt 中的 `{env:...}` /
      `{file:...}` 配置替换标记在启动进程前被拒绝。
- [ ] `yh-bench hermes <spec>` 只接受 format 6 的 `bare` profile、精确
      `Hermes Agent v<version> (<date>)` 首行（源码安装可含 Hermes 自带的
      revision 后缀）、Provider/Model，以及工作区外相互分离的空
      `hermes_home` 与 `usage_directory`。
- [ ] Hermes 版本探测不访问更新网络；静态空 `context_engine` toolset
      不暴露 Tool；用量文件必须是 64 KiB 内的普通文件并严格校验字段、
      完成状态、API 调用上限和观测到的 Provider/Model。
- [ ] Hermes 的 estimated cost 不得写成 actual cost；system prompt
      降级为 user 前缀、prompt 位于 argv、workspace rules 未被证明关闭、
      Python launcher 哈希不覆盖依赖图等限制必须进入报告。
- [ ] `yh-bench y-harness-cf003-restart <spec>` 只接受 format 9 的空工作区
      和哈希锁定二进制；真实服务重启先保留 abandoned Turn 为 `running`，
      显式精确 Turn recovery 后为 `interrupted`，新 Turn 无 Tool 完成，
      独立 oracle 前后均保持一次调用/一次 effect。
- [ ] Claude Code/Codex 共享 Provider 预检必须核对相同请求坐标与真实
      wire sidecar；只要 Model 元数据、协议、Tool、推理、Context、沙箱、
      预算或调用上限未对齐，机器结论必须保持 `not_comparable`。
- [ ] Codex/Grok Build 共享 Responses 预检必须覆盖模型目录、主 Agent 和
      所有辅助 Model 请求；辅助调用漂移到产品默认 Model 或其失败被静默
      吞掉时，不得把主请求对齐表述为整个 Turn 的 Model 对齐。
- [ ] 所有 released-product 适配器始终输出
      `claim_eligible: false`；单产品 conformance 不能作为效果超越证据。

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
cargo build --locked --bin yh -p y-harness
cargo build --locked --bin yh-tui -p y-harness-tui
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

## J. 发布

- [ ] `LICENSE-MIT` 与 `LICENSE-APACHE` 存在。
- [ ] `cargo package --locked -p y-harness` 成功。
- [ ] Git 工作区干净。
- [ ] 远程 CI 在精确发布提交上全绿。
- [ ] Release notes 明确 unsupported platform、外部 Gateway 和沙箱边界。

“永久零 Bug”不是可证明属性。最终发布声明应为：在指定提交和日期上，
所有支持范围的可执行证据通过，且没有已知 Critical/High 缺陷。
