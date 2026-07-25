# Y-Harness 中文快速开始

Y-Harness 是通用 Agent Harness 引擎，不是绑定某个业务的聊天客户端。
最短体验路径使用内置确定性模型；真实模型通过精确版本的 HTTPS JSON
Gateway 或宿主自定义 `LanguageModel` 接入。

## 1. 安装

需要 Rust 1.88 或更新版本。

```bash
cd /path/to/Y-Harness
./scripts/install.sh
yh --version
```

安装脚本只包装标准的 `cargo install --locked --features https-model`，
不会安装后台服务或修改系统配置。也可以不安装，直接把下文的 `yh`
替换为 `cargo run --locked --`。

## 2. 两分钟体验

运行一次完整 Agent Loop：

```bash
yh demo "你好，Y-Harness"
```

TUI 是独立选装产品，不编进引擎。安装并进入全屏 TUI：

```bash
./scripts/install-tui.sh
yh-tui --demo
```

TUI 支持：

```text
Enter           发送并执行一个 Turn
Ctrl/Alt+Enter  输入多行
Tab、←/→        切换输入区与 Inspector 面板
PageUp/Down     滚动权威会话投影
Ctrl+N          创建新 Thread
Ctrl+R          刷新 State、事件、Approval 与 Task 投影
Esc             取消运行中的 Turn
F1 或 ?         打开帮助
```

也可使用 `/new`、`/thread <id>`、`/graph <id>`、`/events`、
`/approvals`、`/tasks`、`/refresh`、`/cancel`、`/help` 和 `/quit`。
连接真实配置时执行 `yh-tui --config y-harness.json`。

演示使用内置确定性模型和 `echo` Tool，不访问网络。State 写入当前目录
的 `.y-harness/state.db`，派生 Trace 写入 `.y-harness/traces/`。

`yh-tui` 只通过 Protocol v10 调用 `yh`：它不读取 SQLite，不构造
Model/Tool/Policy，也不拥有权威状态。Desktop、Web、IM 等后续产品遵守
同一边界并独立选装。

## 3. 初始化持久化服务项目

```bash
mkdir my-harness
yh init my-harness
cd my-harness
yh doctor
```

`init` 创建：

```text
my-harness/
├── .gitignore
├── .y-harness/
└── y-harness.json
```

它使用 no-clobber 语义：配置已经存在时直接失败，不覆盖用户内容。
默认配置使用本地演示模型。`doctor` 验证配置版本、模型构造、凭据、
Provider 权限和数据目录边界，但不会打开或创建数据库。

## 4. 运行 Protocol v10 服务

`serve` 通过 stdin/stdout 读写一行一个 JSON 的 Protocol v10 帧：

```bash
yh serve y-harness.json
```

直接运行时看起来像“停住”，是因为它正在等待客户端输入。可以用一条
初始化请求验证：

```bash
printf '%s\n' \
  '{"id":"init-1","protocol_version":"10","command":{"method":"initialize"}}' \
  | yh serve y-harness.json
```

首次服务启动后会创建：

```text
.y-harness/
├── state.db       Thread、Turn、Item 和事件日志
├── approvals.db   持久化审批 Inbox
└── tasks.db       Task Graph、租约、消息和结算
```

三个 SQLite 权威库均使用 WAL、`synchronous=FULL` 和精确 schema
校验。服务重启后 Thread 与 Task Graph 仍可恢复。

## 5. 运行真实 Task Worker

仓库包含一个只依赖 Python 标准库的语言无关客户端：

```bash
YH_BIN="$(command -v yh)" \
python3 /path/to/Y-Harness/examples/task_worker_client.py \
  /absolute/path/to/my-harness/y-harness.json
```

示例会：

1. 初始化并协商能力；
2. 创建 `collect → synthesize` Task DAG；
3. claim 第一个 Task；
4. 通过租约保护的 Mailbox 发送消息；
5. 完成第一个 Task；
6. claim、读取消息并完成第二个 Task；
7. 验证整个图进入 terminal 状态。

Worker 身份不允许在请求中自报。stdio 使用 `local-process`；mTLS
网络宿主使用客户端叶证书的 SHA-256 指纹。租约时间只取服务端时钟。

## 6. 接入真实模型 Gateway

复制生产配置模板：

```bash
cp /path/to/Y-Harness/config/y-harness.https.example.json y-harness.json
```

修改 `endpoint`，然后只通过环境变量提供 Bearer Token：

```bash
export YH_MODEL_TOKEN='replace-with-real-secret'
yh doctor y-harness.json
yh serve y-harness.json
```

配置只保存 `bearer_secret_reference` 和环境变量名，永不保存 Token。
`exclusive_root_ca_pem_path` 可指定项目目录内的私有 CA；启用后不会
重新混入系统或 WebPKI 根证书。

Gateway 必须实现
[`protocol.md`](protocol.md) 所述的精确 Model Gateway v2 契约。
Y-Harness 不伪装不同厂商 API 具有相同语义；厂商适配应放在 Gateway
或宿主 Provider 中。

## 7. 作为 Rust 引擎嵌入

最小 Agent Loop：

```bash
cargo run --locked --example embedded
```

完整 Task Orchestrator：

```bash
cargo run --locked --example orchestrated
```

示例分别位于
[`examples/embedded.rs`](../examples/embedded.rs) 和
[`examples/orchestrated.rs`](../examples/orchestrated.rs)。

## 8. 安全与产品边界

- `serve` 是持久化 stdio 服务，适合由进程主管或受信宿主启动。
- 网络暴露必须使用现有 mandatory-mTLS host，不能把裸 JSONL socket
  直接当成生产网络服务。
- 默认 Policy 拒绝未注册 Tool；演示配置只允许内置 `echo`。
- Workspace Provider 默认拒绝文件系统 Task。
- 目录隔离和 Git Worktree 不是 OS 沙箱；不可信执行器必须继续经过
  Process Broker。
- Web 与 Desktop 是未来客户端，不属于首版 Runtime 内核。

完整验收步骤见
[`acceptance-checklist.zh-CN.md`](acceptance-checklist.zh-CN.md)。
