# Y-Harness 中文快速开始

Y-Harness 是通用 Agent Harness 引擎，不是绑定某个业务的聊天客户端。
最短体验路径使用内置确定性模型；真实模型通过精确版本的 HTTPS JSON
Gateway、可选 OpenAI Responses Provider、受 Process Broker 管控的
JSON command，或宿主自定义 `LanguageModel` 接入。

## 1. 安装

需要 Rust 1.88 或更新版本。

```bash
cd /path/to/Y-Harness
./scripts/install.sh
yh --version
```

安装脚本只包装标准的
`cargo install --locked --features http-probe,https-mcp,https-model,https-skill`，
不会安装后台服务或修改系统配置。`https-mcp` 和 `https-skill` 只用于
操作员显式配置的远程 MCP 或签名包获取；作为库嵌入时仍可不编译这些
Feature。也可以不安装，直接把下文的 `yh` 替换为
`cargo run --locked --`。

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
Ctrl+R          刷新 State、会话、事件、Approval 与 Task 投影
Esc             取消运行中的 Turn
F1 或 ?         打开帮助
```

空 Thread 会显示首个 Turn 的明确入口；短会话贴近 Composer，避免重要
内容与输入区被大面积空白分隔。容量显示同时给出已用/上限和不会把非零
压力舍入为 `0%` 的比例；Header 的 `thread events` 是当前 Thread 的
容量，Activity 的 `global sequence` 是数据库全局事件序号，两者不会
混用。TUI 只从权威 State Item 推导已实际参与决策的 Model；检测到
`local/demo` 时会持续标记“确定性演示、无网络”，不会把它伪装成真实
LLM。Header 明确使用 `LAST MODEL`：历史 State 只能证明上一条决策，
不能推断下一 Turn 的当前 Route。此增强不读取配置文件，也不改变
Protocol。

Sessions 面板列出最近 64 个权威 Thread，并显示分叉 Thread 的直接父级
及父流版本；选中后按 Enter 即可恢复。
`/name <标题>` 设置当前 Thread 的权威名称，单独输入 `/name` 清除名称。
`/fork` 从当前已结算末尾创建独立子 Thread；`/fork <terminal-turn-id>`
可从更早的已终结 Turn 分叉。也可使用 `/new`、`/sessions`、
`/thread <id>`、`/graph <id>`、`/events`、
`/approvals`、`/tasks`、`/runtime`、`/trace`、`/refresh`、`/cancel`、
`/resume`、`/cancelwait`、`/help` 和 `/quit`。
连接真实配置时执行 `yh-tui --config y-harness.json`。

如果出现 `Engine protocol ... did not match TUI protocol ...`，说明独立
安装的 `yh` 与 `yh-tui` 来自不同源码修订；Cargo 包版本相同也不能代表
协议坐标相同。请在同一个仓库检出版本中依次重新执行：

```bash
./scripts/install.sh
./scripts/install-tui.sh
```

演示使用内置确定性模型和 `echo` Tool，不访问网络。State 写入当前目录
的 `.y-harness/state.db`，派生 Trace 写入 `.y-harness/traces/`。

`yh-tui` 只通过 Protocol v37 调用 `yh`：它不读取 SQLite，不构造
Model/Tool/Policy，也不拥有权威状态。Desktop、Web、IM 等后续产品遵守
同一边界并独立选装。

TUI 默认给 Turn 配置有限的持久 Approval 等待时间。当前只有“模型本轮只
提出一个 Tool call，且 Policy 返回 `ask`”时会释放执行 Worker：Header
显示 `WAITING`，原进程 Operation 可以被忘记，而 Turn 仍以 State 中的
`Waiting` 坐标保持 `Running`。审批必须由另一个经过认证、具备审批权限的
主体结算；随后输入 `/resume` 会按 `thread_id + turn_id + wait_id +
revision` 精确恢复，`/cancelwait` 会取消尚未 Claim 的 `Waiting` 或
`Ready`。等待期间输入普通文本不会偷偷创建第二个 Turn。Engine 重启后，
TUI 会通过 `get_turn_execution` 重新发现该坐标。

这不是通用工作流调度器：批量 Tool call、`HumanInput`、Approval Inbox
修复队列/墓碑、跨进程 Worker Lease、未知副作用 `NeedsReconciliation`
和跨进程 Resume 结果回执仍未实现。等待到期会在 Resume 路径重新校验，
也可由显式配置 `temporal` 的参考宿主通过租户级有界 due 索引主动结算；
Core 和 Protocol 请求处理器本身不启动调度线程。

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
默认配置使用本地演示模型。`doctor` 先以只读权限预检已有的六个 SQLite
权威库，再验证配置版本、模型构造、凭据、Provider 权限和数据目录边界。
它会将每个库报告为 `ready` 或 `will be created`，但不会创建、初始化、
迁移数据库或生成备份。旧版、残缺、未知 Schema 会在任何外部 Model、
MCP 或 Memory 进程启动前失败关闭，并给出对应迁移命令。

## 4. 运行 Protocol v37 服务

`serve` 通过 stdin/stdout 读写一行一个 JSON 的 Protocol v37 帧：

```bash
yh serve y-harness.json
```

直接运行时看起来像“停住”，是因为它正在等待客户端输入。可以用一条
初始化请求验证：

```bash
printf '%s\n' \
  '{"id":"init-1","protocol_version":"37","command":{"method":"initialize"}}' \
  | yh serve y-harness.json
```

需要容器/Kubernetes 探针时，可在配置中显式加入：

```json
{
  "http_probe": {
    "bind_address": "127.0.0.1:8081",
    "max_connections": 64,
    "request_timeout_ms": 2000,
    "status_timeout_ms": 1000,
    "shutdown_timeout_ms": 5000
  }
}
```

`GET /livez` 只证明同一进程内的 Handler 能返回权威状态；`GET /readyz`
只在仍可接收一个新 Turn 的 `ready` 状态返回 200，容量耗尽和单向停机均
返回 503。两者都不会探测或担保 Model、MCP、Memory、Registry、Effect
目标或多节点仲裁。未配置时没有 HTTP 监听器；不含 `http-probe` Feature
的自定义最小构建会明确拒绝这段配置。

进程主管停止 `yh serve` 时无需先关闭 stdin：Unix 的 SIGTERM/SIGINT 与
Windows 的 Ctrl-C 会停止接收下一条完整协议帧，随后进入同一个单向 drain
和有界资源结算流程。已经接收的帧仍会写完完整响应。该信号策略只属于参考
Host；嵌入式宿主可用 `serve_jsonl_until_cancelled` 接入自己的生命周期。
SIGKILL 等不可捕获终止不在优雅停机承诺内。

首次服务启动后会创建：

```text
.y-harness/
├── state.db       Thread、Turn、Item 和事件日志
├── approvals.db   持久化审批 Inbox
├── tasks.db             Task Graph、租约、消息和结算
├── workflows.db         Workflow Run、等待、迁移和命令证据
├── human-handoffs.db    人工接管队列、Claim 租约和结算证据
└── effects.db           外部副作用意图、执行租约、不确定态与对账证据
```

六个 SQLite 权威库均使用 WAL、`synchronous=FULL` 和精确 schema
校验。服务重启后 Thread、Task Graph、Workflow Run 与 Human Handoff
以及 Effect 均可恢复。Human Handoff 只记录人工所有权生命周期；它不会隐式暂停
Turn、路由 IM、唤醒对话或执行业务操作。库级 Temporal Driver 可以由
嵌入宿主显式调用来推进到期 Workflow 等待、过期 Claim、过期 Effect
执行租约，以及 Agent Loop 的持久 Approval 等待；Agent Loop 等待通过
租户级有界 due 索引和精确 stream fence 收敛，不扫描完整 Thread。Effect
租约到期只进入 `unknown`，绝不自动重试。Reference
Service 默认仍不轮询；需要宿主承担时间与生命周期时，显式加入：

SQLite 部署采用受控本地、不可变命名空间契约：从任一 Coordinator 打开前，
直到引用同一库的所有 Coordinator 与 caller-held guard 全部释放，数据库主文件
及其 `-wal`／`-shm` 旁路文件都不得重命名、删除、替换或热切换。只支持 SQLite
锁语义可靠的本地文件系统；不得让不受控的同权限进程绕过该生命周期修改这些路径。
运行时 same-file 检查只能在此契约内拒绝可观察的替换与别名，不是持久 store UUID，
也不能把契约外的路径 ABA、共享网络文件系统或热替换变成安全能力。

```json
{
  "temporal": {
    "poll_interval_ms": 1000,
    "scan_limit": 64
  }
}
```

`poll_interval_ms` 允许 100–86,400,000，`scan_limit` 允许 1–256，
且是“每个事实源、每个 tick”的上限。启用后服务使用与 Protocol 相同
的固定 Authority，漏掉的节拍不会补跑；关闭 stdin 时先停止轮询，再
清理 Protocol Operation 和 MCP。它只推进已有 CAS 状态，不会自动
执行 Task、Effect、Tool、补偿或消息路由。可直接复制
`config/y-harness.temporal.example.json` 并用 `yh doctor` 检查。

如果嵌入产品需要执行 `pending` Effect，可选装 Governed Effect
Executor API 1，并注册精确版本的 Connector、显式 operation 集合和
幂等契约，再安装默认拒绝之外的执行 Policy。每次 `run_once_as` 只做
一次有界扫描；同一 Claim 的重复调用者不会再次进入 Connector，分发后
的错误、panic、超时或取消一律落为 `unknown`。Core 不会替宿主启动
消费者线程，也不管理 Channel、凭据、回执真伪或自动对账。可运行：

```bash
cargo run --locked --example effect_executor
```

如果外部结果已经进入 `unknown`，可选装 Governed Effect Reconciler API 1，
注册精确 capability/operation 和 `authoritative_read_only` 查询契约，再
显式放行对账 Policy。它只查询权威目标状态，不会重试原操作；有效的
`Applied` / `NotApplied` 证据经现有 revision/attempt/lease CAS 收敛，
查询错误、panic、超时、取消、畸形证据或 `StillUnknown` 均保持原状态。
宿主仍负责轮询节奏、凭据、Connector 隔离与回执真伪。可运行：

```bash
cargo run --locked --example effect_reconciler
```

如果 Connector 使用其他语言实现，可采用精确 JSON Effect Connector
protocol 1。两个适配器分别处理执行和只读对账，经 `ProcessBroker` 直接
传参而不经过 shell，清空继承环境并限制输入、输出、时间与并发；它们不
会绕过 Policy、Ledger CAS 或 Unknown 规则。可运行真实子进程示例：

```bash
cargo run --locked --example json_effect_connector
```

`yh serve` 也可通过严格的 `effect_consumer` 配置选装常驻执行和对账循环。
两者彼此独立，分别拥有精确 Connector 注册表和非空 allowlist；注册能力
不等于获得调用权限。轮询、失败退避、并发和超时均有界，进程内 cursor
不承担恢复语义，唯一持久恢复权威仍是 Effect Ledger。省略配置时不会启动
后台任务。每个 Effect Connector 都必须配置真实的 `command_sha256`；
Broker 会在装配时及每次 dispatch 前，在同一个取消与总超时预算内重新
测量命令文件。摘要漂移时不会进入子进程，恢复原字节后可继续运行。这个
机制不是原子 OS exec 绑定，也不覆盖解释器、参数脚本、动态库或同权限
文件系统竞态。完整配置与边界见：

执行侧还可选配持久 `governor`。它以可信的“租户 + capability + operation +
policy_id”作为执行通道，在 durable Claim 之后、Connector 入口之前原子完成
固定窗口限流、连续失败熔断和单一 half-open probe；状态保存在独立的
`effect-governance.db`，重启后不会清零。它不会从任意 Effect JSON 猜测业务
收件人或供应商目标，也不会解析 `reason_code` 来判断故障。合法的 Connector
`Unknown` 是一次健康的协议响应；只有 Harness 能证明的 panic、错误、超时
或非法证据才累计熔断。权威只读 reconciliation 不受执行熔断影响。详见
[`ADR 0141`](adr/0141-durable-effect-dispatch-governance.md)。

需要凭据的 Effect Connector 可选用引用式环境注入：

```json
{
  "secret_environment": {
    "TARGET_API_TOKEN": {
      "reference": "effect/notification-primary",
      "host_environment": "NOTIFICATION_API_TOKEN"
    }
  }
}
```

配置不含凭据值；`doctor` 通过类型化的 Service Secret 上下文做可用性
探测，运行时再依据真实 `EffectId + operation + phase + attempt + lease`
以及可信 Authority 逐次解析。值只进入不可克隆、可清零的进程请求缓冲区，
不会进入 Effect Ledger 或 JSON Connector stdin。操作系统与子进程收到的
必要副本不在清零承诺内。`environment_from_host` 仍只是普通配置投影，
不能替代这条 Secret 边界。带 Secret 的 Connector 必须配置
`command_sha256`：适配器先在 Provider 查询前测量，Broker 再在子进程入口前
测量；这会拒绝既有漂移，但不是原子 `exec` 绑定。

- [`config/y-harness.effect-consumer.example.json`](../config/y-harness.effect-consumer.example.json)
- [`ADR 0137`](adr/0137-optional-reference-service-effect-consumer.md)
- [`ADR 0138`](adr/0138-dispatch-time-effect-command-digest-locks.md)
- [`ADR 0139`](adr/0139-typed-secret-use-and-effect-credential-custody.md)
- [`ADR 0140`](adr/0140-secret-gated-effect-command-integrity-preflight.md)
- [`ADR 0141`](adr/0141-durable-effect-dispatch-governance.md)

如果 `doctor` 报告迁移要求，先停止所有可能读写对应数据库的服务，再为
回滚文件选择一个尚不存在的路径，并只执行诊断中对应的命令：

```bash
yh state-migrate .y-harness/state.db .y-harness/state-pre-v14.rollback.db
yh approval-migrate .y-harness/approvals.db .y-harness/approvals-pre-v3.rollback.db
yh task-migrate .y-harness/tasks.db .y-harness/tasks-pre-v3.rollback.db
```

不要把三条命令当成固定启动步骤；只迁移 Doctor 指出的旧库。迁移工具
不会覆盖回滚文件，也不会猜测历史 Thread、Approval 或 Graph 的租户。
成功后再次执行 `yh doctor y-harness.json`，确认六个库均为 `ready`。
Workflow、Human Handoff 与 Effect 当前从 Schema 1 起步，不存在可猜测的旧版
迁移路径。

终态 Thread 可以在项目之间迁移，文件只承担传输，State Engine 仍是
唯一语义权威：

```bash
yh thread export <thread-id> thread.yh-thread.json y-harness.json
yh thread import thread.yh-thread.json <target-thread-id> y-harness.json
```

导出不会覆盖已有文件；存在运行中 Turn 时拒绝导出。导入先验证大小、
格式、完整事件序列与 SHA-256，再以目标 Thread ID 原子写入；重复使用
相同目标 ID 只在来源证据一致时幂等成功。

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

## 6. 接入真实模型

### 直接接入 OpenAI Responses

复制直接 Provider 配置，但必须把模板中的模型占位符改为当前项目
实际可用的明确模型 ID：

```bash
cp /path/to/Y-Harness/config/y-harness.openai.example.json y-harness.json
export OPENAI_API_KEY='replace-with-real-secret'
yh doctor y-harness.json
yh-tui --config y-harness.json
```

配置只保存 Secret 引用和环境变量名，不保存 API Key。省略 `endpoint`
时访问 OpenAI 官方 HTTPS Responses endpoint；也可复制
[`y-harness.responses-compatible.example.json`](../config/y-harness.responses-compatible.example.json)
显式指定实现相同 Responses wire contract 的 HTTPS 端点，从而在不改
Rust 的情况下增加兼容 Provider。适配器禁用 redirect、proxy、retry 和
referer，发送 `store: false`，并把响应 Token 使用量和
`x-request-id` 作为可观测证据返回。

流式文本经过有界 SSE 解码进入 TUI，但最终 Response 才是权威结果。
Provider 发送 `parallel_tool_calls: true`，允许 OpenAI 在一次响应中
提出多个函数调用；这不是并行执行授权。Y-Harness 会把完整批次作为
schema-7 单个权威事件持久化，先完成全部 Policy/Approval，再按来源
顺序依次执行 Tool。Tool 调度、Policy、State 和重试仍由 Y-Harness
唯一负责。这与
[OpenAI 官方 Function Calling 流程](https://developers.openai.com/api/docs/guides/function-calling#the-tool-calling-flow)
一致。

State schema 5 会保存 OpenAI 推理模型在 `store: false` 下要求重放的
加密 reasoning continuation。Runtime 把它绑定到实际完成请求的模型
身份与来源，在 Tool 结果后的下一步锁定该模型，并在发送新请求前按
来源过滤其他 Provider 的私有状态。Tool 调度、Policy、Approval 与
State 权威仍全部属于 Y-Harness。若函数调用携带无法重放的 reasoning
项，引擎仍会在任何 Tool 副作用之前明确失败。该边界遵循
[OpenAI 手工管理历史必须重放 output items 的说明](https://developers.openai.com/api/docs/guides/latest-model#update-api-and-model-parameters)。

没有 `OPENAI_API_KEY` 和明确模型 ID 时，Y-Harness 不猜测凭据或默认
模型，也不会把 demo 响应伪装成真实调用。

### Chat Completions、原生 Anthropic/Gemini 与共享 Provider Profile

复制多 Provider 模板，并把其中的四个模型占位符替换为账号实际可用的
明确模型 ID：

```bash
cp /path/to/Y-Harness/config/y-harness.provider-profiles.example.json y-harness.json
export OPENAI_API_KEY='replace-with-real-secret'
export ANTHROPIC_API_KEY='replace-with-real-secret'
export GEMINI_API_KEY='replace-with-real-secret'
yh doctor y-harness.json
yh-tui --config y-harness.json
```

`provider_profiles` 把协议族、Secret 引用、环境变量、HTTPS endpoint、
API 版本、超时、响应字节、并发和输出 Token 上限绑定一次；
`provider_model` 只选择稳定 Harness ID、Profile ID 和厂商模型 ID。因此
同一厂商增加模型、调整路由或更换 API Key 映射只需修改配置，不需增加
Rust 代码。Profile 本身不授予 Tool 权限，也不会把 Secret 暴露到 Runtime
Catalog、State 或 Protocol。

`open_ai_chat_completions` 覆盖广泛实现的 Chat Completions 兼容协议族，
支持 SSE 文本与 Tool delta 交错、并行 Tool 调用、usage-only 终止块、
`max_completion_tokens`/旧式 `max_tokens` 选择，以及 Tool 后续请求所需的
assistant Tool 消息精确回放。公网 endpoint 始终要求 HTTPS；本地
Ollama、vLLM 等可显式设置 `allow_loopback_http: true`，但只接受
`127.0.0.0/8` 或 `::1` 的字面 IP，不接受主机名、跳转或环境代理。
可直接复制
[`y-harness.openai-chat-local.example.json`](../config/y-harness.openai-chat-local.example.json)，
把 endpoint、模型 ID 与 `LOCAL_MODEL_API_KEY` 调整为本机服务的明确值。

`anthropic_messages` 使用原生 `/v1/messages`、`x-api-key` 和固定
`anthropic-version`；原生 `tool_use/tool_result`、并行 Tool 请求、SSE 事件
与 usage 都有独立解码。`gemini_generate_content` 使用原生
`generateContent`/`streamGenerateContent` 和 `x-goog-api-key`；
`functionCall/functionResponse`、并行调用、`usageMetadata` 以及 Gemini
要求后续回放的 `thoughtSignature` 都保留在来源绑定的 continuation 中。
两者都只让 Provider **提出** Tool 调用，Policy、Approval、执行、State、
重试与完成条件仍由 Y-Harness 负责。

Provider Profile 是配置复用与治理边界，不是“万能兼容层”。当前一等
协议族是 OpenAI Responses、OpenAI Chat Completions、Anthropic Messages
和 Gemini `generateContent`；其他协议继续通过精确 Gateway、JSON-command
Broker 或嵌入式 `LanguageModel` 适配，不能仅靠改 endpoint 冒充已支持
协议。

### 通过任意语言的 JSON command 接入 Provider

若 Provider 没有内置适配器，可复制
[`y-harness.command-model.example.json`](../config/y-harness.command-model.example.json)。
无需修改 Rust：配置中的绝对路径程序会从 stdin 收到一个完整
`ModelRequest` JSON。样例显式选择 `protocol: "settlement_v1"`，stdout
必须返回且仅返回一个终态对象：

```json
{
  "status": "completed",
  "output": {"type": "message", "content": "final response"},
  "usage": {
    "input_tokens": 120,
    "output_tokens": 35,
    "cached_input_tokens": 0,
    "reasoning_tokens": 8,
    "cost_usd_ticks": null
  },
  "provider_model": "vendor/model-version",
  "provider_request_id": "request-id"
}
```

需要调用 Tool 时，`output` 也可为：

```json
{"type":"tool_call","call_id":"provider-call-1","name":"lookup","input":{"q":"Y-Harness"}}
```

若 Provider 明确报告失败，可返回：

```json
{
  "status": "failed",
  "kind": "rate_limited",
  "message": "provider rate limit",
  "http_status": 429,
  "retry_after_ms": 1000
}
```

失败对象只提供事实；是否重试、切换 Model 或结束 Turn 仍由 Runtime
策略决定。`message` 必须先由适配器去除响应正文和秘密。省略 `protocol`
会保留兼容的 `output_v1`，此时 stdout 仍是一个裸 `ModelOutput`，不会
自动探测或猜测 settlement。

程序必须显式选择 `unrestricted` 或 macOS Seatbelt，工作目录会被规范化，
继承环境会被清空，只传入 `environment_from_host` 中逐项映射的值。输入、
stdout、stderr、排队加执行时间和并发均有上限；取消 Turn 会传播给
Process Broker。该 Model 以 External 来源进入普通 catalog/route、
Agent Loop、Tool、Policy、State 和 Trace 路径。

`unrestricted` 只表示显式允许，不是沙箱。settlement-v1 只保留 Provider
实际提供且通过边界校验的 usage、费用、request ID、实际结算模型、
continuation 和 typed failure；缺失值保持缺失。命令协议仍不支持
provisional streaming。详见
[ADR 0104](adr/0104-configured-brokered-json-command-models.md) 与
[ADR 0108](adr/0108-versioned-json-command-model-settlement.md)。

## 7. 装配 Tool、MCP 与 Agent Memory Hub

`tools` 可以注册 shell-free JSON command。引擎清空子进程继承环境，
只传配置中按名称映射的宿主环境变量，并要求显式选择
`unrestricted` 或 macOS Seatbelt：

```text
Model ToolCall
  → Tool Registry
  → Policy
  → Process Broker
  → JSON request on stdin
  ← one JSON result on stdout
```

同一模型响应中的 Tool 默认按源顺序串行执行。只有实现方能够保证与
同批任意安全调用（包括自身）重叠仍具备语义安全时，才可为 JSON
command 配置 `"batch_execution": "parallel_safe"`；根级
`max_parallel_tool_calls` 的合法范围为 1–64，默认 4。引擎仍会先完成
整批 Policy/Approval，并按模型原始顺序写入结果。MCP Tool 不做安全性
猜测，默认保持串行。

完整配置见
[`y-harness.openai-command.macos.example.json`](../config/y-harness.openai-command.macos.example.json)，
最小 Tool 程序见
[`json_tool_uppercase.py`](../examples/json_tool_uppercase.py)。
完整语义见
[ADR 0098](adr/0098-explicit-bounded-parallel-tool-execution.md)。

每个 MCP server 也必须显式声明启动权限。若要把 MCP Tool 暴露给
模型，`tools.allow` 必须逐个列出远端 Tool 名；目录缺少任一选定项时
整批注册失败，不会部分授权。注册后的 Tool 仍经过普通 Policy 和
State 路径。

`mcp_servers` 中的条目可用 `"enabled": false` 保留但停用。停用条目
不会启动进程、发现 Tool、注册 Policy 权限，也不能作为 Memory
Provider 的依赖。启用条目可用 `command_sha256` 锁定命令文件：

```bash
shasum -a 256 /absolute/path/to/mcp-server
yh doctor y-harness.json
```

摘要必须是 64 位小写十六进制。`doctor` 会报告“启用数/配置数”和
“已锁定命令数/已启用 stdio 数”；未设置摘要不等于已锁定。该摘要用于发现
启动前的本地漂移，只覆盖 `command` 文件本身，不覆盖参数中的脚本、
解释器依赖或动态库，也不替代 Process Broker/Seatbelt。修改 MCP
配置后需重启服务。

远程 MCP 使用独立的 `https_mcp_servers` 列表，不需要修改 Rust。复制
[`y-harness.https-mcp.example.json`](../config/y-harness.https-mcp.example.json)，
设置精确 Endpoint、环境变量名以及显式 Tool allow-list：

```bash
export YH_MCP_TOKEN='replace-with-real-secret'
yh doctor config/y-harness.https-mcp.example.json
```

该适配器当前只支持 MCP Streamable HTTP 的无状态 JSON-response 子集。
它要求无 userinfo/query/fragment 的 HTTPS URL，禁用跳转、环境代理、
HTTP/Tool 自动重试、SSE 重连和过期 Session 请求重放，并有界读取响应。
私有服务可用项目内 `exclusive_root_ca_pem_path` 替换 WebPKI 根。
返回 `text/event-stream` 的服务会明确失败；OAuth、任意 Header、SSE
和有状态远程 Session 尚未实现，不会被伪装成支持。远程 Tool 仍按
`namespace + allow` 精确注册，并经过普通 Policy/Approval/State。

Agent Memory Hub 可以复用同一受监管 MCP session，作为
`MemoryProvider` 注入 Context，而无需把 Memory Hub 的 Python 模块、
Markdown 或索引格式导入 Y-Harness。macOS 生产模板见
[`y-harness.openai-amh.macos.example.json`](../config/y-harness.openai-amh.macos.example.json)。
`yh doctor` 会启动并健康检查配置的 Memory Provider，然后有界关闭
MCP session。

## 8. 加载项目自定义 Skill

参考服务可以加载项目目录内的声明式 Skill 包，不需要修改或重新编译
Y-Harness：

```bash
yh skill install \
  /path/to/Y-Harness/examples/skills/concise-assistant.skill.json \
  y-harness.json
yh skill list y-harness.json
yh skill verify y-harness.json
yh package activate concise-assistant@1.0.0 y-harness.json
```

`install` 会校验包并按内容摘要规范化存入项目 `skills/` 目录，但不会
自动激活。`skill` 与 `package` 是同一治理入口的别名。`activate` 会
自动纳入精确依赖闭包，完整预检 Model、Tool、MCP、签名、预算和 Skill
图，然后原子更新配置；也可以参考：

```bash
cp /path/to/Y-Harness/config/y-harness.skill.example.json y-harness.json
yh doctor y-harness.json
```

`package_files` 只声明可用包，`activate` 另外精确声明本次启用的名称和
版本；磁盘上存在文件不会自动授权。所有路径必须留在配置项目目录内，
包内容摘要、依赖、所需 Tools 和总 Token 预算会在服务接收 Turn 前
完成验证。`yh doctor` 会逐条输出
`skill lock: <name>@<version> <content_sha256>`，可直接与评审过的
锁定清单比较。Skill 只向 Context 提供声明式
instructions/resources，不能执行代码或绕过 Tool Policy。

项目文件属于操作员直接信任的配置输入，不等价于第三方发布者签名。
对离线取得或来自网络的第三方包，不要把内层 `package` 抽出来伪装成
本地信任。先在 `skills.trust` 中配置发布者公钥；如发布者要求透明度
收据，还要配置独立日志公钥。例如（下列公钥必须替换为实际审核值）：

```json
{
  "skills": {
    "package_files": [],
    "external_package_files": [],
    "activate": [],
    "trust": {
      "publishers": [
        {
          "key_id": "publisher-id",
          "public_key_hex": "replace-with-64-lowercase-hex-characters",
          "not_before_ms": null,
          "not_after_ms": null,
          "transparency": "required"
        }
      ],
      "transparency_logs": [
        {
          "log_id": "audit-log",
          "public_key_hex": "replace-with-64-lowercase-hex-characters"
        }
      ]
    }
  }
}
```

只有 trust、空包列表和空激活列表的配置可先通过 `doctor`，便于在访问
网络前检查密钥与策略。安装离线签名包：

```bash
yh skill install-external /path/to/package.signed-skill.json y-harness.json
```

或从一个没有跳转、查询参数和凭据的精确公共 HTTPS URL 获取；名称、
版本和内容摘要都必须由操作员在命令中锁定：

```bash
yh skill install-https \
  https://skills.example.com/concise-assistant-1.0.0.json \
  concise-assistant@1.0.0 \
  <64位小写内容SHA-256> \
  y-harness.json
```

如果发布方提供 Y-Harness Catalog format 1，可先由独立可信渠道取得
Catalog **原始文件**的 SHA-256，再做只读搜索：

```bash
yh package search-https \
  https://skills.example.com/catalog.json \
  <64位小写Catalog-SHA-256> \
  research \
  y-harness.json
```

安装一个精确版本及其由包清单实际声明的精确依赖闭包：

```bash
yh package install-catalog \
  https://skills.example.com/catalog.json \
  <64位小写Catalog-SHA-256> \
  research-helper@1.0.0 \
  y-harness.json
```

该命令要求 Catalog 条目按 `name@version` 唯一排序、每个包都有独立
HTTPS URL 和内容摘要；下载后再以**包内真实 manifest** 解析依赖，拒绝
Catalog 缺项、摘要冲突、未安装的 yanked 依赖、循环、超过 256 个包或
64 MiB 的闭包。所有包必须携带配置已信任的 External 签名信封。完整
闭包验证成功后才写入 `skills/`，仍保持 inactive；Catalog 原始字节和
确定性来源回执分别缓存到
`.y-harness/package-cache/catalogs/` 与 `receipts/`，相同内容身份发生
漂移会失败。

显式升级到一个审核过的目标版本可用：

```bash
yh package upgrade-catalog \
  https://skills.example.com/catalog.json \
  <64位小写Catalog-SHA-256> \
  research-helper@1.1.0 \
  y-harness.json
```

`upgrade-catalog` 不采用 `latest` 或隐式 SemVer 范围；它先完成相同的
精确闭包获取，再调用普通 `activate` 完整预检并原子更新配置。失败时
已经验证并写入的内容最多作为 inactive 缓存存在，不会获得 Context、
Tool 或执行权限。

公共 Catalog 之外还可以配置具名私有 Registry。以下命令仍要求操作员
从独立可信渠道提供 Catalog 原始字节的 SHA-256：

```bash
export YH_SKILL_REGISTRY_TOKEN='<运行时凭据>'
yh package registry-search company/internal <64位小写Catalog-SHA-256> research y-harness.json
yh package registry-install company/internal <64位小写Catalog-SHA-256> research-helper@1.0.0 y-harness.json
yh package registry-upgrade company/internal <64位小写Catalog-SHA-256> research-helper@1.1.0 y-harness.json
```

Registry 配置固定一个精确 Catalog URL、允许下载包的 HTTPS origin 列表、
可选 Bearer Secret 引用与可选项目内独占 CA 文件。凭据在每次 Catalog 或
Package 请求前重新解析，不写入配置、缓存、来源回执、Runtime Catalog 或
TUI；Package URL 在解析凭据和发起网络请求前必须命中 origin 白名单。
安装仍保持 inactive，只有显式 activate/upgrade 才可能改变下一代 Runtime。
配置样例见
[`y-harness.skill-registry.example.json`](../config/y-harness.skill-registry.example.json)。

当前仍不声称支持 npm/git 安装、Registry 联邦、镜像协商、OAuth 或任意
可执行扩展。公共格式样例见
[`y-harness.skill-catalog.example.json`](../config/y-harness.skill-catalog.example.json)。

两条路径都会在首次写盘前验证内容摘要、发布者签名、有效期、撤销以及
必需或已提供的透明度收据，并以
`<digest>.signed-skill.json` 保存完整签名信封。随后执行
`yh package activate <name@version> y-harness.json`。服务启动、依赖解析、
资源读取和每次 Context 编译
都会重检实时信任；`doctor` 会输出 publisher 与 transparency 锁。
不会自动下载缺失依赖或执行包内代码。

先停用，再移除：

```bash
yh package deactivate concise-assistant@1.0.0 y-harness.json
yh package remove concise-assistant@1.0.0 y-harness.json
```

每次激活、停用、移除或回滚都会先把旧配置按 SHA-256 保存到
`.y-harness/config-history/`。`yh package history` 列出可回滚版本，
`yh package rollback <config-sha256>` 在完整预检后原子恢复。包会移入
`.y-harness/skill-trash/`，可恢复而非直接删除。安装新版后激活同名新
版本即可完成受治理更新，旧包和配置版本仍可用于回滚。当前 CLI 仍不
包含依赖下载、市场或私有注册表认证。TUI 中使用 `/doctor` 预检，使用
`/reload` 在无运行 Turn 时切换 Engine 代际并重新挂载同一 Thread。

演示模型保持确定性，不用于证明模型遵循 instructions；真实 Provider
会通过普通 `ModelRequest.context` 接收已解析 Skill。

## 9. 配置长会话语义压缩

Context Engine 的语义压缩接口可由任意语言实现，不需要修改 Rust。复制
配置模板：

```bash
cp /path/to/Y-Harness/config/y-harness.command-compactor.example.json y-harness.json
export PROVIDER_API_KEY='replace-if-the-compactor-needs-a-model'
yh doctor y-harness.json
```

把 `conversation.compaction.process.command` 改成已有的绝对可执行文件。
每次需要压缩时，该程序从 stdin 收到一个 JSON 对象：

```json
{
  "thread_id": "thread-id",
  "turns": [],
  "older_omitted_turns": 0,
  "retained_turns": [],
  "current_prompt": "current question",
  "output_budget_tokens": 4096,
  "output_budget_bytes": 262144
}
```

程序在 stdout 返回且仅返回：

```json
{"summary":"bounded semantic summary"}
```

`turns` 是按时间排序、经过边界检查的完整遗漏 Turns；取消信号不会序列
化给子进程，而是由 Runtime 直接传给 Process Broker。完整 stdin 信封
上限为 1 MiB，`input_budget_bytes` 还单独限制遗漏历史，摘要再经过
独立 Token/字节预算、非权威标记和结构校验。命令失败、输出为空、格式
错误或超预算都会令当前 Turn 明确失败，不会静默伪装为完整上下文。

原始 Items 永远保留在 State；摘要正文只作为当次派生 Context，
State 仅保存 compactor 名称、覆盖 Turn IDs、未覆盖数量、源/内容
SHA-256 与计量。`yh doctor` 会显示会话窗口和当前 compactor。进程仍
必须显式选择 `unrestricted` 或 macOS Seatbelt；前者不是沙箱。

该命令适配器只用于异步 `ConversationCompactor`。`TokenCounter` 是
模型请求热路径上的同步接口，参考服务不会用阻塞子进程伪装成精确
Tokenizer；需要精确计数时由 Rust 宿主注册原生实现。详见
[ADR 0105](adr/0105-configured-brokered-conversation-compaction.md)。

## 10. 配置完成条件 Verifier

`verifiers` 可按名称注册多个独立完成条件，不需要修改 Rust。复制模板：

```bash
cp /path/to/Y-Harness/config/y-harness.verifier.example.json y-harness.json
export VERIFIER_API_KEY='replace-if-needed'
yh doctor y-harness.json
```

每个程序会从 stdin 收到不含取消令牌的不可变候选快照：

```json
{
  "thread_id": "thread-id",
  "turn_id": "turn-id",
  "items": [],
  "candidate": "assistant candidate"
}
```

通过时 stdout 返回：

```json
{"status":"passed","summary":"completion conditions satisfied"}
```

不通过时返回：

```json
{"status":"failed","reason":"missing required evidence","retryable":true}
```

`retryable: true` 会让 Agent Loop 进入下一次 Model 修正步骤；
`false` 会明确令 Turn 失败。Runtime 会重新校验 outcome 文本和大小，
按稳定名称顺序执行所有 Verifiers，并把每个结果写入 State。Verifier
不能自行提交 Turn，也不能绕过 Tool Policy、Approval 或 State。

完整 stdin 上限为 1 MiB，嵌套 Tool JSON 在启动进程前检查；stdout、
stderr、并发和时间也独立有界。Turn 的精确取消令牌不交给外部程序，
而是由 Runtime 单独传给 Process Broker。`unrestricted` 仍不是沙箱。
详见 [ADR 0106](adr/0106-configured-brokered-verification.md)。

## 11. 配置独立 Evaluation Grader

Evaluation 与在线完成判定严格分离。复制模板并把 Grader `command`
改成现存绝对路径：

```bash
cp /path/to/Y-Harness/config/y-harness.eval.example.json y-harness.json
export GRADER_API_KEY='replace-if-needed'
yh doctor y-harness.json
yh eval \
  /path/to/Y-Harness/evals/configured-example-suite.json \
  /path/to/Y-Harness/evals/configured-example-baseline.json \
  y-harness.json
```

每个 Grader 从 stdin 收到一个不可变的 `case + execution` 快照，从
stdout 返回：

```json
{"score":0.95,"passed":true,"rationale":"required evidence is present"}
```

输入完整上限为 4 MiB，嵌套 metadata 与 Tool JSON 在启动进程前检查；
输出字段严格、分数和理由由 Evaluation Engine 再次规范化。每个
Grader 有独立取消令牌、超时和并发边界，来源写入 format-2 report，
baseline 必须匹配精确来源。

`yh eval` 使用配置中的 Model、Tool、Context、Verification、Skill 和
Memory，但 State 为进程内隔离实例，不会打开项目的生产 State、
Approval 或 Task 数据库。`yh serve` 不构造 Evaluation Grader，也不
获取其环境或进程权限。Grader 不能修改 Agent Loop、调用 Tool 或提交
Turn；需要在线完成门禁时使用上一节的 Verifier。详见
[ADR 0107](adr/0107-configured-brokered-evaluation-graders.md)。

## 12. 接入自有模型 Gateway

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
[`protocol.md`](protocol.md) 所述的精确 Model Gateway v7 契约。
每个直接适配器只接受自己声明的 wire contract；配置自定义 endpoint
不代表不同厂商原生 API 语义相同。未支持协议仍应放在 Gateway、JSON
command Broker 或宿主 Provider 中。

无需修改 Rust 代码即可配置多个已支持的 Provider/Model。复制
`config/y-harness.provider-profiles.example.json`，先在
`provider_profiles` 中声明可复用的协议与 Secret/传输边界，再在
`models` 中为每个模型设置唯一 `id`、Profile 引用和明确模型 ID，最后在
`model_route.models` 中按尝试顺序列出这些 ID：

```bash
export OPENAI_API_KEY='replace-with-real-secret'
export ANTHROPIC_API_KEY='replace-with-real-secret'
export GEMINI_API_KEY='replace-with-real-secret'
yh doctor y-harness.json
```

`model` 单模型形式与 `models` + `model_route` 目录形式互斥。Route
必须含 1–16 个已注册且不重复的 ID；`attempt_timeout_ms` 范围为
1–86,400,000。可选 `timeout_cooldown_ms` 为 0（关闭）或
1–86,400,000，且至少需要两个 Route Model。它只冷却由 Runtime
attempt deadline 明确判定的超时；普通 Provider 字符串错误不会被猜成
健康状态。未冷却候选失败时，冷却候选仍作为最后兜底。
可选 `retry` 对象显式开启同模型重试：`max_retries` 为 1–8，
`initial_delay_ms` 和 `max_delay_ms` 均为 1–60,000，默认分别为 250
和 5,000。只有类型化的限流、过载、服务端和传输失败可重试；旧式
错误字符串、认证、配额、请求、模型不可用、内容策略和协议失败不会
被猜测为瞬时错误。重试与首次调用共享同一个 Model attempt deadline，
等待可取消，收到任何临时流内容后立即禁止重试和 Route fallback。
顶层 `max_model_attempts_per_step` 同时约束同模型重试和 Route fallback，
合法范围为 1–144，默认 16；Runtime 会在超限 Provider 调用发生前失败。
结合默认 `max_steps = 32`，默认 Turn 最多跨越 512 次 Runtime 管理的
Model 调用。此边界不虚构对 Compactor、Verifier、Tool 或 MCP
实现内部隐藏模型调用的可见性。
`yh doctor` 会在 Provider 构造和服务启动前报告目录、精确 Route、
冷却值和重试边界。TUI `/runtime` 显示无凭据的活动目录；`/reload` 先
执行 doctor，再在已结算 Turn 边界优雅替换 Engine 进程并恢复同一持久
Thread；`/doctor` 会把有界、终端控制字符清理后的预检报告加载到
Runtime 面板，`/packages` 是活动 Skill 锁视图的入口。当前没有运行中
Turn 的实现替换、自动发现、通用错误熔断或按价格/负载猜测路由。

## 13. 作为 Rust 引擎嵌入

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

## 14. 安全与产品边界

- `serve` 是持久化 stdio 服务，适合由进程主管或受信宿主启动。
- 网络暴露必须使用现有 mandatory-mTLS host，不能把裸 JSONL socket
  直接当成生产网络服务。
- 默认 Policy 拒绝未注册 Tool；演示配置只允许内置 `echo`。
- 配置中的 JSON command Tool 和逐项选中的 MCP Tool 才会进入允许
  列表；发现目录本身不授予执行权限。
- Workspace Provider 默认拒绝文件系统 Task。
- 目录隔离和 Git Worktree 不是 OS 沙箱；不可信执行器必须继续经过
  Process Broker。
- Web 与 Desktop 是未来客户端，不属于首版 Runtime 内核。

完整验收步骤见
[`acceptance-checklist.zh-CN.md`](acceptance-checklist.zh-CN.md)。
