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
`cargo install --locked --features https-mcp,https-model,https-skill`，
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

Sessions 面板列出最近 64 个权威 Thread，并显示分叉 Thread 的直接父级
及父流版本；选中后按 Enter 即可恢复。
`/name <标题>` 设置当前 Thread 的权威名称，单独输入 `/name` 清除名称。
`/fork` 从当前已结算末尾创建独立子 Thread；`/fork <terminal-turn-id>`
可从更早的已终结 Turn 分叉。也可使用 `/new`、`/sessions`、
`/thread <id>`、`/graph <id>`、`/events`、
`/approvals`、`/tasks`、`/refresh`、`/cancel`、`/help` 和 `/quit`。
连接真实配置时执行 `yh-tui --config y-harness.json`。

演示使用内置确定性模型和 `echo` Tool，不访问网络。State 写入当前目录
的 `.y-harness/state.db`，派生 Trace 写入 `.y-harness/traces/`。

`yh-tui` 只通过 Protocol v18 调用 `yh`：它不读取 SQLite，不构造
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

## 4. 运行 Protocol v18 服务

`serve` 通过 stdin/stdout 读写一行一个 JSON 的 Protocol v18 帧：

```bash
yh serve y-harness.json
```

直接运行时看起来像“停住”，是因为它正在等待客户端输入。可以用一条
初始化请求验证：

```bash
printf '%s\n' \
  '{"id":"init-1","protocol_version":"18","command":{"method":"initialize"}}' \
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

配置只保存 Secret 引用和环境变量名，不保存 API Key。Provider 固定
访问 OpenAI 官方 HTTPS Responses endpoint，禁用 redirect、proxy、
retry 和 referer，发送 `store: false`，并把响应 Token 使用量和
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

### 通过任意语言的 JSON command 接入 Provider

若 Provider 没有内置适配器，可复制
[`y-harness.command-model.example.json`](../config/y-harness.command-model.example.json)。
无需修改 Rust：配置中的绝对路径程序会从 stdin 收到一个完整
`ModelRequest` JSON，并在 stdout 返回且仅返回一个 `ModelOutput`：

```json
{"type":"message","content":"final response"}
```

需要调用 Tool 时也可返回：

```json
{"type":"tool_call","call_id":"provider-call-1","name":"lookup","input":{"q":"Y-Harness"}}
```

程序必须显式选择 `unrestricted` 或 macOS Seatbelt，工作目录会被规范化，
继承环境会被清空，只传入 `environment_from_host` 中逐项映射的值。输入、
stdout、stderr、排队加执行时间和并发均有上限；取消 Turn 会传播给
Process Broker。该 Model 以 External 来源进入普通 catalog/route、
Agent Loop、Tool、Policy、State 和 Trace 路径。

`unrestricted` 只表示显式允许，不是沙箱。当前 stdout 契约只承载
`ModelOutput`，不会虚构 Provider usage、费用、request ID、实际结算模型、
continuation、typed HTTP failure 或流式增量；需要这些能力时使用原生
`LanguageModel` 或版本化 HTTPS Gateway。详见
[ADR 0104](adr/0104-configured-brokered-json-command-models.md)。

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
```

`install` 会校验包并按内容摘要规范化存入项目 `skills/` 目录，但不会
自动激活。请把命令打印的相对路径和精确 `name@version` 分别加入
`skills.package_files` 与 `skills.activate`；也可以参考：

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

两条路径都会在首次写盘前验证内容摘要、发布者签名、有效期、撤销以及
必需或已提供的透明度收据，并以
`<digest>.signed-skill.json` 保存完整签名信封。随后仍需把打印的路径
加入 `skills.external_package_files`，并把精确身份加入
`skills.activate`。服务启动、依赖解析、资源读取和每次 Context 编译
都会重检实时信任；`doctor` 会输出 publisher 与 transparency 锁。
不会自动修改配置、获取依赖或执行包内代码。

移除前必须先从配置的 `activate` 以及对应的 `package_files` 或
`external_package_files` 撤销授权；随后执行：

```bash
yh skill remove concise-assistant@1.0.0 y-harness.json
```

包会移入 `.y-harness/skill-trash/`，可恢复而非直接删除。当前 CLI
支持精确公共 HTTPS 安装，但没有自动更新、依赖下载、市场、私有注册
表认证或热加载；修改配置或包后需重启服务。即使发布者已经被撤销，
在取消配置引用后仍可执行移除，清理路径不会被信任失败锁死。

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
除明确实现的 OpenAI Responses Provider 外，Y-Harness 不伪装不同
厂商 API 具有相同语义；其他厂商适配应放在 Gateway 或宿主 Provider
中。

无需修改 Rust 代码即可配置多个已支持的 Provider/Model。复制
`config/y-harness.route.example.json`，在 `models` 中为每个模型设置
唯一 `id`、明确的 Provider 参数及各自的环境变量名，再在
`model_route.models` 中按尝试顺序列出这些 ID：

```bash
export OPENAI_API_KEY='replace-with-real-secret'
export YH_MODEL_TOKEN='replace-with-real-secret'
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
`yh doctor` 会在 Provider 构造和服务启动前报告目录、精确 Route、
冷却值和重试边界。修改目录或 Route 后需要受控重启服务；当前没有
热加载、自动发现、通用错误熔断或按价格/负载猜测路由。

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
