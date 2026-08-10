# Tool Trace：中转站工具调用证据链

Tool Trace 面板用于分开证明三层事实：MCP Server 是否真实可调用、
Y-Harness 是否完成发现/注册并把 Tool 发给模型、Provider 是否返回符合契约的
结构化 Tool Call。面板只显示无凭据证据，不显示 API Key、Bearer Token 或
MCP 子进程参数。

## 验证矩阵

不要只用自写 fixture 对外下结论。当前本机矩阵包含：

| Server | 来源与版本 | 隔离 Tool | 原生 MCP 往返 |
|---|---|---|---|
| Official Filesystem | `@modelcontextprotocol/server-filesystem@2026.7.10` | `read_text_file` | PASS：读取隔离文件并返回 `Y-HARNESS-OFFICIAL-FILESYSTEM-MCP-OK` |
| Official Everything | `@modelcontextprotocol/server-everything@2026.7.4` | `echo` | PASS：返回 `Echo: Y-HARNESS-OFFICIAL-EVERYTHING-MCP-OK` |
| Agent Memory Hub | 本机 `agent-memory-hub-1.1.1` / `1.28.1` | `brain_stats` | PASS：返回结构化 brain 统计 |

前两项来自 MCP Steering Group 维护的 reference-server 仓库。官方将这些 Server
定位为协议参考实现，而不是生产托管服务；Agent Memory Hub 则是本机真实业务 MCP。
三项均使用正式 MCP SDK Client 完成 `initialize → tools/list → tools/call`，不是仅靠
进程存活或配置文件推断。

Agent Memory Hub 在本机 `macos_seatbelt` 启动策略下初始化失败，切换为
`unrestricted` 后通过；因此它证明 MCP/Host/Provider 链路，但不能作为 Seatbelt
兼容性通过证据。默认截图配置不启用它。

## OpenAI 工具名称映射

Y-Harness 内部保留稳定的 namespaced Tool identity，例如：

```text
official_everything.echo
```

OpenAI Chat Completions 的 function name 不允许点号。适配器在单次请求内将其可逆
映射为：

```text
official_everything__echo
```

若可读别名与已有 Tool 冲突或超过 64 bytes，则使用确定性的 SHA-256 摘要别名。
Provider 返回后必须先反向映射为内部 identity，再进入 Runtime 授权和执行。普通合法
名称保持不变。相关单测同时覆盖编码、`tool_choice`、响应及 continuation 回放。

## 当前可截图复现

本地配置默认只启用 Official Everything 的只读 `echo`，避免多个 Schema 相互污染：

```bash
cd /Users/liuyang/Desktop/AIAgent/Y-Harness
export AIJTOKEN_API_KEY="$(security find-generic-password -s claude-aijtoken -w)"
target/debug/yh-tui \
  --engine target/debug/yh \
  --config y-harness.aijtoken.local.json
```

在 Composer 中发送：

```text
必须调用 official_everything.echo，参数 message 为 Y-HARNESS-OFFICIAL-EVERYTHING-MCP-OK；不要使用任何其他工具，不要直接回答。
```

Turn 结束后发送 `/trace`。建议终端至少 160×40，并让截图同时包含对话区与
Tool Trace Inspector。

## 截图必须同时包含

1. `MCP DISCOVERY + REGISTRATION`：`official-everything` 为 PASS，注册了
   `official_everything.echo`。
2. `MODEL REQUEST CONTRACT`：`advertised 1`，工具列表包含该 Tool。
3. `tool_choice specific(official_everything.echo)`：不能以 `auto` 自主不调用解释。
4. `request sha`：绑定同一份 Provider-neutral 请求。
5. `PROVIDER SETTLEMENT`：HTTP/Provider failure、结构化 Tool Call 数和耗时。
6. 最终 `VERDICT`。

## 2026-08-03 隔离结果

三台 MCP 均先独立完成原生调用，再分别作为唯一 MCP Tool 进入 Y-Harness：

| 唯一 Tool | Y-Harness discovery/request | Provider 结果 |
|---|---|---|
| `official_fs.read_text_file` | PASS，`specific`，request `fd9f8711…` | 连续 3 次 HTTP 502；上游报 400 |
| `official_everything.echo` | PASS，`specific`，request `58141912…` | 连续 3 次 HTTP 502；上游报 400 |
| `agent_memory.brain_stats` | PASS，`specific`，request `249a63e4…` | 连续 3 次 HTTP 502；上游报 400 |

控制实验中，原先可得到普通 Message 的合法 `uppercase` 请求在同一时段也变成
HTTP 502。这表明当前工具通道存在时间相关的不稳定性，不应归因于某一个 MCP
Server 或某一种 Schema。

## Verdict 含义

| Verdict | 结论 |
|---|---|
| `STRUCTURED_TOOL_CALL_OK` | Provider 返回结构化 Tool Call，Harness 可以继续 Tool Loop。 |
| `MCP_TOOL_NOT_REGISTERED` | MCP 初始化、`tools/list`、allowlist 或注册阶段失败。 |
| `REGISTERED_MCP_TOOL_NOT_SENT` | MCP Tool 已注册，但没有进入模型请求。 |
| `TOOL_CALL_FLATTENED_TO_TEXT` | 返回文本像 Tool Call，但未进入结构化字段。 |
| `PROVIDER_TOOL_CONTRACT_VIOLATION` | `required`/`specific` 下返回普通文本且结构化调用数为 0。 |
| `PROVIDER_REQUEST_FAILED` | Provider 在产生有效模型决策前失败或超时，包括 HTTP 502。 |
| `MODEL_CHOSE_TEXT_UNDER_AUTO` | `auto` 下模型选择文本；这不能单独证明 Provider 有问题。 |

## 责任边界

MCP Endpoint 不会直接发给模型。Y-Harness 先连接 MCP Server，执行
`initialize`、`tools/list` 并注册 Tool，再把选定 Tool 的 JSON Schema 放进模型请求。
模型只负责返回名称与参数；实际 `tools/call` 由 Harness 执行。因此，原生 MCP
调用、Harness Tool Trace 和 Provider settlement 三组证据必须同时保留，不能拿模型
自己的文字说明替代任何一层。
