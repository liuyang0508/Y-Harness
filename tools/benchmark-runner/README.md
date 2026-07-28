# External benchmark runner

`yh-bench` is an optional released-product adapter. It consumes public CLI
surfaces and depends on Y-Harness only for the already tested bounded Process
Broker. It is not part of the Harness semantic Core.

The Claude Code adapter accepts an exact-versioned JSON specification:

```json
{
  "format_version": 1,
  "run_id": "claude-probe-001",
  "benchmark_version": "adapter-conformance-v1",
  "case_id": "fixed-output",
  "program": "/absolute/path/to/claude",
  "expected_cli_version": "2.1.143 (Claude Code)",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/isolated/workspace",
  "workspace_snapshot": "empty-workspace-v1",
  "profile": "bare",
  "provider": "yh-loopback-anthropic-messages",
  "provider_base_url": "http://127.0.0.1:12345",
  "model": "claude-haiku-4-5-20251001",
  "reasoning_effort": "medium",
  "system_prompt": "Return only the requested fixed text.",
  "prompt": "Reply exactly YH-ADAPTER-OK",
  "timeout_ms": 30000,
  "max_budget_usd": 0.10,
  "max_turns": 1,
  "inherit_environment": [
    "ANTHROPIC_API_KEY"
  ],
  "home": "/absolute/empty/platform-home",
  "claude_config_dir": "/absolute/empty/claude-config",
  "temp_dir": "/absolute/empty/temp"
}
```

Run it without a shell-expanded command:

```bash
cargo run --locked -p y-harness-benchmark-runner -- \
  claude-code /absolute/path/to/spec.json > external-run.json
```

`bare` requires API-key authentication, an explicit loopback Anthropic
Messages Provider, exact effort and Turn ceiling, and three distinct initially
empty state directories. The adapter owns Provider routing, platform/config/
temp discovery and nonessential-traffic controls while `--bare` suppresses
ambient hooks, plugins, settings, memory, and keychain auth. `product` must not
declare bare controls, permits the installed product profile, and records
ambient configuration as unsupported. Both profiles disable Tools, Skills,
MCP, session persistence, and interactive permission prompts in external-run
format 1.

The Claude output is an external-run format-1 report. It pins the adapter
binary, observed CLI version, and product executable by SHA-256; hashes
prompts, records inherited environment names without their values,
distinguishes product errors from adapter errors, and retains the bounded raw
JSON result. The track is `adapter_conformance`, with
`claim_eligible: false`.

Claude Code documents `max-budget-usd` as a maximum budget option, but an
individual provider call can settle above the requested value before the CLI
returns `error_max_budget_usd`. Reports therefore preserve both
`requested_max_budget_usd` and `actual_cost_usd`; callers must not treat the
requested value as a hard pre-spend fence.

The released `2.1.143` bare probe also showed one unauthenticated Provider
`HEAD /` before its single Messages request, product prompt/date blocks beyond
the requested system prompt, and isolated config writes despite
`--no-session-persistence`. These are retained as unsupported controls rather
than hidden by the adapter.

The second adapter consumes Codex's stable non-interactive JSONL surface:

```json
{
  "format_version": 2,
  "run_id": "codex-probe-001",
  "benchmark_version": "adapter-conformance-v1",
  "case_id": "fixed-output",
  "program": "/absolute/path/to/codex",
  "expected_cli_version": "codex-cli <exact-version>",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/isolated/workspace",
  "workspace_snapshot": "empty-workspace-v1",
  "profile": "bare",
  "provider": "yh-loopback-responses",
  "provider_base_url": "http://127.0.0.1:12345/v1",
  "model": "<exact-model>",
  "reasoning_effort": "medium",
  "system_prompt": "Return only the requested fixed text.",
  "prompt": "Reply exactly YH-ADAPTER-OK",
  "timeout_ms": 30000,
  "inherit_environment": [
    "CODEX_API_KEY"
  ],
  "home": "/absolute/empty/platform-home",
  "codex_home": "/absolute/empty/codex-home"
}
```

```bash
cargo run --locked -p y-harness-benchmark-runner -- \
  codex /absolute/path/to/spec.json > external-run.json
```

`bare` requires distinct initially empty platform and Codex homes, owns the
child home/`CODEX_HOME`/`CODEX_SQLITE_HOME` environment, and accepts only an
explicit loopback Responses Provider. It disables user configuration, rules,
plugins, Apps, multi-Agent behavior, bundled Skills, and automatic Skill/App
instructions. `product` must not supply bare-profile state or routing and
permits ambient product configuration. Both profiles use `--ephemeral`,
`--sandbox read-only`, approval policy `never`, exact reasoning effort,
disabled web search, and bounded JSONL parsing. The output is external-run
format 2. Codex does not expose settled Provider/Model identity, cost, or
product/API duration in this stream, so those fields remain empty or `null`;
they are never guessed. Built-in Tools remain visible, and Codex materializes
state inside the isolated Codex home despite `--ephemeral`.

The dedicated Codex CF-003 driver executes the deterministic uncertain-effect
fixture without an external Model API:

```json
{
  "format_version": 7,
  "run_id": "codex-cf003-001",
  "benchmark_version": "fault-conformance-v1",
  "case_id": "cf-003-uncertain-non-idempotent-tool-effect",
  "program": "/absolute/path/to/codex",
  "expected_cli_version": "codex-cli 0.145.0",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "fixture_program": "/absolute/path/to/yh-fault-fixture",
  "fixture_spec": "/absolute/path/to/fixture-spec.json",
  "expected_fixture_spec_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/empty/workspace",
  "workspace_snapshot": "empty-directory-v1",
  "codex_home": "/absolute/empty/codex-home",
  "model": "gpt-5.4",
  "timeout_ms": 30000
}
```

```bash
cargo run --locked --release -p y-harness-benchmark-runner -- \
  codex-cf003 /absolute/path/to/spec.json > external-run.json
```

Format 7 pins released Codex `0.145.0`, the analyzed official source tag, the
fixture and both specs. Its bounded loopback Responses Provider follows the
released product's deferred `tool_search` path before selecting the MCP Tool.
The report correlates exact Provider requests, strict Codex JSONL, and the
independent durable oracle. `passed` means only one invocation and one effect
were observed without replay in this single process. Built-in advertised
Tools, outer unrestricted process isolation, absent binary-to-source
reproducibility, and unexercised product restart/resume remain explicit;
`claim_eligible` is always false.

The companion format-8 driver exercises a real process restart:

```json
{
  "format_version": 8,
  "run_id": "codex-cf003-restart-001",
  "benchmark_version": "fault-conformance-v1",
  "case_id": "cf-003-restart-after-uncertain-effect",
  "program": "/absolute/path/to/codex",
  "expected_cli_version": "codex-cli 0.145.0",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "fixture_program": "/absolute/path/to/yh-fault-fixture",
  "fixture_spec": "/absolute/path/to/fixture-spec.json",
  "expected_fixture_spec_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/empty/workspace",
  "workspace_snapshot": "empty-directory-v1",
  "codex_home": "/absolute/empty/codex-home",
  "model": "gpt-5.4",
  "timeout_ms": 30000,
  "effect_wait_timeout_ms": 10000
}
```

```bash
cargo run --locked --release -p y-harness-benchmark-runner -- \
  codex-cf003-restart /absolute/path/to/spec.json > external-run.json
```

The first process persists a Thread and unresolved function call, enters the
`hold_after_first_effect` fixture, and is controller-cancelled only after the
one-effect journal boundary is synchronized. Codex re-groups its MCP child, so
the controller writes an identity-bound release marker and waits for that
detached fixture rather than claiming full process-tree cleanup. The second
process executes `codex exec resume` for the exact rollout Thread. Its Provider
requires Codex's source-defined synthetic `aborted` function output and returns
a fixed message without selecting another Tool. Passing requires the same
Thread, an appended rollout, zero resumed MCP calls, and one effect before and
after resume. This is a new Turn on a resumed Thread, not continuation of the
interrupted Turn; format 8 remains non-comparative and claim-ineligible.

The Y-Harness restart adapter uses external-run format 9:

```json
{
  "format_version": 9,
  "run_id": "yh-cf003-restart-001",
  "benchmark_version": "cf003-v1",
  "case_id": "cf-003-y-harness-restart-after-uncertain-effect",
  "program": "/absolute/path/to/yh",
  "expected_cli_version": "yh 0.1.0",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "fixture_program": "/absolute/path/to/yh-fault-fixture",
  "fixture_spec": "/absolute/path/to/fixture-spec.json",
  "expected_fixture_spec_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/empty/workspace",
  "workspace_snapshot": "empty-workspace-v1",
  "timeout_ms": 30000,
  "effect_wait_timeout_ms": 10000
}
```

```bash
cargo run --locked --release -p y-harness-benchmark-runner -- \
  y-harness-cf003-restart /absolute/path/to/spec.json
```

The adapter drives three real `yh serve` processes over protocol v20: setup,
fault injection, then restart. Its spec-bound JSON-command Model selects the
one stdio MCP Tool before the first process is killed at the durable
post-effect/pre-result boundary. Restart must first retain the exact Turn as
`running`; permissioned recovery then interrupts only the expected Turn at the
State compare-and-append boundary, and a separate Tool-free Turn completes.
The external fixture must remain at one invocation and one effect throughout.
Format 9 is claim-ineligible and does not assert descendant-process exit,
in-place Turn continuation, distributed failure detection, or Model reasoning
quality.

The Grok Build adapter consumes the official headless JSON surface:

```json
{
  "format_version": 3,
  "run_id": "grok-build-probe-001",
  "benchmark_version": "adapter-conformance-v1",
  "case_id": "fixed-output",
  "program": "/absolute/path/to/grok",
  "expected_cli_version": "grok <exact-version>",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/isolated/workspace",
  "workspace_snapshot": "empty-workspace-v1",
  "profile": "bare",
  "provider": "yh-loopback-responses",
  "models_base_url": "http://127.0.0.1:43123/v1",
  "model": "grok-4.5",
  "reasoning_effort": "low",
  "system_prompt": "Return only the requested fixed text.",
  "prompt": "Reply exactly YH-ADAPTER-OK",
  "timeout_ms": 30000,
  "inherit_environment": [
    "XAI_API_KEY"
  ],
  "home": "/absolute/empty/home",
  "grok_home": "/absolute/empty/grok-home",
  "prompt_directory": "/absolute/empty/prompt-directory"
}
```

```bash
cargo run --locked -p y-harness-benchmark-runner -- \
  grok-build /absolute/path/to/spec.json > external-run.json
```

`bare` injects exact empty `HOME`, `USERPROFILE`, and `GROK_HOME` directories
instead of inheriting ambient product state. It requires an explicit Provider
label and an adapter-owned `http://127.0.0.1:<port>/v1` custom-model endpoint;
the endpoint environment cannot be inherited. `product` must omit
`provider`, `models_base_url`, `home`, and `grok_home`, and may inherit its
normal environment explicitly.
`prompt_directory` must be empty and outside the benchmark workspace. Both
profiles use a create-exclusive prompt file that is owner-only on Unix and
removed after execution; Windows callers must protect the supplied directory
ACL.
They also use an exact Model and reasoning effort; one maximum Turn;
`dontAsk`; the product's `read-only` sandbox; disabled Memory, planning,
Subagents, questions, web Tools, and automatic updates; and a `read_file` Tool
allowlist. Grok Build's always-on MCP meta-tools and session persistence remain
declared unsupported controls. The one-Turn ceiling applies to main-agent
rounds; auxiliary Model calls such as session-title generation are not
bounded or counted by that field.

External-run format 3 preserves Grok Build's observed `modelUsage`, Turn count,
and cost only when the product reports complete cost. Complete cost includes
`actual_cost_usd_ticks` at exactly 10 billion ticks per USD and is rejected if
the product's float projection disagrees. Missing or partial cost remains
`null` with no tick field; the requested Model is never copied into observed
Models.
A real released-Grok Build `0.2.112` deterministic fixed-output record is
checked in under
[`evidence/2026-07-28-grok-build-fixed-output`](evidence/2026-07-28-grok-build-fixed-output).
It remains `claim_eligible: false`; the request sidecar observed one auxiliary
title call plus the one main-agent call, and there is no comparative Grok
Build run.

The Pi adapter consumes the released coding agent's JSONL lifecycle:

```json
{
  "format_version": 4,
  "run_id": "pi-probe-001",
  "benchmark_version": "adapter-conformance-v1",
  "case_id": "fixed-output",
  "program": "/absolute/path/to/pi",
  "expected_cli_version": "0.82.1",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/isolated/workspace",
  "workspace_snapshot": "empty-workspace-v1",
  "profile": "bare",
  "provider": "openai",
  "model": "<exact-model>",
  "thinking": "low",
  "system_prompt": "Return only the requested fixed text.",
  "prompt": "Reply exactly YH-ADAPTER-OK",
  "timeout_ms": 30000,
  "inherit_environment": [
    "OPENAI_API_KEY"
  ],
  "pi_agent_dir": "/absolute/empty/pi-agent"
}
```

```bash
cargo run --locked -p y-harness-benchmark-runner -- \
  pi /absolute/path/to/spec.json > external-run.json
```

`bare` injects the exact empty `PI_CODING_AGENT_DIR` rather than inheriting
ambient Pi configuration. `product` must omit `pi_agent_dir` and may retain
explicitly inherited product configuration. Both profiles run without session
persistence, Tools, extensions, Skills, prompt templates, themes, project
context files, or project trust; startup network refresh is disabled. The
prompt is sent on stdin and must not contain leading or trailing whitespace
because Pi trims piped input.

External-run format 4 accepts the optional session header followed by Pi's
bounded `AgentSessionEvent` stream. `agent_settled`, rather than an
intermediate `agent_end`, is the required terminal event because automatic
retries may execute more than one Agent run. Turn count, reported assistant
cost, stop reason, and observed provider/model identities come only from
validated Pi events. Pi has no built-in sandbox or documented hard monetary
ceiling, and Tools are disabled, so this remains adapter-conformance evidence.
A real released-Pi deterministic fixed-output record is checked in under
[`evidence/2026-07-28-pi-fixed-output`](evidence/2026-07-28-pi-fixed-output).
It remains `claim_eligible: false`; there is no comparative Pi run.

The OpenCode adapter consumes the released CLI's line-delimited run events:

```json
{
  "format_version": 5,
  "run_id": "opencode-probe-001",
  "benchmark_version": "adapter-conformance-v1",
  "case_id": "fixed-output",
  "program": "/absolute/path/to/opencode",
  "expected_cli_version": "<exact-version>",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/isolated/workspace",
  "workspace_snapshot": "empty-workspace-v1",
  "profile": "bare",
  "model": "openai/<exact-model>",
  "variant": "low",
  "system_prompt": "Return only the requested fixed text.",
  "prompt": "Reply exactly YH-ADAPTER-OK",
  "timeout_ms": 30000,
  "inherit_environment": [
    "OPENAI_API_KEY",
    "PATH"
  ],
  "home": "/absolute/empty/opencode-home"
}
```

```bash
cargo run --locked -p y-harness-benchmark-runner -- \
  opencode /absolute/path/to/spec.json > external-run.json
```

`bare` requires an initially empty home, owns its XDG roots and empty
authentication content, uses an in-memory database, and disables project
configuration, default/external plugins, external Skills, LSP downloads,
Claude compatibility inputs, and updates. `product` omits `home`; ambient
authentication, global configuration, instructions, and MCP definitions
remain. Both profiles generate one primary agent with
all Tools denied, disable title generation and snapshots, send the user prompt
only on stdin, and retain the requested agent prompt only by digest in the
report. `variant` is optional. Model, variant, and agent prompt reject
OpenCode's `{env:...}` / `{file:...}` configuration substitutions so recorded
controls cannot be silently rewritten.

External-run format 5 requires one stable Session identity and ordered
`step_start`/`step_finish` JSONL pairs. It rejects Tool events, malformed
settlement, cross-Session output, and events after a product error. Successful
cost is summed only from validated `step-finish` records; error-stream cost and
settled Model identity remain `null`/empty because OpenCode does not expose
complete evidence for them. The requested agent prompt remains additive to
OpenCode's own product/provider instructions, and the CLI exposes neither a
hard monetary nor a hard provider-call ceiling. OpenCode may still initialize
or update its plugin SDK dependency cache; that limitation is recorded rather
than hidden. A real released-OpenCode deterministic fixed-output record is
checked in under
[`evidence/2026-07-28-opencode-fixed-output`](evidence/2026-07-28-opencode-fixed-output).
It remains `claim_eligible: false`; there is no comparative OpenCode run.

The Hermes Agent adapter consumes its released one-shot output plus the
side-channel usage report:

```json
{
  "format_version": 6,
  "run_id": "hermes-probe-001",
  "benchmark_version": "adapter-conformance-v1",
  "case_id": "fixed-output",
  "program": "/absolute/path/to/hermes",
  "expected_cli_version": "Hermes Agent v0.19.0 (2026.7.20)",
  "expected_product_executable_sha256": "<64 lowercase hex bytes>",
  "workspace": "/absolute/isolated/workspace",
  "workspace_snapshot": "empty-workspace-v1",
  "profile": "bare",
  "provider": "openrouter",
  "model": "openai/gpt-5.5",
  "system_prompt": "Return only the requested fixed text.",
  "prompt": "Reply exactly YH-ADAPTER-OK",
  "timeout_ms": 30000,
  "inherit_environment": [
    "OPENROUTER_API_KEY"
  ],
  "hermes_home": "/absolute/empty/hermes-home",
  "usage_directory": "/absolute/empty/usage-directory"
}
```

```bash
cargo run --locked -p y-harness-benchmark-runner -- \
  hermes /absolute/path/to/spec.json > external-run.json
```

Format 6 supports only `bare`. It requires initially empty, pairwise-disjoint
Hermes-home and usage directories outside the workspace, clears undeclared
environment values, disables the system managed-scope overlay, enables safe
mode, maps platform home discovery to the isolated Hermes home, and selects
Hermes's static empty `context_engine` toolset. The adapter pre-seeds the
product's update cache solely for the version probe, so `hermes --version`
does not perform its normal update network request. The usage file is
create-exclusive, owner-only on Unix, bounded to 64 KiB, parsed strictly, and
removed after the run.

`expected_cli_version` is the exact first `--version` line. A packaged install
normally uses the base line shown above; a source install may append Hermes's
own ` · upstream …` / ` · local …` revision suffix, which must also match
exactly.

Hermes `0.19.0` accepts one-shot input only in a process argument and exposes
no separate system-prompt flag. The adapter therefore sends the requested
instruction as a labeled user-message prefix; it records both limitations
instead of claiming system-role parity or prompt secrecy. The released
one-shot path also does not prove that workspace instructions are ignored.
`estimated_cost_usd` remains raw estimated evidence and is never promoted to
`actual_cost_usd`. Observed Model identity, Provider, tokens, API-call count,
and completion flags come only from the validated usage report. A Python
console-launcher digest may not identify the installed package graph, so that
limitation is machine-readable too. A real released-Hermes deterministic
fixed-output record is checked in under
[`evidence/2026-07-28-hermes-fixed-output`](evidence/2026-07-28-hermes-fixed-output).
It remains `claim_eligible: false`; there is no comparative Hermes run.

The checked-in
[`2026-07-28-harness-control-preflight`](evidence/2026-07-28-harness-control-preflight/)
record reuses the format-1 Claude Code and format-2 Codex adapters against one
running deterministic Provider fixture. Its `preflight.json` verdict is
`not_comparable`: the same requested Model identifier triggered Codex fallback
metadata, and the actual requests retained different protocols, Tool surfaces,
reasoning representations, and sandbox semantics. This preflight is a
machine-checked refusal to overclaim, not an additional report format or a
Harness-effect result.

The follow-up
[`2026-07-28-responses-control-preflight`](evidence/2026-07-28-responses-control-preflight/)
record uses the format-2 Codex and format-3 Grok Build adapters against one
Responses fixture. Their main calls align on `gpt-5.4`, prompts, effort, and
read-only sandbox requests, but the verdict remains `not_comparable`. The
sidecar proves that Grok attempted an auxiliary `grok-4.5` title call, received
HTTP 422, and silently continued; Tool, Context, reasoning-summary, permission,
call-count, and identity-settlement controls also differ.

Deterministic Tool fault injection has its own dependency and evidence
boundary in [`y-harness-fault-fixture`](../fault-fixture/README.md).
