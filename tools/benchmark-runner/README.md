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
  "model": "claude-haiku-4-5",
  "system_prompt": "Return only the requested fixed text.",
  "prompt": "Reply exactly YH-ADAPTER-OK",
  "timeout_ms": 30000,
  "max_budget_usd": 0.10,
  "inherit_environment": [
    "ANTHROPIC_API_KEY"
  ]
}
```

Run it without a shell-expanded command:

```bash
cargo run --locked -p y-harness-benchmark-runner -- \
  claude-code /absolute/path/to/spec.json > external-run.json
```

`bare` requires Claude Code API-key authentication and suppresses ambient
hooks, plugins, settings, memory, and keychain auth. `product` permits the
installed product profile but marks ambient configuration as an unsupported
control. Both profiles disable Tools, Skills, MCP, session persistence, and
interactive permission prompts in adapter-format v1.

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
  "model": "<exact-model>",
  "system_prompt": "Return only the requested fixed text.",
  "prompt": "Reply exactly YH-ADAPTER-OK",
  "timeout_ms": 30000,
  "inherit_environment": [
    "CODEX_API_KEY",
    "CODEX_HOME",
    "PATH"
  ],
  "codex_home": "/absolute/empty/codex-home"
}
```

```bash
cargo run --locked -p y-harness-benchmark-runner -- \
  codex /absolute/path/to/spec.json > external-run.json
```

`bare` requires the declared `CODEX_HOME` to be empty before execution and to
match the inherited environment exactly. `product` must not supply a
`codex_home` and permits ambient product configuration. Both profiles
use `--ephemeral`, `--sandbox read-only`, approval policy `never`, disabled web
search, and bounded JSONL parsing. The output is external-run format 2. Codex
does not expose observed Model identity, cost, or product/API duration in this
stream, so those fields remain empty or `null`; they are never guessed.

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
instead of inheriting ambient product state. `product` must omit `home` and
`grok_home` and may inherit its normal environment explicitly.
`prompt_directory` must be empty and outside the benchmark workspace. Both
profiles use a create-exclusive prompt file that is owner-only on Unix and
removed after execution; Windows callers must protect the supplied directory
ACL.
They also use an exact Model and reasoning effort; one maximum Turn;
`dontAsk`; the product's `read-only` sandbox; disabled Memory, planning,
Subagents, questions, web Tools, and automatic updates; and a `read_file` Tool
allowlist. Grok Build's always-on MCP meta-tools and session persistence remain
declared unsupported controls.

External-run format 3 preserves Grok Build's observed `modelUsage`, Turn count,
and cost only when the product reports complete cost. Complete cost includes
`actual_cost_usd_ticks` at exactly 10 billion ticks per USD and is rejected if
the product's float projection disagrees. Missing or partial cost remains
`null` with no tick field; the requested Model is never copied into observed
Models.
There is no checked-in live Grok Build result yet, so this adapter provides
contract evidence only.

Deterministic Tool fault injection has its own dependency and evidence
boundary in [`y-harness-fault-fixture`](../fault-fixture/README.md).
