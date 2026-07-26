# External benchmark runner

`yh-bench` is an optional released-product adapter. It consumes public CLI
surfaces and depends on Y-Harness only for the already tested bounded Process
Broker. It is not part of the Harness semantic Core.

The first adapter accepts an exact-versioned JSON specification:

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

The output is an external-run format-1 report. It pins the adapter binary,
observed CLI version, and product executable by SHA-256; hashes prompts,
records inherited environment names without their values, distinguishes
product errors from adapter errors, and retains the bounded raw JSON result. The track is
`adapter_conformance`, with `claim_eligible: false`.

Claude Code documents `max-budget-usd` as a maximum budget option, but an
individual provider call can settle above the requested value before the CLI
returns `error_max_budget_usd`. Reports therefore preserve both
`requested_max_budget_usd` and `actual_cost_usd`; callers must not treat the
requested value as a hard pre-spend fence.

Deterministic Tool fault injection has its own dependency and evidence
boundary in [`y-harness-fault-fixture`](../fault-fixture/README.md).
