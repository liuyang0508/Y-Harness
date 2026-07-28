# Codex adapter conformance probe

This directory preserves one real released-Codex adapter run, not a
comparative benchmark or a Harness-effect result.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Released CLI: Codex `0.145.0`
- Analyzed source: official tag `rust-v0.145.0`, commit
  `25af12f7e61572b0bc18ddb1008be543b91519b0`
- Executable SHA-256:
  `1da3f4e0e96028b8a771814293c3033dafd1971f943f6c7e79b0897fe705f590`
- Adapter executable SHA-256:
  `02a0dc688c84be6bfe99b5b5273d86654441a8fdd72c7f5abadf7903e7d3af09`
- Provider fixture SHA-256:
  `ffdaa14bb95e474ad2a4cfc44ebac5e9ad19d28203a6b9ca565fa2feb7c13782`
- Adapter track: `adapter_conformance`
- Claim eligible: no

The exact adapter input is [`spec.json`](spec.json), and the adapter output is
[`result.json`](result.json). The released binary called the deterministic
loopback Responses fixture in [`provider.mjs`](provider.mjs) and returned
`YH-CODEX-ADAPTER-OK`. The adapter fixed the exact binary digest, CLI version,
loopback Provider, Model, reasoning effort, prompt digests, empty initial
platform home and `CODEX_HOME`, adapter-owned `CODEX_SQLITE_HOME`, read-only
product sandbox request, approval policy, and environment-name allowlist.

[`provider-request.jsonl`](provider-request.jsonl) is the fixture's sanitized
request log. It corroborates one `/v1/responses` request with the requested
Model and `medium` reasoning effort. The automatic Skills and Apps instruction
blocks were absent. Six built-in Tools remained visible:
`exec_command`, `write_stdin`, `update_plan`, `request_user_input`,
`apply_patch`, and `view_image`. The request sidecar is external corroboration,
not part of Codex's JSONL settlement.

The run began with an empty workspace, platform home, and `CODEX_HOME`. The
workspace and platform home remained empty, while Codex created installation
state and several SQLite databases under the isolated Codex home despite
`--ephemeral`. Mapping both platform-home variables and `CODEX_SQLITE_HOME`
kept source-defined user Skill discovery and SQLite state away from the host
home. Automatic Skill instructions were also disabled.

Codex's JSONL reports token usage and Turn settlement but does not echo the
settled Provider or Model identity, cost, or product/API duration. Format 2
therefore leaves those fields empty or `null`. It also exposes neither a hard
spend fence nor a hard Provider-call ceiling.

The released binary is hash-pinned, but it was not reproducibly built from the
analyzed source commit. This fixed-output record proves released-CLI and
adapter protocol conformance only. It exercises no Tool call, interruption,
restart, recovery, or comparable live Model and supports no claim that either
Harness is better.
