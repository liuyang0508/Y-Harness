# Codex CF-003 fault-conformance probe

This directory preserves one real released-product fault run, not a
comparative benchmark.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Released CLI: Codex `0.145.0`
- Analyzed source: official tag `rust-v0.145.0`, commit
  `25af12f7e61572b0bc18ddb1008be543b91519b0`
- Adapter track: `fault_conformance`
- Case: `CF-003 uncertain-non-idempotent-tool-effect`
- Claim eligible: no

The exact driver input is [`spec.json`](spec.json), the exact fixture input is
[`fixture-spec.json`](fixture-spec.json), the controller-owned durable record
is [`journal.jsonl`](journal.jsonl), and the adapter report is
[`result.json`](result.json).

Codex first used its source-defined deferred `tool_search` path to discover
`mcp__yh_fault::commit_effect`. The deterministic Provider then selected that
Tool. The fixture synchronized one synthetic non-idempotent effect and exited
before returning a Tool result. Codex observed `Transport closed`, sent one
failed `function_call_output` to the Provider, completed the Turn, and did not
invoke the effect again. The independent oracle therefore reported one
invocation, one effect, and `uncertain_effect_not_replayed`.

This record does not exercise product restart or resume, does not establish
cross-product Model parity, and does not measure Model reasoning quality.
Codex built-in Tools remained advertised, although the deterministic Provider
selected only the pinned search and MCP operations. The product used its
read-only sandbox, while the outer Y-Harness Process Broker truthfully reports
`unrestricted`. The released binary is hash-pinned, but it was not
reproducibly built from the analyzed source commit; the result records that
boundary explicitly.
