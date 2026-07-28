# Claude Code adapter conformance probe

This directory preserves one real released-Claude Code adapter run, not a
comparative benchmark or a Harness-effect result.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Released CLI: Claude Code `2.1.143`
- Executable SHA-256:
  `2701c6cfd68483f8faf0316a1ba6481a1455a90645ada179f0c48d8c36d722ef`
- Analyzed reconstructed source: `liuyang0508/claude-code-source-code`
  version `2.1.88`, commit
  `3da94d5e5f2b99c9d82b0d8f09448b04775cd41f`
- Adapter executable SHA-256:
  `19db0b5d6d1d1bb93ddf66a3a279e38c226fd656b143ebc1b301742ae785d49b`
- Provider fixture SHA-256:
  `d8c2c3abde00eb1a98f491610e8890a3f65d458099d35f069f272080d31267e9`
- Adapter track: `adapter_conformance`
- Claim eligible: no

The exact adapter input is [`spec.json`](spec.json), and the adapter output is
[`result.json`](result.json). Released Claude Code called the deterministic
loopback Anthropic Messages fixture in [`provider.mjs`](provider.mjs) and
returned `YH-CLAUDE-ADAPTER-OK`. The product settlement reported the exact
requested Model, one Turn, 10 input tokens, 5 output tokens, and
`0.000035000000000000004` USD under Claude Code's price table. The last value
is a product projection for this loopback fixture, not incurred Provider spend.

The adapter fixed the binary digest, CLI version, loopback Provider, Model,
`medium` effort, one-Turn ceiling, budget, empty initial workspace and isolated
state directories, disabled Tools/Skills/MCP discovery, `dontAsk` permission
mode, and environment-name allowlist. It also disabled nonessential traffic,
telemetry, error reporting, and auto-update in the child environment.

[`provider-request.jsonl`](provider-request.jsonl) is the fixture's sanitized
request log. Claude Code first issued an unauthenticated `HEAD /` probe, then
one authenticated streaming `/v1/messages?beta=true` request. The Model request
contained no Tools. The requested `medium` effort appeared on the wire as
enabled thinking with a 31,999-token budget; the product did not emit a
thinking block for the fixture's deterministic text response. The request
sidecar is external corroboration, not part of Claude Code's JSON settlement.

The workspace, platform home, and temp directory remained empty. Claude Code
still created `.claude.json`, a backup, and a sessions directory under the
isolated config directory despite `--no-session-persistence`. Its request also
contained three product system blocks and a current-date context block in the
user message. Those product behaviors are explicitly retained as unsupported
controls rather than described as eliminated.

The reconstructed source snapshot documents the relevant `--bare`,
`ANTHROPIC_BASE_URL`, auth, and nonessential-traffic paths, but it is older
than the released executable and is not the released binary's reproducible
source coordinate. Exact 2.1.143 behavior is therefore established by the
hash-pinned binary run and request log, not inferred from the 2.1.88 source.

This record exercises no Tool call, interruption, restart, recovery, or shared
cross-product workload. It proves released-CLI and adapter protocol
conformance only and supports no claim that either Harness is better.
