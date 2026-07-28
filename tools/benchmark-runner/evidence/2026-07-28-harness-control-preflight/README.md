# Harness-control preflight

This directory preserves one real two-product control preflight. It is not a
comparative benchmark and supports no product-quality or Harness-effect claim.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Released Claude Code: `2.1.143`, executable SHA-256
  `2701c6cfd68483f8faf0316a1ba6481a1455a90645ada179f0c48d8c36d722ef`
- Released Codex: `0.145.0`, executable SHA-256
  `1da3f4e0e96028b8a771814293c3033dafd1971f943f6c7e79b0897fe705f590`
- Adapter executable SHA-256:
  `19db0b5d6d1d1bb93ddf66a3a279e38c226fd656b143ebc1b301742ae785d49b`
- Shared Provider fixture SHA-256:
  `ddc8c2357dd1f0a4d1a2a7ecb7d723abc8b7bc363a4c1973bf25ac2279a0408f`
- Verdict: `not_comparable`
- Harness-effect claim eligible: no

[`claude-spec.json`](claude-spec.json) and
[`codex-spec.json`](codex-spec.json) requested the same Provider label, Model
identifier, system prompt, user prompt, `medium` reasoning-effort label,
30-second timeout, and empty-workspace class. Both released products called
the same running [`provider.mjs`](provider.mjs) process and returned
`YH-HARNESS-CONTROL-OK`. The exact product reports are
[`claude-result.json`](claude-result.json) and
[`codex-result.json`](codex-result.json); the machine-readable verdict is
[`preflight.json`](preflight.json).

The sanitized [`provider-request.jsonl`](provider-request.jsonl) proves the
actual request order: Claude Code's `HEAD /` probe, one Anthropic Messages
request, then one Codex Responses request. It also preserves the differences
that invalidate a Harness comparison. Claude Code sent no Tools and requested
enabled thinking with a 31,999-token budget. Codex exposed five built-in Tools
inside its read-only sandbox and requested `medium` effort with automatic
summary. The products also used different wire protocols and injected
different built-in Context.

Most importantly, the shared Model string did not establish a shared Model
coordinate. Claude Code settled `claude-haiku-4-5-20251001`; Codex emitted a
product event stating that metadata for that identifier was unavailable and
that fallback metadata would be used. Codex JSONL also did not settle the
Provider or Model identity. A shared string accepted by a synthetic fixture is
therefore not proof that the products ran the same Model implementation.

The next comparative workload must use a Provider and Model implementation
natively supported and identically settled by both products. Tool surface,
Context injection, sandbox semantics, budget semantics, and Provider-call
ceilings must also be equalized first. If the two products expose no natively
supported common Model coordinate, this pair remains unavailable to the
Harness-control track. Until then, the only valid conclusion is that the
adapters can execute a shared fixture while meaningful Harness parity remains
unestablished.
