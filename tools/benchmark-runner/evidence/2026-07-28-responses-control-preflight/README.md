# Responses-control preflight

This directory preserves one real two-product preflight for released Codex and
Grok Build. It is not a comparative benchmark and supports no product-quality
or Harness-effect claim.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Released Codex: `0.145.0`, executable SHA-256
  `1da3f4e0e96028b8a771814293c3033dafd1971f943f6c7e79b0897fe705f590`
- Released Grok Build: `0.2.112`, executable SHA-256
  `5cf05fe670b1818561daf7566b580a5de6b81149166499d61072e49640b541a4`
- Adapter executable SHA-256:
  `19db0b5d6d1d1bb93ddf66a3a279e38c226fd656b143ebc1b301742ae785d49b`
- Shared Provider fixture SHA-256:
  `329266186315ad3dba0eb338a498fee60d9bda58ccad9e3de7a480aba7bd4077`
- Verdict: `not_comparable`
- Harness-effect claim eligible: no

[`codex-spec.json`](codex-spec.json) and
[`grok-build-spec.json`](grok-build-spec.json) requested the same Provider
label, `gpt-5.4` main Model identifier, system and user prompts, `medium`
reasoning effort, 30-second timeout, empty-workspace class, and read-only
product sandbox. Both released products used the same running
[`provider.mjs`](provider.mjs) process, sent their main request to
`/v1/responses`, and returned `YH-RESPONSES-CONTROL-OK`. Codex emitted no
fallback-Model event, while Grok Build settled `gpt-5.4` in its main result.
The exact reports are [`codex-result.json`](codex-result.json) and
[`grok-build-result.json`](grok-build-result.json).

The sanitized [`provider-request.jsonl`](provider-request.jsonl) prevents this
partial alignment from being overstated. Codex made one accepted Model
request. Grok Build first loaded the shared model catalog, then issued a title
request with its `grok-4.5` default rather than the selected `gpt-5.4`. The
fixture rejected that off-coordinate request with HTTP 422; Grok Build
silently continued and completed its accepted `gpt-5.4` main request. The
product's returned `modelUsage.*.modelCalls = 1` therefore still does not
describe every attempted Model call associated with the command.

The main Tool and Context surfaces also remained different. Codex exposed six
built-ins; Grok Build exposed `read_file`, `search_tool`, and `use_tool`, plus
the separate `session_title` Tool on the rejected auxiliary request. Grok
requested concise reasoning summaries, and the products used different
permission modes and built-in Context. Codex JSONL still did not settle Model
or Provider identity.

[`preflight.json`](preflight.json) records the machine-readable verdict.
Same main protocol and Model identifier are necessary but insufficient:
every auxiliary Model call, Tool schema, Context, permission, budget, call
ceiling, and identity settlement must also be controlled before a shared Tool
workload can support a Harness-effect claim.
