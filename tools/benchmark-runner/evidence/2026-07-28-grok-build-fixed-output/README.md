# Grok Build adapter conformance probe

This directory preserves one real released-Grok Build adapter run, not a
comparative benchmark or a Harness-effect result.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Official stable release: `grok 0.2.112 (9bbd559437aa)`
- Official artifact URL:
  `https://x.ai/cli/grok-0.2.112-macos-aarch64`
- Executable SHA-256:
  `5cf05fe670b1818561daf7566b580a5de6b81149166499d61072e49640b541a4`
- macOS signature: `Developer ID Application: X.AI Corporation (5Y6N3AJ54S)`
- Signature timestamp: `2026-07-25 04:00:07`
- Release-contract source coordinate:
  [`02d9359`](https://github.com/xai-org/grok-build/tree/02d9359435d0e9c20a20945679389cdce441e431)
- Public snapshot `SOURCE_REV`:
  `1adcd1f477870e4a97bacbd6be78c8a3bfbac46d`
- Adapter executable SHA-256:
  `0b156cf3956ab8473d468cc7acafe1ec5660ae72afb7d261703613bf91c6add1`
- Provider fixture SHA-256:
  `db0caf5a4c407e5734bb864a8a6414940f4959fca45f9bf87d4ab7deb9ed6df3`
- Adapter track: `adapter_conformance`
- Claim eligible: no

The exact adapter input is [`spec.json`](spec.json), and the adapter output is
[`result.json`](result.json). The released binary called the deterministic
loopback Responses fixture in [`provider.mjs`](provider.mjs) and returned
`YH-GROK-BUILD-ADAPTER-OK`. The adapter fixed the exact binary digest, CLI
version, loopback Provider, Model, reasoning effort, prompt digests, one
main-agent Turn, isolated state roots, read-only product sandbox request, and
environment-name allowlist.

[`provider-request.jsonl`](provider-request.jsonl) is the fixture's sanitized
request log. It records one model-catalog request followed by two inference
requests. The first inference request generated a session title; the second
was the main Agent request. This is an important product boundary:
`--max-turns 1` and the returned `modelUsage.*.modelCalls = 1` describe the
main Agent loop, while the fixture observed two Model requests. Format 3
therefore does not claim a hard Provider-call ceiling.

The main request corroborates the exact Model, reasoning effort, system and
user prompts, streaming mode, and the visible `read_file`, `search_tool`, and
`use_tool` Tools. Grok Build keeps its MCP meta-tools visible even when the
adapter requests only `read_file`; that limitation is preserved rather than
hidden. The sidecar digest is not embedded in `result.json`, so it remains
corroborating evidence rather than product settlement.

The run began with empty `HOME`, `GROK_HOME`, workspace, and prompt
directories. The ordinary home, workspace, and prompt directory remained
empty, including successful removal of the private prompt file. Grok Build
materialized documentation, logs, model cache, SQLite search state, and one
session under the isolated `GROK_HOME`, as its headless contract permits.

The official installer supports an exact version but does not publish or
verify an artifact checksum in the downloaded script. This run independently
records the artifact SHA-256 and verifies the embedded Apple Developer ID
signature; it does not claim equivalent platform-signing evidence for Linux or
Windows. The release binary exposes only the abbreviated build coordinate
`9bbd559437aa`; this record does not pretend that it established a matching
full public-source commit.

The deterministic response proves released-CLI and adapter protocol
conformance only. It exercises no Tool call or recovery path, uses no
comparable live Model, reports no complete Provider cost, does not
independently verify the product sandbox, and supports no claim that either
Harness is better.
