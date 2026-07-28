# ADR 0110: Source-pinned released Hermes Agent adapter

- Status: accepted
- Date: 2026-07-28

## Context

The competitive protocol requires real released-product execution before any
comparative effect claim. Hermes Agent had extensive source analysis but no
released-CLI adapter.

Official release `v2026.7.20`, commit
[`3ef6bbd`](https://github.com/NousResearch/hermes-agent/tree/3ef6bbd201263d354fd83ec55b3c306ded2eb72a),
publishes package version `0.19.0`. Its `hermes --oneshot PROMPT` path writes
only the final response to stdout, and `--usage-file PATH` writes completion,
Provider, Model, token, API-call, session, service-tier, and estimated-cost
facts even for many failures.

That surface has important limits:

- the prompt is a process argument, not stdin;
- no one-shot system-prompt flag exists;
- one-shot unconditionally enables YOLO approval behavior;
- `--safe-mode` skips plugin/MCP discovery, but its environment flags do not
  fully propagate through the one-shot `AIAgent` construction to prove that
  every workspace/config rule is ignored;
- normal `--version` output performs an update check;
- the usage cost is explicitly estimated, not billed cost; and
- a Python console-script digest may fingerprint only the launcher.

The static `context_engine` toolset is empty in the pinned source. With an
empty Hermes home and default compressor, it yields no Tool schemas. This is a
valid Tool-disabled conformance cell, but it does not measure Hermes's Agent
loop or broad Tool runtime.

## Decision

- Add `hermes` to the independent `y-harness-benchmark-runner` and emit
  external-run format 6. Do not import Hermes libraries into the semantic
  Core.
- Support only `bare`. Require exact executable/version/digest, explicit
  Provider and Model, bounded prompts, an environment-name allowlist, and
  initially empty, pairwise-disjoint Hermes-home and usage directories outside
  the workspace.
- Own Hermes configuration variables, set `HERMES_HOME`, disable managed
  scope through a nonexistent explicit directory, map platform home variables
  to the isolated Hermes home, enable safe mode, create an empty owner-only
  `.env`, and select only `context_engine`.
- Prefix the user message with the requested benchmark instruction under an
  explicit label. Record that it has user-message rather than system-role
  authority and that both strings are visible in process arguments. Limit the
  prompt to 16 KiB for cross-platform argument bounds.
- Pre-seed the isolated `.update_check` with the expected version and a
  probe-only revision, then compare the exact first version line. Accept the
  product's optional revision suffix for source installs without weakening
  that exact comparison. The probe receives invalid loopback proxies as a
  second network fence if the expected version is wrong.
- Create the usage file exclusively, mode `0600` on Unix, outside the
  workspace and Hermes home. Read at most 64 KiB, reject symlinks,
  missing/unknown fields, schema violations, contradictory status, and more
  than the pinned source default of 90 API calls. Remove it on drop.
- Require full Model/Provider/token/completion evidence on success. Allow
  source-defined nullable fields on a coherent product failure. Derive a
  single attempted user Turn only when at least one API call was reported.
- Preserve `estimated_cost_usd`, status, and source only in raw evidence.
  Always leave `actual_cost_usd` unavailable.
- Fix `claim_eligible` to `false`, declare missing system-role parity,
  argv privacy, workspace-rule isolation, hard spend/caller-selected call
  ceilings, package-graph identity, and live comparison as unsupported.

## Consequences

Y-Harness can now exercise the real released Hermes one-shot boundary and
distinguish product failure from adapter failure without manufacturing cost or
identity facts. One released `0.19.0` fixed-output record uses an isolated
platform home and deterministic loopback Provider. It remains
`claim_eligible: false`. The adapter is intentionally narrower than Hermes
itself.

The empty Tool cell cannot establish Hermes Agent-loop effectiveness. The
one-shot CLI cannot provide prompt secrecy or system-instruction parity, and
the product still persists session state inside its isolated home. A
source-checkout project `.env` may fill otherwise undeclared environment
variables even though the adapter's empty user `.env` prevents it from
overwriting declared inherited values. No comparative claim follows.

## Rejected alternatives

- Treat `--safe-mode` help text as proof of complete one-shot isolation: the
  pinned construction path does not carry every flag into `AIAgent`.
- Add a `product` profile: ambient context-engine selection can reintroduce
  tools, so Tool-disabled authority would be false.
- Report the estimate as actual cost: changes the meaning of product evidence.
- Copy the requested Model into observed fields: the usage sidecar already
  provides settled identity and must be authoritative.
- Reuse formats 1–5: none combines plain stdout with a mandatory usage sidecar
  and estimated-only cost.
- Patch or import Hermes to add stdin/system-prompt controls: benchmarks the
  patch rather than the released product.

## Evidence

- `hermes::tests::bare_profile_rejects_environment_collisions_and_oversized_argv_prompt`
- `hermes::tests::environment_maps_platform_home_to_isolated_hermes_home`
- `hermes::tests::command_uses_safe_empty_tools_but_truthfully_places_prompt_in_argv`
- `hermes::tests::path_boundaries_require_three_disjoint_directories`
- `hermes::tests::success_preserves_observed_identity_and_estimate_without_promoting_cost`
- `hermes::tests::usage_rejects_contradictions_missing_fields_and_excessive_calls`
- `hermes::tests::product_failure_remains_settled_without_inventing_a_turn`
- `hermes::tests::checked_in_live_evidence_preserves_identity_cost_and_home_boundaries`
- `tools/benchmark-runner/evidence/2026-07-28-hermes-fixed-output`
- official Hermes Agent one-shot, usage, parser, toolset, context-engine,
  safe-mode, and version source at `3ef6bbd`

## Related decisions

- [ADR 0079: external benchmark evidence boundary](0079-external-benchmark-evidence-boundary.md)
- [ADR 0081: bounded Codex external adapter](0081-bounded-codex-external-adapter.md)
- [ADR 0082: bounded Grok Build external adapter](0082-bounded-grok-build-external-adapter.md)
- [ADR 0090: source-pinned released Pi adapter](0090-source-pinned-released-pi-adapter.md)
- [ADR 0109: source-pinned released OpenCode adapter](0109-source-pinned-released-opencode-adapter.md)
