# ADR 0109: Source-pinned released OpenCode adapter

- Status: accepted
- Date: 2026-07-28

## Context

The competitive protocol requires released products to execute through their
real public surfaces before Y-Harness can make any comparative effect claim.
Claude Code, Codex, Grok Build, and Pi already had independent bounded
adapters. OpenCode was represented only by source analysis.

The official OpenCode snapshot
[`7534d23`](https://github.com/anomalyco/opencode/tree/7534d23551f665e65080809975b4ca5c7d63807b)
exposes `opencode run --format json`, which emits one JSON object per line for
completed step, reasoning, text, Tool, and error events. Its own process tests
pin that event order. The same source also exposes explicit environment
controls for project configuration, external plugins, update/model refresh,
authentication content, XDG roots, and an in-memory database.

This is product evidence, not a Harness capability. Putting the adapter in the
semantic Core would couple Y-Harness to a competitor's CLI and blur the
evaluation boundary.

## Decision

- Add `opencode` to the independent `y-harness-benchmark-runner` package and
  emit external-run format 5.
- Require an absolute executable, exact CLI version, executable SHA-256,
  exact `provider/model`, bounded prompt fields, isolated workspace coordinate,
  timeout, and an explicit environment-name allowlist.
- Send the user prompt only on stdin. Supply the requested additive agent
  prompt through bounded `OPENCODE_CONFIG_CONTENT`; never put either prompt in
  process arguments or output controls. Reject OpenCode `{env:...}` and
  `{file:...}` substitution tokens in every caller-controlled configuration
  string so the effective prompt, Model, and variant cannot be silently
  rewritten.
- Run `--pure run --format json` with one generated primary agent. Deny every
  Tool through that agent's permission rules, disable title generation,
  project configuration, external plugins, automatic update/compaction/model
  refresh, sharing, snapshots, formatter/LSP activation, and durable session
  storage.
- In `bare`, require one caller-provided empty home and own `HOME`,
  `USERPROFILE`, `OPENCODE_TEST_HOME`, every XDG root, and empty
  `OPENCODE_AUTH_CONTENT`; also disable default plugins, external Skills, LSP
  downloads, and Claude compatibility inputs. Reject inherited collisions.
  `product` deliberately retains ambient authentication, global configuration,
  instructions, and MCP definitions and records that limitation.
- Parse no more than 4,096 events inside the existing two-MiB stdout bound.
  Require one stable Session identity, ordered non-overlapping
  `step_start`/`step_finish` pairs, bounded text/reasoning, finite nonnegative
  token/cost fields, and no Tool event.
- Preserve successful complete cost by summing validated `step-finish` cost.
  Preserve cost as unavailable for an error stream because the error event
  does not prove the failed step's full cost. Never copy the requested Model
  into `observed_models`: OpenCode's run JSONL does not expose settled Model
  identity.
- Classify a valid product error separately from spawn, timeout, truncation,
  malformed JSONL, sequence, or contract failures.
- Fix `claim_eligible` to `false`. The adapter is conformance machinery, not
  evidence that Y-Harness or OpenCode is better.

## Consequences

Y-Harness can now preserve a source-pinned OpenCode released-CLI execution
under the same binary, process, privacy, and non-claim discipline as the other
product adapters. Format 5 retains raw bounded events, reported completed-step
cost, finish reason, and exact unavailable fields.

OpenCode adds product/provider instructions to the requested agent prompt,
does not expose settled Model identity or distinct API duration, and has no
adapter-proven hard monetary or provider-call ceiling. The Process Broker
clears undeclared environment values but is not an OS sandbox. OpenCode may
still initialize or update its plugin SDK dependency cache; the adapter records
that unsupported control. A live OpenCode record and any same-Model comparative
workload remain separate work.

## Rejected alternatives

- Import OpenCode libraries or event types: creates source and release
  coupling instead of testing the released boundary.
- Reuse format 2 or 4: their Codex and Pi lifecycle/evidence semantics differ.
- Treat the requested Model as observed: invents evidence absent from JSONL.
- Report zero cost on errors: confuses missing settlement evidence with a
  measured zero.
- Enable Tools to measure Agent-loop quality in the first adapter: authority
  parity and deterministic Tool fixtures are not established yet.
- Load ambient project files in `bare`: invalidates the isolation claim.
- Permit OpenCode configuration substitutions in pinned fields: makes their
  recorded digests differ from the effective values and may read undeclared
  environment or files.

## Evidence

- `opencode::tests::bare_profile_owns_ambient_state_and_command_keeps_prompt_on_stdin`
- `opencode::tests::profile_and_model_validation_reject_ambiguous_authority`
- `opencode::tests::jsonl_preserves_step_cost_and_finish_reason`
- `opencode::tests::jsonl_rejects_tools_cross_session_and_trailing_errors`
- Official OpenCode run JSONL process tests at snapshot `7534d23`
- zero-default and all-feature workspace gates

## Related decisions

- [ADR 0079: external benchmark evidence boundary](0079-external-benchmark-evidence-boundary.md)
- [ADR 0081: bounded Codex external adapter](0081-bounded-codex-external-adapter.md)
- [ADR 0082: bounded Grok Build external adapter](0082-bounded-grok-build-external-adapter.md)
- [ADR 0090: source-pinned released Pi adapter](0090-source-pinned-released-pi-adapter.md)
