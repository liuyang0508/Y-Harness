# ADR 0090: Source-pinned released-Pi benchmark adapter

- Status: Accepted
- Date: 2026-07-27

## Context

Pi ships both an in-process evaluation adapter and a released non-interactive
coding-agent CLI. Y-Harness had source-level Pi findings but no executable
released-product boundary, so even adapter conformance could not be measured
under the same external-run evidence model used for Claude Code, Codex, and
Grok Build.

Pi's JSON mode emits an optional session header followed by
`AgentSessionEvent` JSONL. `agent_end` is not a terminal session signal:
automatic retry may emit another Agent run before `agent_settled`.

## Decision

- Add external-run format 4 and a released `pi` CLI adapter outside the
  semantic Core.
- Require an exact executable SHA-256, exact observed CLI version, absolute
  workspace, bounded output, timeout, and explicit inherited environment
  names.
- Give `bare` runs an exact empty `PI_CODING_AGENT_DIR`; `product` runs may
  retain explicitly inherited ambient configuration.
- Use Pi's public flags to disable session persistence, Tools, extensions,
  Skills, prompt templates, themes, context files, project trust, and startup
  refresh. Select the exact Provider, Model, reasoning level, and system
  prompt.
- Send the user prompt through stdin. Reject leading or trailing whitespace
  because Pi trims piped input and the report's prompt digest must describe
  the bytes the Model receives.
- Parse at most 4,096 JSONL events within the existing two-MiB process-output
  ceiling. Require valid Agent/Turn ordering and terminal `agent_settled`;
  allow repeated Agent runs for Pi's retry lifecycle.
- Reject Tool execution or nonempty Tool results because the adapter requested
  `--no-tools`.
- Derive Turn count, terminal stop reason, cost, and observed Provider/Model
  identities only from completed assistant messages. Never copy requested
  Model identity into observed evidence.
- Keep `claim_eligible: false`. The adapter measures contract conformance, not
  Harness quality, and no live or comparative Pi record is checked in.

## Consequences

The benchmark runner can now execute an exact released Pi binary and preserve
its real lifecycle, retry, Model, and cost evidence without linking Pi code
into Y-Harness. Bare runs exclude Pi's normal configuration directory and
project resources.

Tools are disabled, Pi has no built-in sandbox or documented hard monetary
ceiling, and its JSONL does not separate product from Provider duration. These
remain explicit unsupported controls. Provider credentials and launcher
dependencies remain caller-supplied and are not recorded.

## Rejected alternatives

- Use Pi's in-process eval adapter: it does not test the released CLI boundary
  and would import another Harness into the benchmark process.
- Treat `agent_end` as terminal: it truncates valid automatic retries.
- Pass the prompt in argv: operating-system argument limits are narrower than
  the existing bounded prompt contract and may expose prompt text in process
  listings.
- Enable read-only Tools: Pi explicitly documents no built-in sandbox, so this
  would add ambient host authority to a conformance probe.
- Infer missing cost or Model identity: requested controls are not observed
  settlement evidence.

## Evidence

- `pi::tests::bare_command_disables_ambient_capabilities_and_uses_stdin`
- `pi::tests::profile_owns_bare_agent_directory_and_rejects_trimmed_prompts`
- `pi::tests::jsonl_accepts_retries_and_preserves_reported_model_and_cost`
- `pi::tests::jsonl_rejects_tool_execution_and_events_after_settlement`

## Source coordinate

- Pi
  [`cee5ff7`](https://github.com/earendil-works/pi/tree/cee5ff7520d8828bed9955ef00419e995d1f91e0)
  `packages/coding-agent/src/cli/args.ts`,
  `packages/coding-agent/src/modes/print-mode.ts`,
  `packages/coding-agent/src/core/agent-session.ts`, and
  `packages/ai/src/types.ts`
