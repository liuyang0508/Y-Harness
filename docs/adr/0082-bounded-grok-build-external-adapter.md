# ADR 0082: Grok Build evidence uses its bounded headless JSON surface

- Status: Accepted
- Date: 2026-07-26

## Context

Grok Build is now an official open-source Agent, TUI, and Harness rather than
only a Provider-side product claim. Its released CLI exposes headless JSON with
the final text, stop reason, session and request IDs, main-agent Turn count,
Token usage, per-Model usage, and cost when complete.

That surface still differs materially from the existing adapters. Prompts
cannot be read directly from standard input, all headless sessions persist,
MCP meta-tools remain present under a Tool allowlist, and the CLI has no
documented hard monetary ceiling. Missing cost can mean unreported or partial
cost, not zero.

## Decision

- Add an explicit Grok Build adapter to the independent
  `y-harness-benchmark-runner`; do not add Grok product behavior to Core.
- Pin CLI version and executable SHA-256 before Model work. Launch without a
  shell through the bounded Process Broker and retain only bounded stream
  bytes, hashes, and parsed JSON evidence.
- Define external-run format 3 for the Grok-specific controls and optional
  evidence. Record the requested Model, reasoning effort, one-Turn ceiling,
  product `read-only` sandbox, and observed Models from `modelUsage`.
- In `bare`, inject exact empty `HOME`, `USERPROFILE`, and `GROK_HOME`
  directories and inherit only an explicit environment-name allowlist that
  must contain `XAI_API_KEY`. Keep `product` as an ambient-configuration
  profile.
- Write the prompt to a create-exclusive file outside the workspace, make it
  owner-only on Unix, pass only its path to the CLI, and remove it on every
  ordinary return path. Reject non-UTF-8 paths because the product CLI accepts
  UTF-8 arguments; on Windows declare the inherited prompt-directory ACL.
- Request `--verbatim`, exact Model and effort, JSON output, `dontAsk`,
  `read-only`, `--max-turns 1`, and a `read_file` Tool allowlist. Disable
  Memory, planning, Subagents, questions, web Tools, and automatic updates.
- Preserve product errors as evidence. Validate success and failure envelopes,
  bounded IDs/text, Turn count, usage, observed Model identities, and paired
  dollar/tick cost. If usage or cost is incomplete, reject reported cost
  floats and serialize actual cost as `null`.
- Declare session persistence, MCP meta-tools, project instructions, lack of a
  hard spend fence, caller-asserted workspace identity, and unverified product
  sandbox behavior as unsupported controls.
- Keep every result on `adapter_conformance` with `claim_eligible: false`.

## Consequences

Y-Harness can now execute a pinned released Grok Build binary and preserve
truthful Model, Turn, Token, and complete-cost evidence without confusing Grok
4.5 with the Harness or claiming benchmark parity.

The one-Turn read-only profile intentionally does not measure Agent-loop or
Tool effectiveness. A live conformance record, multi-Turn Tool cases, and
same-Model cross-Harness runs remain separate future evidence.

## Evidence

- Official Grok Build snapshot
  [`47348d1`](https://github.com/xai-org/grok-build/tree/47348d13ec4508dcfe440e34c6d511bb02998fb2)
  defines the CLI flags, headless JSON envelope, isolation roots, and
  incomplete-usage semantics used here.
- `y-harness-benchmark-runner` tests cover bare-home authority, prompt-file
  privacy and cleanup, bounded command construction, successful usage/cost
  normalization, and product errors without spend.
- [`external-run-format.md`](../external-run-format.md) defines the retained
  evidence and non-claim boundary.
