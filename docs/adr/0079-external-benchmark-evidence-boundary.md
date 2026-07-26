# ADR 0079: External benchmark evidence stays outside the semantic Core

- Status: Accepted
- Date: 2026-07-26

## Context

Y-Harness regression tests establish local invariants but cannot prove a
comparative product claim. Released products expose different CLIs, settings,
models, authority, accounting, and output envelopes. Normalizing those
differences inside the Harness Core would couple engine semantics to product
automation and make adapter behavior look authoritative.

The first live Claude Code probe also demonstrated why raw evidence matters:
the requested `haiku` alias was accounted under observed model
`MiniMax-M2.7`, and a requested `max-budget-usd` of 0.02 returned a product
error after reporting 0.056875 actual cost. Requested controls cannot be
silently relabeled as observed facts.

## Decision

- Keep released-product adapters in the independent
  `y-harness-benchmark-runner` workspace package.
- Define external-run format 1 as an evidence envelope, not an Evaluation
  score. It records SHA-256 coordinates for both adapter and product
  executables, exact product version, prompt fingerprints, requested controls,
  observed models and cost, bounded raw output, and explicit unsupported
  controls.
- Make the initial Claude Code adapter an `adapter_conformance` track with
  `claim_eligible: false`. It verifies an exact CLI version before paid work,
  launches without a shell through the bounded Process Broker, clears the
  environment before inheriting an explicit name allowlist, disables Tools,
  Skills, MCP, persistence, and interactive approvals, and retains no stderr
  content.
- Distinguish a valid product error from adapter failure. A nonzero CLI exit
  carrying a valid result envelope is product evidence, not parser failure.
- Treat `max-budget-usd` as a requested product control, not a hard spend
  fence. Preserve actual reported cost independently.
- Do not add generic adapter traits, scoring, cross-product schemas, or
  superiority summaries until a second adapter proves a shared abstraction.

## Consequences

The repository can now preserve truthful external execution evidence without
granting a product adapter authority over Harness behavior. Format-1 output is
usable by later blind graders and aggregation, but it supports no quality or
Harness-effect claim on its own.

The first adapter deliberately excludes Tool workloads, Turn steering,
cross-product model parity, stochastic trials, and statistical aggregation.
Those enter only through separately versioned benchmark cases and adapters.

## Evidence

- `y-harness-benchmark-runner` unit tests validate the exact command boundary,
  specification rejection, and observed Claude Code budget-error envelope.
- [`competitive-benchmark.md`](../competitive-benchmark.md) defines the claim
  and execution rules that remain authoritative.
