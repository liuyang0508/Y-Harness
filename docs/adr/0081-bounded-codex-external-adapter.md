# ADR 0081: Codex external evidence preserves unavailable facts

- Status: Accepted
- Date: 2026-07-26

## Context

The first external adapter proved that a released CLI can be pinned and
observed without entering the Harness Core, but its result fields reflected
Claude Code's single JSON envelope. Codex exposes a different stable surface:
`exec --json` emits an ordered JSONL event stream. That stream reports Turn
settlement and token usage, but not the settled Model identity, product/API
duration, actual cost, or a hard monetary budget.

Treating those missing values as zero, copying the requested Model into an
observed field, or deriving product time from adapter wall time would create
false evidence.

## Decision

- Add Codex as a second explicit adapter in
  `y-harness-benchmark-runner`; do not import Codex implementation code or move
  product behavior into the semantic Core.
- Pin the exact CLI version and executable SHA-256 before Model work, clear the
  child environment, invoke without a shell, bound both output streams and
  execution time, and retain the parsed JSONL events plus exact stream hashes.
- Parse JSONL as a bounded state machine: one initial Thread, one Turn, ordered
  Item events, and exactly one terminal completion, failure, or fatal error.
  Unknown or post-terminal events fail as adapter errors rather than being
  silently ignored.
- Run with ephemeral persistence, a read-only Codex sandbox, approval policy
  `never`, disabled web search, an exact Model request, and the benchmark
  system prompt as an explicit developer-instruction override.
- Require `bare` runs to use an empty, exact `CODEX_HOME`, API-key
  authentication, ignored user configuration, and ignored exec-policy rules.
  Preserve `product` as a separate ambient-configuration profile.
- Keep Codex built-in Tools visible as an unsupported control. Read-only
  sandboxing limits authority but is not equivalent to disabling Tools.
- Serialize unavailable product metrics as `null` and observed Models as an
  empty list. External-run format 2 remains an evidence envelope, and every
  Codex adapter result remains `claim_eligible: false`.
- Reuse the common evidence envelope and validation helpers, but keep
  product-specific command construction and normalization explicit. Two
  adapters do not justify a trait that hides materially different controls.

## Consequences

Y-Harness can now preserve bounded Codex product evidence without pretending
that the Claude Code and Codex CLIs expose equivalent controls. A future
aggregator must handle declared missing data and may compare only cells whose
unsupported-control sets satisfy the pre-registered benchmark.

No live Codex result is created by this decision. Adapter contract tests prove
parsing and authority construction only; a pinned released binary must still
execute a real case before Codex contributes product evidence.

## Evidence

- Codex official snapshot
  [`61a4488`](https://github.com/openai/codex/tree/61a44880a85d2fd0d8770908dea5733495e571c8)
  defines the `exec` flags and typed JSONL events used by the adapter.
- `y-harness-benchmark-runner` tests cover bare-profile controls, prompt
  delivery, successful settlement, product failure, and post-terminal
  rejection.
- [`external-run-format.md`](../external-run-format.md) defines the retained
  evidence and non-claim boundary.
