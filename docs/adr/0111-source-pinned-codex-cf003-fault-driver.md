# ADR 0111: Codex CF-003 follows the released product's deferred Tool path

- Status: Accepted
- Date: 2026-07-28

## Context

The controller-owned CF-003 fixture could already prove its own durable
one-effect oracle, but no released product had executed it. Reusing the
ordinary fixed-output adapter would omit MCP process failure, while moving
Codex-specific behavior into the Harness Core would make the comparison
circular.

Codex `0.145.0` also does not directly advertise configured MCP Tools to
`gpt-5.4`. Its source-defined large-search path advertises `tool_search`,
returns the selected namespaced Tool in `tool_search_output`, and only then
accepts the MCP function call. An unknown Model falls back to metadata that can
hide this behavior and is not a truthful substitute.

## Decision

- Add one source-pinned `codex-cf003` driver to the independent benchmark
  runner, not to the Harness Runtime or the fault fixture.
- Pin released Codex `0.145.0`, analyzed official tag `rust-v0.145.0`, exact
  product/adapter/fixture/spec hashes, and recognized Model `gpt-5.4`.
- Use an isolated empty workspace and `CODEX_HOME`, cleared environment,
  read-only product sandbox, noninteractive approval, disabled user rules,
  persistence, web search, request compression, and multi-Agent Tools.
- Implement only the bounded loopback Responses surface required by this case.
  Validate exact authorization, path, framing, Model, three requests, deferred
  Tool-search output, failed function output, and terminal Codex JSONL.
- Keep the fixture as the independent effect authority. A passing format-7
  report requires one invocation, one committed effect, and no replay.
- Permit Codex item notifications to cross `turn.started` in the adapter
  parser, because the released app-server forwards them asynchronously. Still
  require one Thread start, one Turn start before terminal settlement, a final
  assistant message, validated usage, and no event after terminal.
- Keep `claim_eligible: false`. Record advertised built-in Tools, unrestricted
  outer process isolation, lack of reproducible binary-to-source equivalence,
  and absence of product restart/resume.

## Consequences

Y-Harness now has one real released-product uncertain-effect record whose
Provider protocol, product lifecycle, and durable effect can be correlated
without external API cost. It also tests the product's actual deferred Tool
discovery instead of manufacturing a direct call with fallback metadata.

The result does not compare product quality, test a real LLM, disable every
Codex built-in Tool, prove restart behavior, or establish parity with another
product. Those remain separate cells.

## Evidence

- Benchmark-runner unit tests close source/case coordinates, configuration,
  Tool namespace projection, deferred search output, exact function output,
  crossed JSONL notification order, and checked-in evidence integrity.
- The checked-in Codex `0.145.0` record completed three deterministic Provider
  requests, observed `Transport closed`, and retained one invocation and one
  effect with `uncertain_effect_not_replayed`.
