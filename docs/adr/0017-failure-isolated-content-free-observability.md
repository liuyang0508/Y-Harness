# ADR 0017: Failure-isolated, content-free Runtime observability

- Status: Accepted
- Date: 2026-07-25

## Context

Operators need phase latency, settlement outcome, provider correlation, token
usage, and cost evidence. Letting observers receive prompts or capability
payloads by default would create a second ungoverned data path. Letting an
exporter block or fail the Agent Loop would also make diagnostics part of the
control plane.

State remains the authoritative ordered execution record. Observability is a
derived, best-effort operational view and must not change Turn settlement.

## Decision

- Emit a typed observation for each externally awaited Runtime phase.
- Include only Thread and Turn identity, phase, capability identity, monotonic
  duration, and outcome class by default.
- Accept token usage, optional cost, and provider request identity only when a
  model provider reports them; the Runtime does not estimate missing values.
- Do not include prompts, context, tool inputs, tool results, or model content.
- Keep observers synchronous and explicitly non-blocking. Isolate both returned
  errors and panics, and count dropped observations.
- Provide a hard-bounded in-memory collector for local diagnostics. When full,
  it keeps earlier evidence and counts rejected records instead of allocating
  without limit.
- Keep durable event export separate and derived from the State journal.

## Consequences

Telemetry cannot silently acquire user content or become required for a Turn
to complete. Operators can correlate provider-side diagnostics and account for
provider-reported usage without confusing estimates with billing evidence.

Production network exporters will need their own bounded queue and worker. They
must preserve the same failure isolation and expose queue loss explicitly.

## Rejected alternatives

- Put prompt and output bodies into every span: creates unnecessary privacy and
  secret exposure.
- Await asynchronous exporters inside Runtime phases: exporter latency and
  failure would alter Agent Loop behavior.
- Infer token counts and cost in the Runtime: provider tokenization, cache
  accounting, and billing rules are not authoritative locally.
- Use an unbounded trace buffer: converts an observability outage into a memory
  exhaustion failure.
