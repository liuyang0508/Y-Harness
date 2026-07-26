# ADR 0084: Provider-reported Model identity is observation evidence

- Status: Accepted
- Date: 2026-07-26

## Context

The registered Y-Harness Model identity identifies a configured capability and
its operator-assigned origin. It is authoritative for routing, failover,
Policy evidence, and provider-continuation binding, but it does not prove which
upstream model settled a call. A configured alias or gateway may resolve to a
different concrete model.

This distinction is observable in real contracts. OpenAI Responses returns the
Model ID used to generate a response. Claude Code and Grok Build product
results account usage under per-Model keys. The checked-in Claude Code
conformance record requested `haiku` but reported usage under
`MiniMax-M2.7`.

Copying the configured or registered identity into an observed field would
invent evidence. Treating a Provider-reported string as routing authority
would let untrusted response metadata change Harness control semantics.

## Decision

- Add optional `provider_model` evidence to `ModelResponse` and
  `PhaseObservation`.
- Bound it to 1–256 non-control bytes and validate it before the response
  reaches the Agent Loop or an Observer.
- Require the direct OpenAI Responses adapter to preserve the successful
  response object's required `model` field for both JSON and SSE settlement.
- Let other adapters omit the field when their call-level protocol does not
  report it. Never copy the requested or registered identity into it.
- Keep the registered Model identity and origin authoritative for routing,
  failover, Tool provenance, Policy, and continuation binding.
- Keep Provider-reported Model identity in content-free Observability alongside
  usage, exact cost, and request correlation. Do not advance State or client
  protocol schemas for best-effort accounting metadata.
- Advance the exact HTTPS JSON model-gateway API from `4` to `5`.

## Consequences

Operators can distinguish the Harness capability that was selected from the
upstream Model that reported settling the call. Alias resolution and gateway
routing no longer disappear from Runtime observations.

The field is Provider-reported evidence, not independently attested truth.
Default Observability remains best effort; this change does not make upstream
identity durable. A future requirement for durable billing or compliance
evidence must define retention, trust, and State migration explicitly instead
of silently promoting telemetry.

## Rejected alternatives

- Copy the configured Model string: requested state is not observed evidence.
- Replace registered provenance with the reported Model: response metadata
  must not control routing or continuation authority.
- Advance State and protocol schemas now: no current recovery invariant needs
  Provider accounting metadata to execute safely.
- Store an unbounded Provider payload: unnecessary privacy and allocation
  surface.

## Sources

- [OpenAI Responses API response schema](https://platform.openai.com/docs/api-reference/responses/object)
- [OpenAI model guidance documents alias routing](https://developers.openai.com/api/docs/guides/latest-model)
- [Y-Harness external evidence boundary and live alias mismatch](0079-external-benchmark-evidence-boundary.md)
- [Source-pinned Grok Build adapter decision](0082-bounded-grok-build-external-adapter.md)
