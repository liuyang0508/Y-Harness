# ADR 0087: Explicit configured Model catalog and route

- Status: Accepted
- Date: 2026-07-27

## Context

The Runtime already owns a collision-safe `ModelRegistry` and an explicit
ordered failover route, but the reference service could configure only one
Model. Operators therefore had to write a Rust host to use the existing route
contract, even when every provider adapter and credential source was already
available.

Implicit fallback, automatic provider discovery, or a magic “best model” name
would weaken provenance and make cost, continuation affinity, and failure
behavior difficult to audit.

## Decision

- Preserve the existing single `model` object as the compatible simple form.
- Add an alternative `models` catalog plus required `model_route`. The two
  forms are mutually exclusive.
- Treat each configured `id` as the operator-owned stable registry alias.
  Vendor model strings, endpoints, and environment-backed Secret references
  remain adapter configuration rather than routing identities.
- Require one to sixteen exact, unique route identities. Reject duplicate
  catalog IDs, duplicate route entries, unknown route entries, mixed
  single/catalog forms, and attempt timeouts outside 1–86,400,000 milliseconds
  before provider construction or credential resolution.
- Register every configured Model through the existing trust-bearing
  `ModelRegistry`, then construct the Runtime through its existing ordered
  failover contract. Do not add a second service-only router.
- Try route entries in declared order under one explicit per-attempt timeout.
  Preserve Runtime rules that forbid fallback after provisional output,
  cancellation, total Turn deadline, or an origin-bound continuation.
- Allow a default-disabled timeout cooldown configured from 1 millisecond to
  24 hours for multi-Model routes. ADR 0099 owns its Runtime semantics; the
  service does not add a second health router.
- Allow an independent default-disabled `retry` object with 1–8 additional
  calls and 1–60,000 millisecond delay bounds. ADR 0101 owns typed eligibility,
  backoff, deadline, streaming, and Observability semantics; the service does
  not parse Provider diagnostics.
- Resolve each Model's API key from its own explicit environment mapping.
  Configuration and diagnostic output never contain the resolved value.
- Freeze the catalog and route at service startup. Configuration changes
  require a controlled service restart.

## Consequences

An operator can add OpenAI Responses Models and compatible Y-Harness HTTPS
gateway-backed vendor Models, assign stable aliases, use separate API-key
environment variables, and define an ordered fallback route without changing
Rust code. `yh doctor` reports the exact catalog size and route.

This is not a universal native vendor protocol. A vendor without an in-tree
adapter must be exposed through the exact HTTPS Model Gateway API or gain a
reviewed adapter. There is no ambient endpoint guessing, hot reload, general
error health scoring, load/price selection, or silent alias replacement.

## Rejected alternatives

- Infer a route from catalog order: catalog storage order is not execution
  authority.
- Allow both `model` and `models`: two competing active configurations are
  ambiguous.
- Put aliases in a separate mutable map: the registry identity already is the
  durable alias and collision boundary.
- Resolve credentials before route validation: malformed configuration should
  fail without touching Secret providers.
- Add automatic fallback to every registered Model: registration does not
  imply execution consent.

## Evidence

- `reference_cli::service::tests::config_is_strict_and_data_directory_cannot_escape`
- `reference_cli::service::tests::shipped_real_provider_configs_follow_the_strict_schema`
- `doctor_validates_an_explicit_ordered_model_catalog`
- `runtime::tests::model_failover_route_rejects_empty_duplicate_and_unknown_identities`
- `runtime::tests::model_failover_records_each_attempt_and_settled_provenance`

## Related decisions

- [ADR 0018](0018-model-registry-and-provenance.md)
- [ADR 0027](0027-secret-references-and-https-model-gateway.md)
- [ADR 0070](0070-explicit-bounded-model-failover.md)
- [ADR 0076](0076-governed-service-capability-assembly.md)
- [ADR 0099](0099-observable-model-attempt-timeout-cooldown.md)
- [ADR 0101](0101-bounded-typed-model-retry-policy.md)
