# ADR 0083: Exact provider model-cost ticks

- Status: Accepted
- Date: 2026-07-26

## Context

`ModelUsage` originally represented cost as integer millionths of one US
dollar. That avoided floating-point aggregation, but it cannot preserve the
exact cost emitted by Grok Build: its source contract uses ten billion integer
ticks per USD and publishes the float only as a projection. Rounding that
evidence to micro-USD would make Observability disagree with the provider.

Provider price tables are not Runtime authority. Missing, partial, estimated,
or float-only costs must not be presented as exact billing evidence.

## Decision

- Represent optional model-call cost as `cost_usd_ticks`, where one USD equals
  `MODEL_COST_USD_TICKS_PER_USD = 10_000_000_000` ticks.
- Keep the amount unsigned and integer so equality, serialization, and
  aggregation do not introduce floating-point drift.
- Require adapters to omit cost when the provider marks it incomplete or when
  exact conversion to this scale would require rounding.
- Do not infer cost from Token counts or a local price table in Runtime.
- Advance the exact HTTPS JSON model-gateway API coordinate from `3` to `4`.
  Do not deserialize the old `cost_microusd` name as ticks because that would
  silently multiply its meaning by ten thousand.

## Consequences

The Core can preserve exact costs down to one ten-billionth of a USD, including
the source-pinned Grok Build evidence, without adding a decimal arithmetic
dependency. Existing gateway implementations must adopt API `4` and rename or
convert their cost field explicitly.

This is a fixed USD normalization contract, not a multi-currency billing
system. A future provider that reports another currency or finer exact
precision will require a separately versioned extension; the Runtime must not
round it into this field.

## Rejected alternatives

- Keep micro-USD and round: destroys provider evidence.
- Store dollars as `f64`: permits drift and makes exact reconciliation
  impossible.
- Add a generic currency/decimal algebra now: no current adapter requires
  multi-currency arithmetic, so the extra type and validation surface would be
  speculative.
- Accept `cost_microusd` as a Serde alias: the same integer would acquire a
  different unit without an explicit migration.
