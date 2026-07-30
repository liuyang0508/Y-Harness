# ADR 0132: Authoritative TUI runtime projection

- Status: accepted
- Date: 2026-07-30

## Context

The full-screen TUI needs to distinguish deterministic demo output from a real
Model and present State pressure without misleading operators. The client does
not own Engine configuration, Model routing, State, or provider construction.
Reading `y-harness.json` would duplicate configuration semantics and still
would not prove which routed Model actually settled a historical decision.

A durable Item proves the registered Model that produced that Item. It does
not prove which Model a future Turn will select after route, configuration,
health, retry, or failover changes.

## Decision

- Derive displayed Model identity only from the newest durable
  `AssistantMessage`, `ToolCall`, or `ProviderContinuation` in the current
  Protocol Thread projection.
- Label the Header value `LAST MODEL`. Never present it as the current or next
  route. A future current-route display requires a new typed Engine capability
  and compatibility decision; the TUI must not parse configuration or inspect
  Engine storage.
- Mark an exact durable `local/demo` decision as deterministic and
  no-network in the Header and its Assistant record. Do not apply that label to
  unrelated Models or predict that the next Turn remains a demo.
- Render State capacity from the complete typed `StateCapacity`: include used
  and limit values, preserve the authoritative pressure level, and display a
  nonzero value below one percent as `<1%` rather than `0%`.
- Keep empty-state guidance and short-conversation positioning entirely in
  replaceable client rendering. They create no Engine State and change no
  Protocol semantics.
- Preserve Protocol v28 and all Engine/durable coordinates.

## Consequences

The TUI is more explicit without acquiring a second source of truth. After an
Engine configuration change, historical output remains accurately attributed
while the next route stays unknown until the Engine records a new decision.
An empty Thread cannot identify its configured Model through the current
Protocol; this is an intentional non-claim rather than a reason to read local
configuration.

## Rejected alternatives

- Parse `y-harness.json`: duplicates strict Engine configuration, fails for
  remote hosts, and confuses requested route with settled identity.
- Label the Composer as demo from the last Item: predicts a future routing
  decision that historical evidence cannot prove.
- Continue displaying nonzero pressure as `0%`: numerically rounded but
  operationally misleading.
- Add client-only synthetic Model status to State: gives presentation code
  authority over execution evidence.
