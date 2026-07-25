# ADR 0041: Bounded protocol Operation retention

- Status: Accepted
- Date: 2026-07-25

## Context

Protocol Operations retain a final model response and a bounded provisional
event ring until the client forgets them. A hard-coded capacity of 4,096 was
finite but unsafe as a default: individual response and stream limits allow
roughly two MiB of retained content per Operation, so an authenticated caller
could drive worst-case process-local retention toward eight GiB. Runtime Turn
admission bounds simultaneous work, but terminal Operation records remain until
explicit release.

## Decision

- Retain at most 64 running plus terminal Operations per `ProtocolHandler` by
  default.
- Let hosts configure an exact retention limit from 1 through 4,096 with
  `with_operation_retention_limit`; reject values outside that range.
- Reject `turn.start` before creating a worker when the registry is full.
- Keep polling, cancellation, event reads, and terminal forgetting available
  while full.
- Never evict a running or unobserved terminal Operation automatically. A client
  releases terminal capacity through `operation.forget`.

## Consequences

The default worst-case content retention is reduced from approximately eight
GiB to approximately 128 MiB, before container and provider-specific memory.
Hosts can choose a different bound deliberately. Existing clients that already
forget terminal Operations continue without behavior changes; clients that
leak handles receive an actionable capacity error instead of exhausting the
process.

The maximum remains available for controlled deployments and is not a
recommendation. This count bound does not measure allocator overhead or
provider memory; production resource limits remain necessary.

## Rejected alternatives

- Keep 4,096 as the default: finite is not synonymous with operationally safe.
- Automatically evict the oldest terminal Operation: a slow client could lose
  the only process-local final result without an explicit cursor gap contract.
- Persist Operation records: durable execution truth already belongs to State,
  and duplicating it would introduce reconciliation ambiguity.
