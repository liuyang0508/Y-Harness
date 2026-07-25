# ADR 0049: Failure-atomic, terminal-reserved approvals

- Status: Accepted
- Date: 2026-07-25

## Context

The in-memory Approval Inbox mutated a pending record in place and validated
the terminal form afterward. A valid record close to the 512 KiB durable limit
could make settlement or orphaning exceed that limit. The method then returned
an error while leaving the in-memory record terminal and invalid. SQLite did
not have this exact corruption because it transformed a decoded local copy
before its transaction update.

Merely making the mutation failure-atomic would still admit a pending record
that could never reach any supported terminal form. Pending admission therefore
needs a lifecycle budget, not only a current-shape budget.

## Decision

- Build settlement and orphan candidates from a clone, validate the complete
  candidate, and replace the in-memory record only after validation succeeds.
- Reserve 4,608 bytes below the 512 KiB record ceiling for every pending
  record. The reserve covers one maximum 4 KiB denial/orphan reason plus status,
  revision, timestamp, and JSON structural growth.
- Continue to allow terminal records to use the complete 512 KiB ceiling.
- Pin the reserve with tests at the exact pending ceiling for both a
  maximum-reason denial and a maximum-reason orphan transition.
- Reject a pending record one byte above its lifecycle ceiling before either
  Memory or SQLite persistence.

## Consequences

An Approval Inbox method cannot report a failed transition after partially
changing its in-memory authority. Every newly accepted pending record can be
settled or orphaned with every currently valid terminal payload, so recovery
does not discover a size-induced permanently pending record.

The reserve is intentionally conservative and reduces maximum request payload
capacity by 4,608 bytes. Any future terminal fields or larger reason limits
must update the reserve and its exact-boundary tests together.

ADR 0063 subsequently advances the Approval Inbox to schema 2, adds the
deciding actor, raises the record ceiling to 525,312 bytes, and raises the
pending terminal reserve to 5,120 bytes. The failure-atomic admission rule and
exact-boundary tests remain unchanged in principle; ADR 0063 owns the current
numeric contract.

This tightens the unpublished pre-release schema baseline without changing its
serialized shape. A manually created development row above the new pending
lifecycle ceiling is rejected rather than silently grandfathered into an
unsettleable state.

## Rejected alternatives

- Mutate first and restore on error: rollback code can itself drift as fields
  are added.
- Return the error but retain the terminal mutation: the API result and
  authority would disagree.
- Keep the full limit for pending records: failure atomicity alone does not
  provide terminal liveness.
