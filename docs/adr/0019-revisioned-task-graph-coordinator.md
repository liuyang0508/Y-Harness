# ADR 0019: Revisioned Task Graph coordinator

- Status: Accepted
- Date: 2026-07-25

## Context

`TaskGraph` already enforces DAG ordering, bounded messages and Artifacts,
expiring leases, and fencing tokens as a pure serializable aggregate. Leaving
persistence entirely to each host would make atomicity and recovery optional,
and an ordinary last-writer-wins save could resurrect stale worker state.

## Decision

- Define a `TaskCoordinator` port around create, load, and compare-and-swap of
  complete revisioned Task Graph snapshots.
- Reject stale saves with a typed conflict containing graph identity, expected
  revision, and actual revision. The coordinator does not retry a mutation
  whose assumptions may now be stale.
- Keep snapshot identity, revision, and graph fields private. Callers can
  inspect them and mutate only through `TaskGraph` domain methods; snapshots
  cannot be externally deserialized and forged.
- Revalidate graph invariants and a 64 MiB encoded-size ceiling before every
  create/save and after every durable load.
- Bound Tasks, dependencies, messages, Artifacts, identifiers, and text at the
  aggregate boundary.
- Provide in-memory semantic parity and a SQLite implementation using WAL,
  `synchronous=FULL`, a five-second busy timeout, and one immediate transaction
  per CAS.
- Keep lease fencing in the domain aggregate. Persistence makes issuance and
  settlement durable but never converts an expired or replaced token into
  valid ownership.

## Consequences

Multiple processes on one coordinator database can compete safely and recover
after restart. A caller that loses a revision race must reload and recompute its
operation against current dependencies, leases, and terminal state.

The v1 SQLite representation rewrites one bounded graph JSON snapshot per
successful CAS. It favors correctness and simple recovery for moderate DAGs;
it is not a claim of high-throughput distributed scheduling or multi-node
consensus. Measured demand can justify a later incremental/event-backed
coordinator without changing the `TaskCoordinator` contract.

## Rejected alternatives

- Last-writer-wins snapshots: can overwrite a current lease or completion with
  stale state.
- Automatic blind CAS retry: reapplying an old claim or completion after a
  conflict can violate current dependency and fencing assumptions.
- Public snapshot fields or deserialization: allow bypassing domain mutation
  invariants.
- Introduce a distributed database before a single-host correctness slice:
  adds operational surface without first proving the coordinator contract.
