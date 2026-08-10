# ADR 0128: Durable lease-fenced Human Handoff ownership

- Status: accepted
- Date: 2026-07-29

## Context

Approval answers whether one proposed action may proceed. Workflow coordinates
time, external signals, and process lifecycle. Thread handoff prepares bounded
context for another conversation. None of those contracts proves which human
actor currently owns an escalated case, prevents two operators from handling
it simultaneously, or safely returns abandoned work to a queue after restart.

Adding a boolean `human_active` to Thread or Workflow would have no owner,
lease, fencing token, retry identity, audit transitions, or cross-process
compare-and-swap. Reusing Approval would incorrectly make ownership transfer a
one-shot decision.

## Decision

- Add an independent `HumanHandoff` aggregate. It references one authoritative
  same-tenant Thread or Workflow Run through a typed
  `HumanHandoffSubject`. A host-supplied `HumanHandoffSubjectResolver` must
  confirm that the subject exists before creation.
- Model four states:
  - `queued`, discoverable by an authorized operator queue;
  - `claimed`, owned by one trusted actor under one finite claim fence;
  - `resolved`, with a content-free outcome code and bounded summary;
  - `cancelled`, with a content-free reason code.
- Claim, renew, release, explicit expiration, resolve, and cancel are distinct
  commands. Claim and renewal use server application time. At the exact
  expiration boundary the lease is expired; an old owner cannot renew,
  release, or resolve it.
- Give each ownership period a unique `HumanHandoffClaimId`. Releasing or
  expiring returns the case to the same queue, but a later operator must use a
  new claim identity. A stale owner cannot act on the later claim.
- Bind every stable `HumanHandoffCommandId` and complete typed command to the
  trusted actor with SHA-256. Exact actor/content replay is idempotent before
  revision comparison. Reusing an identity from another actor or with changed
  content fails closed.
- Retain immutable, contiguous, actor-attributed transitions. Deserialization
  reconstructs the complete current projection, claim expirations, and every
  actor-bound command digest instead of trusting cached fields.
- Treat 4,096 transitions and 16 MiB of encoded state as work-admission
  ceilings. Claim and renewal consume work capacity. Reserve two additional
  transitions and 278,528 additional encoded bytes exclusively for release or
  expiration recovery and terminal resolution or cancellation. A case may
  therefore use its final work slot to become `claimed`, then expire back to
  the queue and still cancel, or resolve the active claim directly.
- Keep the reserve finite: the absolute hard boundaries are 4,098 transitions
  and 17,055,744 encoded bytes. Exact actor/content duplicate recognition still
  precedes admission at either boundary. Deserialization rejects work
  transitions placed in the reserved transition window. These rules do not
  change the serialized aggregate or command representation.
- Persist cases through a tenant-partitioned, revision-CAS Coordinator.
  Memory and SQLite implementations share create, load, queued-list, and apply
  semantics. SQLite schema 1 uses WAL, `synchronous=FULL`, immediate
  transactions, bounded text reads, explicit metadata, projection validation,
  and fail-closed partial/unknown layouts.
- Queue discovery is stable and finite: priority descending, request time
  ascending, and case identity ascending. The cursor contains all three
  coordinates. Only `queued` cases appear.
- Advance Client Protocol 27 to 28. The optional surface creates, reads, lists,
  pages transitions, and applies typed commands. Create/get/list, claim,
  renewal, expired-claim management, resolution, and cancellation use distinct
  permissions.
- The reference service resolves Thread subjects through authoritative State,
  resolves Workflow subjects through the Workflow Engine, shares the same
  fixed tenant authority, and stores cases independently in
  `human-handoffs.db`.

## Bounds and recovery

- One case admits claim work through 4,096 transitions and 16 MiB of encoded
  state. Its recovery/settlement-only hard limits are 4,098 transitions and
  17,055,744 encoded bytes.
- One actor-bound command is limited to 128 KiB. Resolution summaries are
  limited to 64 KiB.
- Claim leases are 1 second through 7 days.
- Coordinator pages contain 1–256 snapshots. Protocol queue and transition
  pages contain 1–64 entries and at most 4 MiB.
- A new command requires the exact current revision. Exact replay returns the
  committed snapshot without advancing the revision.
- Schema 1 is the first Human Handoff store. There is no inferred migration
  from Approval, Thread handoff, or Workflow data.

## Authority boundaries and non-claims

`ActorIdentity` is trusted attribution supplied by an embedding host or
transport; constructing its strings is not authentication. `LocalProcess`
remains valid for embedded and single-user operation, but it is not evidence
of an individual employee. An enterprise operator product must map its
authenticated principal through `ProtocolAuthorizer` and grant the exact
Human Handoff permissions.

This aggregate controls ownership evidence only. Creating or claiming a case
does not automatically pause an Agent Turn, reroute an IM channel, mutate a
Workflow, deliver a Workflow signal, or authorize business-system actions.
The application composes those effects through Policy, Workflow waits,
Channels, and governed Connectors. There is no atomic cross-database outbox in
this slice, so a Handoff resolution and a later Workflow signal remain two
explicit idempotent operations.

The resolution summary is bounded content-bearing operator data stored in the
case database. Encryption at rest, redaction, retention, legal hold, external
Artifact storage, skill-based routing, notifications, automatic expiration
polling, workload balancing, and multi-node availability are not claimed.

## Rejected alternatives

- Reuse Approval: a decision has no renewable exclusive owner or queue return.
- Add ownership fields to Workflow: this couples human identity/lease policy to
  time and signal coordination.
- Infer an owner from the last message sender: channel messages are not a
  fencing protocol and may be delayed, duplicated, or reordered.
- Automatically let another operator take an expired claim: the expiration
  transition and later claim would collapse into one unauditable mutation.
- Accept actor or timestamps in request JSON: the transport and host, not
  caller-authored strings, own attribution and time.
