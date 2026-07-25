# ADR 0063: Attributed, separation-of-duty approvals

- Status: Accepted
- Date: 2026-07-25

## Context

Approval Inbox schema 1 durably stored a decision but not the authenticated
requester or settler. Protocol authorization proved that one certificate could
invoke `approval.settle`, but the resulting record could not answer who made
the decision. The same authenticated principal could also start a Turn and
approve its request. A protocol-only comparison would leave embedded and
competing settlement paths able to bypass the invariant.

Historical schema-1 records contain no evidence from which either identity can
be reconstructed. Treating a guessed operator or certificate as fact would
corrupt the audit record.

## Decision

- Add provider-neutral `ApprovalActor` identities. A current actor is either
  the embedding process boundary or an authority-scoped authenticated subject.
  `UnattributedLegacy` is reserved for migrated terminal evidence and is
  rejected on current submission and settlement paths.
- Carry the authenticated Turn initiator through `TurnExecutionOptions` into
  every `ApprovalRequest`.
- Map an mTLS client certificate to authority
  `mtls-certificate-sha256` and its exact lowercase leaf-certificate
  fingerprint. This is certificate identity, not a claim about a human name,
  tenant, or role.
- Persist the deciding actor beside the immutable decision.
- Enforce `requested_by != decided_by` inside Memory and SQLite Inbox
  settlement, after revision validation and before the candidate record
  commits. A failed self-approval leaves the pending record and revision
  unchanged.
- Advance Approval Inbox schema from 1 to 2 and client protocol from 6 to 7.
  Schema 2 also raises the record ceiling to 525,312 bytes and reserves 5,120
  bytes in pending records for the largest supported terminal form.
- Require explicit offline `yh approval-migrate <database> <backup>` for a
  populated schema-1 SQLite inbox. The migration creates and validates a
  SHA-256-bound, no-clobber rollback backup before changing the source.
- Convert legacy pending records to `orphaned`; they cannot safely remain
  actionable without a requester identity. Preserve settled decisions and
  orphan records, marking unavailable historical identities as
  `UnattributedLegacy`.

## Correctness and recovery

Migration fingerprints every authoritative row and its indexes, rechecks that
fingerprint under an immediate transaction, rewrites records in bounded
16-record pages, and publishes current-writer metadata in the same commit.
Interruptions after preflight, after backup publication, or before commit
leave either the untouched legacy source or a reusable validated backup.

The largest supported one-Turn fixture contains 256 records and 133,038,080
bytes of schema-1 record bodies. A release-mode run completed the migration
itself in 844.781 milliseconds on 2026-07-25. This number is
environment-specific evidence, not a latency service-level objective.

## Security boundary

The invariant separates exact authenticated actors. It does not prove that two
certificates belong to different people, organizations, tenants, or roles.
Subject/SAN policy, tenant ownership, role assignment, cryptographically signed
decision receipts, key lifecycle, retention, and notifications remain separate
work. Direct non-Inbox `ApprovalHandler` implementations remain an embedding
boundary; production workflows requiring durable attribution and separation
must use an Approval Inbox.

`LocalProcess` is deliberately one actor. A local process cannot create a
durable request and then approve it as though a second operator existed. A host
that needs multiple local roles must authenticate them and supply stable,
authority-scoped subjects rather than arbitrary display names.

## Rejected alternatives

- Record only a transport log: it is not atomically joined to the approval CAS
  and cannot enforce separation.
- Accept a caller-supplied approver label: an untrusted client could choose a
  second name and self-approve.
- Infer legacy identities: the source contains no supporting evidence.
- Leave legacy pending requests actionable: their requester cannot be compared
  with a future settler.
- Call mTLS attribution a signature: transport authentication does not produce
  a portable decision receipt signed over the exact request and decision.
