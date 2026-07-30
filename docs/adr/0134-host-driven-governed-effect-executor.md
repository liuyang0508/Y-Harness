# ADR 0134: Host-driven default-deny Governed Effect Executor

- Status: accepted
- Date: 2026-07-30

## Context

ADR 0133 introduced a durable Effect Ledger but intentionally stopped before
external execution. A Ledger proves that an immutable request existed before a
worker acted, owns one finite attempt lease, and records authoritative
settlement. It does not select a Connector, authorize external entry, impose a
Connector deadline, or decide what an uncertain Provider failure means.

Embedding those decisions in each product host would recreate subtly different
outbox consumers. Putting a polling loop, credential manager, or Channel
implementation in Core would instead collapse Harness semantics into one
deployment product.

The hard boundary is dispatch. Before a Connector is called, cancellation,
Policy denial, and persistence failure prove that no external call was made.
After dispatch, timeout, panic, cancellation, or an opaque error does not prove
that the target rejected the operation. Blindly returning such a lease to
`pending` could duplicate a payment, message, notification, or mutation.

## Decision

- Add embedded Governed Effect Executor API 1 as an optional host-composed
  module over an existing `EffectEngine`.
- Keep execution host-driven. One `run_once_as` call performs one bounded
  pending-page sweep. Core creates no polling thread, durable scheduler cursor,
  leader election, Connector process, or shutdown lifecycle.
- Register Connectors by one exact capability, an explicit non-empty operation
  set, API coordinate, trust origin, and one of two duplicate-suppression
  contracts: target-enforced or Connector-enforced. Wildcards, replacement,
  fallback, and API-version guessing are rejected.
- Capture Connector descriptors exactly once under panic isolation. Connector
  selection uses only the frozen descriptor.
- Install a default-deny execution Policy. Policy receives the exact authority,
  operation, bounded input and digest, Connector origin, and idempotency
  contract before any Claim mutation. Policy panic, error, invalid denial
  evidence, timeout, or cancellation fails closed without a Claim.
- Scan only authoritative tenant-local `pending` projections. Revalidate the
  complete custom Coordinator page for bounds, continuation, order, tenant,
  revision, aggregate validity, and state before performing any mutation.
- Derive Claim lease and command identities from the caller-stable cycle,
  trusted actor and tenant, Effect identity, observed revision, and attempt.
  Precompute all identities before concurrent mutation.
- Claim through the existing Effect revision-CAS command. If an exact Claim
  command is already committed, report it and never enter the Connector from
  the duplicate caller. A different concurrent mutation is fenced.
- Supply a Connector only the claimed immutable request, target idempotency
  key, input digest, exact attempt/lease, authority, lease deadline, and a
  cooperative cancellation token. The request intentionally has neither
  `Debug` nor serialization.
- Bound Policy and Connector time independently. A host cancellation observed
  before dispatch is authoritatively recorded as not applied and immediately
  retryable. Connector panic, error, invalid outcome, deadline, or cancellation
  after dispatch is recorded as `unknown`; it is never inferred retryable.
- Accept `Applied` only with a valid content-free receipt. Accept `NotApplied`
  only as an authoritative assertion with a bounded reason code and optional
  bounded retry delay. Invalid Connector evidence becomes
  `connector.invalid_outcome` uncertainty.
- Settle through the exact post-Claim revision and lease. Reports contain
  identities, revisions, lease fences, reason codes, and times, but omit input,
  idempotency keys, receipts, Provider bodies, and Provider diagnostics.
- Execute eligible records with a bounded concurrency window and restore
  source identity order in the returned report.

## Bounds

- One sweep inspects 1–256 pending records.
- One Executor admits 1–64 concurrent attempts.
- One registry contains at most 256 Connectors, each with 1–256 exact
  operations and at most 64 KiB of encoded descriptor metadata.
- Policy deadlines range from 1 millisecond through 60 seconds.
- Connector deadlines and Claim leases are positive and bounded at 7 days.
- The Claim lease must strictly outlive the Connector deadline plus an explicit
  settlement reserve.
- Connector-selected retry delay is bounded at 7 days.
- Effect Ledger input, identifier, transition, and durable-state bounds remain
  authoritative and are not widened by this API.

## Failure classification

| Boundary | Evidence | Durable action |
|---|---|---|
| before Claim | missing Connector, Policy denial/failure, host cancellation | no mutation |
| after Policy, before Claim commit | clock/store failure or revision loss | no Connector entry |
| exact Claim replay | duplicate command already durable | skip Connector |
| after Claim, before dispatch | host cancellation observed | `RecordNotApplied`, retry now |
| after dispatch | panic, error, timeout, host cancellation | `RecordUnknown` |
| valid Connector `Applied` | bounded receipt | `RecordApplied` |
| valid Connector `NotApplied` | authoritative absence | reject or explicit retry |
| settlement CAS loss | another exact revision won | report fenced; durable state wins |
| Executor worker stops unexpectedly | execution phase cannot be inferred | content-free failure; durable state wins |

## Consequences and non-claims

The same Engine can now support independently installed Channel, payment,
notification, or other external-effect Connectors without adding business
behavior to Core. Hosts may invoke the sweep from a service timer, queue
consumer, CLI, test harness, or their own distributed scheduler.

This API does not make arbitrary Connectors safe. The operator must validate
the advertised idempotency contract, inject credentials through an appropriate
Secret boundary, sandbox untrusted implementations, and reconcile every
`unknown` Effect against an authoritative target. The Executor does not verify
receipt truth, compensate applied effects, renew long-running leases,
automatically reconcile uncertainty, persist its page cursor, expose a new
Protocol command, or provide multi-node work ownership.

## Rejected alternatives

- Execute directly during Effect creation: persistence would no longer precede
  external entry and request latency would own recovery semantics.
- Put a permanent consumer in Core: cadence, lifecycle, deployment topology,
  and availability belong to the embedding host.
- Retry every Connector error: an error after dispatch is not proof of absence.
- Treat duplicate Claim success as execution authority for every caller: two
  consumers could enter the same Connector concurrently with one lease.
- Route by wildcard or first compatible Connector: execution authority must
  remain an exact configured choice.
- Include Provider messages in reports: diagnostics are neither settlement
  evidence nor safe ambient telemetry.
