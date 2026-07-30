# ADR 0135: Host-driven authoritative read-only Effect reconciliation

- Status: accepted
- Date: 2026-07-30

## Context

ADR 0133 deliberately makes an expired or inconclusive external attempt
`unknown`; ADR 0134 preserves that uncertainty for every failure after
Connector dispatch. This avoids duplicate external mutations, but uncertainty
must eventually be resolved against authoritative target state. Letting every
product host invent this path would fragment Policy, timeout, evidence,
redaction, and concurrency semantics.

Reconciliation differs fundamentally from execution. A reconciliation query
must never cause, retry, or compensate the operation being examined. It may be
repeated after an uncertain query response or concurrently by multiple hosts.
Only the resulting Ledger settlement is a mutation, and the existing
revision/attempt/lease boundary already fences it.

Adding a reconciliation work lease would serialize queries, but it would also
add a second lifecycle to Effect schema 1. A crashed holder would require its
own expiry and recovery semantics. Since a truthful reconciliation lookup is
read-only, duplicate lookup is safe while duplicate settlement is already
CAS-fenced.

## Decision

- Add embedded Governed Effect Reconciler API 1 as an optional host-composed
  module over an existing `EffectEngine`.
- Keep reconciliation host-driven. One `run_once_as` call performs one bounded
  tenant-local `unknown` page sweep. Core creates no polling thread, durable
  cursor, query lease, leader election, Connector process, or shutdown
  lifecycle.
- Register Connectors by one exact capability, explicit non-empty operation
  set, API coordinate, trust origin, and the
  `authoritative_read_only` contract. Wildcards, replacement, fallback, and
  API guessing are rejected.
- Require `query` to be side-effect-free. It may inspect the immutable
  operation, input, digest, target idempotency key, uncertain attempt/lease,
  and Authority, but it must not create, retry, compensate, or mutate the
  external Effect.
- Capture Connector descriptors exactly once under panic isolation and route
  only through frozen metadata.
- Install a default-deny pre-query Policy. Policy failure, panic, invalid
  denial evidence, timeout, or cancellation performs no query and no durable
  mutation.
- Revalidate the complete custom Coordinator page for bounds, continuation,
  order, tenant, revision, aggregate validity, and `unknown` state before
  starting any worker.
- Bound Policy and lookup duration independently, isolate panic/error/drop,
  propagate cooperative cancellation, and restore stable source order after a
  bounded concurrency window.
- Accept `Applied` only with a valid content-free receipt. Accept
  `NotApplied` only with a bounded reason and optional bounded retry delay.
  `StillUnknown`, malformed evidence, error, panic, timeout, cancellation, or
  clock failure leaves the Ledger unchanged in `unknown`.
- Derive reconciliation command identity from cycle, trusted actor and tenant,
  Effect identity, observed revision, uncertain attempt/lease, outcome kind,
  and exact evidence digest. Settle only through existing
  `ReconcileApplied` or `ReconcileNotApplied` revision CAS.
- Exact repeated settlement is recognized as duplicate. A different concurrent
  settlement is fenced. Durable Effect state always wins over a worker report.
- Reports contain only identities, revisions, fences, reason codes, and
  absolute retry time. They omit Effect input, idempotency key, receipt,
  Provider response, credentials, and diagnostics.

## Bounds

- One sweep inspects 1–256 unknown Effects.
- One Reconciler admits 1–64 concurrent Policy/lookup attempts.
- One registry contains at most 256 Connectors, each with 1–256 exact
  operations and at most 64 KiB of encoded descriptor metadata.
- Policy deadlines range from 1 millisecond through 60 seconds.
- Lookup deadlines and Connector-selected retry delays are bounded at 7 days.
- Existing Effect Ledger identity, input, receipt, transition, and aggregate
  bounds remain authoritative.

## Failure classification

| Boundary | Evidence | Durable action |
|---|---|---|
| before query | missing Connector, Policy denial/failure, cancellation | none |
| query error, panic, timeout, or cancellation | no authoritative target fact | remain `unknown` |
| invalid Connector evidence | no valid settlement fact | remain `unknown` |
| valid `StillUnknown` | target cannot prove either outcome | remain `unknown` |
| valid `Applied` | bounded authoritative receipt | `ReconcileApplied` |
| valid `NotApplied` | authoritative absence | reject or explicit retry |
| exact settlement replay | same command already durable | report duplicate |
| settlement CAS loss | another revision won | report fenced; durable state wins |
| worker panic | phase cannot be safely inferred | content-free failure; durable state wins |

## Consequences and non-claims

Unknown external Effects now have a reusable, governed convergence path without
adding Channel, payment, notification, or other business behavior to Core. A
service timer, queue consumer, CLI, test harness, or distributed scheduler may
invoke bounded sweeps.

API 1 deliberately permits duplicate target queries. Correctness depends on the
registered Connector honoring its authoritative read-only contract; Y-Harness
cannot prove that an arbitrary implementation is truthful or side-effect-free.
The API does not persist cadence or backoff, provide multi-node query ownership,
inject credentials, sandbox Connectors, verify receipt truth, compensate an
applied Effect, or expose a new Protocol command. Those remain embedding-host
and deployment responsibilities.

## Rejected alternatives

- Retry the original operation while uncertain: this can duplicate external
  side effects.
- Treat an opaque lookup error as not applied: absence of evidence is not
  evidence of absence.
- Add a durable reconciliation lease in schema 1: it creates another recovery
  lifecycle without improving safety for genuinely read-only queries.
- Put a permanent reconciliation poller in Core: cadence, topology, shutdown,
  and availability belong to the host.
- Persist `StillUnknown` on every sweep: repeated no-change observations would
  consume the finite transition budget without changing authoritative state.
- Include Provider bodies or receipts in reports: ambient telemetry is not a
  safe evidence store.
