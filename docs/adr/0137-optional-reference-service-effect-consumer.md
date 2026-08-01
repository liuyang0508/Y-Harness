# ADR 0137: Optional reference-service Effect consumer lifecycle

- Status: accepted
- Date: 2026-07-30

## Context

The Effect Ledger, Governed Effect Executor, authoritative Reconciler, and
brokered JSON-command adapters form a complete embedded execution boundary, but
they deliberately start no background task. A deployable reference service
needs an operator-controlled lifecycle that visits durable pending and unknown
Effects without moving polling, credentials, or target-specific behavior into
Core.

Registration alone cannot authorize an external action. Execution and
reconciliation also have different safety contracts: execution may mutate a
target only after a durable Claim, while reconciliation must be an
authoritative read-only query. One combined Connector list or implicit
registration-derived Policy would erase that distinction.

## Decision

- Add an optional strict `effect_consumer` object to service configuration
  schema 1. Omitting it starts no consumer. An object with neither `execution`
  nor `reconciliation` is invalid.
- Configure execution and reconciliation independently. Each mode owns:
  - a 100–86,400,000 ms poll interval and failure backoff;
  - its existing bounded Executor or Reconciler configuration;
  - a non-empty exact capability/operation allowlist;
  - a non-empty mode-specific JSON-command Connector registry.
- Registration and authorization remain separate. Every allow entry must match
  one exact configured Connector operation; duplicates, wildcards, fallback,
  missing Connectors, and unsupported entries are rejected. Registered
  operations not present in the allowlist remain inert.
- Execution Connectors declare target- or Connector-enforced idempotency.
  Reconciliation Connectors separately acknowledge the
  `authoritative_read_only` contract. They cannot substitute for one another.
- Connector API versions are host-owned constants rather than configurable
  claims. Every Connector declares a trust-bearing `origin_id` separately from
  its routing capability. Process command, working directory, environment projection,
  isolation, concurrency, input/output, and timeout continue through the
  existing `ProcessBroker`.
- ADR 0138 subsequently requires a dispatch-time command SHA-256 lock for every
  configured execution and reconciliation Connector.
- Require each process timeout to fit inside its enclosing execution or lookup
  timeout. Existing Executor lease validation independently requires enough
  post-execution settlement reserve.
- `yh doctor` validates store readiness before constructing any external
  capability, then validates Effect commands, environment availability,
  registries, exact Policy coverage, and timeout relationships without
  creating a store or starting a loop.
- `yh serve` uses the same immutable fixed Authority as Protocol requests. It
  starts execution and reconciliation as independent tasks, so a slow
  read-only lookup cannot block execution cadence or vice versa.
- Each loop retains only a disposable page cursor and monotonic process-local
  cycle counter. Restarting loses both safely: pending Claim CAS prevents
  duplicate Connector entry, and reconciliation queries are repeatable under
  their read-only contract.
- Missed interval ticks are skipped. A failed sweep, or a page where every
  attempted operation is unavailable, applies the configured process-local
  backoff. Durable eligibility and truth remain in the Ledger.
- Diagnostics are stderr-only, content-free health transitions. They expose a
  fixed mode and count, never Effect input, idempotency keys, receipts,
  process output, environment values, or Provider errors.
- On service EOF, stop Effect admission and cooperatively cancel/wait for
  in-flight sweeps before Temporal, Protocol, and MCP shutdown. A 30-second
  bound converts a stuck loop into an explicit service failure.

## Consequences and non-claims

The reference service can now consume configured durable Effects without
compiling target adapters into Rust. Core remains task-free, and GUI, TUI, API,
or other clients still interact only through the typed Protocol.

Multiple service processes may inspect the same pending page. The Ledger's
revision CAS and deterministic Claim identity ensure only a committed fresh
Claim enters execution. Reconciliation may perform duplicate target reads by
design; only exact settlement CAS mutates the Ledger.

This lifecycle does not prove that a Connector is honest, make an unrestricted
process a sandbox, provide dynamic config reload, distribute rate limits, own a
Secret manager, verify business receipts, or provide a durable per-target
circuit breaker. A fixed-tenant deployment still needs tenant-appropriate
credentials and Connector behavior. Operators must evaluate and sandbox each
external command. The ADR 0138 command-file measurement is not an atomic OS exec
binding or transitive dependency proof. ADR 0139 subsequently adds optional
typed per-dispatch Secret resolution to this reference lifecycle; it does not
turn the service into a vault, rotation controller, or proof of Connector
honesty. ADR 0140 subsequently requires credential-bearing commands to pass a
digest preflight before Provider resolution and retain the Broker's second
measurement before child entry, without claiming an atomic OS exec binding.
ADR 0141 subsequently adds durable fixed-window and circuit governance for a
trusted `tenant + capability + operation + policy_id` execution lane. It does
not reinterpret arbitrary Effect input as a finer recipient, account, endpoint,
or other business target.

## Rejected alternatives

- Put a poller inside `EffectEngine`: embeds deployment and task ownership in
  Core and complicates deterministic embedding.
- Infer Policy from Connector registration: turns availability into authority.
- Reuse one Connector registry for both modes: weakens the read-only
  reconciliation boundary.
- Persist a second service cursor database: creates another recovery authority
  without improving Ledger correctness.
- Log Connector errors for convenience: may disclose target bodies, secrets,
  or immutable Effect requests.
- Retry missed ticks in a burst: can amplify an outage and defeat configured
  concurrency bounds.
