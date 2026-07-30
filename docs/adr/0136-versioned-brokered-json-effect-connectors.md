# ADR 0136: Versioned brokered JSON-command Effect Connectors

- Status: accepted
- Date: 2026-07-30

## Context

Governed Effect Executor API 1 and Reconciler API 1 define safe embedded
dispatch and convergence boundaries, but requiring every target adapter to be
compiled into a Rust host would make operational extension expensive. A
shell-free external command is a useful lowest-common-denominator adapter for
existing SDKs and private integrations.

Direct process spawning would bypass the existing Process Broker controls.
An unversioned JSON body would also permit silent request or settlement drift.
Execution and reconciliation must remain distinct: the latter asserts an
authoritative read-only contract and must never become an alternate execution
entry point.

The adapter is a prerequisite for later reference-service configuration. A
polling service without a configured source of target Connectors would be dead
infrastructure, so lifecycle wiring is deliberately not added in this slice.

## Decision

- Add exact JSON Effect Connector protocol 1 with four public strict envelope
  types:
  - `JsonEffectExecutionRequest` and `JsonEffectExecutionResponse`;
  - `JsonEffectReconciliationRequest` and
    `JsonEffectReconciliationResponse`.
- Every stdin request and stdout response carries
  `protocol_version: 1`. Missing, unknown, malformed, or mismatched fields fail
  closed. No protocol guessing or compatibility fallback is allowed.
- Keep the live `CancellationToken` in-process. The JSON request is
  cancellation-free; the exact token is passed separately to `ProcessBroker`.
- Request envelope types intentionally omit `Debug`; serialization exists only
  because the selected command must receive the bounded request.
- Execution requests carry Effect identity, Authority, operation, target
  idempotency key, immutable input and digest, attempt, lease, and lease
  deadline. Reconciliation requests carry the same immutable lookup evidence
  except the obsolete execution deadline.
- Add `JsonCommandEffectConnector` and
  `JsonCommandEffectReconciliationConnector`. Each captures and validates its
  Process Broker descriptor at construction, then exposes the frozen Effect
  descriptor to the existing exact registry for independent validation.
- Reuse `JsonProcessConfig` and `ProcessBroker`: absolute executable and
  working directory, direct arguments without a shell, cleared/replaced
  environment, bounded queue/execution time, bounded stdin/stdout/stderr,
  concurrency admission, process-group settlement where supported, and honest
  isolation metadata.
- Add `ExecutionPhase::Effect` so broker cancellation is classified accurately
  rather than mislabeled as an in-Turn Tool.
- Bound encoded stdin by the existing 1 MiB JSON-command limit. Output is
  bounded by configured process retention and then strictly decoded.
- Never include stderr, stdout bodies, Provider diagnostics, Effect input,
  idempotency keys, or receipts in adapter errors. Process errors become a
  content-free Effect failure except for typed cancellation/timeout, which
  retain `ExecutionPhase::Effect`.
- Return parsed outcomes to the existing Executor/Reconciler. Those layers
  remain responsible for Policy, receipt/reason validation, fail-closed
  post-dispatch classification, Unknown preservation, and durable CAS.

## Wire examples

Execution stdin:

```json
{
  "protocol_version": 1,
  "effect_id": "effect-42",
  "authority": {"actor": {"kind": "local_process"}},
  "operation": {"capability": "notification.send", "operation": "send"},
  "idempotency_key": "target-key-42",
  "input": {"artifact_ref": "message-42"},
  "input_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "attempt": 1,
  "lease_id": "lease-42",
  "lease_expires_at_ms": 2000
}
```

Execution stdout:

```json
{
  "protocol_version": 1,
  "outcome": {
    "outcome": "unknown",
    "reason_code": "provider.timeout"
  }
}
```

Reconciliation uses the corresponding request without
`lease_expires_at_ms`. Its nested outcome is one exact
`applied`, `not_applied`, or `still_unknown` Reconciler outcome.

## Consequences and non-claims

Embedding hosts can implement Effect target adapters in any language that can
read and write one bounded JSON document. The adapter still cannot make an
unrestricted process a sandbox, verify Connector honesty, provision
credentials, or prove a reconciliation command is read-only.

Protocol 1 is an embedded process-wire coordinate. It changes no Effect Ledger
schema, client Protocol v29 command, service configuration schema, or durable
State. The reference service does not yet accept Connector configuration or
start an Effect consumer. Operators must not confuse availability of the
adapter with an installed production lifecycle.

## Rejected alternatives

- Spawn command strings through a shell: reintroduces injection and ambient
  authority.
- Serialize the live cancellation token: it has process-local identity and no
  truthful wire representation.
- Reuse the Tool JSON envelope: Effects have durable attempt, lease,
  idempotency, and reconciliation semantics that ordinary Tool calls do not.
- Use one untagged response shape for execution and reconciliation: it weakens
  the read-only boundary and makes accidental mode substitution harder to
  detect.
- Log command stdout/stderr on failure: external diagnostics may contain
  credentials, request content, or Provider bodies.
- Add service polling in the same change without configured Connectors: that
  would create unreachable lifecycle code rather than a usable extension path.
