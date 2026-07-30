# ADR 0140: Secret-gated Effect command integrity preflight

- Status: accepted
- Date: 2026-07-30

## Context

ADR 0138 makes the reference Effect command digest mandatory and remeasures it
inside `DigestLockedProcessBroker` before delegating to a child launcher. ADR
0139 resolves Effect credentials immediately before entering that Broker.

That order prevented a drifted executable from starting, but a drift already
present at call entry could still cause one Secret Provider lookup before the
Broker rejected it. Moving all Secret resolution into every Process Broker
would enlarge the public Broker contract, duplicate authority logic, and still
would not create an atomic executable-to-`exec` binding across platforms.

## Decision

- A JSON-command Effect Connector with `secret_environment` now requires its
  frozen `ProcessBrokerDescriptor` to advertise
  `DispatchSha256 { sha256 }`. Unmeasured commands fail configuration before a
  Provider can be attached.
- Every credential-bearing execution and reconciliation performs two bounded
  measurements of the exact command:
  1. the adapter preflights the frozen descriptor digest before any per-dispatch
     Secret Provider request;
  2. the existing `DigestLockedProcessBroker` remeasures the command before
     delegating to the child launcher.
- The first measurement, every Provider lookup, JSON encoding, the second
  measurement, process queue, and execution share the configured process
  deadline. Cancellation wins before either measurement or Provider future.
- An already drifted command therefore causes no per-dispatch Provider lookup
  and no child entry. If bytes drift after the preflight, the second
  measurement still prevents child entry.
- Non-credential Effect commands preserve their previous single Broker
  measurement and timeout semantics.
- Provider and digest diagnostics remain content-free at the public Effect
  boundary.

## Consequences and non-claims

- Credential issuance is no longer attempted for steady-state command drift.
- There remains a time-of-check/time-of-use window between the first
  measurement, Provider lookup, second measurement, and OS process creation.
  A change racing after the first measurement can still cause a Provider audit
  or issuance before the second measurement rejects it.
- Even the second measurement is not an atomic file-descriptor-to-`exec`
  binding. Symlink and regular-file checks plus exact bytes reduce drift risk;
  they do not certify loaded transitive libraries, interpreters, scripts,
  kernel state, or the target SDK.
- A custom Process Broker descriptor is trusted evidence. A dishonest broker
  can claim a digest it does not enforce; the adapter's own first measurement
  still applies, but Engine code cannot prove the broker's second gate.
- A future platform-specific prepared-executable contract may close more of the
  race, but it must define cancellation, concurrency permits, sandbox wrapping,
  file-descriptor lifetime, interpreter behavior, and Windows/macOS/Linux
  parity before entering the public API.

## Rejected alternatives

- Keep one Broker-only measurement: blocks child entry but queries the Provider
  for drift already present at call entry.
- Resolve credentials during service assembly: defeats per-dispatch lifetime,
  rotation, and exact Effect audit context.
- Add an optional preflight without requiring measured integrity: silently
  gives credential-bearing unmeasured Connectors a weaker contract.
- Claim the two measurements are atomic launch integrity: the OS boundary and
  transitive artifact graph are not bound by this implementation.

## Evidence

- `execution::effect::tests::executable_drift_is_rejected_before_secret_provider_resolution`
- `execution::effect::tests::secret_provider_resolution_shares_the_process_deadline`
- `execution::effect::tests::secret_environment_uses_typed_effect_context_and_never_enters_json`
- `execution::effect::tests::secret_resolution_failure_and_precancellation_block_process_entry`
- `execution::digest::tests::digest_locked_broker_remeasures_each_dispatch_and_recovers_after_restore`
- `configured_effect_consumer_degrades_recovers_stops_and_does_not_replay_terminal_effects`
