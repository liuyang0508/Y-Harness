# ADR 0138: Dispatch-time Effect command digest locks

- Status: accepted
- Date: 2026-07-30

## Context

The reference Effect consumer freezes configuration at startup but previously
accepted a mutable command path. MCP's optional command SHA-256 is intentionally
only startup drift detection. Reusing that check would allow a long-running
service to execute different command bytes after startup while still appearing
locked.

Copying a command into a content-addressed directory is not an atomic execution
measurement. A process with the same operating-system authority can replace or
modify that copy, and relocation can change interpreter, sibling-file, or
dynamic-library behavior. Y-Harness must improve operational integrity without
claiming a stronger boundary than it enforces.

## Decision

- Add `ProcessExecutableIntegrity` to `ProcessBrokerDescriptor`. Historical
  descriptors default to `unmeasured`; a digest wrapper reports the exact
  `dispatch_sha256` value it enforces.
- Add `DigestLockedProcessBroker`, bound to one absolute command path and one
  lowercase SHA-256:
  - construction performs a bounded initial measurement;
  - the path must be a regular file without a symlink;
  - each command is limited to 256 MiB;
  - every request must select that same path;
  - every dispatch remeasures the file before delegating;
  - measurement shares the request's existing cancellation token and total
    queue-plus-execution deadline;
  - measurement time is subtracted before the delegate receives the request.
- Add optional `command_sha256` to the shared reference-service one-shot JSON
  process configuration. When present, assembly wraps the selected unrestricted
  or sandbox broker before any mapped environment values are acquired.
- Require `command_sha256` for every configured Effect execution and
  reconciliation Connector. A missing, malformed, mismatched, oversized,
  symlinked, or drifted command fails closed before child-process entry.
- Keep Effect settlement semantics unchanged. An execution-side measurement
  failure after Claim becomes `unknown`; a reconciliation-side failure leaves
  the Effect unknown. No drift failure is converted into a retry of an external
  mutation.
- Keep State, Effect Ledger, Protocol, JSON Effect wire, and service config
  schema coordinates unchanged. The configuration field is additive for other
  one-shot commands and mandatory only inside the new pre-1.0 Effect consumer.

## Consequences and non-claims

A reviewed Effect command cannot drift between ordinary service dispatches
without detection. Restoring the exact bytes lets later dispatches recover
without restarting the service. The frozen Broker descriptor also lets an
embedding host distinguish measured from unmeasured execution.

This is not an atomic bind between measurement and the operating-system exec.
It does not cover a shebang interpreter, arguments that name scripts, dynamic
libraries, package siblings, target credentials, or a hostile same-authority
filesystem race after measurement. A production deployment still needs a
trusted artifact installation path, operating-system containment, and
platform-specific signed-code or immutable-image controls appropriate to its
threat model.

Credential custody remains separate. `environment_from_host` is still an exact
cleared-environment projection, not a general Secret manager or per-Effect
short-lived Secret flow. Adding that flow requires an Effect-native Secret
request context instead of inventing fake Thread and Turn identities.

## Rejected alternatives

- Reuse MCP's startup-only check: does not detect drift during a resident
  service lifetime.
- Call the check atomic executable pinning: overstates the operating-system
  guarantee and hides transitive dependencies.
- Copy arbitrary commands into a digest directory: relocation can change
  behavior and same-authority mutation remains possible.
- Put command measurement in `EffectEngine`: mixes deployment artifact policy
  into task-free semantic Core.
- Require locks for every historical JSON command immediately: breaks existing
  Model, Tool, Compactor, Verifier, and Grader configurations without an
  Effect-specific safety reason.

## Evidence

- `execution::digest::tests::digest_locked_broker_remeasures_each_dispatch_and_recovers_after_restore`
- `execution::digest::tests::digest_locked_broker_rejects_invalid_digest_mismatch_and_symlink`
- `reference_cli::service::tests::effect_consumer_requires_explicit_exact_authority_and_bounded_timeouts`
- `configured_effect_consumer_degrades_recovers_stops_and_does_not_replay_terminal_effects`
