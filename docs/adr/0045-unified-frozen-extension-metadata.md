# ADR 0045: Unified frozen extension metadata

- Status: Accepted
- Date: 2026-07-25

## Context

Model identity and Tool descriptors had explicit panic boundaries, while
Memory, Secret, Verifier, and Grader registries still invoked extension
`descriptor()` methods directly. Process-backed Tool/Model adapters also
re-entered `ProcessBroker::descriptor()` whenever a host inspected isolation.
One panicking extension could therefore unwind startup or operator-facing
introspection despite equivalent metadata already being protected elsewhere.

## Decision

- Own one Kernel helper for synchronous capability metadata capture. It invokes
  extension code inside `catch_unwind` and maps panic to a content-free
  `InvalidCapability`.
- Use that boundary for Model identity and Tool, Memory, Secret, Verifier, and
  Grader descriptors before any registry mutation.
- Capture Process Broker descriptors when constructing JSON command Tool/Model
  adapters, validate the broker and sandbox-mechanism identities, and return a
  frozen clone for later introspection.
- Never format or inspect the panic payload.
- Continue to validate every captured descriptor with its subsystem-specific
  version, name, operation, size, and schema rules.

## Consequences

All current executable extension metadata follows the same fail-closed
boundary. Registration and adapter construction either retain one validated
snapshot or make no mutation. State, Policy, Context, Evaluation, and operator
introspection no longer need to re-enter extension metadata code.

The boundary does not prove that a broker truthfully enforces its claimed
isolation; concrete integration tests and host policy remain required. Rust's
global panic hook also runs before capture and remains a host responsibility.

## Rejected alternatives

- Catch separately in every registry: duplicated wording and subtly different
  payload handling would drift.
- Treat descriptors as harmless getters: trait implementations are arbitrary
  executable code.
- Re-query metadata to detect changes: mutable metadata breaks deterministic
  provenance; lifecycle changes need an explicit versioned replacement flow.
