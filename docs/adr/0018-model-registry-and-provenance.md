# ADR 0018: Model registry and durable output provenance

- Status: Accepted
- Date: 2026-07-25

## Context

The Agent Loop originally accepted one `LanguageModel` object directly. That
was sufficient to prove execution, but did not provide the collision checks,
trust-bearing origin, deterministic discovery, or selection semantics already
used for Tools, Memory providers, Verifiers, and Graders.

The Model and Tool traits also lived in Runtime while their registries belonged
to Kernel, creating a conceptual dependency cycle.

## Decision

- Kernel owns the provider-neutral `LanguageModel` and `Tool` contracts.
- `ModelRegistry` validates portable provider/model identities, rejects
  replacement, preserves `CapabilityOrigin`, and enumerates deterministically.
- Runtime can select a registered model by exact identity. Unknown identities
  return a typed error before execution.
- The direct Runtime constructor is reserved for statically linked built-ins.
  Extension hosts use registry selection.
- Every newly recorded assistant message and model-requested Tool call retains
  the selected model identity and origin in authoritative State.
- Legacy events without model provenance remain readable and represent those
  fields as absent rather than inventing a trusted origin.
- External executable models still cross the Process Broker; registry
  membership does not grant filesystem, network, Tool, or Policy authority.

## Consequences

Hosts can register built-in, trusted in-process, and external model adapters
through one contract while retaining the trust decision. Evaluation and
incident review can attribute model-produced state without relying on
best-effort telemetry.

Model routing, failover, and load balancing remain separate policies. The
registry resolves identity; it does not silently switch providers. ADR 0070
adds an explicit bounded Runtime failover route without changing that registry
boundary.

## Rejected alternatives

- Global mutable model registries: introduce hidden process state and make
  tests and embedding nondeterministic.
- Replace an existing identity during registration: makes one configuration
  mean different code depending on load order.
- Infer origin from the model implementation type: Rust trait objects do not
  carry an operator trust decision.
- Record provenance only in Observability: telemetry is best-effort and is not
  the authoritative execution journal.
