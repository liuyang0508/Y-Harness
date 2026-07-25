# ADR 0055: Bounded capability registries

- Status: Accepted
- Date: 2026-07-25

## Context

Capability registries rejected duplicate identities and validated individual
names, but most accepted an unbounded number of entries. Operator-assigned
extension-origin strings were retained without a common size rule. Tool
descriptors could also be accepted at registration and make every later Model
request fail only when its complete JSON exceeded the Runtime boundary.

The Skill Registry bounded each package but not the sum of retained package
content. `TraceCollector` rejected zero capacity but could attempt an
arbitrarily large caller-directed allocation. MCP discovery duplicated its
returned catalog into staging collections before a common registry ceiling was
applied.

## Decision

- Validate every registered `CapabilityOrigin`: extension identities contain
  1–256 non-control bytes.
- Give mutable capability registries a shared maximum of 4,096 entries unless a
  subsystem already has a tighter bound, such as 64 Evaluation graders.
- Check capacity before invoking extension metadata whenever the registration
  API permits it.
- Bound one serialized Tool descriptor at 1 MiB and total Tool Registry
  descriptor metadata at 8 MiB. Count bytes with a bounded streaming serializer
  rather than allocating a second complete JSON value.
- Validate Tool schemas iteratively before serialization, with a depth limit of
  64 and a 65,536-node limit, including the traversal worklist itself.
- Preserve atomic Tool batch registration: origin, count, descriptor, collision,
  and aggregate-byte checks all complete before publication.
- Bound Memory Provider and Verifier descriptions at 4 KiB.
- Bound aggregate Skill package content retained by one registry at 64 MiB, in
  addition to existing per-package and canonical-encoding bounds.
- Preflight MCP catalog and requested-Skill counts before cloning or staging
  them.
- Bound a `TraceCollector` to 1–65,536 records and reject oversized observation
  identity metadata before observer delivery or retention.

## Consequences

Configuration and discovery fail at their authority boundary instead of
creating a registry that fails every later Turn or attempts a pathological
secondary allocation. Registration remains deterministic and failure-atomic.
The fixed ceilings provide an auditable pre-1.0 baseline; future configurability
must retain hard maxima and cannot weaken serialized request limits.

Registry limits account for metadata and Skill package content owned by
Y-Harness. They cannot account for arbitrary heap state privately retained
inside a host-supplied in-process implementation. Such implementations remain
trusted extensions, while untrusted executable capabilities belong behind the
out-of-process Tool/Process Broker boundary.

## Rejected alternatives

- Rely only on process memory limits: embedded deployments need deterministic
  API-level rejection and may share a process with unrelated workloads.
- Check complete Model requests only at execution: a valid-looking registry
  could make every Turn fail after State mutation.
- Serialize Tool descriptors into a temporary `Vec` to measure them: this would
  duplicate the exact potentially oversized allocation being rejected.
- Make all ceilings unlimited by default and operator-configurable: that turns
  omission or parsing mistakes into memory-exhaustion authority.
