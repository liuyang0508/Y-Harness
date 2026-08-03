# ADR 0154: frozen Tool Capability View execution fence

- Status: accepted
- Date: 2026-08-03

## Context

The Tool Registry is the complete executable catalog for one Runtime. Until now,
every Model step received every registered descriptor, so membership in the
registry and disclosure to the Model happened to be identical. Progressive
disclosure cannot safely be added by filtering `ModelRequest.tools` alone: a
malformed or adversarial Provider could still name a hidden-but-registered Tool,
and the Runtime would resolve it from the global Registry before Policy and
execution.

Capability discovery is a Context and decision-quality concern. Execution
authority remains a deterministic Runtime concern, independent of what the
Model claims it saw.

## Decision

1. Every `HarnessRuntime` owns one immutable `ToolCapabilityView` containing an
   ordered descriptor snapshot and exact name-membership set. The default view
   exposes all registered Tools and preserves existing behavior.
2. Embedded hosts may use `with_model_visible_tools` to freeze an exact
   registered subset, including an empty text-only view. Unknown and duplicate
   selections fail configuration before a Turn starts. Selection never registers
   a Tool, changes Policy, or grants execution authority.
3. Every Model request uses descriptors from that frozen View. The Runtime
   validates every returned Tool call against the same View before recording a
   Tool-call decision, invoking Policy, requesting Approval, or executing any
   Tool. A same-response batch is rejected atomically when any call is hidden.
4. Authorization, sequential execution, and parallel execution repeat the View
   membership fence before resolving the executable from the Registry. The
   Registry remains the source of implementation, origin, cancellation, and
   batch-safety metadata after disclosure membership succeeds.
5. Approval continuation reconstructs the original Model request with the
   active frozen View. A Tool absent from the View or a changed request digest
   fails closed before approval settlement can reach execution.

## Consequences

- Model visibility is no longer equivalent to executable registration.
- A hidden registered Tool cannot bypass progressive disclosure through a raw
  Provider response, including in a multi-call batch or approval continuation.
- The exact serialized Model-request SHA-256 continues to bind approval resume
  to the descriptor snapshot without a durable-schema or Protocol change.
- Future Coordinator, Skill search, and MCP discovery work can replace the
  static projection strategy while reusing the execution fence.

## Non-claims

- The first slice is a Runtime-generation-static View, not intent-aware or
  per-step dynamic projection.
- It does not yet provide a complete `CapabilityGeneration` digest covering
  Model route, Tool origins/schemas, Skill locks, MCP catalogs, and Policy.
- It does not implement Tool search, MCP `list_tools` on demand, Skill retrieval,
  or automatic cluster selection.
- A View is not authorization. Every disclosed Tool still passes ordinary
  Policy, Approval, Authority, cancellation, and execution governance.
