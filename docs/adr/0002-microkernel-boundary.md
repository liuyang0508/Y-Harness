# ADR 0002: Microkernel and typed capability boundary

- Status: Accepted
- Date: 2026-07-25

## Decision

The kernel permanently owns:

- identity and lifecycle;
- Thread, Turn, Item, Task, Artifact, and Checkpoint state transitions;
- typed capability registries and version negotiation;
- policy, approval, sandbox selection, secrets references, and risk decisions;
- budgets, cancellation, retry classification, and recovery boundaries;
- ordered runtime events and durable trace semantics.

Capabilities register through separate typed contracts. We will not introduce
a universal `register(anything)` API.

Initial capability families are Model, Tool, Skill, Memory, Context Source,
MCP Adapter, Execution Environment, Evaluator, Grader, and Reporter.

Built-in and external capabilities use the same contracts and collision rules.
A capability cannot silently replace another capability. Origin, version, and
trust metadata are retained at registration and in traces.

Observer hooks cannot change execution. Behavior-changing middleware is a
separate, explicitly privileged contract.

Verification answers whether one run satisfies its completion conditions.
Evaluation compares behavior across datasets, scenarios, versions, and
baselines. They share trace evidence but remain separate layers.

## Trust modes

```text
built-in            → in process
trusted extension   → in process only by explicit operator policy
untrusted extension → supervised process / sandbox boundary
remote capability   → authenticated protocol boundary
```

The first executable slice supports built-ins and trusted in-process test
capabilities only. It deliberately rejects silent name collisions. The
out-of-process broker is required before untrusted executable extensions can
be installed.

