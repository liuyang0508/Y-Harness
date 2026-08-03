# ADR 0150: Authoritative Protocol service admission status

## Status

Accepted.

## Context

`initialize` proved protocol compatibility and `doctor` proved that a new
process could preflight configuration and stores, but neither represented the
current admission state of an already running host. A deployment adapter could
infer liveness from a connection, yet it could not distinguish a host that was
ready for another Turn from one whose finite Operation registry was full or
whose one-way shutdown drain had begun.

Adding an HTTP-only boolean would create a second state authority outside the
typed Engine boundary. Polling Providers, MCP servers, or Effect targets in a
generic health command would also conflate process readiness with the health
of optional external dependencies and could expose tenant or credential data.

## Decision

- Advance the exact client protocol to `34` and add the independently
  authorized `service.status` capability plus `get_service_status` command.
- Return one bounded, content-free `service_status` result containing:
  `admission`, `running_operations`, `retained_operations`, and the configured
  `operation_retention_limit`.
- Derive `admission` under the same authoritative Handler state used by Turn
  admission:
  - `ready` while the host accepts Turns and one retention slot remains;
  - `at_capacity` while the host is live and accepting in principle but every
    slot is occupied; and
  - `draining` after the one-way shutdown transition.
- Keep the command available during drain so an authenticated supervisor can
  observe that the host is live but no longer ready.
- Treat a successful status response as Protocol-process liveness. Only
  `ready` establishes readiness for one additional Turn. The result makes no
  claim about Model, MCP, Memory, Registry, Effect target, network, or other
  external dependency health.
- Keep the status API on `ProtocolHandler` so future HTTP/Kubernetes, API, GUI,
  or sidecar adapters translate the same projection instead of opening Engine
  stores or maintaining independent booleans.
- Require explicit `service.status` authorization for remote principals. Counts
  are host-wide operational metadata, so a multi-tenant gateway must grant the
  permission only to an appropriate operator principal.

## Consequences

Deployment clients can now distinguish live, ready, saturated, and draining
hosts without inferring state from errors. Status reads allocate no unbounded
collections, perform no external calls, mutate no state, and disclose no
Thread, tenant, prompt, Tool, or credential identities.

Protocol v33 clients fail exact negotiation and must upgrade. This slice does
not add an HTTP listener, Kubernetes probe endpoint, dependency health graph,
multi-node load signal, or durable service-health history. Those belong in
independent deployment/observability adapters over this typed source.
Follow-up [ADR 0151](0151-bounded-http-deployment-probes.md) adds the optional
bounded HTTP translation while retaining this authority boundary.

## Rejected alternatives

- Reuse `initialize` as readiness: compatibility remains valid during drain or
  capacity exhaustion.
- Add an unauthenticated HTTP `/health`: duplicates lifecycle state and weakens
  the existing authenticated protocol boundary.
- Probe every configured dependency: turns optional remote outages into a
  generic host-liveness failure and risks leaking operational topology.
- Report only `healthy: true/false`: hides whether operators should drain,
  forget terminal Operations, or investigate process failure.

## Verification

- Handler tests cover `ready`, exact retention saturation, successful
  cancellation, and persistent `draining` after one-way shutdown.
- Wire tests bind `get_service_status`, `service.status`, result shape, and
  Protocol v34.
- A real configured `yh serve` process returns `ready` with zero retained and
  running Operations after its durable stores and capabilities have opened.
