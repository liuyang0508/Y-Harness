# ADR 0151: Bounded HTTP deployment probes translate Protocol admission

## Status

Accepted.

## Context

Protocol v34 made process liveness and new-Turn admission authoritative, but a
Kubernetes probe cannot speak the typed JSONL protocol directly. Reimplementing
readiness in the reference host, opening SQLite from a sidecar, or probing every
Model and Connector would create a second and contradictory health authority.

The endpoint is normally unauthenticated inside a pod or private service
network, so it must disclose no tenant, Thread, prompt, capability, credential,
or dependency topology. It also needs finite connection, framing, time, and
shutdown bounds; a health endpoint must not become an unbounded slow-client
attack surface.

## Decision

- Add the optional `http-probe` Cargo feature and keep its implementation in
  the transport adapter module, outside Agent Loop and State semantics.
- Accept exactly one request per TCP connection and only these routes:
  - `GET /livez`: `200` whenever the authoritative status source answers,
    including `at_capacity` and `draining`;
  - `GET /readyz`: `200` only for `ready`, otherwise `503`.
- Obtain both results from `ServiceStatusSource`. `ProtocolHandler` implements
  that port by returning the same `service_status` projection used by Protocol
  v34; the adapter never reads durable stores or calls external dependencies.
- Return small, fixed, plain-text bodies. Unknown routes, unsupported methods,
  malformed framing, non-zero request bodies, oversized headers, source
  failures, and timeouts fail closed with bounded HTTP responses where the
  socket remains writable. Every response disables caching and closes the
  connection.
- Bound simultaneous connections, request read/write time, status time, header
  bytes, and graceful connection drain. Isolate connection task panics and
  return content-free shutdown counters.
- Let the embedding host own lifecycle. Stopping a probe must not stop the
  Engine. The optional reference-service `http_probe` configuration binds the
  adapter to the exact in-process `ProtocolHandler`; omission preserves the
  zero-listener default, and a binary without the feature rejects the setting
  during configuration loading. The reference host begins the Handler's
  irreversible drain before stopping its optional background services, so the
  still-live probe stops admitting work throughout that shutdown interval.
- Default examples bind literal loopback. Binding a non-loopback address is an
  explicit operator decision; this adapter supplies neither TLS nor client
  authentication and must remain behind the deployment network boundary.

## Consequences

Kubernetes, container, and local supervisors can consume conventional probes
without duplicating Engine lifecycle state. Saturation removes a pod from new
work while keeping liveness true; one-way drain makes readiness false without
misreporting process death.

The endpoints do not certify Model reachability, MCP health, database replica
freshness, Effect-target truth, multi-node quorum, or end-to-end SLOs. Those are
separate dependency and observability signals. This decision also does not add
a general HTTP Agent API, metrics endpoint, service discovery, TLS termination,
or orchestration manifest.

## Rejected alternatives

- Read SQLite from the probe: process-local Operation capacity and drain state
  are not durable-store facts.
- Make `/livez` fail at capacity: Kubernetes would restart a live saturated
  process and amplify load.
- Call every dependency from `/readyz`: an optional downstream outage would
  erase the more precise admission fact and could leak topology.
- Add an HTTP boolean inside the Agent Loop: transport concerns would become
  Core semantics and independent clients could disagree.
- Use an HTTP framework for two fixed routes: it adds a larger dependency and
  configuration surface without improving this deliberately narrow adapter.

## Verification

- Adapter tests cover all three admission states, liveness/readiness mapping,
  source failure, malformed method/path/body handling, strict header bounds,
  configuration limits, and graceful report settlement.
- A real all-feature `yh serve` child binds the configured endpoint, returns
  live and ready from its in-process Handler, and exits cleanly after stdin EOF.
- Zero-default builds retain no listener implementation and reject configured
  probe use instead of silently ignoring it.
