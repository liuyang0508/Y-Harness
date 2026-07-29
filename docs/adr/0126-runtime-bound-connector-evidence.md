# ADR 0126: Runtime-bound Connector evidence

- Status: accepted
- Date: 2026-07-29

## Context

Enterprise workflows need to distinguish model text and ordinary Tool output
from facts observed in an authoritative source system. A JSON convention such
as `_authoritative: true` is forgeable, has no registered implementation
identity, and can drift away from the exact output it supposedly supports.
Putting source credentials, business fields, or vendor-specific records in
Core would also violate the Harness/application boundary.

The existing chain already records a Tool call, its Policy decision with exact
registered origin, and its result. The missing contract is a small,
provider-neutral way for an in-process Connector Tool to report source
provenance while leaving authority binding to the Harness.

## Decision

`Tool::execute_with_evidence` is an additive method with an ordinary-Tool
default. An evidence-aware Connector may return a `ToolExecutionResult`
containing its structured output and at most 64 validated claims. A claim
contains only:

- a portable source coordinate;
- an opaque bounded resource locator and exact source revision;
- a non-zero observation time and optional freshness boundary; and
- an optional bounded source idempotency key.

A claim is not authority. Runtime binds it to:

- the exact registered Tool name and `CapabilityOrigin`;
- the trusted `AuthorityContext` used for execution; and
- SHA-256 of the exact validated structured output.

The bound records are stored in the same `ToolResult` event as the output.
Invalid or failed Tool results retain no Connector evidence. State validates
record bounds, uniqueness, output digest, Thread tenant, and the preceding
ToolCall→Policy-origin chain on append and projection. Model-visible Context
reconstructs `ToolResult` without Connector evidence; Evaluation, State,
Protocol observers, and audit export may inspect the durable record.

State event and snapshot schemas advance from 13 to 14. Thread archive format
advances from 3 to 4 and refuses tenant rebinding when either execution
binding or Connector evidence carries a different tenant. Protocol advances
from 25 to 26 only to advertise State/snapshot schema 14; it gains no command
that authors Connector evidence.

MCP, JSON-command, and ordinary Tools retain the compatibility default. Their
JSON output cannot self-elevate into Connector evidence. A host that wants an
external endpoint to become an authoritative Connector must install a trusted
adapter implementing the typed in-process contract.

## Consequences

- Core remains business- and vendor-neutral.
- Output and evidence cannot be partially committed.
- Registered origin, tenant, and output tampering fail closed after restart,
  snapshot recovery, or archive import.
- Privileged provenance cannot accidentally become model instructions.
- Existing Tool implementations require no changes.
- Evidence establishes provenance and freshness, not truth by fiat; domain
  Policy and Evaluation still decide whether a source/revision is sufficient.
- Schema-1 through schema-13 SQLite stores require the existing backup-first
  migration before a schema-14 writer opens them.

## Rejected alternatives

- Trusting magic JSON fields: forgeable and not registry-bound.
- Making every Tool authoritative: destroys the distinction the feature needs.
- Adding CRM/order/customer fields to Core: leaks application semantics into
  the Harness.
- Storing evidence in a second event: permits output/evidence partial commit
  and complicates recovery.
- Sending bound evidence to the Model: increases prompt-injection and
  authority-confusion risk without helping Tool-result reasoning.
