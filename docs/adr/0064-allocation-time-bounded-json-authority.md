# ADR 0064: Allocation-time bounded JSON authority

- Status: Accepted
- Date: 2026-07-25

## Context

Subsystems had hard encoded-byte ceilings, but several public and extension
paths measured a `serde_json::Value` by first serializing it into a complete
temporary `Vec`. The limit therefore rejected oversized data only after the
engine had already made the allocation it was intended to prevent. Deep or
extremely wide caller-owned values could also drive recursive serialization or
an unbounded traversal worklist before a byte check.

Transport frame limits alone do not close this gap. Embedded callers, custom
Evaluation targets, Event Store implementations, MCP clients, command
adapters, and durable records can reach engine APIs without entering through
one network frame.

## Decision

- Use one crate-internal JSON authority utility for iterative
  `serde_json::Value` shape validation and bounded streaming serialization.
- Admit at most 64 nested JSON container levels and 65,536 JSON value nodes.
  Check a container's complete child count before extending the iterative
  worklist, so validation cannot allocate a caller-sized pending vector first.
- Measure encoded size with a counting `Write` implementation that stops on
  the first byte beyond the subsystem ceiling.
- Materialize JSON only through a bounded `Write` implementation whose initial
  capacity is capped and whose buffer never crosses the declared ceiling.
- Validate every caller/provider-controlled `Value` before invoking Serde
  serialization. Typed collections retain their independent domain count and
  byte limits; the JSON node ceiling does not replace them.
- Apply the rule at Approval submission and migration, Tool registration and
  execution, Runtime Model requests and responses, State events and snapshots,
  Context history/compaction, MCP schemas/arguments/results, CLI adapters,
  Evaluation inputs/results, and trace export.
- Keep raw transport, process-output, HTTP-body, SQLite-text, and protocol-frame
  ceilings. They bound the first materialization from external bytes; the
  shared JSON rules bound subsequent structural traversal and retained
  encoding.

This is a fail-closed validation tightening. It changes neither durable schema
coordinates nor protocol versions.

## Consequences

An oversized, deep, or wide value fails before State mutation, external process
start, MCP registration/call, approval submission, or trace growth, depending
on the owning boundary. Exact byte limits remain subsystem-specific while
structural complexity has one reviewable baseline.

Bounded serializers can perform partial writes into their private temporary
buffer before returning an error. Callers must not publish that buffer until
serialization succeeds. The provided helper returns no buffer on failure.

The rule bounds engine-owned secondary allocation. It cannot recover memory
already allocated by a trusted in-process caller to construct a `Value`, nor
does it substitute for host/process memory limits. Untrusted executable
extensions still belong out of process.

## Rejected alternatives

- Serialize with `serde_json::to_vec` and check `len()` afterward: the rejected
  allocation has already happened.
- Enforce only encoded bytes: tiny JSON syntax can still describe excessive
  structural work and recursive depth.
- Enforce only depth: one shallow array or object can still create a huge
  worklist.
- Rely only on transport limits: embedded and durable-provider paths bypass a
  transport, and later serialization can expand escaped content.
- Make limits caller-configurable without hard maxima: a missing setting would
  restore memory-exhaustion authority.
