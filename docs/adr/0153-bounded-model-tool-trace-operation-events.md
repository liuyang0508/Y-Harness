# ADR 0153: bounded Model Tool Trace operation events

- Status: accepted
- Date: 2026-08-03

## Context

Protocol v34 made process admission observable, but a client still could not
distinguish the Tools actually advertised to one Model attempt from the Tools
later proposed by the Provider. Showing only durable Tool calls also hid failed
attempts, route failover, Tool-choice policy, and assistant text that merely
resembled serialized Tool syntax.

Putting complete schemas, prompts, credentials, or raw Provider payloads into a
client event would create a second sensitive trace authority. An unbounded Tool
list would also let optional diagnostics exhaust the retained operation buffer.

## Decision

1. Advance the exact client protocol to v35 and add two provisional operation
   events around every Runtime-owned Model attempt:
   `tool_trace_request` and `tool_trace_response`.
2. Request evidence contains the one-based Model step and attempt, registered
   Model route identity, SHA-256 of the exact provider-neutral `ModelRequest`,
   effective Tool-choice policy, total advertised descriptor count, and at most
   64 credential-free Tool names plus an explicit truncation flag.
3. Response evidence contains duration, a bounded settlement class, structured
   Tool-call count, a boolean Tool-syntax-in-text diagnostic, and only already
   governed Provider identity/failure metadata. It contains no prompt, Tool
   arguments, Tool results, schema bodies, headers, credentials, or arbitrary
   failure text.
4. Tool Trace events share the existing per-Operation ordered event buffer,
   count/byte limits, paging, eviction marker, authorization, and retention
   lifecycle. They are provisional diagnostics; authoritative Model decisions,
   Tool calls, Policy decisions, and Tool results remain in State.
5. Runtime Catalog MCP entries add only credential-free transport, endpoint,
   enabled state, and the namespaced Tools actually registered in the active
   host generation. This projection grants no discovery or execution authority.

## Consequences

- TUI and future clients can render the real Model/Tool contract and attempt
  settlement without importing Provider implementations.
- Route retry/failover attempts remain individually visible while one eventual
  durable decision stays authoritative.
- Loss or eviction of Tool Trace events does not invalidate a Turn or its State.
- Protocol v35 changes no durable State, Approval, Task, Workflow, Handoff, or
  Effect schema.

## Non-claims

- A request SHA-256 is not a persisted Capability generation or per-step
  Capability View receipt.
- Tool Trace does not expose hidden reasoning, full HTTP payloads, Tool schema
  bodies, or credentials.
- MCP Runtime Catalog metadata is not runtime progressive discovery and does not
  let a client register or invoke a Tool.
