# ADR 0039: Supervised protocol Operation tasks

- Status: Accepted
- Date: 2026-07-25

## Context

The typed protocol starts each Turn asynchronously and retains a process-local
`OperationStatus` for polling. The original implementation detached the Turn
task and discarded its `JoinHandle`. Runtime capability panics are isolated at
their governed boundary, but an unexpected panic elsewhere in the Agent Loop
could still terminate the detached task while leaving its Operation permanently
`running`. Clients could neither distinguish that failure nor forget the
record.

## Decision

- Run the Turn in a Tokio worker task and retain its `JoinHandle` in a dedicated
  supervisor task.
- Map ordinary Runtime results to the existing completed, cancelled, timed-out,
  and failed statuses.
- Map a panicked worker to the fixed message
  `operation task panicked before protocol settlement`; never inspect, format,
  or return the task panic payload.
- Map any other premature worker stop to the fixed message
  `operation task stopped before protocol settlement`.
- Update the retained Operation under the same registry lock used by polling
  and forgetting.

## Consequences

Every background worker that stops while the protocol process is alive has one
terminal process-local status, including unexpected Agent Loop panics. Clients
can observe and forget the failed Operation. The extra supervisor is one small
Tokio task per active Turn and requires no dependency or second persistence
mechanism.

Rust invokes the process-global panic hook before Tokio reports a panicked
`JoinHandle`. The protocol never returns the payload, but production hosts must
still govern their panic hook and stderr. If the executor or process itself
stops, process-local supervisors stop too; clients must reconcile authoritative
Thread events after restart as already required.

## Rejected alternatives

- Continue detaching workers: a panic can strand `running` forever.
- Convert `JoinError` with `to_string`: the formatted error may contain
  provider-controlled panic content.
- Persist Operation records: Operations are disposable transport state; durable
  Thread and Turn truth belongs exclusively to the State Engine.
