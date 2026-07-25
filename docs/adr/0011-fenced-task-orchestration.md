# ADR 0011: Task orchestration uses DAGs and fenced leases

- Status: Accepted
- Date: 2026-07-25

## Decision

Orchestration Core is a pure, serializable `TaskGraph` with:

- validated complete acyclic dependencies;
- deterministic ready ordering by priority and Task identity;
- bounded claims and expiring ownership leases;
- unique fencing tokens and monotonic attempt numbers;
- explicit complete, fail, cancel, blocked, and requeue transitions;
- transitive upstream-failure propagation;
- ordered bounded Task messages;
- digest-bearing Artifact references;
- workspace isolation requirements without platform-specific implementation.

Completing, failing, or heartbeating a Task requires its current unexpired lease
token. When a lease expires, the Task may be claimed again with a new token.
The old worker cannot settle the new attempt even if it finishes late.

## Persistence and availability boundary

`TaskGraph` is the deterministic aggregate, not a distributed database. A
multi-process host must persist each mutation atomically and compare its graph
version. Fencing protects settlement identity only when the backing
coordinator also serializes mutations.

The current slice therefore does not claim distributed high availability.
SQLite/event persistence, watch streams, and retry-on-conflict belong to the
protocol/coordinator slice.

## Workspace boundary

Tasks may request no workspace, an isolated writable workspace, or a shared
read-only workspace. The pure Task Graph does not invoke Git or create
Worktrees. The later Orchestrator layer now delegates preparation and cleanup
to a replaceable Workspace Provider, as specified by
[ADR 0073](0073-governed-task-workspaces-and-pinned-git-worktrees.md). An
untrusted execution environment remains subject to independent Policy and
sandbox rules.

## Rationale

A lease without a fencing token cannot prevent a paused worker from overwriting
a later attempt. A scheduler without explicit dependencies cannot distinguish
ready, waiting, and permanently blocked work. Keeping both semantics in the
pure aggregate makes them testable independently of a TUI, database, or
sub-Agent transport.
