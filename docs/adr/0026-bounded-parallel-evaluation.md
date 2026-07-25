# ADR 0026: Evaluation owns bounded local parallelism

- Status: Accepted
- Date: 2026-07-25

## Decision

Evaluation runs cases and graders concurrently through two independent,
operator-configurable limits. It uses the existing Tokio runtime and adds no
scheduler dependency. Results are sorted by stable case and grader identity,
so completion timing cannot change the report.

The engine owns a fallback case deadline and cooperative cancellation token.
A timed-out target receives cancellation and a finite cleanup grace period.
Every grader has its own deadline. Panics and task cancellation become
content-free per-case or per-grader errors; panic payloads never enter reports.

An immutable `EvaluationSample` is shared with graders by `Arc`, avoiding a
full Turn-history clone per parallel grade.

## Resource boundary

The current API returns a complete in-memory report. It therefore admits at
most 64 cases and 64 graders per batch and bounds encoded case, suite, and
captured execution sizes. Larger datasets are caller-chunked until a durable
streaming report sink exists. This is an explicit scalability boundary, not an
unbounded-memory promise.

## Orchestration boundary

Evaluation owns local fan-out needed to evaluate one bounded batch.
Orchestration continues to own distribution across workers, leases, dependency
graphs, workspace isolation, and durable task coordination. Evaluation does
not create a second distributed scheduler.

The bounded runner is exercised end to end by the versioned smoke gate in
[ADR 0067](0067-versioned-harness-smoke-evaluation-gate.md).

## Consequences

- independent cases and pure graders use available local concurrency;
- one hung or panicking extension cannot stall or abort the whole batch;
- reports remain reproducible despite nondeterministic execution timing;
- callers must chunk datasets larger than the materialized-report boundary;
- streaming report persistence remains a later, separately verified feature.
