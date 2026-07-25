# ADR 0010: Evaluation is not live Verification

- Status: Accepted
- Date: 2026-07-25

## Decision

Evaluation Engine executes validated, deterministically ordered cases through
an `EvaluationTarget`, captures each outcome once, and supplies the same
immutable sample to independently registered graders.

Target failure and grader failure are isolated report outcomes. They do not
abort the remaining suite. Scores must be finite and normalized to `0.0..=1.0`;
rationales and errors are bounded before entering reports.

Regression baselines use exact case/grader requirements:

- missing cases or grades fail;
- grader errors fail;
- scores below the configured minimum fail;
- requirements may also demand the grader's explicit pass flag.

## Runtime adapter

`HarnessRuntime` implements `EvaluationTarget` by creating an isolated Thread
for each case and applying its memory scope and timeout. The resulting State
evidence remains available for trace inspection. Each case gets a fresh
cancellation token.

The first runner is sequential to keep ordering and resource behavior
predictable. Bounded parallel evaluation will be added through Orchestration,
not through a second scheduler hidden inside Evaluation.

This initial scheduling decision is superseded by
[ADR 0026](0026-bounded-parallel-evaluation.md): Evaluation now owns only
local, bounded execution concurrency, while cross-worker distribution remains
an Orchestration concern.

## Boundary from Verification

Verification participates in a live Turn and can decide complete, retry, or
fail. Evaluation consumes captured behavior and produces comparison evidence.
A grader cannot send feedback into the active model/tool loop.

The first repository-owned executable suite and exact baseline are defined by
[ADR 0067](0067-versioned-harness-smoke-evaluation-gate.md). Explicit
format-version and Grader-origin binding are defined by
[ADR 0069](0069-origin-bound-versioned-evaluation-artifacts.md).

This separation prevents benchmark-specific judges, expected answers, or
dataset metadata from silently affecting production execution.
