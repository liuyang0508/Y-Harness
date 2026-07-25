# ADR 0067: Versioned Harness smoke evaluation is an executable gate

- Superseded in part by: ADR 0069 (explicit format 2 and Grader-origin binding)

## Status

Accepted.

## Context

Evaluation types, bounded runners, graders, reports, and exact baselines are
necessary infrastructure, but they are not evidence that a repository has a
working regression evaluation. A real gate also needs versioned inputs,
versioned acceptance thresholds, a reproducible target, a machine-readable
result, and a failing process status when behavior regresses.

The first gate must exercise Harness behavior rather than a mock comparison,
while remaining safe and portable in ordinary local and CI environments. It
must not require credentials, network access, external models, durable ambient
state, or platform-specific containment.

## Decision

The repository owns two reviewed fixtures:

- `evals/harness-smoke-suite.json`;
- `evals/harness-smoke-baseline.json`.

`yh eval-smoke` compiles those exact fixtures into the reference binary,
decodes and revalidates them through the public `EvaluationSuite` and
`EvaluationBaseline` constructors, and executes them against the same demo
model, Tool Registry, Policy Engine, Agent Loop, and State Engine used by the
reference host. The gate substitutes only an isolated `MemoryEventStore` for
the demo's durable SQLite store.

The v1 suite contains ASCII and Unicode cases. Two pure graders require:

1. exact final assistant output;
2. one completed, ordered, and call-ID-correlated
   User → ToolCall → Policy Allow → ToolResult → Assistant sequence.

The command emits a versioned JSON envelope containing the full evaluation
report and exact baseline comparison. It exits nonzero when any requirement is
missing, errors, falls below its minimum score, or fails its required-pass
condition. CI runs the command after the all-feature test suite.

Evaluation root boundaries revalidate potentially deserialized suites,
baselines, and reports. Report validation rejects duplicate identities,
non-finite or out-of-range scores, invalid rationale/error bounds, unsupported
JSON shapes, and oversized captured executions before comparison.

## Consequences

- The Evaluation row in the completion audit now has executable repository
  evidence instead of API tests alone.
- The gate is local, fast, credential-free, cross-platform, and does not create
  ambient files.
- Case and grade order is deterministic. Runtime-generated IDs and timestamps
  remain evidence and are intentionally not claimed to be byte-for-byte
  deterministic.
- The smoke suite proves a narrow generic Harness contract. It does not claim
  model quality, external-provider availability, adversarial safety, large
  dataset throughput, statistical significance, or application-specific
  usefulness.
- Additional suites and graders can use the public Evaluation contracts
  without changing the Agent Loop. Larger datasets remain caller-chunked under
  the materialized batch limits.

## Rejected alternatives

- **Treat Evaluation unit tests as a regression baseline:** proves the
  framework implementation, not an end-to-end Harness behavior.
- **Use a live hosted LLM:** introduces nondeterminism, credentials, cost, and
  network availability into the minimum quality gate.
- **Write evaluation output into the repository:** creates ambient mutable
  state and review noise; CI artifacts can capture stdout when retention is
  desired.
- **Compare entire report bytes:** generated identities and timestamps are
  legitimate runtime evidence and would make a semantic gate flaky.
