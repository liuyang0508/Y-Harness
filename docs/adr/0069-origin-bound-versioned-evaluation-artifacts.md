# ADR 0069: Origin-bound versioned Evaluation artifacts

- Status: Accepted
- Date: 2026-07-25

## Context

Evaluation Graders are registered with a validated, trust-bearing
`CapabilityOrigin`, but format-1 reports retained only the Grader name.
Baselines also selected results by case and Grader name alone. Replacing a
built-in Grader with an external implementation under the same name could
therefore satisfy a previously reviewed baseline while discarding the trust
boundary that baseline assumed.

Suites, baselines, and reports were described as versioned artifacts, but their
public serialized roots did not carry an explicit format coordinate. Only the
reference CLI's outer output envelope had a schema number.

## Decision

- Define `EVALUATION_FORMAT_VERSION = 2`.
- Put `format_version` on every serialized `EvaluationSuite`,
  `EvaluationBaseline`, and `EvaluationReport` root.
- Require exact format 2 again at execution and comparison boundaries, even
  for values assembled directly or deserialized without constructors.
- Add `grader_origin` to every `GradeRecord`, including timeout, error, panic,
  and failed-case results.
- Add `grader_origin` to every `BaselineRequirement`. A comparison requires
  case identity, Grader identity, and origin to match before evaluating score
  or pass/fail thresholds.
- Preserve origin through bounded concurrent task scheduling; a task failure
  must not fabricate or downgrade an unknown identity.
- Advance the `yh eval-smoke` output schema and checked-in suite/baseline
  fixtures to 2.

## Consequences

Persisted Evaluation evidence now identifies the trust domain that produced
each score, and a reviewed baseline cannot silently accept a same-name Grader
from another origin. Artifact readers reject missing, old, or future format
coordinates instead of decoding them permissively.

Format-1 artifacts cannot be upgraded automatically because they never
recorded Grader origin. A baseline owner must select the intended registered
origin and regenerate or explicitly edit the artifact under review. Historical
reports remain untrusted legacy evidence rather than being relabeled.

This format is a library/CLI artifact contract, not a Y-Harness client-protocol
surface; Protocol 9 and State schema 4 therefore remain unchanged.

## Rejected alternatives

- Infer origin from the current registry while comparing: current registration
  state is not evidence of which implementation produced a persisted report.
- Record origin in reports but not baselines: reviewers could see the
  difference, but the automated release gate would still accept it.
- Keep an implicit format coordinate in filenames or Git history: exported
  artifacts must remain self-describing outside this repository.
- Default missing origin to `BuiltIn`: that would manufacture the strongest
  trust class for legacy evidence.
