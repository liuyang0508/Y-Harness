# External-run format 1

External-run format 1 preserves one released-product CLI execution without
turning it into a comparative score. It is produced by the independent
`y-harness-benchmark-runner` package, not by the Harness Runtime.

## Top-level contract

| Field | Meaning |
|---|---|
| `format_version` | Exact integer `1`; readers must reject another value. |
| `adapter` | Adapter name/version, product name, observed CLI version, and SHA-256 of both adapter and product executables. |
| `coordinate` | Caller-assigned run, benchmark and case identities; caller-asserted workspace snapshot; start time and host platform. |
| `controls` | Requested profile/model/budget/timeout, observed model identities, prompt fingerprints, authority, inherited environment names, unsupported controls, and claim eligibility. |
| `execution` | Exactly one `completed`, `product_error`, or `adapter_error` settlement. |

`completed` and `product_error` contain the same `settlement` shape:

- process exit code and adapter-observed wall time;
- product-reported total/API duration, Turn count, and actual cost;
- result subtype;
- byte counts and SHA-256 for stdout and stderr;
- the bounded parsed product JSON as `raw_result`.

`product_error` means the released CLI settled and returned a valid result
envelope describing failure. `adapter_error` means the CLI could not be
started, timed out, exceeded retention, or violated its pinned output
contract. When a child settlement exists, `adapter_error.process` retains its
exit code, truncation flags, byte counts, and stream digests; broker failures
before settlement use `process: null`. These statuses are not interchangeable.

## Authority and privacy

The adapter uses Y-Harness's shell-free Process Broker with an absolute
executable and workspace, exact environment-name allowlist, timeout, and
two-MiB limit per output stream. Environment values are used but never written
to the report. Stderr content is not retained; only its length and digest are
recorded. Product JSON is retained because it is the primary execution
evidence and may contain product-generated session identities.

The initial Claude Code adapter always records:

```json
{
  "track": "adapter_conformance",
  "claim_eligible": false,
  "tools": "disabled",
  "permission_mode": "dont_ask"
}
```

It cannot support a Harness-effect, product-quality, or superiority claim.
Unsupported controls are data, not prose omitted by an aggregator.

## Evidence

The first real format-1 record and its exact input are preserved under
[`tools/benchmark-runner/evidence/2026-07-26-claude-code-probe`](../tools/benchmark-runner/evidence/2026-07-26-claude-code-probe/).
The run used Claude Code `2.1.143`, returned the requested fixed text, and
observed `MiniMax-M2.7` behind requested model alias `haiku`. This is adapter
conformance evidence only.
