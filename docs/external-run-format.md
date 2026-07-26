# External-run formats

External-run formats preserve one released-product CLI execution without
turning it into a comparative score. They are produced by the independent
`y-harness-benchmark-runner` package, not by the Harness Runtime.

- format 1 is the original Claude Code single-result envelope;
- format 2 adds explicit unavailable product metrics for the Codex JSONL
  envelope without weakening strict format-1 readers.

## Top-level contract

| Field | Meaning |
|---|---|
| `format_version` | Exact integer `1` or `2`; readers must select one exact schema and reject unsupported values. |
| `adapter` | Adapter name/version, product name, observed CLI version, and SHA-256 of both adapter and product executables. |
| `coordinate` | Caller-assigned run, benchmark and case identities; caller-asserted workspace snapshot; start time and host platform. |
| `controls` | Requested profile/model/timeout and budget when exposed, observed model identities, prompt fingerprints, authority, inherited environment names, unsupported controls, and claim eligibility. |
| `execution` | Exactly one `completed`, `product_error`, or `adapter_error` settlement. |

`completed` and `product_error` contain the same `settlement` shape:

- process exit code and adapter-observed wall time;
- product-reported total/API duration and actual cost; format 1 requires
  numbers, while format 2 uses exact `null` when Codex does not expose them;
  an adapter must never infer them;
- the validated Turn count represented by the retained product envelope;
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

The Claude Code adapter emits format 1 and always records:

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

The Codex adapter emits format 2 and likewise fixes `claim_eligible: false`.
It runs stable `codex exec --json` with an exact CLI version and executable
digest,
`--ephemeral`, a read-only product sandbox, approval policy `never`, disabled
web search, and a developer-instruction override. `bare` additionally requires
an empty, caller-provided `CODEX_HOME`, API-key authentication,
`--ignore-user-config`, and `--ignore-rules`; `product` retains ambient product
configuration and records that limitation.

Codex JSONL does not expose settled Model identity, product/API duration,
actual cost, or a documented hard monetary ceiling. Format 2 therefore records
an empty `observed_models` list and `null` for those unavailable numeric
fields. Codex built-in Tools also remain available within its read-only
sandbox. These differences make the adapter suitable for conformance evidence,
not a controlled Harness comparison.

## Evidence

The first real format-1 record and its exact input are preserved under
[`tools/benchmark-runner/evidence/2026-07-26-claude-code-probe`](../tools/benchmark-runner/evidence/2026-07-26-claude-code-probe/).
The run used Claude Code `2.1.143`, returned the requested fixed text, and
observed `MiniMax-M2.7` behind requested model alias `haiku`. This is adapter
conformance evidence only.

The Codex adapter is source- and contract-tested against official snapshot
[`61a4488`](https://github.com/openai/codex/tree/61a44880a85d2fd0d8770908dea5733495e571c8).
No real Codex result is checked in yet, so it contributes no live product
evidence.
