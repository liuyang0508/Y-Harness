# Deterministic Tool fault fixture

`yh-fault-fixture` is a controller-owned stdio MCP process for the first
crash-after-effect oracle. It is independent from the Harness Core and from
released-product adapters.

Its only Tool is explicitly annotated as non-idempotent and destructive. The
first valid call appends and synchronizes the effect record, then exits with
code 86 before emitting a Tool result.

The fixture consumes this strict format-1 spec:

```json
{
  "format_version": 1,
  "fixture_id": "cf-tool-uncertain-001",
  "case": "crash_after_first_effect",
  "expected_fixture_executable_sha256": "<64 lowercase hex bytes>",
  "journal": "/absolute/controller-owned/path/journal.jsonl",
  "operation_id": "effect-001",
  "expected_payload_sha256": "<64 lowercase hex bytes>"
}
```

The controller owns the journal and lifecycle:

```bash
yh-fault-fixture prepare /absolute/path/to/spec.json
yh-fault-fixture serve /absolute/path/to/spec.json
yh-fault-fixture inspect /absolute/path/to/spec.json
```

`prepare` never replaces an existing journal. `serve` holds an exclusive
lock, validates the exact fixture executable digest, and synchronizes both
the invocation and effect records before the intentional crash. `inspect`
runs only after settlement and returns the exact spec, executable, and
journal fingerprints with a deterministic oracle:

- one invocation and one effect: `uncertain_effect_not_replayed`, pass;
- more than one effect: `duplicate_effect`, fail;
- more than one invocation without a second effect: `duplicate_invocation`, fail;
- no invocation, a partial effect boundary, or invalid journal: fail or reject.

The observation remains `claim_eligible: false` until it is correlated with a
pinned product run and restart trace. See
[`docs/fault-fixtures.md`](../../docs/fault-fixtures.md).
