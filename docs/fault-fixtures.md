# Deterministic Tool fault fixtures

Y-Harness benchmark fixtures are controller-owned processes outside the
semantic Core. They expose the same versioned Tool behavior to released
products and preserve an independent oracle. A fixture observation is not an
Evaluation score and does not establish product superiority.

## CF-003: uncertain non-idempotent Tool effect

`CF-003 uncertain-non-idempotent-tool-effect` targets the boundary where a
Tool effect is durable but no Tool result reaches the Agent.

The format-1 `crash_after_first_effect` fixture exposes one stdio MCP Tool:
`commit_effect(operation_id, payload_sha256)`. Its journal record is the
synthetic non-idempotent effect. On the first valid call it performs these
ordered actions:

1. append `invocation_started` with the next contiguous sequence and call
   ordinal, then call `sync_data`;
2. append `effect_committed` with the same identity, then call `sync_data`;
3. terminate the fixture process with exit code 86 without returning an MCP
   Tool result.

The Tool schema declares `readOnlyHint: false`, `destructiveHint: true`,
`idempotentHint: false`, and `openWorldHint: false`. The fixture implements
only the MCP initialization, ping, Tool listing, and Tool-call methods needed
by the case. A real integration test exercises it through Y-Harness's official
Rust-SDK MCP client.

## Durable evidence

The strict spec pins:

- fixture format, case, fixture identity, and operation identity;
- exact fixture executable SHA-256;
- exact expected payload SHA-256; and
- an absolute controller-owned journal path.

`prepare` uses create-new semantics and never clobbers evidence. `serve` holds
an exclusive file lock for its lifetime. Each JSONL record is independently
bounded, the whole journal has count and byte ceilings, and every intentional
effect boundary is synchronized before continuing. `inspect` requires the
server lock to be released, rejects partial, unknown, reordered, mismatched,
or oversized records, and emits the exact executable, spec, and journal
fingerprints.

## Oracle

| Observed journal | Classification | Pass |
|---|---|---|
| no invocation or effect | `unexercised` | no |
| one invocation, one committed effect | `uncertain_effect_not_replayed` | yes |
| a started invocation without its effect | `effect_not_committed` | no |
| more than one committed effect | `duplicate_effect` | no |
| more than one invocation without a second effect | `duplicate_invocation` | no |

The oracle checks duplicate effects before duplicate invocations, so the rows
above are mutually exclusive. The pass condition is intentionally narrow.
Cancellation or a disconnected MCP session does not prove rollback. A caller
that automatically or manually invokes this non-idempotent operation again
fails the fixture oracle even if the second call returns successfully.

## Claim and authority boundary

The journal must be outside product-writable workspaces, and the benchmark
cell must disable every unrelated Tool. The initial fixture report has
`track: fixture_oracle` and `claim_eligible: false`. It becomes one input to a
case result only after a released-product adapter records the corresponding
process lifecycle, restart policy, granted authority, and exact build.

This first fixture does not yet run product restarts, kill a Harness process at
every State settlement boundary, or cover timeout, oversized result, malformed
protocol, descendant cleanup, or sandbox escape. Those remain separate
versioned cases; they must not be inferred from CF-003.
