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

The companion `hold_after_first_effect` case uses the same synchronized
journal contract but withholds the first Tool result until it sees an
identity-bound controller release marker. This creates a deterministic window
in which a product process can be cancelled after the effect but before a Tool
output is recorded. The release marker is bounded, must contain the exact
fixture identity, is create-new in the driver, and is not evidence that the
product cleaned up every descendant.

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

For the Y-Harness format-9 cell, the same spec may additionally bind a
JSON-command Model call identity, exact registered Tool name, distinct trigger
and post-restart prompts, and fixed audit message. `model` accepts only a
bounded request exposing exactly that Tool and one of those two latest user
prompts. This supplies deterministic decisions without moving Model behavior
into Runtime Core.

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
cell must disable unrelated Tools where the released product exposes that
control and record every Tool that cannot be removed. The fixture report has
`track: fixture_oracle` and `claim_eligible: false`. It becomes one input to a
case result only after a released-product adapter records the corresponding
process lifecycle, restart policy, granted authority, and exact build.

The first released Codex `0.145.0` format-7 run now correlates the fixture with
a hash-pinned product, a deterministic loopback Provider, its source-defined
deferred Tool-search path, exact advertised Tool identities, and a successful
one-effect/no-replay oracle. It explicitly records that product restart was not
exercised and that Codex built-in Tools remained advertised. The record is
preserved under
[`tools/benchmark-runner/evidence/2026-07-28-codex-cf003-probe`](../tools/benchmark-runner/evidence/2026-07-28-codex-cf003-probe/).

A released Codex `0.145.0` format-8 run also cancels the first product process
at the held effect boundary and resumes the exact persisted Thread in a second
process. The resume Provider observes the source-defined synthetic `aborted`
output, selects no Tool, and both fixture inspections retain one invocation
and one effect. Codex re-groups its MCP child, so this run uses and records the
controller release marker instead of claiming complete descendant cleanup.
The record is preserved under
[`tools/benchmark-runner/evidence/2026-07-28-codex-cf003-restart`](../tools/benchmark-runner/evidence/2026-07-28-codex-cf003-restart/).

A format-9 Y-Harness run drives real `yh serve` processes, the configured
JSON-command Model, stdio MCP, and SQLite State. After the controller kills the
faulting process, restart first observes the abandoned Turn still `running`;
protocol-v19 exact-Turn recovery then marks it `interrupted` without a Tool
result, and a new Turn completes without selecting a Tool. Both fixture
inspections remain one invocation and one effect. The record is preserved
under
[`tools/benchmark-runner/evidence/2026-07-28-y-harness-cf003-restart`](../tools/benchmark-runner/evidence/2026-07-28-y-harness-cf003-restart/).

CF-003 still does not kill a Harness process at every State settlement
boundary or cover timeout, oversized result, malformed protocol, descendant
containment, sandbox escape, or every product. Both restart cells start a new
Turn on the same Thread rather than continuing the interrupted Turn in place.
Those boundaries must not be inferred from this case.
