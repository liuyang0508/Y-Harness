# External-run formats

External-run formats preserve one released-product CLI execution without
turning it into a comparative score. They are produced by the independent
`y-harness-benchmark-runner` package, not by the Harness Runtime.

- format 1 is the original Claude Code single-result envelope;
- format 2 adds explicit unavailable product metrics for the Codex JSONL
  envelope without weakening strict format-1 readers;
- format 3 adds Grok Build's headless JSON controls, optional complete cost
  with its exact integer tick evidence, and observed Model usage;
- format 4 adds Pi's JSONL Agent-session lifecycle, explicit Provider and
  reasoning controls, observed Model identity, and reported assistant cost;
- format 5 adds OpenCode's run JSONL step lifecycle, isolated configuration
  controls, and complete-step cost without inventing error cost or Model
  identity;
- format 6 adds Hermes Agent's one-shot response plus strict bounded usage
  sidecar, observed Provider/Model identity, and explicit estimated-cost
  semantics; and
- format 7 is a separate Codex CF-003 fault-conformance envelope. It correlates
  a deterministic local Responses Provider, released-product JSONL, and the
  independent MCP effect oracle without turning them into a score; and
- format 8 adds a controller-cancelled Codex process, exact persisted rollout
  identity, same-Thread `exec resume`, source-defined synthetic `aborted`
  output, and before/after no-replay observations; and
- format 9 drives Y-Harness itself through real typed stdio service processes,
  SQLite restart, permissioned exact-Turn recovery, and the same independent
  one-effect oracle.

## Top-level contract

| Field | Meaning |
|---|---|
| `format_version` | Exact integer `1` through `9`; readers must select one exact schema and reject unsupported values. |
| `adapter` | Adapter name/version, product name, observed CLI version, and SHA-256 of both adapter and product executables. |
| `coordinate` | Caller-assigned run, benchmark and case identities; caller-asserted workspace snapshot; start time and host platform. |
| `controls` | Requested profile/provider/model/timeout and budget when exposed, observed model identities, prompt fingerprints, authority, inherited environment names, unsupported controls, and claim eligibility. |
| `execution` | Exactly one `completed`, `product_error`, or `adapter_error` settlement. |

The table's execution variants describe formats 1–6. Formats 7–9 keep the same
adapter, coordinate, and non-claim control principles but have fault-specific
execution objects. Formats 7/8 retain strict Codex JSONL and deterministic
Provider summaries; format 9 retains typed Y-Harness Protocol/State recovery
evidence. All validate the fixture independently. Their `passed` field means
only that the pre-registered fault experiment satisfied its closed contracts.

`completed` and `product_error` contain the same `settlement` shape:

- process exit code and adapter-observed wall time;
- product-reported total/API duration and actual cost; format 1 requires
  numbers, format 2 uses exact `null` when Codex does not expose them, and
  format 3 preserves Grok Build cost only when the product marks it complete,
  while format 4 sums cost from validated completed Pi assistant messages and
  format 5 sums only successful validated OpenCode `step-finish` records;
  format 6 keeps Hermes's estimate in raw evidence and reports actual cost as
  unavailable;
  complete format-3 settlements add `actual_cost_usd_ticks`, where
  `10_000_000_000` ticks equal one USD, and reject disagreement with the
  product's float projection; an adapter must never infer these fields;
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

The Codex CF-003 driver emits format 7 and fixes both the released product and
the analyzed official source to Codex `0.145.0`. It requires empty workspace
and `CODEX_HOME` directories, clears ambient environment, supplies only the
owned home and a loopback Provider token, disables request compression,
multi-Agent Tools, user configuration, rules, web search, persistence, and
interactive approval, and preapproves only the configured fixture MCP Tool.
The product sandbox is read-only; the outer Process Broker remains explicitly
unrestricted rather than being mislabeled as containment.

The loopback Provider accepts only bounded, uncompressed
`POST /v1/responses` requests with exact authentication. It follows Codex's
source-defined deferred Tool path: the first response selects `tool_search`,
the second selects the surfaced namespaced MCP Tool, and the third settles only
after receiving the failed `function_call_output`. Request bodies and both
Tool outputs are fingerprinted. The fixture report independently pins its
executable, semantic spec, and journal. Format 7 passes only when Codex settles
normally, all three Provider requests validate, and the oracle observes one
invocation and one effect without replay.

Format 7 remains `claim_eligible: false`. Codex built-in Tools are still
advertised, the deterministic Provider does not measure reasoning quality, the
released binary has not been reproducibly derived from the analyzed source,
and product restart/resume is not exercised.

Format 8 retains the same pinned Codex and fixture coordinates but removes
`--ephemeral`. The first product process is cancelled after the controller
observes the exact synchronized effect boundary and before any Tool output is
persisted. The controller then discovers one bounded, symlink-free rollout
under the owned `CODEX_HOME`, validates its `session_meta` Thread UUID, exact
function call, and absent output, and resumes that Thread in a second process.
The resume Provider requires the original call, exact synthetic
`function_call_output: "aborted"`, and the new user Turn before returning a
fixed assistant message without selecting a Tool.

Passing format 8 additionally requires the resumed JSONL Thread ID to match,
the same rollout file to grow and change digest, zero resumed MCP Tool calls,
and both independent fixture inspections to retain one invocation and one
effect. Codex places its MCP child in another process group, so the report
records a controller-owned identity-bound release marker and verified fixture
settlement rather than claiming complete descendant cleanup. Recovery starts a
new Turn on the same Thread; it does not resume the interrupted Turn's stack.
The deterministic Provider, binary/source, built-in Tool, containment, and
claim-eligibility limitations remain explicit.

Format 9 uses an empty workspace and hash-pinned release `yh`,
`yh-fault-fixture`, and `yh-bench` executables. The fixture's optional
spec-bound Model mode accepts the complete JSON-command `ModelRequest`,
requires exactly the registered `fault.commit_effect` Tool, selects that Tool
for the trigger prompt, and returns a fixed Tool-free message only for the
post-restart audit prompt. The controller creates a Thread in a clean service
process, starts the fault Turn in a second process, waits for the exact durable
effect boundary, reads the still-running Thread projection, and kills that
service before a Tool result can settle.

The restarted service must first observe the same Turn still `running`; this
proves restart did not perform unsafe implicit takeover. The controller then
sends protocol-v19 `recover_thread` with the exact abandoned Turn identity.
The same Turn must become `interrupted` with one Tool call and zero Tool
results. A separate new Turn must complete with the fixed assistant message
and no Tool. Fixture inspections after interruption and after the new Turn
must both remain exactly one invocation and one effect.

Format 9 records that only the product process was killed and that the held
fixture required its identity-bound release marker; it does not claim generic
descendant containment. It also records that interrupted execution is not
continued in place and deterministic Model reasoning is not a quality
measurement. It is always `claim_eligible: false`.

The Grok Build adapter emits format 3 and also fixes
`claim_eligible: false`. `bare` runs inject empty exact `HOME`, `USERPROFILE`,
and `GROK_HOME` roots while inheriting only declared secrets such as
`XAI_API_KEY`; `product` runs retain explicitly inherited ambient
configuration. The adapter supplies the prompt through a private
create-exclusive file, makes it owner-only on Unix, removes it after execution,
requests one exact Turn and reasoning effort, disables Memory, planning,
Subagents, questions, web Tools, and updates, and requests the product's
`read-only` sandbox with `dontAsk`. Windows reports inherited prompt-directory
ACLs as an unsupported control.

Grok Build's `read_file` and always-on MCP meta-tools remain visible, and the
product persists its session beneath the isolated Grok home. Format 3 records
those limitations, the requested reasoning effort and Turn ceiling, and the
product sandbox. It derives observed Models only from `modelUsage`; a complete
cost retains both the float projection and exact integer ticks. Absent or
partial cost remains `null`, never zero, and has no tick field.

The Pi adapter emits format 4 and fixes `claim_eligible: false`. `bare`
injects an exact empty `PI_CODING_AGENT_DIR`; `product` may retain explicitly
inherited ambient configuration. Both profiles use JSON output, ephemeral
sessions, exact Provider/Model/reasoning/system-prompt controls, and disable
Tools, extensions, Skills, prompt templates, themes, context files, project
trust, and startup network refresh.

Format 4 accepts an optional Pi session header and then validates the bounded
Agent-session lifecycle through terminal `agent_settled`. It permits repeated
`agent_end` events because Pi can automatically retry a failed Model call.
Turn count, terminal stop reason, reported assistant cost, and observed
Provider/Model identity are taken only from completed assistant events. Pi
reports the explicitly requested Provider separately from the combined
requested Model coordinate. It does not expose distinct product/API durations,
a hard monetary ceiling, or a built-in sandbox. Disabling Tools also means the
adapter does not measure Pi's Harness loop effectiveness.

The OpenCode adapter emits format 5 and fixes `claim_eligible: false`. `bare`
requires an initially empty home, owns its XDG roots and empty authentication
content, uses an in-memory product database, and disables default/external
plugins, external Skills, LSP downloads, and Claude compatibility inputs.
`product` deliberately retains ambient authentication, global configuration,
instructions, and MCP definitions. Both profiles pass the prompt on stdin,
select one exact `provider/model` plus optional variant, suppress project
configuration, external plugins, updates, model refresh, compaction, sharing,
snapshots, title generation, and formatter/LSP activation, and deny all Tools
through a generated primary agent. Caller-controlled Model, variant, and agent
prompt reject `{env:...}` / `{file:...}` configuration substitutions. The
explicitly requested Provider is reported separately from the combined
requested Model coordinate.

Format 5 accepts only one stable Session's ordered `step_start`, optional
reasoning/text, and `step_finish` records, or one final product error. Tool
events, overlapping steps, cross-Session events, malformed token/cost facts,
and trailing error events fail the adapter contract. A successful stream sums
finite nonnegative completed-step cost and retains the terminal finish reason.
An error stream has `actual_cost_usd: null`, because its final event does not
prove complete failed-step cost. OpenCode's JSONL does not expose settled Model
identity or distinct product/API duration, so those fields remain empty or
`null`. Its agent prompt is additive to product/provider instructions, and
neither a hard spend ceiling nor a hard provider-call ceiling is claimed.
OpenCode may still initialize or update its plugin SDK dependency cache; the
adapter records that unsupported control.

The Hermes Agent adapter emits format 6 and fixes `claim_eligible: false`.
Only `bare` is supported. The caller supplies initially empty, pairwise
disjoint Hermes-home and usage directories outside the workspace. The adapter
owns Hermes configuration/safe-mode environment, disables the system managed
scope, maps platform home discovery to the isolated Hermes home, creates an
empty private `.env`, selects the static empty `context_engine` toolset, and
uses the source-pinned 90-call product default as a validation ceiling rather
than pretending it is caller-selectable. Its offline version-probe cache
prevents `hermes --version` from checking for updates.

Hermes `0.19.0` has no one-shot stdin or system-prompt control. Format 6
therefore records that both requested instruction and prompt appear in process
arguments and that the instruction is only a labeled user-level prefix.
Workspace instructions are not claimed disabled. The create-exclusive usage
file is owner-only on Unix, limited to 64 KiB, and must contain the pinned flat
schema. Success requires a nonempty UTF-8 response, coherent completion flags,
one or more API calls, observed Model and Provider identities, and all token
fields. A coherent product failure may retain nullable fields. Contradictory
exit/completion state, unknown/missing fields, excessive API calls, malformed
numbers, or a missing sidecar are adapter errors.

The usage report labels cost as estimated. Format 6 preserves that value,
status, and source only under `raw_result.usage`; `actual_cost_usd` remains
`null`. The product persists isolated session state, its source-checkout
`.env` fallback can fill otherwise undeclared variables, and the hashed Python
launcher need not identify its dependency graph. These are explicit
unsupported controls.

## Evidence

The first real format-1 record and its exact input are preserved under
[`tools/benchmark-runner/evidence/2026-07-26-claude-code-probe`](../tools/benchmark-runner/evidence/2026-07-26-claude-code-probe/).
The run used Claude Code `2.1.143`, returned the requested fixed text, and
observed `MiniMax-M2.7` behind requested model alias `haiku`. This is adapter
conformance evidence only.

The ordinary format-2 Codex adapter is source- and contract-tested against official snapshot
[`61a4488`](https://github.com/openai/codex/tree/61a44880a85d2fd0d8770908dea5733495e571c8).
It has no live fixed-output record.

The source-pinned format-7 driver is additionally tested against official tag
[`rust-v0.145.0`](https://github.com/openai/codex/tree/25af12f7e61572b0bc18ddb1008be543b91519b0).
One real released-product CF-003 record is preserved under
[`tools/benchmark-runner/evidence/2026-07-28-codex-cf003-probe`](../tools/benchmark-runner/evidence/2026-07-28-codex-cf003-probe/).
It passed the narrow no-replay oracle and remains non-comparative,
`claim_eligible: false` evidence.

The source-pinned format-8 driver uses the same official source coordinate.
One real released-product restart record is preserved under
[`tools/benchmark-runner/evidence/2026-07-28-codex-cf003-restart`](../tools/benchmark-runner/evidence/2026-07-28-codex-cf003-restart/).
It passed the same-Thread, synthetic-abort, and one-effect/no-replay gates,
while explicitly recording detached-fixture release and new-Turn recovery.
It remains non-comparative, `claim_eligible: false` evidence.

One real format-9 Y-Harness process-restart record is preserved under
[`tools/benchmark-runner/evidence/2026-07-28-y-harness-cf003-restart`](../tools/benchmark-runner/evidence/2026-07-28-y-harness-cf003-restart/).
It passed the no-implicit-takeover, exact explicit recovery, same-Thread,
new-Turn completion, and one-effect/no-replay gates. It remains
non-comparative, `claim_eligible: false` evidence.

The Grok Build adapter is source- and contract-tested against official snapshot
[`47348d1`](https://github.com/xai-org/grok-build/tree/47348d13ec4508dcfe440e34c6d511bb02998fb2).
No real Grok Build result is checked in yet, so it also contributes no live
product evidence.

The Pi adapter is source- and contract-tested against official snapshot
[`cee5ff7`](https://github.com/earendil-works/pi/tree/cee5ff7520d8828bed9955ef00419e995d1f91e0).
One real released-Pi fixed-output record is preserved under
[`tools/benchmark-runner/evidence/2026-07-28-pi-fixed-output`](../tools/benchmark-runner/evidence/2026-07-28-pi-fixed-output/).
It used a deterministic loopback Provider and completed the released CLI's
JSONL lifecycle. It remains non-comparative, `claim_eligible: false`
adapter-conformance evidence.

The OpenCode adapter is source- and contract-tested against official snapshot
[`7534d23`](https://github.com/anomalyco/opencode/tree/7534d23551f665e65080809975b4ca5c7d63807b).
One real released-OpenCode fixed-output record is preserved under
[`tools/benchmark-runner/evidence/2026-07-28-opencode-fixed-output`](../tools/benchmark-runner/evidence/2026-07-28-opencode-fixed-output/).
It used an immutable custom Provider file, a deterministic loopback Provider,
and an isolated bare home. It remains non-comparative,
`claim_eligible: false` adapter-conformance evidence; settled Model identity
remains unavailable.

The Hermes Agent adapter is source- and contract-tested against official
release `v2026.7.20`, commit
[`3ef6bbd`](https://github.com/NousResearch/hermes-agent/tree/3ef6bbd201263d354fd83ec55b3c306ded2eb72a).
One real released-Hermes fixed-output record is preserved under
[`tools/benchmark-runner/evidence/2026-07-28-hermes-fixed-output`](../tools/benchmark-runner/evidence/2026-07-28-hermes-fixed-output/).
It used a locked source install, isolated platform/Hermes home, and
deterministic loopback Provider. It remains non-comparative,
`claim_eligible: false` adapter-conformance evidence; estimated cost is not
promoted to actual cost.
