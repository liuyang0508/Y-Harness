# Competitive Harness benchmark

This protocol defines the evidence required before Y-Harness may claim better
runtime effect than Codex, Claude Code, Pi Agent Harness, Hermes Agent,
OpenCode, or Grok Build. It is a pre-registration document, not a benchmark
result.

## Claim boundary

Architecture and product effect are different claims:

- an architecture claim is supported by public contracts, invariants, code
  review, and focused tests;
- an effect claim is supported by controlled tasks executed through the real
  products or public Harness interfaces;
- a source-code reconstruction is never used as the executable Claude Code
  baseline; the released product is the baseline;
- repository size, feature count, test count, language choice, and local unit
  coverage do not establish answer quality.

Until released products have executed the same versioned workloads under
declared controls, the only permitted statement is: **Y-Harness has implemented
a governed local baseline; comparative effectiveness is unverified.**

External-run formats 1/2/3/4/5/6 plus Codex CF-003 formats 7/8 and released
Claude Code, Codex, Grok Build, Pi, OpenCode, and Hermes Agent CLI adapter
contracts now exist, with one checked-in real Claude Code
ambient-product result, one real deterministic Claude Code fixed-output
result, one real Grok Build fixed-output result, one real Pi fixed-output
result, one real OpenCode fixed-output result, one real Hermes fixed-output
result, one real Codex fixed-output result, one shared-Provider Claude
Code/Codex Harness-control preflight, one shared-Responses Codex/Grok Build
preflight, and two Codex CF-003 fault records. All have
`claim_eligible: false`: they prove bounded adapters and registered fault
behavior, not that either Harness is better. See
[`external-run-format.md`](external-run-format.md).

Grok Build is an official open-source coding Agent, TUI, and Harness, so it is
a source-level baseline. Grok 4.5 remains a separate Model coordinate and is
the audited Grok Build snapshot's checked-in default. A released Grok Build
run may enter the product-default track; it enters the Harness-control track
only when the same Model and authority controls can actually be fixed. The
Grok Build adapter has one real released-binary fixed-output record, but no
comparative result. Its one main-agent Turn also generated an auxiliary title
call, so it establishes neither a hard Provider-call ceiling nor Harness
effect. See
[`reference-analysis.md`](reference-analysis.md).

## Two tracks

The benchmark keeps two questions separate.

### Harness-control track

Measure the Harness contribution with as many variables fixed as the products
permit:

- identical model and exact model version;
- identical provider endpoint, credentials class, reasoning effort,
  temperature, output limit, and provider tool settings;
- identical repository or artifact snapshot;
- identical system/developer task instructions;
- identical Tool schemas and deterministic Tool fixtures;
- identical filesystem, network, process, and approval authority;
- identical context and monetary budgets;
- identical hardware class and network region;
- clean state before every run.

If a product cannot accept one of these controls, the difference is recorded
and the run cannot support a pure Harness-effect claim.

The first real preflight is preserved under
[`2026-07-28-harness-control-preflight`](../tools/benchmark-runner/evidence/2026-07-28-harness-control-preflight/).
Released Claude Code `2.1.143` and Codex `0.145.0` used one running
deterministic Provider fixture and requested the same Provider label, Model
identifier, prompts, `medium` effort label, timeout, and empty-workspace class.
Both returned the same fixed text. The result is nevertheless
`not_comparable`: Claude used Anthropic Messages with no Tools and enabled
thinking, while Codex used OpenAI Responses with five built-in Tools,
automatic reasoning summary, and a read-only product sandbox. Codex also
reported that the shared Claude Model identifier had no Model metadata and
that fallback metadata was used; its JSONL did not settle Model or Provider
identity. A shared string and fixture therefore did not establish an
identically implemented Model coordinate. No shared Tool workload may be
promoted to this track until that identity gap and the Tool, Context, sandbox,
budget, and Provider-call-ceiling differences are equalized.

The second real preflight is preserved under
[`2026-07-28-responses-control-preflight`](../tools/benchmark-runner/evidence/2026-07-28-responses-control-preflight/).
It narrowed the gap: released Codex `0.145.0` and Grok Build `0.2.112` used one
Provider process, OpenAI Responses, `gpt-5.4`, matching prompts, `medium`
effort, and read-only product sandbox requests for their main Agent calls.
Codex emitted no fallback-Model event and Grok settled `gpt-5.4` in its main
result. The complete product Turn still failed parity. Grok issued an
auxiliary title request with `grok-4.5`; the fixture rejected it with HTTP 422
and Grok silently continued. Codex exposed six built-in Tools, while Grok
exposed three different main Tools plus the title Tool, injected different
Context, requested a different reasoning summary, and made an extra catalog
request. The machine verdict therefore remains `not_comparable`, and the
single-run wall times support no latency claim.

### Product-default track

Run each released product with its recommended default model and configuration.
This measures the product bundle, not the Harness alone. Results must say
“product-default” and must not be used to attribute a gain to Y-Harness
architecture.

## Workloads

Each workload is versioned, deterministic where possible, and paired with a
machine-checkable oracle.

| Area | Required workload |
|---|---|
| Context | retrieve facts across long histories; preserve tool call/result pairs; recover from text, image, and tool-output overflow |
| Agent Loop | multi-step tool use, steering during execution, cancellation, provider timeout, retry, failover, shared retry/failover attempt exhaustion, auxiliary model calls, and truncated tool arguments |
| Tool Runtime | sequential and parallel-safe calls, exclusive calls, malformed input, oversized result, timeout, descendant cleanup, and sandbox escape probes |
| State | kill at every settlement boundary, reopen state, resume permitted work, and prove uncertain effects are not replayed |
| Memory | same corpus and query set, scoped retrieval, provenance, poisoned-memory rejection, and downstream task gain |
| Skills | discovery, exact version selection, collision, tampering, revocation, dependency failure, and offline-cache behavior |
| Policy | allow/deny/ask, changed arguments after approval, restart while waiting, competing decisions, self-approval, and secret redaction |
| Orchestration | dependency DAG, bounded concurrency, sub-Agent failure, stale worker, mailbox ordering, workspace cleanup, and orphan reconciliation |
| Verification | incomplete-but-plausible answers, retryable correction, false completion, verifier failure, and hard completion rejection |
| Observability | event order, correlation, redaction, exporter failure, bounded retention, and measured instrumentation overhead |
| Evaluation | missing cases, grader failure, baseline drift, origin replacement, reproducibility, and report validation |

Coding tasks may be one workload family, but they do not define the Harness
boundary. Research, data transformation, long-running operations, and
permission-sensitive tasks are also required.

## Metrics

Primary metrics:

- oracle-verified task success;
- false-completion rate;
- unsafe or unauthorized side effects;
- duplicate or replayed uncertain effects;
- successful recovery after injected failure;
- exact Tool-call and Tool-result integrity.

Secondary metrics:

- total provider input/output/reasoning/cache tokens;
- provider cost under one recorded price table;
- time to first useful event, wall-clock completion latency, and p95 latency;
- peak resident memory and persistent-state growth;
- approval count and unnecessary approval rate;
- context compaction count and post-compaction answer accuracy;
- crash, hang, and manual-intervention rate.

The benchmark does not collapse safety and correctness into one opaque score.
Raw per-case outcomes and traces are retained before any summary.

## Execution rules

1. Pin every Harness/product build, adapter, model, task suite, Tool fixture,
   and repository snapshot by immutable identity.
2. Record unsupported controls before execution. Never silently substitute a
   different model, tool, permission, or sandbox.
3. Use isolated workspaces and fresh durable state for independent trials.
4. Treat provider throttling and infrastructure failures separately from
   Harness failures, while retaining both in the report.
5. Run deterministic fault tests once per exact build and stochastic model
   tasks at least ten times per cell. Publish sample count, median, p95, and a
   confidence interval for task success.
6. Preserve failed runs. Excluding a run requires a predeclared infrastructure
   rule and a recorded reason.
7. Grade blinded outputs where a model judge is unavoidable. Deterministic
   oracles take precedence over LLM judges.
8. Publish the exact authority granted to each process. A broader sandbox is a
   different benchmark cell.
9. Keep benchmark adapters outside the semantic Core. They consume public
   protocols or released CLIs like any other client.

## Superiority rule

“Y-Harness outperforms X” is allowed only for a named benchmark version and
track when all of these are true:

- Y-Harness is non-inferior on unauthorized side effects, uncertain-effect
  replay, and false completion;
- its oracle-verified task success is statistically better on the
  pre-registered primary workload aggregate;
- no critical workload family regresses beyond its declared threshold;
- cost and latency changes are reported, not hidden by the success aggregate;
- exact artifacts and raw results are available for reproduction.

Passing Y-Harness's own regression suite is necessary but cannot satisfy this
rule.

## First executable deterministic fault case

`CF-001 provider-continuation-tool-replay` is the first pre-registered local
fault case. It models a stateless reasoning provider that returns opaque state
before a Tool call.

The oracle requires all of the following:

- the capsule is bounded and durably ordered before the Tool call;
- Runtime, not the provider, stamps the actual Model identity and origin;
- the authorized Tool side effect executes exactly once;
- the Tool result's next model step receives the exact capsule;
- failure of the bound Model cannot escape to a different failover Model;
- a completed Tool chain cannot pin a later user Turn;
- provenance tampering fails closed; and
- SQLite reopen retains the capsule without replaying an uncertain effect.

Y-Harness executes this oracle in
`runtime::tests::provider_continuation_is_durable_and_replayed_through_the_tool_loop`,
`runtime::tests::provider_continuation_suppresses_cross_model_failover`,
`runtime::tests::provider_continuation_does_not_pin_a_later_user_turn`,
`runtime::tests::provider_continuation_rejects_tool_call_provenance_tampering`,
`state::tests::sqlite_reopens_and_marks_unfinished_turn_interrupted_once`, and
the OpenAI request/response mapping tests. These are Y-Harness regression
results only. Released-product adapters and raw cross-product results do not
exist yet, so `CF-001` supports no superiority claim.

## Second executable deterministic fault case

`CF-002 crossed-response-turn-steering` models additional user input arriving
while a Model is producing a final message or Tool call.

The oracle requires all of the following:

- acceptance requires the caller's exact active Turn identity;
- acknowledgement follows a durable, actor-attributed queue record;
- a response sampled from pre-steering context is never committed as current;
- provisional text from that response is explicitly invalidated;
- a Tool call crossed before its effect never executes;
- Tool call/result adjacency remains structurally valid;
- queued input becomes Model-visible only through an exact FIFO application
  record at a safe boundary;
- completion with unapplied steering fails closed; and
- pending count and bytes are bounded.

Y-Harness executes the local oracle in
`runtime::tests::steering_is_durable_fenced_and_invalidates_a_crossed_model_response`,
`runtime::tests::steering_crossing_model_inference_discards_a_stale_tool_call`,
`runtime::tests::steering_before_the_tool_effect_preserves_call_result_structure_without_execution`,
`runtime::tests::steering_pending_count_and_bytes_are_bounded_before_durable_acceptance`,
`runtime::tests::steering_remains_open_across_a_retryable_verification_gate`,
`runtime::tests::failed_steering_application_preserves_the_pending_runtime_projection`,
`state::tests::state_authority_enforces_steering_correlation_order_and_completion_fence`,
and
`protocol::tests::steering_protocol_requires_the_exact_running_turn_and_persists_acceptance`.
Those tests establish Y-Harness invariants only. Released-product adapters have
not yet executed `CF-002`, so no relative correctness, latency, or
answer-quality claim follows.

## Third executable deterministic fault case

`CF-003 uncertain-non-idempotent-tool-effect` presents a released product with
one destructive, non-idempotent stdio MCP Tool. The crash variant durably
records its first synthetic effect and terminates before returning a result.
The restart variant holds after the same effect boundary so the controller can
cancel the product before releasing the detached fixture.

The independent oracle requires all of the following:

- fixture executable, semantic spec, operation, payload, and journal are
  fingerprinted;
- preparation never replaces prior evidence;
- invocation and effect records are ordered, independently synchronized, and
  bounded;
- no disconnected Tool call is retried implicitly;
- one invocation produces exactly one uncertain effect;
- any second invocation is reported separately from a second committed
  effect; and
- partial, reordered, mismatched, active, or oversized journals fail closed.

`yh-fault-fixture` and its real official-client integration test execute this
fixture contract. A source-pinned Codex `0.145.0` format-7 driver has also
executed the single-process CF-003 path through deferred Tool discovery and
recorded one effect without replay. A format-8 driver then cancelled the first
Codex process after that effect, resumed the exact persisted Thread, observed
Codex's synthetic `aborted` Tool output, and retained one effect without a
resumed Tool call. This starts a new Turn on the same Thread and uses an
explicit controller release for Codex's re-grouped MCP child; it is not
in-place Turn continuation or a descendant-containment proof.

A format-9 Y-Harness driver now executes the same restart boundary through
real `yh serve` processes, a configured deterministic JSON-command Model,
stdio MCP, SQLite, protocol-v19 exact-Turn takeover, and a new Tool-free Turn.
Restart first preserves the abandoned Turn as `running`, proving recovery was
not implicit; explicit recovery marks that exact Turn `interrupted`, and the
oracle remains one invocation/one effect. All three observations remain
`claim_eligible: false`. See
[`fault-fixtures.md`](fault-fixtures.md).

## Implementation order

The shortest path to credible comparison is:

1. ~~add a versioned external-run result format and one released-CLI adapter;~~
   completed for format 1 plus Claude Code adapter conformance;
2. implement deterministic failure-injection Tool fixtures and state-recovery
   cases; the crash/hold-after-effect fixture, oracle, released Codex
   single-process plus same-Thread restart drivers, and Y-Harness explicit
   restart-recovery driver are complete, while other products, in-place Turn
   continuation, containment, and the remaining fault matrix are open;
3. ~~add OpenCode and Hermes Agent adapters without importing their code;~~
   completed as source-pinned formats 5 and 6. Claude Code has one ambient
   product-profile record plus one real deterministic bare fixed-output record,
   Pi, OpenCode, Hermes, and Grok Build each have one real deterministic
   fixed-output record, Codex has one real deterministic fixed-output record
   plus two real CF-003 records, and Y-Harness has one real process-restart
   CF-003 record;
4. run the Harness-control track with one mutually supported model;
5. add product-default and stochastic task suites only after deterministic
   parity is reproducible.

Provider continuation and durable safe-boundary steering are implemented and
locally fault-tested in Y-Harness. Nine real released-product records, the
source-pinned adapter contracts, and the controller-owned CF-003 fixture are
preserved. The deterministic Claude record still has a distinct Provider wire
protocol and Model coordinate from the other fixed-output cells; it is not a
Harness-control comparison. Execution of CF-001/CF-002 across products, CF-003
other-product cells, in-place interrupted Turn continuation, Linux and Windows
containment, and broader live external integration evidence remain
prerequisites for broad superiority claims, not documentation follow-ups.
