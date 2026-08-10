# ADR 0147: Bounded SWE-agent trajectory adapter

- Status: Accepted
- Date: 2026-08-02

## Context

SWE-agent is a vertical software-engineering Agent rather than a durable
Harness runtime. Its main loop asks a Model for exactly one parsed action,
executes that action through SWE-ReX, appends observation/history state, and
rewrites a `.traj` file. Y-Harness needs executable evidence for that behavior
without importing SWE-agent as its semantic Core or treating SWE-agent's
command blocklist as approval governance.

The public `sweagent run` command has no independent stable version envelope.
The generated trajectory does record `swe_agent_version`, the source Git
commit, SWE-ReX version, Model statistics, terminal status, submission, steps,
history, replay config, and environment label.

## Decision

- Add external-run format 10 and a `swe-agent` adapter to the independent
  benchmark-runner package.
- Support only an isolated `bare` profile with absolute, pairwise-disjoint
  workspace, source, home, output, and temp roots.
- Pin the SWE-agent launcher and selected config by SHA-256. Force imports and
  config/Tool resolution through the caller-supplied source root; validate the
  expected source commit, SWE-agent version, and SWE-ReX version from `.traj`.
- Require an explicit loopback Provider base URL and explicit inherited
  environment-name allowlist. Own Python/SWE-agent discovery environment and
  use an explicit empty `.env` so ambient dotenv files are not loaded.
- Send the problem statement through an owner-only create-exclusive file. Fix
  the exact Model, system template, model-call/cost limits, workspace,
  problem identity, and output root on the CLI.
- Force `open_pr=false` and `apply_patch_locally=false`. Preserve the Tool
  bundles selected by the pinned config so the real software-engineering ACI
  loop is measured.
- Accept only the exact bounded trajectory path. Strictly validate the root,
  history, steps, version coordinates, terminal status, submission, and Model
  statistics before emitting a settlement.
- Map a coherent non-submission trajectory to `product_error`; missing,
  malformed, oversized, escaped, or coordinate-mismatched trajectory evidence
  is `adapter_error`.
- Keep `claim_eligible: false`; do not add a live or comparative record in
  this slice.

## Consequences

Y-Harness can now invoke real SWE-agent code and preserve its native Agent-loop
evidence under the same bounded external-run vocabulary used for other
products. The integration remains outside the Harness semantic Core and does
not weaken Y-Harness's durable Thread/Turn/effect model.

The adapter intentionally records several gaps. `.traj` is rewritten after a
step and is not a durable effect journal. Model call and cost limits are
checked after a call. The blocklist asks the Agent to retry and is not an
authorization boundary. The trajectory does not independently settle
Provider/Model identity, dependency graph, or SWE-ReX container identity. A
reported source commit does not prove the checkout is clean, and the selected
config digest does not transitively pin referenced Tool bundle contents.

## Rejected alternatives

- Replace Y-Harness's Core loop with SWE-agent: this would discard durable
  recovery, effect-ledger, approval, and multi-tenant governance semantics.
- Run SWE-agent through an opaque shell command: this loses exact arguments,
  environment allowlisting, timeout, output bounds, and process evidence.
- Treat stdout logs as settlement: SWE-agent's structured result lives in the
  `.traj`, while stdout is diagnostic logging.
- Call the command blocklist an approval system: blocked actions are fed back
  to the Model for another attempt; no principal grants scoped authority.
- Promote `model_stats.instance_cost` to actual Provider spend: the trajectory
  does not establish billing settlement.

## Evidence

- `swe_agent::tests::spec_requires_loopback_and_safe_output_component`
- `swe_agent::tests::arguments_keep_prompt_off_argv_and_fix_product_actions`
- `swe_agent::tests::trajectory_settles_version_commit_steps_and_submission`
- `swe_agent::tests::trajectory_rejects_wrong_source_or_excess_calls`
- `swe_agent::tests::non_submission_is_a_product_error_not_an_adapter_error`
- `swe_agent::tests::process_broker_run_settles_the_exact_trajectory_file`

## Source coordinate

- SWE-agent
  [`abd7d69`](https://github.com/SWE-agent/SWE-agent/tree/abd7d69724d1413b30fea43d4724bb5b463906b4)
  `sweagent/run/run_single.py`, `sweagent/agent/agents.py`,
  `sweagent/agent/models.py`, `sweagent/tools/tools.py`,
  `sweagent/environment/swe_env.py`, and `sweagent/types.py`
