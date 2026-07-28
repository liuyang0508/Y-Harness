# Y-Harness CF-003 process-restart fault-conformance probe

This directory preserves one real Y-Harness Engine restart run, not a
comparative benchmark.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Engine CLI: `yh 0.1.0`
- Client Protocol: `19`
- Adapter track: `fault_conformance`
- Case: `CF-003 Y-Harness restart-after-uncertain-effect`
- Claim eligible: no

The setup service created one durable Thread. A second real `yh serve` process
used the configured spec-bound JSON-command Model to select the exact
`fault.commit_effect` stdio MCP Tool. The fixture durably recorded one
synthetic non-idempotent effect and withheld its Tool result. The controller
read the authoritative Thread at that boundary and killed the service process.

The held MCP child required the identity-bound
[`release-marker.txt`](release-marker.txt); the record therefore does not
claim complete descendant process-group cleanup or independently prove that
detached process's exit. After the release marker was durably persisted, the
independent oracle reported exactly one invocation and one effect.

A third `yh serve` process opened the same SQLite State. Before takeover it
projected the exact abandoned Turn as `running`, proving service startup did
not infer worker death or replay work. The controller then used protocol-v19
`recover_thread` with that exact Turn identity. The Turn became
`interrupted`, retained one Tool call and zero Tool results, and a new Turn on
the same Thread completed with the fixed Tool-free message
`Y_HARNESS_CF003_RESTART_OBSERVED`. The final oracle still reported one
invocation and one effect.

The exact inputs are [`spec.json`](spec.json) and
[`fixture-spec.json`](fixture-spec.json). The effect oracle is
[`journal.jsonl`](journal.jsonl), and [`result.json`](result.json) contains
bounded process, compatibility, State, and fixture evidence.

This run does not continue the interrupted Turn stack in place, prove a
distributed ownership lease, prove descendant containment, measure real-Model
reasoning quality, or establish competitive superiority.
