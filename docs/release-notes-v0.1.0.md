# Y-Harness Engineering v0.1.0

Y-Harness v0.1.0 is the first general-purpose, headless Agent Harness baseline
built around:

```text
Agent = LLM × Harness = X × Y
```

It ships an embeddable Rust Core/Runtime, Protocol v12 service, thin engine CLI,
an independently installable full-screen TUI, durable SQLite
State/Approval/Task coordination, governed extension contracts, evaluation
gates, and executable examples.
The persistent service can assemble an optional direct OpenAI Responses
Provider, shell-free JSON-command Tools, exact-selected MCP Tools, and Agent
Memory Hub Context without moving Policy or State authority into a client or
provider. Schema 5 adds bounded, origin-bound Provider Continuation so
stateless OpenAI reasoning Tool loops can replay encrypted reasoning state
without transferring Tool authority to the vendor.
Schema 6 adds durable, actor-attributed, exact-Turn steering with crossed
response invalidation and safe Tool boundaries.

## Start

```bash
./scripts/install.sh
./scripts/install-tui.sh
yh demo "hello Y-Harness"
yh-tui --demo
yh init my-harness
cd my-harness
yh doctor
yh serve
```

## Compatibility

- Rust crate: `0.1.0`
- optional TUI package: `0.1.0`
- service configuration: `1`
- client protocol: `12`
- State event/snapshot schema: `6` / `6`
- Approval Inbox schema: `2`
- Task Coordinator schema: `1`
- HTTPS Model Gateway API: `3`

Before upgrading older State or Approval databases, stop all writers and use
the documented backup-first migration commands.

## Explicit limitations

- Linux and Windows deny external execution by default but do not yet include a
  tested strong OS sandbox broker.
- Network protocol exposure requires the mandatory-mTLS host; the stdio JSONL
  service is not a raw Internet server.
- OpenAI Responses is the only direct vendor model adapter. Its mapping and
  transport tests are local; a live API pass remains environment-gated.
  Schema-5 origin-bound continuation handles replayable encrypted reasoning;
  a function call whose reasoning state is not replayable still fails before
  Tool execution.
- SQLite offers single-host durability and multi-process CAS, not multi-node
  consensus or distributed high availability.
- Workspace cleanup cannot guarantee recovery after power loss or hostile
  provider behavior.

Release claims apply only to the tagged commit and its recorded local/remote
evidence. Permanent zero-defect software is not a provable claim.
