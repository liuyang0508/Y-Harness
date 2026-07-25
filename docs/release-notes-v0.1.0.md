# Y-Harness Engineering v0.1.0

Y-Harness v0.1.0 is the first general-purpose, headless Agent Harness baseline
built around:

```text
Agent = LLM × Harness = X × Y
```

It ships an embeddable Rust Core/Runtime, Protocol v10 service, thin engine CLI,
an independently installable full-screen TUI, durable SQLite
State/Approval/Task coordination, governed extension contracts, evaluation
gates, and executable examples.

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
- client protocol: `10`
- State event/snapshot schema: `4` / `4`
- Approval Inbox schema: `2`
- Task Coordinator schema: `1`
- HTTPS Model Gateway API: `2`

Before upgrading older State or Approval databases, stop all writers and use
the documented backup-first migration commands.

## Explicit limitations

- Linux and Windows deny external execution by default but do not yet include a
  tested strong OS sandbox broker.
- Network protocol exposure requires the mandatory-mTLS host; the stdio JSONL
  service is not a raw Internet server.
- Direct vendor model adapters and a live vendor Gateway certification are not
  included.
- SQLite offers single-host durability and multi-process CAS, not multi-node
  consensus or distributed high availability.
- Workspace cleanup cannot guarantee recovery after power loss or hostile
  provider behavior.

Release claims apply only to the tagged commit and its recorded local/remote
evidence. Permanent zero-defect software is not a provable claim.
