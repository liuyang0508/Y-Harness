# Y-Harness TUI

Optional full-screen terminal client for the headless Y-Harness Engine.

The TUI is not an execution host and owns no authoritative Agent state. At
runtime it starts `yh serve` (or `yh serve-demo`) and communicates exclusively
through Protocol v37 JSONL over the child process pipes.
Its header renders `READY`, `AT CAPACITY`, or `DRAINING` only from the
Engine's authoritative `service.status` projection.

```bash
cargo install --locked --path clients/tui
yh-tui --demo
```

For a configured persistent Engine:

```bash
yh init my-harness
cd my-harness
yh-tui --config y-harness.json
```

Use `/trace` after a Turn to open the bounded Tool Trace evidence panel. A
complete forced-Tool diagnostic recipe is documented in
[`docs/tool-trace.zh-CN.md`](../../docs/tool-trace.zh-CN.md).

Set `YH_BIN` or pass `--engine /path/to/yh` when the Engine binary is not named
`yh` on `PATH`.

Core controls:

```text
Enter           send
Ctrl/Alt+Enter  newline
Tab, Left/Right move focus and Inspector tabs
PageUp/Down     scroll conversation
Ctrl+N          new Thread
Ctrl+R          refresh projections
Esc             cancel the active Turn
F1 or ?         help
```

The Sessions Inspector lists the latest 64 authoritative Threads and shows
direct parent lineage for forks; select one and press Enter to resume it.
`/name <title>` sets the current Engine-owned name and `/name` clears it.
`/fork [terminal-turn-id]` atomically creates and switches to an independent
child. Other commands include `/sessions`, `/thread <id>`, `/graph <id>`,
`/events`, `/approvals`, `/tasks`, `/runtime`, `/models`, `/skills`, `/packages`, `/trace`,
`/doctor`, `/reload`, `/cancel`, `/resume`, `/cancelwait`, and `/quit`.

Durable-wait control is an optional Protocol capability bundle. The TUI works
with Engines that advertise none of `turn.wait.get`, `turn.wait.resume`, and
`turn.wait.cancel`; in that mode `/resume` and `/cancelwait` fail locally with
a clear capability error and no command is sent. Advertising only part of the
bundle fails initialization closed.
`/cancel` requests cooperative cancellation of the currently attached
process-local Operation; when the Runtime observes it, that request can settle
the active Turn. It is not a substitute for durable-wait cancellation after
the worker has already released. `/resume` consumes an already settled durable
approval and `/cancelwait` closes an exact unclaimed durable wait. Normal text
is not sent as a second Turn while the current Turn is `WAITING` or `READY`.
The client rediscovers that wait from State after a supervised Engine restart;
the process-local Operation that originally reported `waiting` is disposable.

Durable release currently applies only to one non-batch Tool call at a
pre-effect Policy `ask` boundary. The TUI does not imply support for batch
release, `HumanInput`, active background expiry sweeping, Inbox repair queues,
finite worker leases, unknown-effect reconciliation, or cross-process resume
receipts. Approval settlement requires a separately authenticated approver;
`/resume` preserves the original Turn authority rather than adopting the
approver's identity.

`/runtime`, `/models`, `/skills`,
and `/packages` are read-only views of the credential-free active Runtime
catalog. `/doctor` renders the Engine's bounded preflight report in the Runtime
panel. `/reload` first runs that preflight, then replaces the child Engine only
at a settled-Turn boundary and reattaches the same durable Thread.

The client never opens Engine SQLite files or constructs Runtime, Model, Tool,
or Policy implementations. Approval settlement requires a separately
authenticated principal and is intentionally read-only in the local-process
TUI.
