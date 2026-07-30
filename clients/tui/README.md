# Y-Harness TUI

Optional full-screen terminal client for the headless Y-Harness Engine.

The TUI is not an execution host and owns no authoritative Agent state. At
runtime it starts `yh serve` (or `yh serve-demo`) and communicates exclusively
through Protocol v30 JSONL over the child process pipes.

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
`/events`, `/approvals`, `/tasks`, `/cancel`, and `/quit`.

The client never opens Engine SQLite files or constructs Runtime, Model, Tool,
or Policy implementations. Approval settlement requires a separately
authenticated principal and is intentionally read-only in the local-process
TUI.
