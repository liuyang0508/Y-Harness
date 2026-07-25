# Y-Harness TUI

Optional full-screen terminal client for the headless Y-Harness Engine.

The TUI is not an execution host and owns no authoritative Agent state. At
runtime it starts `yh serve` (or `yh serve-demo`) and communicates exclusively
through Protocol v10 JSONL over the child process pipes.

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

Commands include `/thread <id>`, `/graph <id>`, `/events`, `/approvals`,
`/tasks`, `/cancel`, and `/quit`.

The client never opens Engine SQLite files or constructs Runtime, Model, Tool,
or Policy implementations. Approval settlement requires a separately
authenticated principal and is intentionally read-only in the local-process
TUI.
