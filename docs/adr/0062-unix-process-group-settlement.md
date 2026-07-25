# ADR 0062: Bounded Unix process-group settlement

- Status: Accepted
- Date: 2026-07-25

## Context

The Local Process Broker killed and reaped its direct child, but an ordinary
descendant could survive timeout/cancellation or retain inherited stdout/stderr
pipes after the leader exited. This could leak resources and delay pipe
settlement until the outer request deadline.

Direct-child APIs cannot address a process tree. Unix process groups provide a
small, standard containment unit for descendants that do not deliberately move
to another session or group. Windows requires a different Job Object design.

## Decision

- On Unix, atomically place each spawned child in a new process group whose ID
  is the child PID before `exec`.
- Use the safe `nix` signal API from repository code; retain the crate-wide
  `unsafe_code = "forbid"` rule.
- Send `SIGKILL` to the complete group on cancellation, timeout, wait failure,
  direct-child completion, or execution-future drop.
- Reap the direct child and poll for process-group disappearance within the
  existing five-second cleanup grace. Return a cleanup error if the group
  remains observable.
- Kill remaining group members before joining pipe tasks after normal leader
  exit, so an ordinary background descendant cannot retain those pipes.
- Keep the non-Unix implementation unchanged: it settles only the direct child.
- Continue reporting `ProcessIsolation::Unrestricted`. A process group manages
  lifecycle; it removes no filesystem, network, credential, or syscall
  authority.

## Consequences

Ordinary Unix descendants that remain in the inherited group are terminated and
a real-process timeout test proves the descendant PID is gone before settlement
returns. Future drop sends the same group kill, although a synchronous `Drop`
cannot wait for reap evidence.

This is not escape-resistant containment. A hostile process can call
`setsid`/`setpgid`, and process groups are not cgroups, namespaces, macOS
Seatbelt, or Windows Job Objects. Strong Linux sandbox/resource containment and
Windows process-tree containment remain release blockers.

The Unix-only exact `nix` dependency expands the transitive surface by one
crate and remains covered by the MSRV, Clippy, and RustSec gates.

## Rejected alternatives

- Keep direct-child-only cleanup: leaves a demonstrated ordinary descendant
  leak and pipe-retention path.
- Invoke a shell `kill` utility: adds path, shell/tool availability, parsing,
  and error-settlement ambiguity to a kernel lifecycle operation.
- Add local unsafe FFI: violates the crate safety rule when a reviewed safe
  wrapper already exists.
- Claim adversarial tree containment: a Unix process group can be escaped and
  must not be described as a sandbox.
