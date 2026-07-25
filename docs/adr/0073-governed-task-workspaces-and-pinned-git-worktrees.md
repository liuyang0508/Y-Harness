# ADR 0073: Governed Task workspaces and pinned Git Worktrees

- Status: Accepted
- Date: 2026-07-25

## Context

`TaskDefinition::workspace` declared `None`, `Isolated`, or `SharedReadOnly`,
but the Orchestrator previously passed that declaration to `TaskExecutor`
without provisioning or checking anything. Every host had to reproduce
allocation, deadline, cancellation, cleanup, and path-safety behavior. A host
could also report Task success before discovering that its temporary directory
or Git Worktree leaked.

Workspace provisioning is not the same as an operating-system sandbox. A
unique directory prevents accidental sibling overlap, while an unrestricted
process can still write elsewhere. Git Worktrees isolate checked-out trees and
Git indexes, not process authority.

## Decision

- Add embedded Workspace Provider API version 1. A provider exposes frozen
  identity and an honest provisioning class: denied, directory, or Git
  Worktree.
- Separate the lifecycle into `WorkspaceRequest`, provider-owned
  `WorkspaceLease`, and executor-visible `TaskWorkspace`. The executor never
  receives the opaque cleanup token.
- Make `DenyWorkspaceProvider` the Orchestrator default. It accepts only
  `WorkspaceMode::None`; filesystem Tasks fail before executor entry unless the
  host explicitly installs a provider.
- Count preparation inside the configured Task timeout. Panic-isolate provider
  descriptor capture and the construction, polling, and drop of prepare and
  release Futures.
- On preparation cancellation or timeout, cancel the provider and allow one
  second for it to settle partial work. Bound ordinary release to four seconds.
  A Task lease must exceed the Task timeout plus a conservative five-second
  cleanup budget.
- Cancel the Task execution token immediately when its Future settles, then
  release its workspace before coordinator settlement. A release failure turns
  executor success into Task failure; an already failed result retains both
  bounded reasons.
- Provide `LocalDirectoryWorkspaceProvider`. Allocation containers are
  canonical direct children of one non-root managed directory and use mode
  `0700` on Unix. The executor sees only the nested `workspace/`; its private
  bounded marker remains in the provider-owned parent, so the Task may empty or
  remove its own root without erasing cleanup authority. Cleanup refuses
  container symlink, replacement, marker-mismatch, escaped-path, and
  oversized-marker cases. Preparation uses a drop guard so cancellation cannot
  leak its small partial directory.
- Allow an optional canonical shared root for `SharedReadOnly`. Read-only
  behavior remains a trusted-executor contract unless a Process Broker or
  mount policy enforces it.
- Provide `GitWorktreeWorkspaceProvider`. It accepts only a full 40- or
  64-character hexadecimal object ID, creates a detached Worktree, invokes an
  absolute Git executable through an explicit Process Broker, clears inherited
  environment through that broker, never invokes a shell, bounds time/output,
  and exposes only the nested `worktree/` inside a marker-bound managed
  container. Release removes only that exact Worktree and container.
- Keep Workspace allocations process-local and outside Task Graph schema 1.
  `TaskWorkspace` and provider contracts are embedded Rust APIs; Protocol 9 and
  durable Task JSON remain unchanged.

## Consequences

The public Orchestrator now owns a complete
prepare → execute → cancel → release → settle sequence. Concurrent Tasks receive
different isolated roots, timeout and fencing run cleanup, and a Task cannot be
recorded complete while known cleanup failure remains.

The provider class does not claim sandbox strength. Untrusted executors still
run through a Process Broker whose filesystem and network policy includes only
the intended roots. In particular, `SharedReadOnly` is not enforced by normal
directory permissions, and a detached Worktree is not a security boundary.

A power loss, process kill beyond the bounded drain, or hostile provider can
leave an orphan. Built-in release is idempotent and safe to retry while the
`WorkspaceLease` is retained, but no durable cross-host allocation journal or
automatic orphan reaper is claimed. Operators must place managed roots on
bounded storage and reconcile abandoned entries under an explicit
exclusive-ownership procedure. Distributed workspace ownership remains
separate from Task settlement fencing.

The Git provider mutates the source repository's Worktree administration area.
Its Process Broker policy must explicitly allow both that repository metadata
and the managed Worktree root. A pinned revision avoids branch races but does
not verify remote repository authenticity; repository acquisition remains a
host responsibility.

## Rejected alternatives

- Let each `TaskExecutor` allocate its own directory: duplicates critical
  cleanup and deadline behavior and cannot fail settlement on known leaks.
- Create a Worktree for every Task by default: requires Git, mutates repository
  metadata, and makes a coding workflow part of the general Harness kernel.
- Treat directory uniqueness as a sandbox: overstates the security guarantee.
- Pass cleanup credentials to the executor: permits forged or premature
  release.
- Delete any path returned by a provider: enables broad or symlink-directed
  destruction.
- Report success before cleanup and log leaks asynchronously: durable Task
  state would contradict the known lifecycle result.
