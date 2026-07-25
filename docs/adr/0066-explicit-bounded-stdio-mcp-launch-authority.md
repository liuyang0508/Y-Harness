# ADR 0066: Explicit bounded stdio MCP launch authority

- Status: Accepted
- Date: 2026-07-25

## Context

The ordinary external Tool and Model adapters execute one request through a
`ProcessBroker`. A persistent stdio MCP connection is different: its transport
must retain a duplex child process for multiple protocol calls, so the
one-request broker cannot own that lifecycle.

The original MCP transport launched its child directly after validating an
absolute executable, clearing the environment, and applying protocol bounds.
That still left three unsafe ambiguities:

- constructing the client implicitly authorized an unrestricted process;
- the working directory could be inherited from the Runtime; and
- Unix descendants did not share the ordinary process-group settlement path.

The child also inherited stderr, allowing untrusted provider output to reach
the host's diagnostic channel outside Y-Harness bounds and redaction.

## Decision

- Require every `StdioMcpClient` constructor to receive a
  `StdioMcpLaunchAuthority`.
- Make the authority's default and `denied()` mode reject launch before
  `Command::spawn`.
- Require an explicit `unrestricted(maximum_concurrency)` choice to run local
  MCP servers. The validated range is 1–4,096.
- Hold one authority semaphore permit for the complete live MCP session.
  Queueing uses the configured request timeout and cannot create an unbounded
  launch backlog.
- Report the selected authority through the existing `ProcessIsolation`
  vocabulary. `Unrestricted` truthfully means the child retains the Runtime
  user's filesystem, network, credential, and syscall authority.
- On macOS, allow the authority to reuse the same tested Seatbelt policy as the
  one-shot broker. It may deny network and restrict writes to canonical
  operator-supplied roots while retaining read access required by dynamic
  runtimes. `/dev/null` remains writable for ordinary process compatibility.
- Require both executable and working-directory paths to be absolute, clear
  the inherited environment, pass arguments without a shell, and discard child
  stderr.
- On Unix, place the MCP child in a private process group and reuse the
  execution subsystem's bounded descendant settlement during graceful close,
  timeout, and drop. Non-Unix platforms retain the documented direct-child
  boundary.
- Preserve MCP frame, pagination, catalog, argument, result, lifecycle,
  reconnection, and no-automatic-side-effect-retry rules.

`StdioMcpLaunchAuthority` is a transport-specific persistent-process authority,
not a second Agent Policy Engine. Tool calls discovered through MCP still pass
through the ordinary Tool registry, Policy/approval, State, cancellation, and
Verification path.

## Consequences

An embedding host can no longer enable an unrestricted MCP process by calling
the simplest constructor. It must make a visible authority choice and a finite
concurrency decision.

On macOS, the Agent Memory Hub live integration runs one session under
Seatbelt with network denied, offline hashing embeddings selected, and writes
limited to its isolated brain plus the platform temporary directory required
by the shell launcher. The same search/write/read/shutdown round trip proves
that the sandbox path is usable rather than configuration-only.

This closes an implicit launch-authority bypass but does not claim portable
strong OS containment. Seatbelt currently scopes write and network authority,
not reads or every syscall. A hostile unrestricted process can escape a Unix
process group by creating a new session/group. Linux and Windows persistent
MCP sandbox launchers remain unsupported; production hosts on those platforms
must keep the authority denied, explicitly accept unrestricted execution, or
place the complete Runtime inside an external sandbox.

The Rust constructor signature and `StdioMcpConfig.current_dir` shape change
before the first published release. Client protocol and durable schemas do not
change.

## Rejected alternatives

- Reuse the one-shot `ProcessBroker` unchanged: it returns completed output and
  cannot retain a bounded duplex transport across calls.
- Keep direct launch and document it: construction would remain an implicit
  unrestricted execution grant.
- Inherit the Runtime working directory or stderr: both create ambient,
  host-dependent authority and data-flow paths.
- Label a child process as sandboxed merely because it is out of process:
  process separation alone does not restrict operating-system authority.
