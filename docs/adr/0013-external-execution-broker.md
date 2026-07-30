# ADR 0013: External execution is a brokered authority

- Status: Accepted
- Date: 2026-07-25

## Context

CLI Tools and executable Model adapters are necessary extension points, but a
child process is not automatically a sandbox. Direct spawning also introduces
shell injection, inherited-secret exposure, unbounded output, orphan-process,
deadline, and concurrency risks.

The Runtime must not pretend that one platform-specific isolation primitive is
portable or that setting a working directory restricts filesystem access.

## Decision

One-shot executable capabilities use a typed `ProcessBroker` authority.

- The secure default broker denies every request.
- Process requests require absolute executable and working-directory paths.
- Arguments are passed directly; no shell or interpolation layer exists.
- The child environment is cleared and replaced by an exact configured map.
- Arguments, environment, stdin, each output stream, concurrency, and total
  queue-plus-execution time have hard limits.
- Stdout and stderr are drained concurrently under the same deadline. A child
  is killed on cancellation, timeout, or dropped execution future.
- Tool cancellation is reported as Tool phase. The Runtime passes the exact
  Turn cancellation token through its model-step handle to executable Model
  adapters, while the Model-phase future boundary remains the final deadline
  and drop fence.
- `JsonCommandTool`, `JsonCommandModel`, and the versioned execution and
  reconciliation Effect adapters provide typed JSON contracts over the same
  broker. External Tools still enter ordinary Tool Policy; Effect adapters
  still enter their default-deny Policy and durable Ledger boundaries.

Persistent duplex transports cannot use this completion-oriented contract.
Stdio MCP therefore uses a separate default-deny, concurrency-bounded launch
authority while sharing the same isolation vocabulary and Unix process-group
settlement. See
[ADR 0066](0066-explicit-bounded-stdio-mcp-launch-authority.md).

`LocalProcessBroker` explicitly reports `unrestricted`: it reduces process I/O
and lifecycle hazards but gives the child the Runtime user's filesystem and
network authority.

On macOS, `MacOsSeatbeltBroker` is the first concrete sandbox implementation.
It uses the system Seatbelt launcher, defaults to denying network access, and
limits filesystem writes to operator-supplied roots after canonicalization.
Filesystem reads remain allowed, so its reported mechanism is specifically
`macos-seatbelt-write-network`, not a claim of full filesystem isolation.
Additional OS implementations must likewise report the concrete restrictions
they actually enforce. Broker implementations are trusted host components; an
untrusted extension cannot self-assert a stronger isolation class.

## Consequences

Operators can add CLI Tools, model-provider bridges, and external Effect target
adapters without shelling out from the Agent Loop or duplicating cancellation
logic. Process failure and malformed JSON remain bounded capability failures.

Production deployments that require isolation must select a platform broker
whose declared restriction set matches their threat model. Selecting the
unrestricted broker is an explicit host decision and should normally require
stronger Policy for externally sourced Tools.

## Rejected alternatives

- Treating every subprocess as sandboxed: false security boundary.
- Accepting command strings: reintroduces shell parsing and injection.
- Inheriting the complete environment: leaks ambient credentials by default.
- Reading output with `wait_with_output`: permits unbounded allocation.
- Embedding one OS sandbox directly in Tool code: prevents portable policy and
  consistent evidence.
