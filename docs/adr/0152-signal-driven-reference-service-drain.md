# ADR 0152: Signal-driven reference service drain preserves frame atomicity

## Status

Accepted.

## Context

The reference `yh serve` host previously entered its bounded shutdown sequence
only after stdin EOF. Container supervisors normally stop a process with
SIGTERM, and interactive supervisors use Ctrl-C. Leaving the platform default
in place could terminate the process before the Protocol Handler rejected new
Turns, accepted Operations were cancelled and settled, and optional Temporal,
Effect, probe, and MCP resources were closed.

Simply selecting a signal against Tokio's portable `stdin()` is insufficient.
That adapter delegates reads to the Runtime blocking pool; an open pipe can
leave the blocking read alive after the asynchronous future is dropped and
prevent Runtime shutdown. Abrupt `process::exit`, closing a caller-owned file
descriptor, or truncating a partially handled frame would only conceal the
problem.

## Decision

- Keep operating-system signals in the reference Host, outside Engine Core.
  Unix hosts register SIGINT and SIGTERM; Windows hosts register Ctrl-C.
- Add public transport helpers `serve_jsonl_until_cancelled` and
  `serve_jsonl_as_until_cancelled`. They accept the existing monotonic
  `CancellationToken` and observe it only while waiting for the next frame.
  Once a frame is accepted, command handling and the complete response finish
  before cancellation is checked again.
- Preserve the original `serve_jsonl` and `serve_jsonl_as` EOF contracts for
  existing embedders. TLS continues to own its connection lifecycle.
- Bridge reference-service stdin through one named detached OS thread and a
  bounded four-by-8-KiB asynchronous channel. The unavoidable blocking read is
  therefore outside Tokio's blocking pool. Dropping the async receiver releases
  a producer waiting on backpressure; a producer waiting in the OS read cannot
  keep Runtime or process shutdown open.
- Register signal receivers before optional background services start. On a
  signal, stop the JSONL receive loop, call the Handler's irreversible
  `begin_draining`, then run the existing bounded Effect, Temporal, Protocol,
  HTTP-probe, and MCP settlement sequence. Normal stdin EOF follows the same
  sequence.
- Treat signal-stream failure and transport failure as service errors, but run
  resource settlement before returning either error.

## Consequences

A healthy supervised process now exits successfully after SIGTERM even while
its stdin pipe remains open. Clients never receive a truncated response for a
frame the server already accepted, and deployment readiness derives `draining`
from the same Handler during settlement.

The detached stdin reader is deliberately a reference-host implementation
detail, not an Engine thread or a new Client protocol. It consumes at most 32
KiB of queued input plus one 8-KiB producer chunk. An OS thread blocked on an
otherwise idle stdin may live until process exit; it owns no Engine state,
credential, child process, or durable resource. This ADR does not claim
multi-process leader transfer, a global shutdown deadline across every
adapter, or graceful handling of non-catchable termination such as SIGKILL.

## Rejected alternatives

- Depend on stdin EOF: container runtimes do not promise to close stdin before
  SIGTERM.
- Drop Tokio stdin after signal: the hidden blocking-pool read can still hold
  Runtime shutdown open.
- Call `process::exit`: it bypasses the exact settlement and failure evidence
  this lifecycle exists to provide.
- Observe cancellation while handling or writing a frame: that violates JSONL
  request/response atomicity and leaves clients unable to distinguish a lost
  response from an unexecuted command.
- Put signal listeners in Core: embedded, mobile, Web, and multi-tenant hosts
  own different process lifecycles and must choose their own trigger.

## Verification

- A duplex transport test keeps input open, receives one complete initialized
  response, cancels the host token, and proves the server settles without EOF.
- A real Unix `yh serve` child exchanges a valid Protocol frame, receives
  SIGTERM while its stdin pipe remains open, and exits successfully within five
  seconds.
- The existing real service test still proves the normal EOF path.
