# ADR 0054: Bounded direct-child process settlement

- Status: Accepted; Unix descendant boundary superseded by ADR 0062
- Date: 2026-07-25

## Context

The opt-in local Process Broker bounded each request's input, output, and
queue-and-execution timeout, but accepted any nonzero concurrency setting.
After the direct child exited, pipe tasks observed only the absolute request
deadline. A descendant retaining an inherited pipe could therefore make
cooperative cancellation wait until a configured deadline as long as 24 hours.

Cancellation and timeout requested termination and then waited for the direct
child without a cleanup deadline or an explicit reap result. This made the
external-work boundary less precise than the Runtime's cancellation contract.

## Decision

- Accept a local Process Broker concurrency setting only in the inclusive range
  1–4096.
- Keep the Runtime cancellation token active while settling stdin, stdout, and
  stderr tasks after direct-child exit.
- Abort all pipe tasks before cancellation, timeout, or wait-error cleanup.
- Request direct-child termination and wait for its exit for at most five
  seconds.
- Return the normal cancellation or timeout outcome only after direct-child
  settlement succeeds. Report cleanup failure explicitly otherwise.
- Do not expose asynchronous task panic payloads through execution errors.
- State the authority boundary literally: this implementation settles the
  direct child and does not claim to terminate its descendant process tree.

## Consequences

Process admission has a finite operator-controlled ceiling, and cancellation
cannot become stuck solely in pipe settlement. A request's queue-and-execution
deadline may be followed by at most five seconds of direct-child cleanup before
the broker returns.

The direct child is reaped when cleanup succeeds, preventing it from remaining
as a zombie owned by the Runtime. Descendants that escaped the direct process
boundary may remain alive and may retain other operating-system resources.
Cross-platform process-group or job-object containment is therefore a release
blocker, not an implicit property of the local or macOS Seatbelt broker.

ADR 0062 later adds bounded Unix process-group settlement for ordinary
descendants. Escape-resistant Unix containment and Windows Job Objects remain
outside this ADR's direct-child guarantee.

## Rejected alternatives

- Wait indefinitely after requesting termination: cleanup could violate every
  upper-layer shutdown deadline.
- Return immediately after sending a kill request: the Runtime would not know
  whether its direct child had actually been reaped.
- Claim process-tree termination from direct-child APIs: this is not portable
  and is not supported by the current implementation or tests.
- Add a platform process-management dependency now: it would expand the trusted
  execution surface without first defining and testing a portable containment
  contract.
