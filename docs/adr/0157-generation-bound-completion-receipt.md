# ADR 0157: generation-bound completion receipt and terminal boundary

- Status: accepted
- Date: 2026-08-03

## Context

A Model message that stops requesting Tools is only a completion candidate. It
does not prove that the candidate is current, that required verification ran,
or that the Runtime generation which defined those checks is still active.
Persisting verifier outcomes and a later `Completed` event separately also
leaves a crash window in which State cannot distinguish work that must be
reverified from a terminal commit that only needs to be observed.

The completion boundary must also distinguish task delivery from transport
delivery. A business request such as publishing a document may require an
authoritative external Effect before completion. Returning an already completed
answer through TUI, Web, IM, or API happens afterwards and cannot prove its own
delivery before the response is sent.

## Decision

### 1. `EndTurn` is a candidate, not a terminal state

A text response from the Model enters completion verification. It remains an
ordinary durable `AssistantMessage` candidate until deterministic Runtime code
proves that it is the current candidate and that every required completion gate
has settled.

Retryable verification returns evidence to the Agent Loop and permits a new
candidate. Non-retryable failure may settle the Turn only after the same
current-candidate fence. A Model-based Coordinator may select a text-only route,
but that route still uses State, budgets, verification, and this completion
boundary.

### 2. Completion is one atomic State transition

New writes represent success as one optimistic transition equivalent to:

```text
TurnCompleted { turn_id, receipt }
```

State validates the receipt against the exact projection head and commits the
receipt and terminal status atomically. A separate receipt append followed by a
separate receipt-free `Completed` write is forbidden. A conflict requires a
full reload and revalidation of the authoritative projection; refreshing only
the stream head is insufficient.

The bounded receipt binds, directly or by digest:

- the exact source Thread, Turn, candidate Item, candidate content, and Model
  request;
- the ordered Turn evidence prefix accepted for completion;
- the completion contract and frozen execution-generation coordinate;
- the required Verifier manifest and candidate-bound outcomes; and
- the Turn-internal completion evidence covered by this version.

Receipt construction is a pure versioned reduction over authoritative durable
evidence. It excludes clocks, random receipt identities, secrets, hidden model
reasoning, provisional stream text, and mutable descriptions. The same State
and generation therefore produce the same receipt digest.

The receipt stores `source_thread_id` and `turn_id` explicitly. A direct
completion transition requires both identities to match the exact projected
running Turn. Fork and archive import preserve the receipt bytes rather than
rewriting evidence: after the target stream rebinds `Turn.thread_id`, the
receipt is an inherited proof that the source stream completed that Turn, not a
claim that the child stream executed the completion gates again. State permits
that placement only for a stream carrying validated fork/import provenance;
the lineage or archive journal digest proves the copied history, while the
receipt continues to prove its source completion. A stream without such
provenance must reject a source-Thread mismatch.

### 3. Steering fences candidate identity

Before recording a Verifier result, treating a hard Verifier failure as
terminal, or committing completion, Runtime locks the exact active Turn and
checks pending Steering. Accepted Steering is applied first and supersedes the
old candidate. Results produced for that stale candidate do not authorize its
completion.

The final completion CAS is performed while further Runtime Steering is sealed.
The only legal outcomes of a Steering/completion race are:

- Steering commits first, the candidate is superseded, and the Loop continues;
  or
- the receipt commits first and the later Steering submission is rejected.

`Completed` with unapplied Steering is never a valid projection.

### 4. Business delivery readiness precedes completion

The Completion Contract may require answer shape, durable Artifacts, or an
authoritative business Effect. Such requirements are delivery readiness and
must be satisfied before the receipt. An unresolved or `unknown` external
mutation enters a durable wait/reconciliation path; it is never inferred as
successful from Model text or an ordinary Tool result.

### 5. Channel delivery follows completion

Transporting the completed payload through CLI, TUI, Web, Desktop, Mobile, IM,
Voice, IDE, API, SDK, or Webhook is a separate delivery aggregate. A durable
outbox should bind its payload digest to the CompletionReceipt digest and own
retry, acknowledgement, expiry, and dead-letter behavior.

Channel failure does not reopen or rewrite a completed Turn. If sending through
an external channel is itself the user's business task, that send is a governed
Effect whose receipt is a pre-completion requirement, not evidence that the
interactive client received the final response.

### 6. Post-terminal work is isolated derived work

Memory extraction, Thread title generation, follow-up suggestions, experience
candidates, audit export, and offline Evaluation run after completion as
separate idempotent jobs. They read the immutable receipt and evidence prefix,
write separate aggregates or projections, and cannot replace the candidate,
change the receipt, or change `Completed`.

Each job is keyed by the Turn, receipt digest, job kind, and job version. Its
failure is observable and independently retryable. A Memory write requested as
the task itself must instead execute as a governed pre-completion Tool or
Effect. Experience or self-evolution output remains a candidate until it passes
evaluation, approval, versioning, publication, and rollback governance.

## Required ordering

```text
persist current Assistant candidate and Model-request digest
  → run the frozen candidate-bound Verifier manifest
  → at each result/terminal boundary apply accepted Steering first
  → resolve required Turn-internal completion evidence
  → build and locally revalidate the deterministic receipt
  → atomically commit Completed(receipt) against that exact State head
  → enqueue channel delivery and other post-terminal derived jobs
```

## V1 boundary

The first receipt version proves only the candidate, frozen completion
generation, Verifier outcomes, and evidence represented inside the owning Turn.
It does not make cross-aggregate Task Artifacts, Effect Ledger receipts, channel
delivery, or post-terminal job results part of the proof merely because those
subsystems exist. A V1 host must mark such requirements as not required, keep
the Turn waiting, or fail closed; it must not encode an empty collection as
proof of satisfaction.

The generation digest proves equality with the frozen manifest supplied to the
Turn. Its assurance is limited by the measurements present in that manifest; a
declared capability coordinate is not automatically a binary or remote-service
attestation.

Receipt-free completed Turns written by supported legacy State schemas remain
readable for compatibility. They are reported as legacy/unverified, are never
given a synthetic receipt, and cannot be used as evidence that V1 completion
conditions ran. New-schema writers cannot create receipt-free completion.

## Consequences

- Completion becomes independently replayable and no longer depends on a
  caller's in-memory `final_text` value.
- Forked and imported history retains its original proof identity and is
  distinguishable from a completion performed directly in the target stream.
- A crash before the atomic terminal event leaves recoverable non-terminal
  evidence; a crash after it leaves an authoritative completed receipt.
- Quality repair remains inside the Agent Loop, while channel delivery and
  enrichment cannot expand the Turn state machine or corrupt terminal truth.
- Cross-aggregate completion proof requires later authority-fenced immutable
  references and conformance tests; this ADR deliberately does not claim that
  integration is implemented by V1.
