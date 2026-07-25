# ADR 0007: Two-stage Policy and approval settlement

- Status: Accepted
- Date: 2026-07-25

## Decision

Tool authorization has two typed stages:

1. `PolicyEngine` evaluates a fully correlated proposal and returns `allow`,
   `deny`, or `ask`.
2. `ask` creates a kernel-owned `ApprovalRequest` containing an `ApprovalId`,
   authenticated requester, Thread, Turn, call, tool, origin, input, rationale,
   and risk class. An `ApprovalHandler` then returns `approve` or `deny`.

Policy and approval decisions are separate ordered Items. The runtime persists
each decision before it proceeds toward a Tool side effect.

When no approval handler is installed, `ask` is denied. A missing UI or broker
must never silently widen authority.

## Ordering

The executable sequence is:

```text
ToolCall → PolicyDecision
         ├─ allow ───────────────────────────────→ Tool
         ├─ deny → RuntimeError → failed
         └─ ask → ApprovalDecision
                    ├─ approve ──────────────────→ Tool
                    └─ deny → RuntimeError → failed
```

Policy and approval provider failures also settle the Turn as failed. An
approval wait observes the same cancellation signal and Turn deadline as other
external capabilities, with its own `approval` stop phase.

## Rationale

Policy answers whether an action is permissible under machine-enforced rules.
Approval answers whether a human or delegated authority accepts one particular
request. Combining them would make it impossible to distinguish automatic
rules from explicit consent in traces, evaluations, and incident review.

The risk class travels with the request but does not itself grant authority.
Policies choose when approval is mandatory; handlers decide only the request
they receive.

## Current boundary

The durable Approval Inbox provides disconnect-safe request and settlement
storage. Schema 2 persists authority-scoped requesters and deciding actors and
rejects exact-actor self-approval at the CAS boundary. If the process stops
while waiting, recovery marks the Turn interrupted, orphans its request, and
does not execute the Tool. Exact Agent Loop continuation, human/tenant role
identity, and signed decision receipts remain outside this decision.
