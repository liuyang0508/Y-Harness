# ADR 0029: Authorize exact protocol capabilities by certificate principal

- Status: Accepted
- Date: 2026-07-25

## Decision

A trusted transport supplies a `ProtocolPrincipal` to `ProtocolHandler`.
Process stdio uses `LocalProcess`; the mTLS host derives a lowercase SHA-256
fingerprint from the exact client leaf-certificate DER.

Every `ProtocolCommand` maps to one stable permission. Authorization runs after
envelope validation and before any command behavior. The default authorizer
allows only local-process callers. The network reference policy maps exact
certificate fingerprints to exact permission sets:

- unknown fingerprints fail closed;
- unknown permission names are rejected at configuration time;
- authorization panics are caught and become denial;
- denial returns the content-free, non-retryable `forbidden` protocol error;
- `Initialize` advertises only capabilities the principal can actually invoke.

## Rationale

A CA trust root can cover multiple clients with different duties. Treating
every valid client certificate as full protocol authority would let a
read-only client settle approvals, start Turns, or cancel another operation.
Fingerprint grants provide a small auditable baseline without parsing or
assigning semantics to arbitrary X.509 names.

## Boundary

Fingerprint authorization is exact certificate pinning, not a complete IAM
system. It does not provide:

- subject/SAN-to-tenant or role mapping;
- certificate revocation or hot policy reload;
- delegation, groups, or separation-of-duty workflows;
- per-Thread ownership filters;
- signed approval decisions.

Those controls can replace the `ProtocolAuthorizer` behind the same fail-closed
pre-execution boundary. They must not be inferred from mTLS alone.

## Evidence

Tests prove that an authorized certificate sees only its granted capability,
can invoke that capability, receives `forbidden` for an ungranted command, and
that an unknown certificate is denied. The end-to-end mTLS test derives the
same principal from the negotiated peer certificate before serving JSONL.
