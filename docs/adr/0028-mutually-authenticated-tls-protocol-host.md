# ADR 0028: Network protocol hosting requires mutual TLS

- Status: Accepted
- Date: 2026-07-25
- Frame ceilings superseded by: ADR 0042

## Decision

The optional `tls-host` feature accepts the existing JSONL client protocol only
after a successful mutually authenticated TLS handshake. The host receives an
operator-selected server certificate/key and client CA bundle. Clients without
a certificate chaining to that trust root receive no application data.

The host and stdio share the same `ProtocolHandler`, request validation,
response bounding, operation registry, and Agent Loop. Generic `serve_jsonl`
framing is explicitly unauthenticated; a network transport must secure the
stream before calling it.

Independent limits apply to:

- concurrent TCP/TLS sessions;
- TLS handshake duration;
- time between application frames;
- frames per session;
- the existing one-mebibyte request and response frame ceiling.

Capacity exhaustion closes only the newly accepted socket. Handshake and
session failures are isolated and counted without exposing certificate,
request, or panic content. Cooperative server shutdown stops accepting, closes
active sessions, and returns content-free counters.

## Rationale

TLS server authentication alone does not identify clients. Adding bearer
authentication inside the Agent protocol would create a second credential
frame and retain token material in application buffers. Mandatory client
certificates make encryption and connection authentication one verified
transport boundary while leaving protocol commands unchanged.

## Authorization boundary

mTLS proves that a peer possesses a certificate accepted by the configured
client trust root. ADR 0029 derives the leaf-certificate SHA-256 fingerprint as
a principal and gates exact protocol commands through a fail-closed allow-list.
This version still does not interpret certificate subject/SAN values as
tenants, roles, or Tool Policy identities.

Certificate revocation checking and hot trust-root rotation are not claimed.
Deployments should use short-lived client certificates and restart with an
updated trust bundle until those controls are implemented.

## Evidence

The integration test creates a fresh CA plus distinct server and client leaf
certificates. It proves that a client without an identity cannot receive
application data, while the authenticated client completes the ordinary typed
`Initialize` command over TLS.
