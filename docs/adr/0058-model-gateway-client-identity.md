# ADR 0058: Model-gateway client identity

- Status: Accepted
- Date: 2026-07-25

## Context

An exclusive private CA authenticates the model gateway to Y-Harness but does
not authenticate Y-Harness to the gateway. Enterprise deployments may require
TLS client certificates in addition to the bearer credential carried inside
the authenticated channel.

The existing model configuration contains only non-secret references. Putting a
private key into that cloneable configuration would make accidental retention
and logging easier. Resolving the identity on every request would require
rebuilding the TLS client and lose connection pooling.

## Decision

- Accept the combined PEM certificate chain and private key as a bounded,
  non-serializable `SecretValue` only when constructing the Reqwest transport.
- Parse exactly one private key and at least one certificate through Reqwest's
  rustls identity boundary; reject malformed material before registration or
  network activity.
- Retain only the parsed identity inside the pooled TLS client. Do not add it to
  `HttpsJsonModelConfig`, State, protocol, Trace, or debug output.
- Keep ordinary server-authentication-only construction unchanged.
- Rotate the identity by constructing a new transport/model client rather than
  mutating a live connection pool.
- Prove the path with a generated private CA and a real gateway that rejects a
  client without a certificate before accepting the configured identity.

## Consequences

The built-in model transport supports public TLS, exclusive private CA trust,
and private CA plus client-certificate mTLS without introducing a second HTTP
implementation. Bearer secrets remain resolved per Turn; the longer-lived TLS
identity is deliberately tied to the pooled client lifetime.

Hosts remain responsible for resolving the identity from their secret manager
at startup and reconstructing the client before expiry or revocation. The
engine does not claim hot mutation of an active TLS identity.

## Rejected alternatives

- Store identity PEM in `HttpsJsonModelConfig`: that makes secret material
  cloneable configuration and weakens the existing secret boundary.
- Rebuild Reqwest for every request: it discards pooling and makes latency and
  resource use worse without improving the trust model.
- Reuse the bearer token as TLS identity: application authentication and
  certificate authentication are separate protocols and lifecycles.
- Disable server verification when a client certificate is configured: mutual
  authentication requires both directions, not client authentication alone.
