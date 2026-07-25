# ADR 0057: Exclusive private model-gateway roots

- Status: Accepted
- Date: 2026-07-25

## Context

The built-in HTTPS model transport used platform/WebPKI roots. That is suitable
for a public gateway but cannot authenticate a private enterprise CA. Merely
adding a custom root to the ambient set would also retain every public root,
silently widening authority beyond the operator-selected gateway PKI.

TLS material is configuration input and must be bounded before parsing. A PEM
bundle copied into derived debug output would also create unnecessary
certificate disclosure and log growth.

## Decision

- Add an explicit `with_exclusive_root_certificates_pem` configuration mode.
- Bound the PEM input to 1 MiB and 1–64 decoded certificates.
- Parse and validate the bundle during configuration, before a model can be
  registered or a request can begin.
- Configure Reqwest with `tls_certs_only`, disabling native and built-in WebPKI
  roots for that client.
- Report only whether exclusive roots are configured in `Debug`; never print
  the PEM bytes.
- Keep the default public-root behavior unchanged when the mode is absent.
- Prove the built-in Reqwest path with a generated private CA and real local
  TLS server, not only a mocked HTTP transport.

## Consequences

Private gateways can use an operator-selected CA without ambient public trust.
The setting is per pooled model client, so different registered providers may
have different trust roots without global TLS mutation.

This decision adds server-authentication trust only; client-certificate mTLS is
a separate lifecycle decision in ADR 0058. Certificate revocation and rotation
are host configuration lifecycle concerns; changing the bundle requires
constructing a new model client.

## Rejected alternatives

- Merge custom and ambient roots: it does not enforce the claimed private trust
  boundary.
- Disable certificate or hostname verification: this removes authentication
  and is never an acceptable fallback.
- Read an arbitrary CA path inside the transport: file authority and lifecycle
  belong to the host, while the adapter accepts already bounded bytes.
- Trust only mocked-transport tests: they cannot prove the real TLS client
  accepts the private CA or sends the gateway contract over HTTPS.
