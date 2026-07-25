# ADR 0027: Resolve credentials by reference at the HTTPS model boundary

- Status: Accepted
- Date: 2026-07-25

## Decision

Model configuration contains an opaque `SecretReference`, never credential
material. A versioned `SecretProvider` resolves that reference for one
correlated Thread/Turn consumer immediately before the authenticated request.
`SecretValue` cannot be serialized or cloned, renders as `[REDACTED]`, and uses
guaranteed zeroization on drop.

The first resolver reads only host-declared reference-to-environment-variable
mappings. It does not enumerate the environment, retain resolved values, or
include variable names or values in failure messages.

The first network model adapter targets an explicit Y-Harness JSON gateway:

- request body: provider-neutral `ModelRequest`;
- response body: provider-neutral `ModelResponse`;
- request and response header: exact model-gateway API coordinate `"1"`;
- bearer authentication from a short-lived resolved secret;
- HTTPS only with TLS 1.2 or newer;
- redirects, ambient proxies, referers, cookies, and automatic retries disabled;
- one total deadline, bounded concurrency, pooled connections, and incremental
  response-size enforcement;
- HTTP error bodies and malformed response bodies never enter error text.

## Rationale

Vendor APIs do not share one stable schema, and embedding keys in Runtime
configuration would spread them into debug output, persistence, and deployment
artifacts. A small gateway contract isolates vendor translation while keeping
the Harness Agent Loop, State, usage accounting, and Tool decisions typed.

Automatic retry is disabled because a request may have reached the provider
even when its response was lost. Redirects and ambient proxies could move a
bearer credential to authority not selected by the operator.

## Trust boundary

The built-in Reqwest transport enforces the declared network policy. A custom
`HttpModelTransport` is a trusted host component and must provide equivalent
TLS, retry, redirect, proxy, timeout, and response-bound guarantees.

Zeroization reduces credential lifetime in process memory; it does not claim to
erase copies held temporarily by the HTTP/TLS implementation or operating
system. Secret values never enter State, protocol frames, Trace, or default
Observability.

## Consequences

- hosts can replace environment resolution with a vault/KMS provider without
  changing the model adapter;
- the engine gains an authenticated network path without binding Kernel to
  OpenAI, Anthropic, or another vendor;
- the HTTP/TLS implementation is isolated behind the explicit `https-model`
  Cargo feature while all-feature CI keeps the optional path verified;
- direct vendor adapters, SSE vendor protocols, custom roots/mTLS, and a live
  external gateway integration pass remain separate work; the gateway's
  provider-neutral bounded NDJSON mode is specified separately;
- the HTTP/TLS dependency is justified only at this adapter boundary and is
  pinned, MSRV-tested, and RustSec-audited.
