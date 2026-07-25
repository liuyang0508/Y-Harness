# ADR 0033: Pinned HTTPS Skill acquisition

- Status: Accepted
- Date: 2026-07-25

## Context

The Skill Registry can verify an externally supplied signed package, but hosts
still need a standard way to acquire one without giving the model ambient URL,
redirect, proxy, or update authority. Per-resource limits alone also permit a
large aggregate package to trigger excessive digest and JSON allocations.

## Decision

- Bound raw manifest/instruction/resource content to 2 MiB total, canonical
  digest JSON to 16 MiB, dependencies to 256, required Tools to 256, and
  resources to 256. Individual instruction/resource bounds continue to apply.
- Add the opt-in `https-skill` feature. Headless embeddings do not compile the
  HTTP/TLS stack when they do not use remote acquisition.
- Configure one `HttpsSkillSource` with one exact operator-controlled URL.
  Require HTTPS, a host, and at most 8,192 URL bytes; reject userinfo, query, and
  fragment components.
- Require callers to supply exact `SkillId` and lowercase SHA-256 content pin
  for every fetch. A response with different identity or content never reaches
  Registry.
- Use TLS 1.2+, a pooled client, disabled redirects, automatic retries,
  Referer, ambient proxies, and cookies, a five-minute maximum request timeout,
  64 maximum concurrent requests, and a 16 MiB maximum retained response.
- Require successful HTTP status and `application/json`. Check declared length,
  then read chunks incrementally and stop at the configured bound. Never retain
  or echo an error response body.
- Keep transport errors content-free at the source boundary.
- `fetch` returns a pin-checked but still untrusted signed package.
  `fetch_and_register` is the safe convenience path: it verifies publisher
  signature, live revocation/expiry, and transparency policy before the first
  Registry mutation.
- Allow a host-supplied transport only as a trusted component required to
  preserve the same network and resource invariants.

## Consequences

Y-Harness can acquire an exact public package over production TLS without
turning Skills into executable installers or letting remote metadata select the
next URL. Package integrity, publisher authenticity, and transparency remain
separate checks, all required before registration.

The built-in source intentionally has no bearer or signed-URL support because
query credentials are forbidden and no secret should be added casually to an
artifact fetch path. Authenticated private registries need an explicit
`SecretReference` contract. Catalog discovery, dependency fetching, caching,
offline mirrors, update policy, availability failover, and a live external
endpoint integration pass remain separate work.

The response and decoded package coexist briefly during JSON parsing, but both
are independently bounded. The 2 MiB raw aggregate keeps worst-case JSON
escaping within the 16 MiB canonical and transport envelopes.

## Rejected alternatives

- Accept a model-provided URL: turns package retrieval into SSRF and ambient
  install authority.
- Trust package name/version from the response: a registry could substitute a
  different package.
- Follow redirects or environment proxies: the configured authority would not
  be the contacted authority.
- Retry automatically: fetch uncertainty and registry availability policy
  belong to the caller.
- Fetch dependencies recursively: dependency resolution must not acquire
  unreviewed transitive packages.
