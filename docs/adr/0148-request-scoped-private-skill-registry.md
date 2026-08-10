# ADR 0148: request-scoped private Skill Registry

- Status: accepted
- Date: 2026-08-02

## Context

ADR 0145 deliberately limited Catalog acquisition to public HTTPS. That
proved exact discovery and signed dependency resolution, but an enterprise
operator commonly needs an authenticated Registry and an internal certificate
authority. Passing credentials in URLs or CLI arguments, trusting arbitrary
cross-origin package links, or retaining credentials with provenance would
violate the existing Secret and package-supply-chain boundaries.

Registry access must improve operator ergonomics without making a Registry an
authority to select mutable versions, sign packages, activate Skills, or run
publisher code.

## Decision

1. Service schema 1 accepts a bounded, default-empty list of named Skill
   Registries. Each entry fixes one exact HTTPS Catalog endpoint and a sorted,
   unique allowlist of canonical HTTPS origins for Package endpoints.
2. A Registry may be public or use one typed Bearer mapping composed from a
   Secret reference and host environment name. The Secret Provider resolves
   the value under the service's trusted Authority immediately before every
   Catalog and Package request. The value is neither serializable nor
   cloneable and is carried only by that request.
3. A Registry may replace ambient web roots with one bounded project-contained
   PEM CA bundle. Exclusive trust prevents a private endpoint policy from
   silently falling back to public roots.
4. Catalog and Package acquisition retain ADR 0145's independent raw Catalog
   SHA-256, exact target identity, signed manifest dependencies, content pins,
   no redirects/proxy/retry, bounded bodies, yanked/cycle/closure limits, cache
   immutability, and inactive installation. Package-origin admission occurs
   before Secret resolution or network entry.
5. `registry-search`, `registry-install`, and `registry-upgrade` select one
   configured Registry by stable ID. Upgrade still invokes the ordinary full
   activation preflight; install never activates.
6. `doctor`, Protocol v33 Runtime Catalog, and the TUI expose only Registry ID,
   exact credential-free endpoints/origins, authentication class, and private-
   CA presence. They never expose Secret references, environment names,
   credential values, authorization headers, or CA bytes.

## Consequences

- Operators can add or rotate a Registry Bearer value without recompiling or
  modifying package receipts.
- One logical install may resolve a rotated credential separately for the
  Catalog and each Package request. This is intentional and keeps request-time
  authority current.
- Private CA and Bearer behavior is exercised by a real local TLS Registry
  integration test that also scans the resulting project tree for credential
  leakage.
- Runtime Catalog advances to Protocol v33 because its serialized shape adds
  the credential-free `skill_registries` collection. Durable State, Skill API,
  package format, Catalog format, and service configuration schema remain at
  their existing coordinates.

## Non-claims

- Bearer support is not OAuth/OIDC login, token refresh, client certificates,
  arbitrary headers, cookies, query credentials, or tenant-varying Registry
  routing.
- `package_origins` is a destination allowlist, not mirror ordering,
  federation, failover, or content replication.
- The Registry is not trusted to sign packages. Existing publisher signature,
  validity, revocation, and transparency policy remains authoritative.
- This is not npm, git, OCI, delta transfer, background update, implicit
  `latest`, executable plugins, or a hosted marketplace.
