# ADR 0016: RustSec blocks known vulnerable dependencies

- Status: Accepted
- Date: 2026-07-25

## Context

The first local `cargo audit --deny warnings` found
`RUSTSEC-2026-0189`, a high-severity DNS rebinding vulnerability in
`rmcp 0.16.0` Streamable HTTP server transport. Y-Harness used only stdio client
transport, but retaining a dependency version with a known high advisory would
violate the release defect policy and could become exploitable after a feature
change.

## Decision

- Upgrade the official MCP SDK to stable `rmcp 2.2.0`, above the advisory's
  fixed range.
- Disable the unused `server` feature and retain only `client` plus
  `transport-child-process`.
- Adapt calls through the SDK's non-exhaustive request constructor rather than
  struct literals.
- Run pinned `cargo-audit 0.22.2` with `--deny warnings` in CI.
- Treat any future advisory as a failed release gate unless an explicit,
  evidence-backed exception is documented.

The post-upgrade audit scanned 109 locked crate dependencies with no findings.

## Consequences

MCP remains behind the internal `McpClient` port, so the breaking SDK upgrade
required one adapter change rather than a Runtime rewrite. Feature minimization
also reduces accidental exposure to transports Y-Harness does not host.

The latest SDK beta is not selected merely for novelty; the latest non-beta
release satisfying the security floor is pinned and tested.

## Rejected alternatives

- Ignore the advisory because the vulnerable transport is unused: leaves a
  known high-risk version in the graph and relies on feature assumptions.
- Keep the unnecessary server feature: expands attack surface without serving
  a product requirement.
- Upgrade to a beta release: adds unrelated protocol churn to a security fix.
