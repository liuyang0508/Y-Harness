# ADR 0015: MSRV is tested and incompatible transitive updates are pinned

- Status: Accepted
- Date: 2026-07-25

## Context

Crate `rust-version` metadata is useful but incomplete. A real Rust 1.87 build
showed that the selected MCP SDK line uses let-chain syntax stabilized in Rust
1.88.
Separately, `libsqlite3-sys 0.38.x` uses `cfg_select!`, stabilized much later,
without dependency resolution preventing an older compiler from selecting it.

Merely declaring an MSRV therefore produced a false compatibility claim.

## Decision

- Y-Harness MSRV is Rust 1.88.
- The complete fmt, Clippy, test, and rustdoc gates run with Rust 1.88.
- CI repeats tests on Linux, macOS, and Windows at that toolchain.
- `rmcp` remains exactly pinned at the separately security-audited stable
  version.
- `rusqlite` is exactly pinned at `0.39.0`, whose locked
  `libsqlite3-sys 0.37.0` builds on Rust 1.88.
- Every dependency update must pass the real MSRV build; resolver metadata alone
  is not acceptance evidence.

## Consequences

Consumers receive a compiler floor supported by executable evidence. Some newer
SQLite wrapper releases are intentionally deferred despite compatible public
APIs.

Dependency automation may propose upgrades, but CI rejects any proposal that
silently raises the compiler requirement. Raising MSRV requires an explicit
decision and documentation update.

## Rejected alternatives

- Keep Rust 1.87 and downgrade the official MCP SDK: loses the selected
  integration baseline to preserve a one-release compiler difference.
- Declare Rust 1.88 while leaving SQLite dependencies floating: lockfile
  regeneration can reintroduce an undeclared Rust 1.95 requirement.
- Test only current stable: does not validate the compatibility promise.
