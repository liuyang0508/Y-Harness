# ADR 0009: Skills are declarative, exact, and digest pinned

- Status: Accepted
- Date: 2026-07-25

## Decision

A Y-Harness Skill package is immutable declarative context, not executable
plugin code. Its package contract contains:

- a versioned manifest and exact semantic version;
- exact, sorted Skill dependencies;
- required registered Tool names;
- model-facing instructions and explicitly loaded resources;
- an estimated instruction token cost;
- a SHA-256 digest covering manifest, instructions, and resources.

Registration validates names, sizes, normalized resource paths, API version,
content digest, and exact identity collisions. Resolution is deterministic:
requested roots are sorted, dependencies precede dependants, duplicates are
removed, cycles and missing exact versions fail, and required Tools must
already exist.

The first contract deliberately accepts only exact dependency versions. It
does not embed a package-manager range solver into the runtime.

## Loading

Resolved instructions become whole `ContextBlock`s in dependency order and are
never partially truncated by Skill Engine. The caller supplies a Skill token
budget before resolution. Package resources remain unloaded until explicitly
read by normalized relative path.

An instruction that needs a side effect declares a Tool requirement. Execution
still flows through Tool Runtime, Policy, approval, State, and Verification. A
Skill cannot smuggle executable code around those boundaries.

## Supply-chain boundary

The content digest proves integrity against a pinned package value; it does not
prove publisher identity. `CapabilityOrigin` records the operator-assigned
trust origin. ADR 0014 adds publisher signatures; ADR 0032 adds live revocation
and signed transparency receipts; ADR 0033 adds exact pin-bound public HTTPS
acquisition. Catalogs, authenticated private sources, append-only log
consistency, and installation policy remain later supply-chain slices.

Externally obtained packages require operator-controlled origin, identity,
digest, publisher trust, and any receipt required by policy. Runtime resolution
never discovers or updates a package implicitly.

## Rationale

Exact resolution makes one run reproducible and its context traceable. Keeping
installation/fetching outside live execution avoids network-dependent behavior
and prevents a registry update from silently changing an active agent.
