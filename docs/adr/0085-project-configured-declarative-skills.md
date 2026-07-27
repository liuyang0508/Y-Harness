# ADR 0085: Project-configured declarative Skills

- Status: Accepted
- Date: 2026-07-27

## Context

The Skill Engine already validates exact package versions, content digests,
dependencies, required Tools, token budgets, publisher signatures, live
revocation, transparency receipts, and pinned HTTPS acquisition. The reference
service did not expose any Skill configuration, so using those contracts still
required a custom Rust host.

Adding a generic plugin manager first would duplicate existing registries
without delivering a usable path. Automatically scanning directories or
activating every discovered package would also turn filesystem presence into
model-visible authority.

## Decision

- Add one optional `skills` object to service configuration schema 1.
- Require explicit project-relative `package_files`, exact `activate`
  identities, and one aggregate instruction-token budget.
- Canonicalize every package path below the configuration project root, bound
  each encoded file to 16 MiB, deserialize it as Skill API 1, and let the
  existing `SkillRegistry` verify its declared SHA-256 digest and collision
  rules.
- Treat these files as operator-approved `TrustedExtension` inputs. The service
  configuration is already the authority that may launch executables and
  expose Tools; project Skill files add declarative instructions and resources
  but execute no code.
- Resolve the exact dependency graph only after Tools are registered, then add
  the resulting instruction blocks through the existing `ContextEngine`.
- Report the resolved Skill count from `yh doctor`.
- Freeze the configuration at service startup. Do not add directory discovery,
  automatic activation, hot reload, a marketplace, or a second Skill runtime.
- Keep network-fetched packages on the existing `External` path, which requires
  publisher verification and exact URL, identity, and digest pins.

## Consequences

An operator can now add or remove a project Skill without modifying or
recompiling Y-Harness. Unknown versions, missing dependencies or Tools,
duplicate packages, budget overflow, path escape, malformed content, and digest
tampering fail before the service accepts Turns.

Local project approval proves operator intent, not third-party publisher
authenticity. A package copied from an untrusted source must be reviewed before
being named in the trusted project configuration. ADR 0102 adds a separate
signed `external_package_files` path and preserves that external boundary
rather than relabeling downloaded content as a trusted project file.

This is the first product-facing extension-management slice, not a claim that
all registered capability types now have zero-code installation or hot reload.

## Rejected alternatives

- Scan a conventional directory: discovery must not imply activation.
- Activate every listed file: dependencies and top-level intent are different.
- Mark project files `External` without signatures: this would either reject
  the feature or weaken the external authenticity contract.
- Add a dynamic-library plugin ABI: unnecessary for declarative Skills and a
  larger trust and compatibility surface.

## Related decisions

- [ADR 0009](0009-declarative-skill-packages.md)
- [ADR 0014](0014-signed-external-skills.md)
- [ADR 0033](0033-pinned-https-skill-acquisition.md)
- [ADR 0076](0076-governed-service-capability-assembly.md)
- [ADR 0102](0102-governed-signed-external-skill-lifecycle.md)
