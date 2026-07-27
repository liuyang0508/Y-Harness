# ADR 0102: Governed signed External Skill lifecycle

- Status: accepted
- Date: 2026-07-28

## Context

Y-Harness already had two correct but disconnected halves:

- ADR 0033 could acquire an exact identity/digest-pinned signed package over
  bounded public HTTPS and verify it through `SkillTrustStore`; and
- ADR 0091 could install, list, verify, activate, and recoverably remove
  project-local declarative packages.

Joining them by extracting the downloaded `SkillPackage` and storing it as a
project `TrustedExtension` would erase publisher provenance and disable the
live validity, revocation, and transparency checks that distinguish untrusted
network content from operator-authored project input.

Pi demonstrates useful package lifecycle ergonomics across local and remote
sources, but also permits executable in-process extensions with the launching
user's authority. Y-Harness needs the ergonomics without importing that trust
model.

## Decision

- Extend the optional service `skills` object with a separate
  `external_package_files` list and explicit `trust` configuration.
  `package_files` remains the operator-trusted `TrustedExtension` path;
  signed files never silently cross into it.
- Configure publisher Ed25519 keys by exact key ID and canonical lowercase
  32-byte hex. Each publisher may have an inclusive start, exclusive end,
  required/optional transparency policy, and one immutable effective
  revocation. Configure transparency logs with independent Ed25519 keys and
  the same immutable revocation shape.
- Allow a trust-only staged `skills` object with no packages or activations so
  an operator can validate trust before acquisition. Once any package is
  configured, require a non-empty exact activation list as before.
- Add `yh skill install-external <signed-package> [config]`. Verify package
  structure, content digest, publisher signature, validity, revocation, and
  any required/supplied transparency receipt before the first store mutation.
- Add
  `yh skill install-https <url> <name@version> <sha256> [config]` behind the
  existing `https-skill` feature. Validate trust configuration before network
  access, retain ADR 0033's exact URL/identity/digest and transport bounds,
  verify the fetched signed envelope, then use the same storage path as the
  offline command.
- Include `https-skill` in the reference operator install and release binary;
  keep the Rust library feature optional so a headless embedding can exclude
  HTTP/TLS acquisition.
- Canonically store signed packages as
  `skills/<content_sha256>.signed-skill.json` with create-new semantics.
  Reinstallation is idempotent only for an identical signed envelope. A
  changed signature or transparency receipt under the same identity is not
  silently substituted.
- Keep storage and activation separate. Installation prints the exact
  `skills.external_package_files` path and `name@version`, but never edits
  configuration or activates a Skill.
- At service startup, parse signed files through `register_signed` with
  `CapabilityOrigin::External`. Preserve publisher and transparency
  provenance in the resolved set and print it in `yh doctor` locks. Dependency
  resolution, resource reads, and every Context compilation retain the
  existing live trust recheck.
- Make `list` and `verify` fail closed when any installed signed package no
  longer satisfies configured trust. Keep structural scanning independent
  from trust so an operator can still remove a revoked or expired package
  after first removing its activation and configured file reference.
- Keep the 4,096-entry project store, 16 MiB encoded-file limit, project-root
  containment, symlink rejection, duplicate identity rejection, and
  recoverable trash behavior.
- Do not add recursive dependency acquisition, catalog selection, automatic
  update, config mutation, directory activation, hot reload, authenticated
  private registries, or executable package hooks.
- Keep Service configuration schema 1 and Skill package API 1. These are
  additive optional fields and commands; State, Protocol, snapshot, and Model
  Gateway coordinates do not change.

## Consequences

An operator can install an offline or public-HTTPS third-party Skill without
writing Rust and without downgrading it to local trust. A publisher or log
revocation stops subsequent governed use, including a long-running service's
future Context compilations.

The project configuration becomes the local trust-policy authority. It
contains public verification keys, not private signing material. Configuration
changes still require a controlled service restart, and trust distribution
across hosts remains an operator responsibility.

The exact acquisition URL is not persisted into the package envelope. Content,
publisher, log, and configured path remain auditable; a future registry receipt
or mirror provenance format requires a separate versioned contract.

## Rejected alternatives

- Extract the inner package and install it as `TrustedExtension`: discards the
  security property this lifecycle exists to preserve.
- Trust only the caller-supplied SHA-256: integrity is not publisher
  authenticity and does not express revocation.
- Verify only at installation: compromised or expired keys would remain
  effective in long-running Context.
- Refuse removal when trust verification fails: a compromised package must
  remain safely removable.
- Automatically activate or edit JSON configuration: storage presence is not
  Model-context authority and concurrent config mutation is unnecessary.
- Recursively fetch dependencies from package metadata: the remote package
  would gain transitive network-install authority.

## Evidence

- `signed_skill_cli_preserves_external_trust_and_live_revocation`
- `reference_cli::service::tests::external_skill_trust_rejects_noncanonical_keys_and_invalid_policy`
- `skill::https_source::tests::fetches_exact_pin_and_registers_only_after_trust_verification`
- `skill::tests::external_skills_require_a_trusted_strict_signature`
- `skill::tests::required_transparency_is_signed_preserved_and_live_revocable`
- all-feature and zero-default workspace test gates

## Related decisions

- [ADR 0014: signed external Skills](0014-signed-external-skills.md)
- [ADR 0032: live Skill trust and transparency](0032-live-skill-trust-and-signed-transparency-receipts.md)
- [ADR 0033: pinned HTTPS Skill acquisition](0033-pinned-https-skill-acquisition.md)
- [ADR 0085: project-configured declarative Skills](0085-project-configured-declarative-skills.md)
- [ADR 0091: governed project Skill lifecycle](0091-governed-project-skill-lifecycle.md)
