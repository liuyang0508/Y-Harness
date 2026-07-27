# ADR 0091: Governed project Skill lifecycle

- Status: Accepted
- Date: 2026-07-27

## Context

The Runtime could validate and activate exact project Skill package files, but
operators had to copy, inspect, list, and delete those files manually. Package
presence and activation were already intentionally separate; adding lifecycle
UX must not make installation equivalent to Model-context or Tool authority.

Pi demonstrates useful package-management ergonomics, but its packages may
execute in-process TypeScript. Y-Harness Skills are declarative Context inputs
and must retain the existing digest, dependency, Tool, budget, and Policy
boundaries.

## Decision

- Add `yh skill install/list/verify/remove` to the reference CLI. Keep it
  outside the semantic Skill microkernel and reuse `SkillPackage` plus
  `SkillRegistry` validation.
- Scope the store to the configuration project `skills/` directory. Bound
  scanning to 4,096 entries and each package file to 16 MiB; reject malformed,
  digest-mismatched, duplicate-identity, escaping, symlinked Skill entries.
- Canonically serialize installed packages to
  `skills/<content_sha256>.skill.json` with create-new semantics. Reinstalling
  the same identity and digest is idempotent; the same identity with another
  digest fails.
- Do not edit `skills.package_files` or `skills.activate`. Installation prints
  the exact path, identity, and digest, but activation remains an explicit
  reviewed configuration action followed by service restart.
- Make `list` and `verify` revalidate the complete bounded store rather than
  trusting filenames.
- Require exact `name@version` removal. Refuse packages that are configured or
  active. Move accepted removals under the project's data-directory
  `skill-trash/` rather than deleting them.
- Treat local packages as operator-trusted declarative inputs. Network or
  third-party acquisition still requires the signed `External` path; this CLI
  does not weaken publisher or transparency requirements. ADR 0102 later adds
  that separate lifecycle without changing this local command's trust meaning.

## Consequences

Operators gain a usable local Skill lifecycle without recompiling the Engine,
and package files remain exact and reviewable. A disk file grants no Runtime
authority until its path and exact identity are separately activated.

The CLI does not discover a marketplace, fetch dependencies, update versions,
mutate configuration, hot reload a running service, or install executable
extensions. ADR 0102 adds exact pinned public-HTTPS acquisition only.
Installing another exact version is possible, but choosing and activating that
version remains an operator decision.

## Rejected alternatives

- Auto-activate on install: conflates storage with Model-context authority.
- Rewrite project config automatically: introduces concurrent config mutation,
  comment/order loss, and crash recovery for no required runtime benefit.
- Execute package scripts or TypeScript extensions: bypasses Tool Policy and
  process isolation.
- Delete immediately on remove: makes operator mistakes unnecessarily
  difficult to recover.
- Trust the digest filename: a mutable file still requires full package
  validation at every management boundary and Runtime startup.

## Evidence

- `skill_cli_installs_verifies_and_recoverably_removes_exact_packages`
- `doctor_loads_exact_project_skills_and_rejects_content_tampering`

## Related decisions

- [ADR 0009](0009-declarative-skill-packages.md)
- [ADR 0033](0033-pinned-https-skill-acquisition.md)
- [ADR 0085](0085-project-configured-declarative-skills.md)
- [ADR 0088](0088-explicit-mcp-activation-and-extension-locks.md)
- [ADR 0102](0102-governed-signed-external-skill-lifecycle.md)
