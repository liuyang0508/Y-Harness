# ADR 0088: Explicit MCP activation and extension locks

- Status: Accepted
- Date: 2026-07-27

## Context

The reference service could configure governed stdio MCP servers and exact
project Skills, but configuration did not distinguish an installed MCP entry
from an active one. Diagnostics also reported only aggregate Skill counts, so
an operator could not compare the active Skill set with a reviewed lock
without inspecting every package.

Automatically starting every configured executable makes configuration
presence equivalent to process authority. Silently trusting a mutable command
path also makes local drift harder to detect.

## Decision

- Give each MCP server an `enabled` switch that defaults to `true` for
  compatibility.
- Validate MCP IDs across enabled and disabled entries. Construct, discover,
  expose, and grant Policy authority only for enabled entries.
- Allow an enabled stdio server to pin the exact command file with an optional
  lowercase SHA-256 digest. Stream the file through a 256 MiB verification
  ceiling before client construction.
- Keep command locking optional because package-manager shims and development
  binaries may change intentionally. `yh doctor` reports the number of locked
  enabled commands rather than implying that unlocked entries are pinned.
- Report every resolved Skill as
  `<name>@<version> <content_sha256>`. Continue to separate installed
  `package_files` from exact active identities in `activate`.
- Freeze MCP and Skill configuration at service startup. Changes require a
  controlled restart.

## Consequences

A disabled MCP entry grants neither process-launch nor Tool authority and
cannot satisfy a Memory dependency. Operators can review exact active Skill
and optional MCP executable locks through `yh doctor`.

The command digest is startup drift detection, not an executable sandbox or an
atomic operating-system measurement. It covers only the configured command
file, not scripts named in arguments, dynamic libraries, interpreters,
transitive package files, or a hostile filesystem replacement between
verification and execution. Strong containment still belongs to the Process
Broker and operating system.

Project Skill files remain operator-trusted declarative inputs. Network
packages still require the existing signed `External` acquisition path.

## Rejected alternatives

- Start disabled entries but hide their Tools: process execution itself is
  authority.
- Auto-enable discovered MCP servers or Skills: discovery does not prove
  execution or context consent.
- Claim digest locking as sandboxing: integrity evidence does not remove
  filesystem, network, syscall, or child-process authority.
- Add hot reload: it would require an atomic Runtime capability-generation and
  in-flight Turn transition contract that does not yet exist.

## Evidence

- `disabled_mcp_server_grants_no_process_or_tool_authority`
- `optional_mcp_command_pin_is_exact_and_content_sensitive`
- `doctor_loads_exact_project_skills_and_rejects_content_tampering`

## Related decisions

- [ADR 0076](0076-governed-service-capability-assembly.md)
- [ADR 0085](0085-project-configured-declarative-skills.md)
