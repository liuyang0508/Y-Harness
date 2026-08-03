# ADR 0143: governed capability catalog, packages, and reload generations

- Status: accepted
- Date: 2026-08-02

## Context

Y-Harness already had strict Model registration, signed declarative Skills,
explicit MCP activation, and an independent Protocol client. It lacked the
operator lifecycle expected from mature Harness products: a compatible Model
endpoint still required a fixed first-party URL, installed Skills required
manual JSON edits, clients could not inspect the active capability generation,
and configuration changes required an undocumented restart procedure.

Copying an ambient executable package system or mutating registries during a
running Turn would weaken the existing authority model. Installation is not
execution authority, and one Turn must not observe two different Model, Tool,
Skill, Policy, or Context generations.

## Decision

1. The Responses adapter accepts an explicit HTTPS endpoint implementing the
   same OpenAI Responses wire contract. The default remains the official
   endpoint. Redirects, proxies, retries, URL credentials, query strings, and
   fragments remain rejected. API keys remain Secret references resolved from
   explicit environment mappings.
2. `yh skill` and `yh package` are aliases over the declarative Skill package
   lifecycle. Installation remains inactive. Activation selects an exact
   installed identity, adds its exact dependency closure, replaces another
   active version of the same name, assembles the complete candidate service,
   and only then atomically replaces configuration.
3. Every configuration mutation first stores the exact previous bytes under
   `.y-harness/config-history/<sha256>.json`. History and rollback use the
   content digest as identity; rollback performs the same complete preflight.
   Removal refuses active content, removes inactive config references, and
   moves package bytes to recoverable project-local trash.
4. Protocol v32 adds a conditional, permissioned `runtime.catalog` projection.
   It exposes configuration digest, ordered Model route, adapter families,
   credential-free endpoints, Tool names, exact active Skill locks, MCP
   registrations, and reload strategy. It never exposes Secret values, process
   commands, arguments, or child environments.
5. The reference host uses a `restart_boundary` generation strategy. The TUI
   `/reload` command first runs non-mutating `yh doctor`, rejects reload during
   an active Turn, drains the old child Engine, starts a new generation over
   the same durable stores, negotiates Protocol again, and reloads the same
   Thread. `/doctor`, `/runtime`, `/models`, and `/skills` remain client control
   surfaces over Engine-owned validation and Protocol projections.

## Consequences

- Compatible Responses vendors, private compatible gateways, and independent
  API-key mappings can be added in configuration without recompiling Rust.
- Package update and rollback are explicit, exact-versioned, content-addressed,
  and preflighted without granting packages ambient code execution.
- A Turn retains one frozen generation. Reload latency includes process drain
  and startup, but partial in-process mutation and mixed-generation evidence
  are excluded.
- GUI, LUI, IDE, API, and other clients can reuse the Runtime Catalog contract.
  A remote deployment still needs its own authenticated supervisor lifecycle;
  Protocol v32 does not let arbitrary clients replace Runtime code.

## Non-claims

- An endpoint override does not make an incompatible native API
  Responses-compatible. Separate native adapters may implement their own exact
  contracts; see ADR 0144.
- This is not a public package marketplace, dependency downloader, private
  registry client, or executable plugin sandbox.
- `restart_boundary` is not in-place mutation and is not zero-downtime
  multi-node deployment. Running Turns are never migrated between generations.
- Catalog output is operational metadata, not authority to invoke, activate,
  install, or reload a capability.
