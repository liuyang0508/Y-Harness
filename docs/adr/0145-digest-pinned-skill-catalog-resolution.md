# ADR 0145: digest-pinned Skill catalog resolution

- Status: accepted
- Date: 2026-08-02

## Context

Y-Harness could install local, signed offline, or exact HTTPS Skill packages,
then separately activate, update, roll back, and remove them. A package with
dependencies still required an operator to locate and install every exact
dependency manually. Mature Harness products provide search and recursive
package acquisition, but common npm/git plugin flows also execute publisher
code with ambient host authority and often resolve mutable version ranges.

Discovery and acquisition should become convenient without turning an index,
a network response, or installation itself into Context or execution authority.

## Decision

1. The reference CLI accepts format-1 JSON catalogs containing a sorted unique
   list of exact `name@version`, human description, public HTTPS package URL,
   package content SHA-256, yanked marker, and sorted tags. Catalog acquisition
   requires the SHA-256 of the exact raw bytes from an independent operator
   decision. Redirects, ambient proxy, retry, URL credentials, query strings,
   fragments, non-JSON media types, and bodies over 4 MiB remain rejected.
2. `search-https` is read-only capability discovery. It never installs,
   activates, changes configuration, or trusts catalog descriptions as model
   instructions.
3. `install-catalog` fetches the requested exact version, then follows exact
   dependencies from each fetched package's signed manifest rather than the
   catalog description. Every identity and digest must have a catalog entry;
   every newly fetched package must pass publisher, validity, revocation, and
   transparency policy. Missing, conflicting, newly yanked, cyclic, greater
   than 256-package, or greater than 64 MiB closures fail before package writes.
4. Successful content is stored in the ordinary inactive project Skill store.
   Exact catalog bytes and a deterministic root/closure/source receipt are
   retained under `.y-harness/package-cache`. Cache identities are immutable;
   conflicting bytes fail rather than overwrite.
5. `upgrade-catalog` requires an explicit target `name@version`, performs the
   same complete acquisition, and then invokes the existing activation path.
   Activation still assembles and doctors the complete service, retains the
   prior configuration digest, and atomically replaces config. There is no
   implicit `latest`, range solving, background update, or auto-reload.

## Consequences

- Exact dependency acquisition and provenance become repeatable and auditable.
- A failed multi-package write may leave already verified packages in the
  inactive content cache. It cannot grant Context, Tool, Policy, or execution
  authority; a later identical operation safely reuses them.
- Package search and download remain optional reference-host product features,
  not Core Agent Loop or Client Protocol responsibilities.
- The existing Skill API, service config schema, Runtime Catalog, durable
  State, and Protocol coordinates do not change.

## Non-claims

- A pinned catalog digest is operator selection evidence, not publisher
  authenticity. Package signatures and configured live trust remain mandatory.
- This is not npm, git, OCI, Registry federation, private Registry
  authentication, mirror negotiation, delta transfer, arbitrary executable
  plugins, or a hosted marketplace.
- Catalog descriptions and tags are discovery metadata only and never enter
  model Context.
