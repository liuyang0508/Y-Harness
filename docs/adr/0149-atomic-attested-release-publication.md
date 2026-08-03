# ADR 0149: Atomic, attested release publication

## Status

Accepted.

## Context

The original tag workflow created a GitHub Release before the Linux, macOS,
and Windows binaries had all built successfully. Each platform uploaded
directly to that mutable release and used independent checksum files. A failed
late job could therefore leave an incomplete public release, while a rerun
could replace artifacts with `--clobber`. The tag was also not checked against
the Engine package version, TUI package version, or versioned release notes.

Those properties are incompatible with Y-Harness's evidence boundary. A
release is one immutable coordinate derived from one commit; success on one
platform is not evidence for another platform, and an SBOM is inventory rather
than a license-policy decision.

## Decision

- Add `scripts/verify-release-coordinate.sh`. It fails unless the worktree is
  clean, the requested `v<version>` tag resolves exactly to `HEAD`, Engine and
  TUI package versions agree with the tag, versioned notes exist, required
  license/readme inputs exist, the Git diff is valid, and locked Cargo metadata
  resolves.
- Make the tag workflow independently rerun the required formatting, lint,
  zero-default/all-feature tests, TUI PTY, Evaluation, documentation, package,
  RustSec, and calibrated State performance gates before producing artifacts.
- Build and test the exact source separately on Linux, macOS, and Windows.
  Every platform stages its archives as short-lived immutable workflow
  artifacts; no build job can create or mutate a public release.
- Generate one CycloneDX 1.5 JSON SBOM for every Cargo workspace member with a
  version-pinned generator, validate the exact workspace-member count, and
  publish the documents as one archive. SBOM generation does not grant license
  approval.
- Permit only the final job, after every build and SBOM job succeeds, to merge
  the seven expected artifacts, create one `SHA256SUMS`, verify it locally,
  issue GitHub build-provenance attestations, and create the GitHub Release.
- Refuse to mutate an existing release. A defective release receives a new
  version and an explicit advisory or deprecation decision; a rerun does not
  silently replace bytes under the old coordinate.
- Pin every third-party GitHub Action to an exact commit and each installed
  Rust supply-chain tool to an exact version compatible with Rust 1.88.

## Consequences

Publication becomes slower and duplicates important CI work, but it no longer
depends on a concurrently running workflow or publishes a partially successful
matrix. Reviewers can bind a tag, commit, six platform/product archives, the
workspace SBOM archive, a complete checksum manifest, and hosted provenance
attestations.

This does not prove reproducible builds, sign a Git tag, approve transitive
licenses, establish protected-tag policy, or prove that a remote workflow ran.
Those require repository governance and actual execution on the release
commit.

## Rejected alternatives

- Create the release first and upload from matrix jobs: exposes partial public
  state and requires mutation on retry.
- Use only per-file checksum sidecars: makes omission harder to detect and does
  not bind the complete expected set.
- Trust the tag name without comparing package coordinates: allows a release
  label to diverge from shipped metadata.
- Treat SBOM generation as license approval: inventory cannot supply an
  operator's legal policy or exception decision.

## Verification

- `scripts/verify-release-coordinate.sh v0.1.0` passes in a clean synthetic
  exact-tag fixture and fails on the live dirty candidate.
- `actionlint 1.7.12` accepts both checked-in workflows.
- `cargo-cyclonedx 0.5.9` under Rust 1.88 produces six parseable CycloneDX 1.5
  JSON documents for the current six-member workspace; the release packaging
  command preserves all six paths in one archive.

