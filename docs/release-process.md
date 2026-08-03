# Release process

This runbook publishes one immutable Y-Harness source coordinate and its
complete Engine/TUI binary matrix. It does not authorize creating a remote
repository, pushing, tagging, or publishing; those remain explicit owner
actions.

## Supported release evidence

One successful tag workflow must produce:

- Engine archives for Linux, macOS, and Windows;
- independently packaged TUI archives for Linux, macOS, and Windows;
- one archive containing a CycloneDX 1.5 JSON SBOM for every workspace member;
- one `SHA256SUMS` covering all seven archives; and
- GitHub build-provenance attestations for every published file.

The SBOM reports component inventory. It is not a license approval, a
vulnerability guarantee, or proof of reproducible builds.

## Prepare the candidate

1. Update the `version` of `y-harness` and `y-harness-tui` together. Update
   their internal dependency constraint and `Cargo.lock`.
2. Create `docs/release-notes-v<version>.md`. Document protocol, persistent
   schema, archive, Skill, Provider, configuration, and migration coordinates.
3. Run the complete local gate in `docs/release-readiness.md` and resolve every
   known critical or high-severity correctness/security defect.
4. Commit the exact candidate. The worktree, including untracked files, must be
   empty.
5. Create the exact `v<version>` tag on that commit, then run:

   ```bash
   ./scripts/verify-release-coordinate.sh v<version>
   ```

The command performs no network or publication action. It refuses a dirty
worktree, a missing/moved tag, version drift, missing release inputs, invalid
diffs, or an unlocked Cargo graph.

## Publish

After the owner has chosen repository visibility, protected-branch/tag policy,
and license/distribution policy, push the candidate commit and its exact tag.
The tag workflow then:

1. repeats the release gates on the tagged source;
2. tests and packages Linux, macOS, and Windows independently;
3. generates the complete workspace SBOM;
4. stages all outputs without creating a public release;
5. verifies the exact seven-archive set and its unified checksum manifest;
6. attests the files; and
7. creates the release once, only after every dependency succeeds.

The workflow refuses to overwrite an existing release.

## Verify downloaded artifacts

From an empty directory, download the release and validate its manifest:

```bash
gh release download v<version> --repo liuyang0508/Y-Harness
sha256sum --check SHA256SUMS
gh attestation verify y-harness-v<version>-<target>.<archive> \
  --repo liuyang0508/Y-Harness
```

Use `shasum -a 256 -c SHA256SUMS` on systems without `sha256sum`, after first
checking that the local implementation accepts the manifest format.

## Failed or defective release

- A failed pre-publication job leaves only short-lived private workflow
  artifacts and no GitHub Release.
- Do not rerun a successful tag to replace bytes, and do not use `--clobber`.
- For a defect discovered after publication, preserve the evidence, publish an
  advisory/deprecation decision, fix forward with a new version, and exercise
  migration/rollback according to the affected schema runbook.

See [ADR 0149](adr/0149-atomic-attested-release-publication.md) for the trust
boundary and rejected alternatives.
