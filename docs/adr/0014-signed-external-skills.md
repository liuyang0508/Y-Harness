# ADR 0014: External Skills require trusted publisher signatures

- Status: Accepted
- Date: 2026-07-25

## Context

SHA-256 content pins detect mutation but do not establish who published a Skill.
An attacker able to replace both package content and its digest defeats an
integrity-only check. Built-ins and explicitly trusted in-process extensions
have a separate operator trust path; remote or external packages need
authenticity.

## Decision

Externally sourced Skills must enter `SkillRegistry::register_signed`.

- Complete canonical package content is SHA-256 verified first.
- A detached Ed25519 signature covers the domain-separated bytes
  `y-harness-skill-v1\0 || lowercase_content_sha256`.
- Trust roots are raw public keys installed explicitly by the operator; the
  trust store is empty by default and key replacement is rejected. ADR 0032
  adds live validity, immutable revocation, and signed
  transparency-receipt policy.
- Invalid, unknown, malformed, or cryptographically weak public keys fail
  closed.
- Verification uses strict Ed25519 validation.
- The verified publisher key ID is preserved in `RegisteredSkill` and
  `ResolvedSkill` evidence.
- Unsigned registration with an `External` origin is rejected.

Private signing keys are not accepted or managed by the Runtime. Signing belongs
in a publisher's release pipeline.

## Consequences

External package integrity and publisher authenticity are now separate,
auditable claims. Digest substitution and signature reuse across different
package content fail verification.

Threshold signatures, remote source fetching, and append-only log
inclusion/consistency are not implied by this first trust-store contract. Those
remain release/governance work.

## Rejected alternatives

- SHA-256 alone: integrity without publisher identity.
- Trust-on-first-use: silently promotes the first observed attacker key.
- Runtime-held private keys: expands secret custody without helping consumers.
- Signing raw non-canonical archive bytes: makes reproducibility depend on
  packaging details rather than declared Skill content.
