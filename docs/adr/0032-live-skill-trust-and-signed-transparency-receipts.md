# ADR 0032: Live Skill trust and signed transparency receipts

- Status: Accepted
- Date: 2026-07-25

## Context

Publisher signatures establish who signed an immutable Skill package, but a
permanent key allow-list cannot react to compromise or planned expiry.
Verification only at registration also leaves already resolved instructions
usable after a trust-policy change.

Transparency metadata is useful only when it is cryptographically bound to the
exact package and publisher signature. An unverified log URL or entry label
would be attacker-controlled decoration.

## Decision

- Make `SkillTrustStore` a bounded, shared live policy object. Publisher and
  transparency-log roots each admit at most 4,096 Ed25519 keys; replacement is
  rejected and a poisoned policy lock fails closed.
- Give publisher roots optional inclusive `not_before_ms`, exclusive
  `not_after_ms`, and optional/required transparency policy.
- Represent publisher and log-key revocation as an immutable effective
  timestamp plus stable reason code. Repeating the exact record is idempotent;
  attempting to rewrite it is rejected.
- Apply revocation strictly at and after its effective time, including packages
  signed or registered earlier. Key expiry is also checked at every governed
  use.
- Define a signed transparency receipt containing trusted log ID, bounded entry
  ID, and integration time. The log signs domain-separated canonical material:
  log and entry identity, integration time, package content digest, publisher
  key ID, and the exact publisher signature.
- Reject receipts from unknown, revoked, weak, or malformed log roots. Reject
  zero integration time and timestamps more than five minutes ahead of the
  verification observation.
- A publisher may require a receipt. When a receipt is optional but present, it
  is still fully verified.
- Preserve verified log, entry, and integration metadata in registered and
  resolved Skill provenance.
- Revalidate live trust during dependency resolution, resource reads, and every
  Context compilation. Revocation after resolution therefore stops model-facing
  instructions before the next compile.
- Expose canonical publisher and transparency signing bytes. Runtime never
  accepts or stores private signing keys.
- Keep Skill package API coordinate `1`: package digest and publisher-signature
  bytes are unchanged, and the receipt field is optional in serialized signed
  packages.

## Consequences

Operators can expire or revoke a publisher or log root without rebuilding the
Runtime or reconstructing the Skill registry. Already copied application data
cannot be erased, but the first-party resolution, resource, and Context paths
all fail closed before another governed use.

The trust store is an in-memory policy surface, not its own durable control
plane. Production hosts must load trust and immutable revocation records from
an operator-controlled durable configuration source.

A signed receipt proves that a configured log key attested to the exact
package/signature tuple. It does not prove Merkle inclusion, append-only
consistency, cross-log gossip, or public availability. Remote package
acquisition and those stronger transparency properties remain separate work.

## Rejected alternatives

- Remove a key from a map: absence loses revocation time and reason evidence.
- Verify revocation only during registration: long-lived resolved Context would
  continue using compromised instructions.
- Trust unsigned transparency fields: metadata could be substituted with the
  package.
- Let a new revocation overwrite an old one: control-plane history would be
  mutable.
- Store publisher private keys in Runtime: consumption does not require signing
  authority.
