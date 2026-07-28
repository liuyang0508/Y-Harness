# Codex CF-003 restart fault-conformance probe

This directory preserves one real released-product restart run, not a
comparative benchmark.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Released CLI: Codex `0.145.0`
- Analyzed source: official tag `rust-v0.145.0`, commit
  `25af12f7e61572b0bc18ddb1008be543b91519b0`
- Adapter track: `fault_conformance`
- Case: `CF-003 restart-after-uncertain-effect`
- Claim eligible: no

The first Codex process used deferred Tool discovery and entered the pinned
MCP Tool. The fixture durably recorded one synthetic non-idempotent effect,
then withheld the Tool result. The controller observed that exact journal
boundary and cancelled Codex before a `function_call_output` could be
persisted.

Codex starts its MCP child in another process group, so cancelling the outer
Codex process group did not settle the fixture. The controller then wrote the
identity-bound [`release-marker.txt`](release-marker.txt), waited for the
detached fixture to release its journal lock, and recorded this limitation
instead of claiming complete process-tree cleanup.

The second process resumed the exact persisted Thread. Its loopback Provider
observed the original function call, Codex's source-defined synthetic
`function_call_output` value `aborted`, and the new user Turn. It returned a
fixed final message without selecting a Tool. The same rollout was appended,
the resumed JSONL retained the same Thread ID, and the independent fixture
oracle still reported exactly one invocation and one effect.

The exact inputs are [`spec.json`](spec.json) and
[`fixture-spec.json`](fixture-spec.json). The durable oracle is
[`journal.jsonl`](journal.jsonl), and [`result.json`](result.json) contains
the bounded adapter evidence. The full Codex rollout is not retained because
it contains ambient product instructions; only its relative identity, byte
counts, and before/after SHA-256 digests are preserved.

This run starts a new Turn on a resumed Thread; it does not continue the
interrupted Turn in place. It uses a deterministic Provider and therefore
does not measure Model reasoning quality. The released binary is hash-pinned
but was not reproducibly built from the analyzed source commit.
