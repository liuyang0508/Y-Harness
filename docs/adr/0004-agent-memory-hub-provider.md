# ADR 0004: Agent Memory Hub as the reference memory provider

- Status: Accepted
- Date: 2026-07-25

## Context

Agent Memory Hub already provides a local-first governed memory system with:

- structured `MemoryItem` records and raw evidence kept as separate layers;
- project, tenant, sensitivity, validity, provenance, confidence, and
  supersession metadata;
- a single audited write funnel with repairable derived indexes;
- BM25 and vector retrieval, RRF fusion, optional reranking and graph
  expansion;
- a pre-injection Context Firewall and reversible
  `locator/overview/detail` Context Packs;
- adoption/rejection feedback, governance, lifecycle maintenance, and memory
  evaluation suites;
- MCP, CLI, SDK, hooks, and Web surfaces.

Reimplementing these features inside the harness would create two competing
memory products and blur the State, Context, and Memory boundaries.

## Decision

Agent Memory Hub is Y-Harness Engineering's first-party reference integration
and preferred default long-term memory provider.

The kernel defines a versioned, provider-neutral Memory capability contract.
The first adapter uses Agent Memory Hub's MCP surface. The Rust core does not
import Agent Memory Hub's Python implementation and does not read or write its
Markdown, evidence, or index files directly. CLI invocation may be used for
operator diagnostics, but it is not the runtime contract.

The minimum provider operations are:

| Operation | Purpose |
|---|---|
| `search` | retrieve scoped candidates and reversible context packs |
| `read` | bounded deep read by opaque memory reference |
| `write` | submit a governed durable-memory candidate |
| `brief` | obtain a token-budgeted resume summary |
| `feedback` | report adopted, rejected, and ignored candidates |
| `health/capabilities` | negotiate version, features, and degraded state |

Raw conversation or runtime evidence ingestion is a separate optional
capability. It must never imply that raw evidence has become durable knowledge.

Y-Harness owns:

- when memory operations run in the Agent Loop;
- thread/turn correlation, cancellation, deadlines, and retry classification;
- policy approval, tenant/project scope, secret handling, and final run-wide
  token allocation;
- recording requests, decisions, opaque references, degraded behavior, and
  outcomes in the State Engine;
- provider registration, capability negotiation, and replacement.

Agent Memory Hub owns:

- its durable item and evidence schemas;
- audit, write settlement, indexing, retrieval ranking, memory-specific
  firewall, and reversible context packing;
- memory feedback, consolidation, governance, and memory benchmark semantics.

The adapter must preserve Agent Memory Hub context packs and provenance rather
than flattening every result into untyped text. The Context Engine may apply a
final global budget across all context sources, but it must not silently claim
to reproduce Agent Memory Hub's internal retrieval or governance decisions.

## Failure semantics

Read/search/brief failures may degrade to a run without long-term memory when
policy allows, and the degradation is recorded. Writes are never reported as
successful without a provider acknowledgement. An uncertain write outcome is
retried only when the provider contract supplies an idempotency mechanism.

## Consequences

- Other memory backends can implement the same capability contract.
- Agent Memory Hub can evolve its internal storage and ranking independently.
- The MCP transport and capability negotiation must exist before the production
  adapter is enabled.
- Provider conformance tests will validate scope isolation, bounded reads,
  provenance retention, feedback semantics, degraded behavior, and write
  acknowledgement.
