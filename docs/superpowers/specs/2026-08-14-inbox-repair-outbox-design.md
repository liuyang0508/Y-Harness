---
id: spec-2026-08-14-inbox-repair-outbox
title: Durable Inbox Repair Outbox, Orphan Tombstone, Retry-Age Projection, and Repair Worker
status: proposed
date: 2026-08-14
project: y-harness
parent-adr: docs/adr/0158-durable-agent-loop-waiting-and-resume.md
target-pr: (forthcoming, P0 第 1 项)
---

# Durable Inbox Repair Outbox, Orphan Tombstone, Retry-Age Projection, and Repair Worker

## 1. Context and motivation

ADR 0158 §8 documents four canonical State × Approval Inbox crash scenarios that the current Runtime can only repair synchronously at wait creation, at resume, or via best-effort cleanup after State closure. ADR 0158 §8 closes with:

> The current Runtime performs bounded idempotent submit/read repair when a wait is created or resumed and performs bounded best-effort orphan cleanup after State closure. It does not yet persist an Inbox-repair outbox, create an authoritative orphan tombstone before late submission, scan unresolved pairs, or expose retry-age operations. A cleanup failure therefore remains an operator-visible gap rather than a durable queued repair job.

ADR 0158 "Phase 2 still open" lists the same gap as the first sub-item of the still-open Phase 2 work:

> durable Inbox repair outbox, orphan tombstone, retry-age projection, and repair worker.

The release-readiness matrix, release notes, and acceptance checklist all carry the same line as a known release blocker for broader claims. This spec turns that line into a single bounded design that lifts the four repair scenarios from "synchronous best-effort" to "durable queued retry with operator-observable retry age."

## 2. Goal

A Runtime crash between `wait_projection` CAS and the corresponding Inbox `submit`, `settle`, or `orphan_close` becomes a recoverable event rather than a stuck or phantom record. Specifically:

1. **Outbox durability.** Every cross-domain Inbox side effect is journaled to a State-resident outbox in the same transaction as the State projection change that depends on it.
2. **Tombstone authority.** A State terminal transition publishes an authoritative tombstone that any subsequent Inbox settle must consult before mutating state.
3. **Repair autonomy.** A repair worker drains the outbox across process restarts without operator intervention, with bounded retry and exponential backoff.
4. **Operational visibility.** Pending count, retry age, and exhaustion are exposed via SQL view, Rust API, structured log, and the existing `ProtocolServiceStatus` projection.

## 3. Non-goals

- No exactly-once delivery semantics across State and Inbox. ADR 0158 §8 explicitly rejects treating State + Approval as one atomic commit; this design preserves idempotency at the Inbox layer instead.
- No distributed worker ownership, multi-node consensus, fleet placement, or failover. These belong to ADR 0158 Phase 3.
- No Prometheus endpoint, metrics scraper, or external alerting integration. Rust API + structured log + SQL view are sufficient for this PR; Prometheus wiring is a follow-on.
- No change to the existing `MemoryApprovalInbox` API surface beyond the additive tombstone check. New code paths; no breaking rename.
- No generalized "cross-domain repair outbox" framework. This design scopes strictly to Approval Inbox.

## 4. Storage: four new State-SQLite objects

Outbox and tombstone live in the same SQLite database as `wait_projection` so that the journaling CAS and the outbox enqueue share one transaction. This rejects the alternative of placing the outbox in the Inbox SQLite (cross-domain atomicity loss) and the alternative of a third isolated SQLite (extra consistency checks without operational benefit at this scope).

```sql
-- schema-18 (post schema-16) additions

CREATE TABLE inbox_repair_outbox (
    op_id           TEXT PRIMARY KEY,
    wait_id         TEXT NOT NULL,
    op_kind         TEXT NOT NULL CHECK (op_kind IN ('submit','settle','orphan_close')),
    payload_json    BLOB NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending','in_flight','succeeded','exhausted')),
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    last_attempt_ms INTEGER,
    next_attempt_ms INTEGER NOT NULL,
    last_error      TEXT,
    created_ms      INTEGER NOT NULL,
    FOREIGN KEY (wait_id) REFERENCES wait_projection(wait_id)
);

CREATE TABLE inbox_orphan_tombstone (
    wait_id         TEXT PRIMARY KEY,
    tombstoned_ms   INTEGER NOT NULL,
    reason          TEXT NOT NULL CHECK (reason IN ('settled','cancelled','timeout','denied','terminal_failure')),
    source_revision INTEGER NOT NULL
);

CREATE VIEW inbox_retry_age_view AS
    SELECT op_id, wait_id, op_kind, status, attempt_count,
           (CAST((strftime('%s','now')*1000) AS INTEGER) - last_attempt_ms) AS age_ms,
           last_error
    FROM inbox_repair_outbox
    WHERE status IN ('pending','in_flight');

CREATE INDEX idx_outbox_pending ON inbox_repair_outbox(next_attempt_ms) WHERE status = 'pending';
CREATE INDEX idx_outbox_wait    ON inbox_repair_outbox(wait_id);
CREATE INDEX idx_tombstone_wait ON inbox_orphan_tombstone(wait_id);
```

`payload_json` is the **exact immutable** operation argument set: `submit` carries the original `ApprovalRequest` (with its authority), `settle` carries the resolved `ApprovalDecision` plus the originally observed Inbox revision, `orphan_close` carries `approval_id`, observed Inbox revision, and the orphan reason. Re-executing an op must be byte-identical to the original side effect, which is what makes retries idempotent at the Inbox layer.

## 5. Transaction flows

Each of the four ADR 0158 §8 repair scenarios maps onto one or two of these transactions. The boundary marks "what is committed before the next runtime can act":

### 5.1. Wait commit (scenario 1 partial: enqueue submit)

```text
BEGIN
  UPSERT wait_projection (Waiting, source_revision, ...)        -- CAS 1
  INSERT inbox_repair_outbox (op_kind='submit', payload=ApprovalRequest,
                             next_attempt_ms=now_ms)
COMMIT
```

After commit, the Inbox may not yet have a record. The repair worker is responsible for retrying the side effect.

### 5.2. Wait accepted (scenario 3 partial: enqueue settle)

```text
BEGIN
  UPDATE wait_projection (Waiting -> Ready)                     -- CAS 2 (WaitAccepted)
  INSERT inbox_repair_outbox (op_kind='settle', payload=ResumeEvidence,
                             next_attempt_ms=now_ms)
COMMIT
```

`WaitAccepted` precedes the Inbox "consumed" mark so that two processes racing accept cannot both enqueue a `settle` op: the State CAS already rejected the loser.

### 5.3. Wait terminal (scenario 4: tombstone authoritative)

```text
BEGIN
  UPDATE wait_projection (Ready|Executing|NeedsReconciliation -> Closed)
  INSERT inbox_orphan_tombstone (wait_id, reason, source_revision,
                                tombstoned_ms=now_ms)
COMMIT
```

Tombstone is **written before** any Inbox settle arrives at the closed wait. ADR 0158 §3 row 4 ("State is terminal while Inbox is pending or settled") becomes a hard guarantee instead of a best-effort cleanup.

### 5.4. Orphan detected (scenarios 2/4: enqueue orphan_close)

A Runtime-detected orphan — an Inbox record whose `wait_id` is absent from `wait_projection` — is closed by enqueuing a close op, **not** by writing a tombstone. Tombstones are owned exclusively by the State terminal transaction.

```text
BEGIN
  INSERT inbox_repair_outbox (op_kind='orphan_close',
                             payload={approval_id, revision, deny_reason='orphan_no_wait'},
                             next_attempt_ms=now_ms)
COMMIT
```

## 6. Inbox `settle_as` gets a tombstone check

`ApprovalInbox::settle_as` (already present in `src/approval/mod.rs`) is the single side-effecting Inbox mutator. A pre-CAS tombstone check is inserted between the existing tenant authority check and the existing revision CAS:

```text
Inbox.settle_as(approval_id, expected_revision, decision, authority):
  1. existing: tenant authority check
  2. NEW:      SELECT reason FROM inbox_orphan_tombstone
               WHERE wait_id = approval_id.wait_id
  3. NEW:      if row exists:
                 return Err(StaleWaitSettlement {
                   wait_id, tombstone_reason, tombstoned_at_ms
                 })
               ↑ no mutation to Inbox record; caller maps to op succeeded
  4. existing: revision CAS + decision commit
```

This makes tombstone the **single authoritative gate** for any late settlement against a closed wait. Even if the Inbox was reachable after the State terminal, no decision can mutate it.

## 7. Repair worker: coldstart + background tokio task

### 7.1. Cold start (synchronous, during `Runtime::start`)

```text
run_inbox_repair_coldstart(max_ops=64, total_budget_ms=10_000):
  loop:
    rows = SELECT op_id, op_kind, payload_json
           FROM inbox_repair_outbox
           WHERE status='pending' AND next_attempt_ms <= now_ms
           ORDER BY next_attempt_ms ASC
           LIMIT M (M=16)
    if rows empty: return
    for row in rows: try_execute_op(row)
    if budget_exceeded or rows returned < M: return
  ↑ never exceed the startup budget; the background loop picks up what remains
```

`max_ops=64` and `total_budget_ms=10_000` are fixed constants; the precise values are tuned once telemetry exists.

### 7.2. Background loop (tokio task, spawned by `Runtime::start`)

```text
loop:
  sleep(repair_poll_interval(now))  -- 1s when busy, ramps to 60s when idle
  run_inbox_repair_tick(max=16)
  if any row transitioned to 'exhausted':
    log::warn!(structured, ...)
  record_metrics()  -- exposed via ProtocolServiceStatus
on stop signal: finish current op, return
```

`run_inbox_repair_tick` shares the same `try_execute_op` body as cold start but caps at 16 ops per tick to bound tail latency on the Runtime's tokio runtime.

### 7.3. `try_execute_op` outcome table

| Result | New status | Notes |
|---|---|---|
| `Ok(_)` | `succeeded` | Normal completion. `attempt_count += 1`. |
| `Err(StaleWaitSettlement)` | `succeeded` | Tombstone already authoritative; op done. |
| `Err(AlreadySettled)` / `Err(AlreadyOrphaned)` | `succeeded` | Inbox already terminal. |
| `Err(Retryable)` and `attempt_count + 1 < 8` | `pending` | Backoff: `next_attempt_ms = now + 100ms · 2^attempt` capped at 60s. |
| `Err(Retryable)` and `attempt_count + 1 >= 8` | `exhausted` | `log::warn!` with `last_error`. Operator must inspect. |
| `Err(NonRetryable)` (e.g. malformed payload, schema mismatch) | `exhausted` | Same as exhaustion; reason `non_retryable`. |

The 8-attempt budget caps the worst case at roughly 6 minutes of backoff (100ms + 200ms + 400ms + 800ms + 1.6s + 3.2s + 6.4s + 12.8s ≈ 25.5s; the 60s cap raises the 8th attempt to ~50s+). Beyond this the operator must intervene.

## 8. Operational visibility

### 8.1. Rust API

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InboxRepairMetrics {
    pub pending_ops: u64,
    pub in_flight_ops: u64,
    pub exhausted_ops: u64,
    pub oldest_pending_age_ms: u64,
    pub succeeded_ops_window: u64,    // last 24h, sliding
    pub failed_ops_window: u64,
    pub last_tick_at_ms: u64,
    pub coldstart_repaired_at_startup: u64,
}

impl State {
    pub fn inbox_repair_metrics(&self) -> Result<InboxRepairMetrics, HarnessError>;
}
```

### 8.2. `ProtocolServiceStatus` extension

The existing `ProtocolServiceStatus` (returned by `ProtocolHandler::service_status`, exposed by `src/transport/http_probe.rs`) gains a single new field:

```rust
pub struct ProtocolServiceStatus {
    /* existing fields */
    pub inbox_repair: InboxRepairMetrics,   // NEW; additive change
}
```

Old Protocol clients ignore the unknown field.

### 8.3. Probe degraded threshold

When `oldest_pending_age_ms > 300_000` (5 minutes), the HTTP probe response flips to `degraded`. The probe still returns 200 — this is a soft signal for orchestration systems, not a kill switch.

### 8.4. Structured log line

Every worker tick ends with one line:

```text
INFO  inbox_repair_tick scanned=16 succeeded=12 retryable=2 exhausted=0 oldest_pending_age_ms=4321 tick_duration_ms=87
WARN  inbox_repair_exhausted op_id=<uuid> wait_id=<uuid> attempt_count=8 last_error="..."
```

JSON output for log aggregation.

## 9. Schema migration: schema-16 → schema-18

This change advances the schema coordinate. Following ADR 0158 Phase 1 conventions:

- Schema version on `wait_projection` advances to 18.
- All old writers must be stopped before upgrade.
- Old readers + new writers and mixed-writer scenarios remain unsupported.
- Rollback means restoring the validated backup before any schema-18 event is written.
- Migration is **forward-only**: empty outbox and tombstone tables on upgrade; no historical record is rewritten.
- A schema-16 projection without a corresponding schema-18 outbox row is not retroactively re-enqueued — historical waits are treated as having already completed their cross-domain repair during their original closure.

## 10. Test matrix

| Dimension | Cases |
|---|---|
| Happy path | wait commit → submit op succeeds → accept → settle op succeeds → tombstone written; `InboxRepairMetrics` increments monotonically |
| Crash: submit failure | wait commit succeeds, Inbox.submit raises Retryable → outbox row persists as `pending` → tick retries to success |
| Crash: submit during | wait commit + outbox insert committed, Inbox.submit never invoked → coldstart drains the row |
| Crash: settle during | accept committed, worker crash before Inbox.settle → outbox row pending → retry returns `AlreadySettled`, op marked `succeeded` |
| Late decision: tombstone effective | State terminal + tombstone committed → Inbox receives settle post-tombstone → returns `StaleWaitSettlement`, op marked `succeeded` |
| Orphan Inbox | Inbox has pending record, no matching `wait_projection` row → startup scan enqueues `orphan_close` → worker settles with `Deny{reason='orphan_no_wait'}` |
| Backoff exhaustion | 8 consecutive Retryable errors → `status='exhausted'`, `log::warn!` fires |
| Concurrency: worker settle vs State terminal | tombstone commits before worker attempts settle → worker sees `StaleWaitSettlement` |
| Metrics surface | `ProtocolServiceStatus.inbox_repair` populated; admin CLI prints snapshot; probe returns `degraded` when threshold exceeded |
| Migration | schema-16 → schema-18 upgrade leaves outbox + tombstone empty; reads continue; new waits immediately exercise the new path |
| Bounded retries | coldstart `total_budget_ms` honored; tick `max_ops=16` honored |

## 11. Consequences

- A Runtime crash between any State CAS and its Inbox side effect is observable (outbox row), retryable (worker), and bounded (8 attempts, 5-minute age threshold).
- Late Inbox settlements against closed waits become structurally impossible: tombstone is committed before any wait terminal, and `settle_as` checks it before any mutation.
- Repair worker adds one synchronous section to `Runtime::start` (bounded to 10s by default) and one persistent tokio task (1s–60s cadence, capped at 16 ops per tick). These are the only new runtime resources.
- Operator visibility moves from "look at the Inbox for stuck records" to "read `inbox_repair_metrics()` and check `oldest_pending_age_ms`." Exhaustion is logged at WARN.
- The Inbox SQLite continues to own the Inbox record lifecycle; this PR adds only a State-side outbox, a State-side tombstone, and a tombstone check on Inbox mutation.

## 12. Rejected alternatives

- **Outbox in Inbox SQLite.** Loses the same-transaction guarantee with `wait_projection`; reopens the cross-domain atomicity question ADR 0158 §8 explicitly rejects.
- **Third isolated SQLite for repair.** Adds a third consistency domain without operational benefit at this scope; increases on-disk layout complexity.
- **Pre-claim operator semaphore (Phase 3 distributed lease).** Out of scope; ADR 0158 Phase 3 work.
- **Tombstone as a property of the Inbox record itself.** Mixes State semantics into the Inbox store; ADR 0158 §1 mandates State as the single source of truth for wait identity.
- **Prometheus endpoint as part of this PR.** No existing Prometheus wiring in Y-Harness; deferring avoids dragging in a metrics-stack dependency for a single PR.
- **Generic "cross-domain repair outbox."** YAGNI at this scope; one cross-domain pair (State × Inbox) is the only case driving the design. Future cross-domain pairs (e.g. channel outbox) will be evaluated when their repair requirements are concrete.

## 13. References

- ADR 0158 §3 row 4 (State terminal × Inbox pending/settled): canonical crash matrix
- ADR 0158 §8 (idempotent repair matrix): the four scenarios this spec implements durably
- ADR 0158 Phase 2 still-open: source of the four-item list
- ADR 0158 Phase 3 still-open: explicit non-goals (distributed lease, NeedsReconciliation, etc.)
- ADR 0159: bounded indexed expiry (independent schema-1; this PR does not touch it)
- `src/approval/mod.rs`: existing `ApprovalInbox` trait, `MemoryApprovalInbox`, `SqliteApprovalInbox`
- `src/transport/http_probe.rs`: existing `ProtocolServiceStatus` surface
