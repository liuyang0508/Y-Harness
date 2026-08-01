# ADR 0141: Durable Effect dispatch governance by trusted execution lane

- Status: accepted
- Date: 2026-08-01

## Context

The Governed Effect Executor already owns durable Claims, exact Policy gates,
bounded Connector calls, uncertain-outcome settlement, and restart recovery.
It did not own a durable rate limit or circuit breaker. A process-local counter
would reset on restart and diverge across service processes. Calling arbitrary
fields inside Effect input a “target” would also invent authority and semantics
that the Engine does not possess.

The Effect Ledger starts at schema 1 and intentionally has no implicit
migration path. Adding unrelated tables to that database without changing its
schema coordinate would make schema 1 mean two different physical layouts.

## Decision

- Add embedded Effect dispatch-governor API 1 with Memory and independent
  SQLite schema-1 implementations.
- Define one exact lane as trusted `tenant + capability + operation +
  policy_id`. The governor never parses Effect input, reason strings, or
  Connector output to invent a finer business target.
- Freeze policy parameters by a canonical SHA-256 under `policy_id`. Reusing a
  policy identity with different parameters fails closed; deployments must
  choose a new policy revision explicitly.
- Admit after the Ledger commits a fenced Claim and before the Connector
  boundary. A rate or circuit denial therefore proves that no Connector was
  entered and is durably settled as `NotApplied` with an absolute retry time.
- Atomically maintain a deterministic fixed-window count, consecutive
  availability failures, a monotonic circuit epoch, open deadline, and one
  leased half-open probe. Each lane rejects a regressing trusted admission
  clock; late settlements use the lane's latest observed time. The probe
  bypasses the normal rate count so a full window cannot prevent recovery
  testing.
- Use the never-reused Effect lease as the admission identity. Retained
  admission records make uncertain retries idempotent and reject identity
  rebinding. Retention is at least the maximum Effect lease duration; expired
  records are pruned transactionally and the store has a hard admission bound.
- Only Harness-owned typed evidence affects circuit health:
  - a contract-valid Connector outcome, including a valid `Unknown`, is
    healthy transport evidence;
  - Connector panic, error, timeout, or invalid evidence is an availability
    failure;
  - cancellation is abandoned and does not punish the lane.
  No `reason_code` text is interpreted.
- Increment the circuit epoch when opening or reopening. A late settlement
  from an earlier epoch cannot close or reset the newer circuit.
- Keep authoritative read-only reconciliation outside dispatch governance.
  An execution outage must not prevent convergence of an already-unknown
  Effect.
- The reference service may opt into one persistent
  `.y-harness/effect-governance.db`. `doctor` validates an existing store
  read-only and reports schema/configuration facts without creating it.
- A governor admission failure after Claim fails closed and safely reschedules
  the Effect without Connector entry. A governor health-settlement failure
  does not rewrite an authoritative external Effect result; it is exposed by
  `EffectExecutorAttempt::governor_settlement_failed` and degrades reference
  service health. A custom governor's denial is accepted only when its absolute
  retry boundary is valid for the trusted admission time; invalid evidence is
  treated as governor unavailability.

## Consequences and non-claims

- Rate and circuit state survive restart and serialize concurrent local
  processes through SQLite transactions.
- The Effect Ledger remains the sole external-effect truth and recovery
  authority; the separate governor database contains only execution-control
  evidence.
- The two SQLite databases do not form a distributed transaction. A committed
  governor admission may consume capacity even if later Ledger settlement
  fails. That is a conservative availability loss, not an unrecorded target
  call. Conversely, health settlement can fail after a Connector call; this is
  observable but cannot be made atomic with the external system.
- “Per lane” is not “per recipient”, “per account”, or “per vendor endpoint”.
  A deployment that needs a finer trustworthy boundary must expose it as a
  separate registered capability/operation or introduce a future typed target
  coordinate; it must not smuggle authority through arbitrary JSON.
- Fixed-window limiting is deterministic and cheap but permits boundary
  bursts. This ADR does not claim token-bucket smoothing, distributed SQL,
  global multi-region consensus, automatic policy rollout, or target SLA
  certification.
- SQLite provides multi-process serialization on one shared filesystem, not
  multi-node leader election or a networked control plane.

## Rejected alternatives

- Process-local counters: reset on restart and disagree across workers.
- Add tables silently to `effects.db`: changes schema-1 physical meaning
  without a migration coordinate.
- Admit before Claim: requires atomic cross-store Claim and permit ownership or
  leaks permits/probes when another worker wins the Claim.
- Infer targets or failures from request JSON and reason strings: untyped,
  spoofable, and domain-specific.
- Gate reconciliation with the execution circuit: can prevent the only
  authoritative path that resolves an uncertain external result.
- Treat governor settlement failure as target failure: corrupts external truth
  with an internal control-plane outage.

## Evidence

- `effect::governor::tests::memory_governor_enforces_atomic_lane_contract`
- `effect::governor::tests::sqlite_governor_persists_lane_state_across_reopen`
- `effect::governor::tests::sqlite_open_circuit_survives_reopen`
- `effect::governor::tests::independent_sqlite_connections_serialize_one_rate_slot`
- `effect::governor::tests::concurrent_sqlite_bootstrap_is_atomic`
- `effect::governor::tests::stale_epoch_and_duplicate_settlement_cannot_close_new_circuit`
- `effect::governor::tests::read_only_validation_does_not_create_a_missing_store`
- `effect::governor::tests::read_only_validation_rejects_partial_store`
- `effect::governor::tests::lane_rejects_a_regressing_trusted_clock`
- `effect::executor::tests::durable_rate_limit_prevents_connector_entry_and_schedules_exact_retry`
- `effect::executor::tests::availability_failure_opens_circuit_without_parsing_reason_strings`
- `effect::executor::tests::connector_returned_unknown_is_healthy_transport_evidence`
- `effect::executor::tests::unavailable_governor_fails_closed_after_claim_without_connector_entry`
- `effect::executor::tests::invalid_governor_decision_fails_closed_without_connector_entry`
- `effect::executor::tests::governor_settlement_failure_is_visible_without_corrupting_effect_truth`
- `effect::executor::tests::clock_failure_after_governed_dispatch_reports_unsettled_governor_health`
- `reference_cli::service::tests::effect_consumer_requires_explicit_exact_authority_and_bounded_timeouts`
- `configured_effect_consumer_degrades_recovers_stops_and_does_not_replay_terminal_effects`
- `doctor_and_service_reject_a_partial_effect_governor_store_without_mutation`
