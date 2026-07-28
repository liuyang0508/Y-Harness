# Performance baseline

This document records reproducible measurements, not a universal throughput
claim.

## Reference environment

- Dates: 2026-07-28 through 2026-07-29
- CPU: Apple M4 Pro (`arm64`)
- OS: macOS 15.7.3
- Rust: 1.88.0 (`aarch64-apple-darwin`)
- Store: bundled SQLite, WAL, `synchronous=FULL`
- Build: Cargo `release`

## State journal workload

One sample creates an isolated database and Thread, starts one Turn, appends N
small assistant Items one transaction at a time, finishes the Turn, fully
projects the stream, atomically forks the complete settled history, creates a
journal-anchored snapshot, reopens SQLite, and loads through the snapshot path.
Reported values are medians.

```bash
YH_BENCH_EVENTS=1000 \
YH_BENCH_SAMPLES=3 \
cargo run --release --bin yh-state-bench
```

| Events | Implementation | Append median | Events/s | Full projection | Snapshot create | Snapshot load |
|---:|---|---:|---:|---:|---:|---:|
| 100 | full replay before each append | 14.052 ms | 7,117 | 0.080 ms | — | — |
| 200 | full replay before each append | 44.934 ms | 4,451 | 0.163 ms | — | — |
| 1,000 | full replay before each append | 575.983 ms | 1,736 | 1.208 ms | — | — |
| 100 | transactional head + validated cache | 11.421 ms | 8,756 | 0.116 ms | — | — |
| 200 | transactional head + validated cache | 15.091 ms | 13,253 | 0.186 ms | — | — |
| 1,000 | transactional head + validated cache | 59.496 ms | 16,808 | 1.287 ms | — | — |
| 1,000 | snapshot maintenance + terminal reserve, 5 samples | 58.428 ms | 17,115 | 1.677 ms | 3.504 ms | 1.695 ms |
| 1,000 | atomic recovery-byte accounting + byte-bounded pages, 5 samples | 64.141 ms | 15,591 | 2.091 ms | 3.862 ms | 1.734 ms |
| 1,000 | schema-3 approval-continuation baseline, 5 samples | 68.074 ms | 14,690 | 2.942 ms | 5.547 ms | 2.384 ms |
| 1,000 | schema-4 Policy Tool-origin baseline, 5 samples | 65.461 ms | 15,276 | 2.797 ms | 5.192 ms | 2.267 ms |
| 1,000 | protocol-v10 release recheck, 5 samples | 70.265 ms | 14,232 | 2.692 ms | 5.287 ms | 2.257 ms |
| 1,000 | schema-5 Provider Continuation baseline, 5 samples | 71.967 ms | 13,895 | 2.639 ms | 5.134 ms | 2.215 ms |
| 1,000 | schema-6 safe-boundary Steering baseline, 5 samples | 76.981 ms | 12,990 | 2.837 ms | 5.470 ms | 2.360 ms |
| 1,000 | Protocol-16 lineage-summary release recheck, 5 samples | 95.226 ms | 10,501 | 2.888 ms | 5.606 ms | 2.381 ms |
| 1,000 | schema-10 portable-archive release recheck, 5 samples | 100.385 ms | 9,962 | 3.216 ms | 6.277 ms | 2.672 ms |
| 1,000 | schema-11 invocation-context release recheck, 5 samples | 110.934 ms | 9,014 | 3.095 ms | 6.139 ms | 2.587 ms |
| 1,000 | schema-11 release-gate recheck, 5 samples | 78.156 ms | 12,795 | 3.008 ms | 6.025 ms | 2.526 ms |
| 1,000 | typed-Provider-evidence release recheck, 5 samples | 84.551 ms | 11,827 | 3.216 ms | 6.276 ms | 2.637 ms |
| 1,000 | typed-Model-retry release recheck, 5 samples | 81.769 ms | 12,230 | 2.951 ms | 5.747 ms | 2.516 ms |
| 1,000 | signed-External-Skill release recheck, 5 samples | 83.025 ms | 12,045 | 2.905 ms | 5.663 ms | 2.558 ms |
| 1,000 | authenticated-HTTPS-MCP release recheck, 5 samples | 81.167 ms | 12,320 | 2.984 ms | 5.787 ms | 2.450 ms |
| 1,000 | configured-command-Model release recheck, 5 samples | 71.841 ms | 13,920 | 2.957 ms | 6.073 ms | 2.454 ms |
| 1,000 | schema-12 tenant-ownership recheck, 5 samples | 93.103 ms | 10,741 | 2.711 ms | 5.500 ms | 2.410 ms |

At 1,000 events the current path reduced append time by about 83.8% and
increased throughput by about 6.2×. Full projection remains linear and is still
performed for authoritative reads and recovery.

The recent safety rows include one additional transactional recovery-accounting
update per append and validate State through byte-bounded pages. Seven rows
recheck the schema-11 State path after adding attributed per-Turn Context
evidence. The latest schema-12 row adds one exact indexed tenant-ownership
check to each protected write. Their spread is retained as reference-host
variance. They remain inside the existing local regression thresholds;
historical rows are retained to make safety costs and machine variance visible.

## Atomic Thread fork workload

The same benchmark measures the complete Engine operation: bounded parent
recovery, exact parent-prefix hashing, child projection validation, one
immediate SQLite transaction for every child event and projection row, returned
stream revalidation, and child projection. It does not replay Tool effects.

| Date | Parent Items | Samples | Fork median |
|---|---:|---:|---:|
| 2026-07-28 | 1,000 | 5 | 9.270 ms |
| 2026-07-28 (schema 10 recheck) | 1,000 | 5 | 10.216 ms |
| 2026-07-28 (schema 11 recheck) | 1,000 | 5 | 10.090 ms |
| 2026-07-28 (release-gate recheck) | 1,000 | 5 | 9.893 ms |
| 2026-07-28 (typed Provider evidence recheck) | 1,000 | 5 | 10.752 ms |
| 2026-07-28 (typed Model retry recheck) | 1,000 | 5 | 9.666 ms |
| 2026-07-28 (signed External Skill recheck) | 1,000 | 5 | 9.811 ms |
| 2026-07-28 (authenticated HTTPS MCP recheck) | 1,000 | 5 | 9.636 ms |
| 2026-07-28 (configured command Model recheck) | 1,000 | 5 | 11.116 ms |
| 2026-07-29 (schema 12 tenant ownership) | 1,000 | 5 | 9.175 ms |

This is a local regression baseline, not a claim against another Harness or a
production latency SLA.

## Portable Thread-archive workload

After measuring fork, the benchmark exports the same complete 1,000-Item
terminal source journal, validates its source boundary and SHA-256, then
atomically imports it under a fresh target identity. Export includes bounded
State recovery, projection, journal hashing, and archive validation. Import
includes complete archive revalidation, fresh Event identities, one immediate
SQLite transaction, returned-stream validation, and target projection. It does
not include filesystem transfer or JSON file encoding.

| Date | Source Items | Samples | Export median | Import median |
|---|---:|---:|---:|---:|
| 2026-07-28 | 1,000 | 5 | 5.252 ms | 7.789 ms |
| 2026-07-28 (schema 11 recheck) | 1,000 | 5 | 5.004 ms | 8.446 ms |
| 2026-07-28 (release-gate recheck) | 1,000 | 5 | 4.951 ms | 8.175 ms |
| 2026-07-28 (typed Provider evidence recheck) | 1,000 | 5 | 5.170 ms | 8.451 ms |
| 2026-07-28 (typed Model retry recheck) | 1,000 | 5 | 4.720 ms | 7.471 ms |
| 2026-07-28 (signed External Skill recheck) | 1,000 | 5 | 4.842 ms | 7.450 ms |
| 2026-07-28 (authenticated HTTPS MCP recheck) | 1,000 | 5 | 4.723 ms | 7.487 ms |
| 2026-07-28 (configured command Model recheck) | 1,000 | 5 | 4.858 ms | 8.080 ms |
| 2026-07-29 (schema 12 tenant ownership) | 1,000 | 5 | 4.462 ms | 7.575 ms |

This is a local regression baseline, not a cross-product claim or SLA.

## Bounded Thread-summary workload

After the fork and import, each sample creates 61 additional root Threads and requests the
complete 64-Thread recent page. The SQLite query resolves each selected
stream's optional second `ThreadForked` event through the existing
`(thread_id, sequence)` index, decodes it with normal State bounds, and returns
content-free direct lineage.

| Date | Threads | Large-parent Items | Samples | List median |
|---|---:|---:|---:|---:|
| 2026-07-28 | 64 | 1,000 | 5 | 0.234 ms |
| 2026-07-28 (schema 10 recheck) | 64 | 1,000 | 5 | 0.305 ms |
| 2026-07-28 (schema 11 recheck) | 64 | 1,000 | 5 | 0.302 ms |
| 2026-07-28 (release-gate recheck) | 64 | 1,000 | 5 | 0.297 ms |
| 2026-07-28 (typed Provider evidence recheck) | 64 | 1,000 | 5 | 0.298 ms |
| 2026-07-28 (typed Model retry recheck) | 64 | 1,000 | 5 | 0.247 ms |
| 2026-07-28 (signed External Skill recheck) | 64 | 1,000 | 5 | 0.282 ms |
| 2026-07-28 (authenticated HTTPS MCP recheck) | 64 | 1,000 | 5 | 0.271 ms |
| 2026-07-28 (configured command Model recheck) | 64 | 1,000 | 5 | 0.288 ms |
| 2026-07-29 (schema 12 tenant ownership) | 64 | 1,000 | 5 | 0.278 ms |

The page is deliberately finite. This measurement does not claim recursive
ancestor closure or full-history tree projection.

## Local regression gate

The current reference-machine smoke threshold is deliberately about twice the
measured 1,000-event median:

```bash
YH_BENCH_EVENTS=1000 \
YH_BENCH_SAMPLES=5 \
YH_BENCH_MAX_APPEND_MS=120 \
YH_BENCH_MAX_PROJECT_MS=4 \
YH_BENCH_MAX_FORK_MS=25 \
YH_BENCH_MAX_ARCHIVE_EXPORT_MS=25 \
YH_BENCH_MAX_ARCHIVE_IMPORT_MS=25 \
YH_BENCH_MAX_THREAD_LIST_MS=10 \
YH_BENCH_MAX_SNAPSHOT_LOAD_MS=4 \
cargo run --release --bin yh-state-bench
```

The benchmark exits unsuccessfully when a supplied threshold is exceeded.
These numbers are not a product SLA. Hosted CI runs the same workload with
wider 350 ms append, 15 ms full-projection, 50 ms fork/export/import, 50 ms
64-Thread list, and 15 ms snapshot-load smoke thresholds; those thresholds
detect catastrophic regressions but are not used for comparative benchmarking.

## State schema migration workload

The manual ignored test constructs one schema-1 Thread immediately below the
64 MiB recovery boundary, excluding fixture construction from the timer. The
timed operation includes compact SQLite backup creation, backup manifest and
integrity validation, full streaming source/backup event fingerprints, source
stream/recovery-accounting validation, source revalidation, and transactional
metadata commit.

```bash
cargo test --release --locked --lib \
  migrates_largest_supported_thread_fixture -- --ignored --nocapture
```

| Historical events | Recovery charge | Schema path | Elapsed |
|---:|---:|---:|---:|
| 17,946 | 67,103,745 bytes | State 1 → 3 | 286.171 ms |
| 17,946 | 67,103,745 bytes | State 1 → 4 | 290.506 ms |

This single local sample was measured on the reference environment on
2026-07-25. It proves bounded completion at the supported fixture size; it is
not a stable latency SLA. Schema 2 → 4 and schema 3 → 4 use the same complete
backup and streaming fingerprint path and have separate crash-at-each-phase
tests. Schema 6 reuses that backup-first, immutable-history path for
schema-1/2/3/4/5 sources; its maximum-size migration is not relabeled as
measured by this historical schema-4 fixture.

## Approval Inbox schema migration workload

The manual ignored test constructs the maximum supported 256 schema-1 records
for one Turn, excluding fixture construction from the timer. The 133,038,080
record bytes exercise preflight validation, a no-clobber compact SQLite backup,
SHA-256 manifest and full indexed-row fingerprints, source revalidation, and
the bounded transactional schema-2 rewrite.

```bash
cargo test --release --locked --all-features --lib \
  approval::migration::tests::migrates_largest_supported_turn_fixture \
  -- --ignored --nocapture
```

| Historical records | Record bytes | Schema path | Elapsed |
|---:|---:|---:|---:|
| 256 | 133,038,080 bytes | Approval Inbox 1 → 2 | 844.781 ms |

This single local sample was measured on the reference environment on
2026-07-25. It proves bounded completion at the supported fixture size; it is
not a stable latency SLA.
