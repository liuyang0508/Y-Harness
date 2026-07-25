# Performance baseline

This document records reproducible measurements, not a universal throughput
claim.

## Reference environment

- Date: 2026-07-25
- CPU: Apple M4 Pro (`arm64`)
- OS: macOS 15.7.3
- Rust: 1.88.0 (`aarch64-apple-darwin`)
- Store: bundled SQLite, WAL, `synchronous=FULL`
- Build: Cargo `release`

## State journal workload

One sample creates an isolated database and Thread, starts one Turn, appends N
small assistant Items one transaction at a time, finishes the Turn, fully
projects the stream, creates a journal-anchored snapshot, reopens SQLite, and
loads through the snapshot path. Reported values are medians.

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

At 1,000 events the current path reduced append time by about 87.8% and
increased throughput by about 8.2×. Full projection remains linear and is still
performed for authoritative reads and recovery.

The last four rows include one additional transactional recovery-accounting
update per append and validate State through byte-bounded pages. The latest
row rechecks the unchanged State path after the protocol-v10 slice. It remains
inside the existing local regression thresholds; historical rows are retained
to make safety costs and machine variance visible.

## Local regression gate

The current reference-machine smoke threshold is deliberately about twice the
measured 1,000-event median:

```bash
YH_BENCH_EVENTS=1000 \
YH_BENCH_SAMPLES=5 \
YH_BENCH_MAX_APPEND_MS=120 \
YH_BENCH_MAX_PROJECT_MS=4 \
YH_BENCH_MAX_SNAPSHOT_LOAD_MS=4 \
cargo run --release --bin yh-state-bench
```

The benchmark exits unsuccessfully when a supplied threshold is exceeded.
These numbers are not a product SLA. Hosted CI runs the same workload with
wider 350 ms append, 15 ms full-projection, and 15 ms snapshot-load smoke
thresholds; those thresholds detect catastrophic regressions but are not used
for comparative benchmarking.

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
tests.

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
