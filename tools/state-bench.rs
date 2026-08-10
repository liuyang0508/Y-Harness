use std::{
    env,
    error::Error,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::json;
use y_harness::{
    AuthorityContext, CompletionAssurance, CompletionContract, CompletionGeneration, EventId, Item,
    ItemKind, SqliteEventStore, StateEngine, ThreadId, build_completion_receipt,
    completion_model_request_sha256, completion_model_route_sha256,
    completion_runtime_governance_sha256, completion_tool_view_sha256,
    completion_verifier_manifest_sha256,
};

const DEFAULT_EVENTS: usize = 200;
const DEFAULT_SAMPLES: usize = 5;
const MAX_EVENTS: usize = 100_000;
const MAX_SAMPLES: usize = 50;
const THREAD_LIST_PAGE: usize = 64;

struct DatabaseFiles(PathBuf);

impl Drop for DatabaseFiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.0.display()));
    }
}

struct Sample {
    append: Duration,
    full_load: Duration,
    fork: Duration,
    archive_export: Duration,
    archive_import: Duration,
    thread_list: Duration,
    snapshot_create: Duration,
    snapshot_load: Duration,
}

fn main() -> Result<(), Box<dyn Error>> {
    let events = bounded_env("YH_BENCH_EVENTS", DEFAULT_EVENTS, MAX_EVENTS)?;
    let samples = bounded_env("YH_BENCH_SAMPLES", DEFAULT_SAMPLES, MAX_SAMPLES)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let mut results = Vec::with_capacity(samples);
    for sample in 0..samples {
        results.push(runtime.block_on(run_sample(sample, events))?);
    }

    let append = median(results.iter().map(|sample| sample.append).collect());
    let full_load = median(results.iter().map(|sample| sample.full_load).collect());
    let fork = median(results.iter().map(|sample| sample.fork).collect());
    let archive_export = median(results.iter().map(|sample| sample.archive_export).collect());
    let archive_import = median(results.iter().map(|sample| sample.archive_import).collect());
    let thread_list = median(results.iter().map(|sample| sample.thread_list).collect());
    let snapshot_create = median(
        results
            .iter()
            .map(|sample| sample.snapshot_create)
            .collect(),
    );
    let snapshot_load = median(results.iter().map(|sample| sample.snapshot_load).collect());
    let append_seconds = append.as_secs_f64();
    let append_ms = append_seconds * 1_000.0;
    let project_ms = full_load.as_secs_f64() * 1_000.0;
    let fork_ms = fork.as_secs_f64() * 1_000.0;
    let archive_export_ms = archive_export.as_secs_f64() * 1_000.0;
    let archive_import_ms = archive_import.as_secs_f64() * 1_000.0;
    let thread_list_ms = thread_list.as_secs_f64() * 1_000.0;
    let snapshot_create_ms = snapshot_create.as_secs_f64() * 1_000.0;
    let snapshot_load_ms = snapshot_load.as_secs_f64() * 1_000.0;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "benchmark": "sqlite_state_journal",
            "events_per_sample": events,
            "samples": samples,
            "append_median_ms": append_ms,
            "append_events_per_second": events as f64 / append_seconds,
            "project_median_ms": project_ms,
            "fork_median_ms": fork_ms,
            "archive_export_median_ms": archive_export_ms,
            "archive_import_median_ms": archive_import_ms,
            "thread_list_page": THREAD_LIST_PAGE,
            "thread_list_median_ms": thread_list_ms,
            "snapshot_create_median_ms": snapshot_create_ms,
            "snapshot_load_median_ms": snapshot_load_ms,
            "durability": {
                "journal_mode": "WAL",
                "synchronous": "FULL"
            }
        }))?
    );
    enforce_threshold("YH_BENCH_MAX_APPEND_MS", append_ms)?;
    enforce_threshold("YH_BENCH_MAX_PROJECT_MS", project_ms)?;
    enforce_threshold("YH_BENCH_MAX_FORK_MS", fork_ms)?;
    enforce_threshold("YH_BENCH_MAX_ARCHIVE_EXPORT_MS", archive_export_ms)?;
    enforce_threshold("YH_BENCH_MAX_ARCHIVE_IMPORT_MS", archive_import_ms)?;
    enforce_threshold("YH_BENCH_MAX_THREAD_LIST_MS", thread_list_ms)?;
    enforce_threshold("YH_BENCH_MAX_SNAPSHOT_LOAD_MS", snapshot_load_ms)?;
    Ok(())
}

async fn run_sample(sample: usize, events: usize) -> Result<Sample, Box<dyn Error>> {
    let path = env::temp_dir().join(format!(
        "y-harness-bench-{}-{sample}.db",
        EventId::generate()
    ));
    let files = DatabaseFiles(path.clone());
    let state = StateEngine::new(Arc::new(SqliteEventStore::open(&path).await?));
    let thread = state.create_thread().await?;
    let turn = state.start_turn(&thread.id).await?;

    let append_started = Instant::now();
    let mut final_candidate = None;
    let mut final_request_sha256 = None;
    for index in 0..events {
        let content = format!("benchmark-item-{index}");
        let model_request_sha256 = completion_model_request_sha256(&json!({
            "content": &content,
        }))?;
        let candidate = Item::new(ItemKind::AssistantMessage {
            model_id: Some("bench/model".to_owned()),
            model_origin: Some(y_harness::CapabilityOrigin::BuiltIn),
            model_request_sha256: Some(model_request_sha256.clone()),
            content,
        });
        state.append_item(&turn, candidate.clone()).await?;
        final_candidate = Some(candidate.id);
        final_request_sha256 = Some(model_request_sha256);
    }
    let append = append_started.elapsed();
    let running = state
        .load_thread(&thread.id)
        .await?
        .and_then(|thread| thread.turns.into_iter().next())
        .ok_or("benchmark running Turn disappeared")?;
    let generation = CompletionGeneration::new(
        final_request_sha256.ok_or("benchmark produced no Model request")?,
        completion_model_route_sha256(&["bench/model"])?,
        completion_tool_view_sha256(&Vec::<String>::new())?,
        completion_verifier_manifest_sha256(&[])?,
        completion_runtime_governance_sha256(&json!({"benchmark": "sqlite_state_journal"}))?,
        None,
        CompletionAssurance::RuntimeMeasured,
    )?;
    let receipt = build_completion_receipt(
        &running,
        &AuthorityContext::local_process(),
        &final_candidate.ok_or("benchmark produced no completion candidate")?,
        generation,
        CompletionContract::v1_no_external_requirements(),
    )?;
    state.complete_turn(&running, receipt).await?;

    let load_started = Instant::now();
    let projected = state
        .load_thread(&thread.id)
        .await?
        .ok_or("benchmark thread disappeared")?;
    let full_load = load_started.elapsed();
    assert_eq!(projected.turns[0].items.len(), events);
    let fork_started = Instant::now();
    let child = state
        .fork_thread(&thread.id, ThreadId::generate(), None)
        .await?;
    let fork = fork_started.elapsed();
    assert_eq!(child.turns[0].items.len(), events);
    let archive_export_started = Instant::now();
    let archive = state.export_thread(&thread.id).await?;
    let archive_export = archive_export_started.elapsed();
    let archive_import_started = Instant::now();
    let imported = state.import_thread(&archive, ThreadId::generate()).await?;
    let archive_import = archive_import_started.elapsed();
    assert_eq!(imported.turns[0].items.len(), events);
    for _ in 3..THREAD_LIST_PAGE {
        state.create_thread().await?;
    }
    let thread_list_started = Instant::now();
    let summaries = state.list_threads(None, THREAD_LIST_PAGE).await?;
    let thread_list = thread_list_started.elapsed();
    assert_eq!(summaries.threads.len(), THREAD_LIST_PAGE);
    assert!(
        summaries
            .threads
            .iter()
            .find(|summary| summary.thread_id == child.id)
            .is_some_and(|summary| summary.lineage.is_some())
    );
    let snapshot_started = Instant::now();
    state.create_snapshot(&thread.id).await?;
    let snapshot_create = snapshot_started.elapsed();
    drop(state);

    let reopened = StateEngine::new(Arc::new(SqliteEventStore::open(&path).await?));
    let snapshot_load_started = Instant::now();
    let projected = reopened
        .load_thread(&thread.id)
        .await?
        .ok_or("snapshot benchmark thread disappeared")?;
    let snapshot_load = snapshot_load_started.elapsed();
    assert_eq!(projected.turns[0].items.len(), events);
    drop(reopened);
    drop(files);
    Ok(Sample {
        append,
        full_load,
        fork,
        archive_export,
        archive_import,
        thread_list,
        snapshot_create,
        snapshot_load,
    })
}

fn bounded_env(name: &str, default: usize, max: usize) -> Result<usize, Box<dyn Error>> {
    let value = match env::var(name) {
        Ok(value) => value.parse::<usize>()?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 || value > max {
        return Err(format!("{name} must be between 1 and {max}").into());
    }
    Ok(value)
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn enforce_threshold(name: &str, actual_ms: f64) -> Result<(), Box<dyn Error>> {
    let Some(raw) = env::var_os(name) else {
        return Ok(());
    };
    let maximum_ms = raw
        .to_str()
        .ok_or_else(|| format!("{name} must be valid UTF-8"))?
        .parse::<f64>()?;
    if !maximum_ms.is_finite() || maximum_ms <= 0.0 {
        return Err(format!("{name} must be a finite positive number").into());
    }
    if actual_ms > maximum_ms {
        return Err(format!(
            "{name} regression: measured {actual_ms:.3} ms, threshold {maximum_ms:.3} ms"
        )
        .into());
    }
    Ok(())
}
