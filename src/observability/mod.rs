//! Read-only phase observations and trace exports derived from Runtime evidence.

use std::{
    collections::{BTreeMap, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{AsyncWriteExt, BufWriter},
};

use crate::{
    CapabilityOrigin, ExecutionPhase, HarnessError, ItemKind, ModelUsage, StateEvent, StoredEvent,
    ThreadId, TurnId,
    json::{BoundedJsonError, to_bounded_json_vec, validate_value_shape},
    kernel::{validate_capability_name, validate_capability_origin, validate_registry_growth},
};

const MAX_TRACE_RECORDS: usize = 65_536;
const MAX_OBSERVATION_ID_BYTES: usize = 256;
const MAX_TRACE_EXPORT_EVENT_BYTES: usize = 8_392_704;

/// Outcome class for one externally awaited Runtime phase.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationOutcome {
    /// Capability returned successfully.
    Success,
    /// Capability returned a non-control error.
    Error,
    /// Caller cancellation won the wait.
    Cancelled,
    /// Runtime deadline won the wait.
    TimedOut,
}

/// Content-free timing and settlement evidence for one Runtime phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PhaseObservation {
    /// Owning Thread.
    pub thread_id: ThreadId,
    /// Active Turn.
    pub turn_id: TurnId,
    /// Controlled phase.
    pub phase: ExecutionPhase,
    /// Stable capability identity without request content.
    pub capability: String,
    /// Monotonic elapsed duration.
    pub duration_micros: u64,
    /// Settlement class.
    pub outcome: ObservationOutcome,
    /// Provider-reported model usage when available.
    pub model_usage: Option<ModelUsage>,
    /// Opaque bounded provider request ID when available.
    pub provider_request_id: Option<String>,
    /// Provisional provider events rejected during this phase.
    pub stream_events_dropped: u64,
}

/// Synchronous non-blocking observation sink.
///
/// Implementations must not perform blocking I/O. Failures are isolated by the
/// registry and can never change Agent Loop settlement.
pub trait Observer: Send + Sync {
    /// Records one content-free observation or returns a bounded diagnostic.
    fn observe(&self, observation: &PhaseObservation) -> Result<(), String>;
}

/// Registered Observer with its trust-bearing origin.
pub struct RegisteredObserver {
    /// Stable observer name.
    pub name: String,
    /// Host-assigned implementation origin.
    pub origin: CapabilityOrigin,
    /// Observation sink.
    pub observer: Arc<dyn Observer>,
}

/// Failure-isolated deterministic Observer registry.
#[derive(Clone, Default)]
pub struct Observability {
    observers: Arc<BTreeMap<String, RegisteredObserver>>,
    dropped: Arc<AtomicU64>,
}

impl Observability {
    /// Creates an empty no-op registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one Observer without allowing name replacement.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        origin: CapabilityOrigin,
        observer: Arc<dyn Observer>,
    ) -> Result<(), HarnessError> {
        let name = name.into();
        validate_capability_origin(&origin)?;
        validate_capability_name("observer", &name)?;
        let observers = Arc::get_mut(&mut self.observers).ok_or_else(|| {
            HarnessError::InvalidConfiguration(
                "cannot register observers after Observability was shared".to_owned(),
            )
        })?;
        validate_registry_growth("observer", observers.len(), 1)?;
        if observers.contains_key(&name) {
            return Err(HarnessError::DuplicateCapability(format!(
                "observer {name}"
            )));
        }
        observers.insert(
            name.clone(),
            RegisteredObserver {
                name,
                origin,
                observer,
            },
        );
        Ok(())
    }

    /// Emits one observation while isolating Observer errors and panics.
    pub fn emit(&self, observation: &PhaseObservation) {
        if !observation_metadata_is_bounded(observation) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        for registered in self.observers.values() {
            let result = catch_unwind(AssertUnwindSafe(|| {
                registered.observer.observe(observation)
            }));
            if !matches!(result, Ok(Ok(()))) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns the number of Observer failures or panics isolated so far.
    #[must_use]
    pub fn dropped_observations(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Bounded in-memory observation sink for tests and local diagnostics.
pub struct TraceCollector {
    capacity: usize,
    records: Mutex<VecDeque<PhaseObservation>>,
    dropped: AtomicU64,
}

impl TraceCollector {
    /// Creates a collector with a hard retained-record capacity.
    pub fn new(capacity: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_TRACE_RECORDS).contains(&capacity) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "trace collector capacity must be 1-{MAX_TRACE_RECORDS}"
            )));
        }
        Ok(Self {
            capacity,
            records: Mutex::new(VecDeque::with_capacity(capacity)),
            dropped: AtomicU64::new(0),
        })
    }

    /// Returns a stable snapshot in observation order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<PhaseObservation> {
        self.records
            .lock()
            .map_or_else(|_| Vec::new(), |records| records.iter().cloned().collect())
    }

    /// Returns records rejected because the collector was full or poisoned.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Observer for TraceCollector {
    fn observe(&self, observation: &PhaseObservation) -> Result<(), String> {
        if !observation_metadata_is_bounded(observation) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Err("trace observation metadata is invalid".to_owned());
        }
        let mut records = self
            .records
            .lock()
            .map_err(|_| "trace collector lock poisoned".to_owned())?;
        if records.len() >= self.capacity {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        records.push_back(observation.clone());
        Ok(())
    }
}

fn observation_metadata_is_bounded(observation: &PhaseObservation) -> bool {
    [
        observation.thread_id.as_str(),
        observation.turn_id.as_str(),
        observation.capability.as_str(),
    ]
    .into_iter()
    .all(|value| {
        !value.is_empty()
            && value.len() <= MAX_OBSERVATION_ID_BYTES
            && !value.chars().any(char::is_control)
    }) && observation
        .provider_request_id
        .as_ref()
        .is_none_or(|value| {
            !value.is_empty()
                && value.len() <= MAX_OBSERVATION_ID_BYTES
                && !value.chars().any(char::is_control)
        })
}

/// Writes stored events as newline-delimited JSON without changing State.
pub async fn export_jsonl(
    path: impl AsRef<Path>,
    events: &[StoredEvent],
) -> Result<(), HarnessError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .await
            .map_err(|error| HarnessError::Trace(error.to_string()))?;
    }

    let mut writer = BufWriter::new(
        File::create(path)
            .await
            .map_err(|error| HarnessError::Trace(error.to_string()))?,
    );
    for event in events {
        validate_export_event_json_shape(event)?;
        let mut line = to_bounded_json_vec(event, MAX_TRACE_EXPORT_EVENT_BYTES).map_err(
            |error| match error {
                BoundedJsonError::LimitExceeded => HarnessError::Trace(format!(
                    "trace event exceeds {MAX_TRACE_EXPORT_EVENT_BYTES} encoded bytes"
                )),
                BoundedJsonError::CannotEncode => {
                    HarnessError::Trace("cannot encode trace event".to_owned())
                }
            },
        )?;
        line.push(b'\n');
        writer
            .write_all(&line)
            .await
            .map_err(|error| HarnessError::Trace(error.to_string()))?;
    }
    writer
        .flush()
        .await
        .map_err(|error| HarnessError::Trace(error.to_string()))
}

fn validate_export_event_json_shape(event: &StoredEvent) -> Result<(), HarnessError> {
    let value = match &event.event {
        StateEvent::ItemAppended { item, .. } => match &item.kind {
            ItemKind::ToolCall { input, .. } => Some(input),
            ItemKind::ToolResult { output, .. } => Some(output),
            _ => None,
        },
        _ => None,
    };
    if value.is_some_and(|value| validate_value_shape(value).is_err()) {
        return Err(HarnessError::Trace(
            "trace event JSON exceeds the supported depth or node count".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        Observability, ObservationOutcome, Observer, PhaseObservation, TraceCollector, export_jsonl,
    };
    use crate::{
        CapabilityOrigin, EventId, ExecutionPhase, Item, ItemKind, ModelUsage,
        STATE_EVENT_SCHEMA_VERSION, StateEvent, StoredEvent, ThreadId, TurnId,
    };

    struct FailingObserver;

    impl Observer for FailingObserver {
        fn observe(&self, _observation: &PhaseObservation) -> Result<(), String> {
            Err("unavailable".to_owned())
        }
    }

    fn observation() -> PhaseObservation {
        PhaseObservation {
            thread_id: ThreadId::from_static("thread-test"),
            turn_id: TurnId::from_static("turn-test"),
            phase: ExecutionPhase::Model,
            capability: "test/model".to_owned(),
            duration_micros: 10,
            outcome: ObservationOutcome::Success,
            model_usage: Some(ModelUsage {
                input_tokens: 10,
                output_tokens: 2,
                cached_input_tokens: 3,
                reasoning_tokens: 1,
                cost_usd_ticks: Some(50_000),
            }),
            provider_request_id: Some("request-test".to_owned()),
            stream_events_dropped: 0,
        }
    }

    #[test]
    fn observer_failures_do_not_escape_the_registry() {
        let mut observability = Observability::new();
        observability
            .register(
                "failing",
                CapabilityOrigin::BuiltIn,
                Arc::new(FailingObserver),
            )
            .expect("register observer");
        observability.emit(&observation());
        assert_eq!(observability.dropped_observations(), 1);
    }

    #[test]
    fn trace_collector_is_bounded_without_evicting_prior_evidence() {
        assert!(TraceCollector::new(super::MAX_TRACE_RECORDS + 1).is_err());
        let collector = TraceCollector::new(1).expect("collector");
        collector.observe(&observation()).expect("first");
        collector.observe(&observation()).expect("second");
        assert_eq!(collector.snapshot().len(), 1);
        assert_eq!(collector.dropped(), 1);
    }

    #[test]
    fn observation_metadata_is_rejected_before_retention_or_delivery() {
        let collector = Arc::new(TraceCollector::new(1).expect("collector"));
        let mut observability = Observability::new();
        observability
            .register("collector", CapabilityOrigin::BuiltIn, collector.clone())
            .expect("register collector");
        let mut oversized = observation();
        oversized.capability = "x".repeat(super::MAX_OBSERVATION_ID_BYTES + 1);

        observability.emit(&oversized);

        assert!(collector.snapshot().is_empty());
        assert_eq!(observability.dropped_observations(), 1);
    }

    #[tokio::test]
    async fn trace_export_rejects_deep_json_before_serialization_growth() {
        let mut deeply_nested = serde_json::Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            deeply_nested = serde_json::Value::Array(vec![deeply_nested]);
        }
        let thread_id = ThreadId::from_static("thread-deep-trace");
        let event = StoredEvent {
            schema_version: STATE_EVENT_SCHEMA_VERSION,
            sequence: 1,
            event_id: EventId::generate(),
            thread_id: thread_id.clone(),
            recorded_at_ms: 1,
            event: StateEvent::ItemAppended {
                turn_id: TurnId::from_static("turn-deep-trace"),
                item: Item::new(ItemKind::ToolResult {
                    call_id: "call-deep".to_owned(),
                    output: deeply_nested,
                    is_error: false,
                }),
            },
        };
        let path = std::env::temp_dir().join(format!(
            "y-harness-trace-export-{}.jsonl",
            EventId::generate()
        ));

        let error = export_jsonl(&path, &[event])
            .await
            .expect_err("deep trace event");
        let _ = tokio::fs::remove_file(path).await;

        assert!(error.to_string().contains("depth or node count"));
    }
}
