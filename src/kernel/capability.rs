//! Provider-neutral executable capability contracts owned by the kernel.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use serde_json::Value;

use super::{
    CancellationToken, HarnessFuture, ModelOutput, ModelRequest, ModelResponse, ModelStreamEvent,
    ToolContext, ToolDescriptor,
};

const MAX_STREAM_DELTA_BYTES: usize = 4_096;
const MAX_STREAM_BYTES: usize = 1_048_576;

/// Synchronous non-blocking sink for provisional model events.
///
/// A sink handles application content and therefore requires the same data
/// governance as model output. Implementations must not perform blocking I/O.
pub trait ModelEventSink: Send + Sync {
    /// Accepts one validated provisional event.
    fn emit(&self, event: &ModelStreamEvent) -> Result<(), String>;
}

struct ModelStreamState {
    sink: Option<Arc<dyn ModelEventSink>>,
    emitted_bytes: AtomicUsize,
    delivered_events: AtomicU64,
    dropped_events: AtomicU64,
}

struct ModelStreamGate {
    state: Mutex<ModelStreamGateState>,
    drained: Condvar,
}

struct ModelStreamGateState {
    open: bool,
    in_flight: usize,
}

impl ModelStreamGate {
    fn new(open: bool) -> Self {
        Self {
            state: Mutex::new(ModelStreamGateState { open, in_flight: 0 }),
            drained: Condvar::new(),
        }
    }

    fn begin(&self) -> Option<ModelStreamDelivery<'_>> {
        let mut state = self.state.lock().ok()?;
        if !state.open {
            return None;
        }
        state.in_flight = state.in_flight.checked_add(1)?;
        Some(ModelStreamDelivery { gate: self })
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.open = false;
        while state.in_flight > 0 {
            state = self
                .drained
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct ModelStreamDelivery<'a> {
    gate: &'a ModelStreamGate,
}

impl Drop for ModelStreamDelivery<'_> {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight = state.in_flight.saturating_sub(1);
        if state.in_flight == 0 {
            self.gate.drained.notify_all();
        }
    }
}

/// Failure-isolated, byte-bounded handle exposed to streaming model providers.
#[derive(Clone)]
pub struct ModelStream {
    state: Arc<ModelStreamState>,
    model_step: u32,
    gate: Arc<ModelStreamGate>,
    cancellation: CancellationToken,
}

impl ModelStream {
    /// Creates a disabled stream that accepts no events.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            state: Arc::new(ModelStreamState {
                sink: None,
                emitted_bytes: AtomicUsize::new(0),
                delivered_events: AtomicU64::new(0),
                dropped_events: AtomicU64::new(0),
            }),
            model_step: 0,
            gate: Arc::new(ModelStreamGate::new(false)),
            cancellation: CancellationToken::new(),
        }
    }

    /// Creates one bounded Turn-level stream over a caller-owned sink.
    #[must_use]
    pub fn new(sink: Arc<dyn ModelEventSink>) -> Self {
        Self {
            state: Arc::new(ModelStreamState {
                sink: Some(sink),
                emitted_bytes: AtomicUsize::new(0),
                delivered_events: AtomicU64::new(0),
                dropped_events: AtomicU64::new(0),
            }),
            model_step: 0,
            gate: Arc::new(ModelStreamGate::new(false)),
            cancellation: CancellationToken::new(),
        }
    }

    #[must_use]
    pub(crate) fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    #[must_use]
    pub(crate) fn for_step(&self, model_step: u32) -> Self {
        Self {
            state: self.state.clone(),
            model_step,
            gate: Arc::new(ModelStreamGate::new(model_step > 0)),
            cancellation: self.cancellation.clone(),
        }
    }

    /// Emits one provisional text fragment without blocking or failing inference.
    ///
    /// Returns `false` when streaming is disabled, the event exceeds a bound,
    /// the Turn budget is exhausted, or the sink fails.
    pub fn emit_text_delta(&self, delta: impl Into<String>) -> bool {
        let Some(sink) = &self.state.sink else {
            return false;
        };
        let delta = delta.into();
        if delta.is_empty() || delta.len() > MAX_STREAM_DELTA_BYTES {
            self.state.dropped_events.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let Some(_delivery) = self.gate.begin() else {
            self.state.dropped_events.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        if self
            .state
            .emitted_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(delta.len())
                    .filter(|next| *next <= MAX_STREAM_BYTES)
            })
            .is_err()
        {
            self.state.dropped_events.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let event = ModelStreamEvent::TextDelta {
            model_step: self.model_step,
            delta,
        };
        let result = catch_unwind(AssertUnwindSafe(|| sink.emit(&event)));
        if matches!(result, Ok(Ok(()))) {
            self.state.delivered_events.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.state.dropped_events.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Returns provider events rejected by bounds or sink failure.
    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        self.state.dropped_events.load(Ordering::Relaxed)
    }

    #[must_use]
    pub(crate) fn delivered_events(&self) -> u64 {
        self.state.delivered_events.load(Ordering::Relaxed)
    }

    /// Returns whether a caller installed a provisional-event sink.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.state.sink.is_some()
    }

    /// Returns the current Model-attempt cooperative stop signal.
    ///
    /// It is cancelled when the attempt settles or when the owning Turn stops.
    /// Providers that launch external work must either observe this token or
    /// guarantee cancellation-safe cleanup when their future is dropped.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(crate) fn close(&self) {
        self.gate.close();
    }
}

/// Object-safe language-model capability consumed by the Agent Loop.
pub trait LanguageModel: Send + Sync {
    /// Stable provider/model identity for evidence and diagnostics.
    fn id(&self) -> &str;

    /// Produces the next message or tool-call decision.
    fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput>;

    /// Produces a decision with optional provider-reported usage and correlation.
    fn complete_with_metadata<'a>(
        &'a self,
        request: ModelRequest,
    ) -> HarnessFuture<'a, ModelResponse> {
        Box::pin(async move { self.complete(request).await.map(ModelResponse::from) })
    }

    /// Produces a decision while optionally emitting provisional text deltas.
    ///
    /// The default preserves non-streaming provider behavior. The returned
    /// response remains authoritative even when provisional events were sent.
    fn complete_streaming<'a>(
        &'a self,
        request: ModelRequest,
        _stream: ModelStream,
    ) -> HarnessFuture<'a, ModelResponse> {
        self.complete_with_metadata(request)
    }
}

/// Executable tool capability.
pub trait Tool: Send + Sync {
    /// Returns validated model-visible metadata.
    fn descriptor(&self) -> ToolDescriptor;

    /// Executes one authorized call.
    fn execute<'a>(&'a self, input: Value, context: ToolContext) -> HarnessFuture<'a, Value>;
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Condvar, Mutex,
            mpsc::{self, TryRecvError},
        },
        thread,
    };

    use super::{MAX_STREAM_DELTA_BYTES, ModelEventSink, ModelStream};
    use crate::ModelStreamEvent;

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<ModelStreamEvent>>,
    }

    impl ModelEventSink for RecordingSink {
        fn emit(&self, event: &ModelStreamEvent) -> Result<(), String> {
            self.events
                .lock()
                .map_err(|_| "recording sink poisoned".to_owned())?
                .push(event.clone());
            Ok(())
        }
    }

    struct PanickingSink;

    struct BlockingSink {
        entered: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl ModelEventSink for PanickingSink {
        fn emit(&self, _event: &ModelStreamEvent) -> Result<(), String> {
            panic!("sink panic")
        }
    }

    impl ModelEventSink for BlockingSink {
        fn emit(&self, _event: &ModelStreamEvent) -> Result<(), String> {
            self.entered
                .send(())
                .map_err(|_| "entry receiver dropped".to_owned())?;
            let (released, wake) = &*self.release;
            let mut released = released
                .lock()
                .map_err(|_| "release lock poisoned".to_owned())?;
            while !*released {
                released = wake
                    .wait(released)
                    .map_err(|_| "release wait poisoned".to_owned())?;
            }
            Ok(())
        }
    }

    #[test]
    fn model_stream_bounds_and_correlates_provisional_content() {
        let sink = Arc::new(RecordingSink::default());
        let stream = ModelStream::new(sink.clone());
        assert!(!stream.emit_text_delta("missing step"));
        let step = stream.for_step(2);
        assert!(step.emit_text_delta("hello"));
        assert_eq!(stream.delivered_events(), 1);
        assert!(!step.emit_text_delta("x".repeat(MAX_STREAM_DELTA_BYTES + 1)));
        assert_eq!(stream.dropped_events(), 2);
        assert_eq!(
            sink.events.lock().expect("events").as_slice(),
            [ModelStreamEvent::TextDelta {
                model_step: 2,
                delta: "hello".to_owned(),
            }]
        );
        step.close();
        assert!(!step.emit_text_delta("late"));
        assert_eq!(stream.dropped_events(), 3);
    }

    #[test]
    fn model_stream_isolates_sink_panics() {
        let stream = ModelStream::new(Arc::new(PanickingSink)).for_step(1);
        assert!(!stream.emit_text_delta("safe"));
        assert_eq!(stream.delivered_events(), 0);
        assert_eq!(stream.dropped_events(), 1);
    }

    #[test]
    fn model_stream_close_waits_for_in_flight_delivery() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let stream = ModelStream::new(Arc::new(BlockingSink {
            entered: entered_tx,
            release: release.clone(),
        }))
        .for_step(1);
        let emitting_stream = stream.clone();
        let emitter = thread::spawn(move || emitting_stream.emit_text_delta("in flight"));
        entered_rx.recv().expect("sink entry");

        let (close_started_tx, close_started_rx) = mpsc::channel();
        let (close_done_tx, close_done_rx) = mpsc::channel();
        let closing_stream = stream.clone();
        let closer = thread::spawn(move || {
            close_started_tx.send(()).expect("close start");
            closing_stream.close();
            close_done_tx.send(()).expect("close done");
        });
        close_started_rx.recv().expect("closer scheduled");
        assert!(matches!(close_done_rx.try_recv(), Err(TryRecvError::Empty)));

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_all();
        assert!(emitter.join().expect("emitter"));
        close_done_rx.recv().expect("close completion");
        closer.join().expect("closer");
        assert_eq!(stream.delivered_events(), 1);
        assert!(!stream.emit_text_delta("late"));
    }
}
