//! Turn execution options and bounded external-operation waits.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::time::{Instant, sleep_until, timeout};

use crate::{
    ApprovalActor, CancellationToken, ExecutionPhase, HarnessError, MemoryScope, ModelEventSink,
    TurnContextInput, isolation::isolate_future,
};

/// Caller-controlled isolation, cancellation, and deadline for one Turn.
#[derive(Clone)]
pub struct TurnExecutionOptions {
    /// Identity attributed to approval requests created by this Turn.
    ///
    /// Embedding hosts must derive authenticated actors from their trusted
    /// caller boundary rather than accepting arbitrary display names.
    pub approval_requester: ApprovalActor,
    /// Scope applied to long-term memory operations.
    pub memory_scope: MemoryScope,
    /// Bounded non-authoritative reference context supplied for this Turn.
    pub context: Vec<TurnContextInput>,
    /// Optional total deadline for external Context, Model, Policy, and Tool work.
    ///
    /// State settlement may continue after this duration so the journal always
    /// receives a deterministic terminal record.
    pub timeout: Option<Duration>,
    /// Cooperative signal shared with the runtime and invoked tools.
    pub cancellation: CancellationToken,
    /// Optional governed sink for provisional model text.
    pub model_event_sink: Option<Arc<dyn ModelEventSink>>,
}

impl Default for TurnExecutionOptions {
    fn default() -> Self {
        Self {
            approval_requester: ApprovalActor::LocalProcess,
            memory_scope: MemoryScope::default(),
            context: Vec::new(),
            timeout: None,
            cancellation: CancellationToken::new(),
            model_event_sink: None,
        }
    }
}

pub(super) fn deadline(timeout: Option<Duration>) -> Result<Option<Instant>, HarnessError> {
    timeout
        .map(|duration| {
            Instant::now().checked_add(duration).ok_or_else(|| {
                HarnessError::InvalidConfiguration(
                    "Turn timeout exceeds the runtime clock range".to_owned(),
                )
            })
        })
        .transpose()
}

pub(super) async fn controlled<F, T>(
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
    phase: ExecutionPhase,
    operation: impl FnOnce() -> F,
) -> Result<T, HarnessError>
where
    F: Future<Output = Result<T, HarnessError>>,
{
    controlled_inner(
        cancellation,
        deadline,
        phase,
        operation,
        None,
        Duration::ZERO,
    )
    .await
}

pub(super) async fn controlled_with_settlement_cancellation<F, T>(
    cancellation: &CancellationToken,
    settlement_cancellation: CancellationToken,
    deadline: Option<Instant>,
    phase: ExecutionPhase,
    operation: impl FnOnce() -> F,
) -> Result<T, HarnessError>
where
    F: Future<Output = Result<T, HarnessError>>,
{
    controlled_inner(
        cancellation,
        deadline,
        phase,
        operation,
        Some(settlement_cancellation),
        Duration::ZERO,
    )
    .await
}

pub(super) async fn controlled_with_settlement_grace<F, T>(
    cancellation: &CancellationToken,
    settlement_cancellation: CancellationToken,
    deadline: Option<Instant>,
    phase: ExecutionPhase,
    settlement_timeout: Duration,
    operation: impl FnOnce() -> F,
) -> Result<T, HarnessError>
where
    F: Future<Output = Result<T, HarnessError>>,
{
    controlled_inner(
        cancellation,
        deadline,
        phase,
        operation,
        Some(settlement_cancellation),
        settlement_timeout,
    )
    .await
}

async fn controlled_inner<F, T>(
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
    phase: ExecutionPhase,
    operation: impl FnOnce() -> F,
    settlement_cancellation: Option<CancellationToken>,
    settlement_timeout: Duration,
) -> Result<T, HarnessError>
where
    F: Future<Output = Result<T, HarnessError>>,
{
    let settlement_signal = settlement_cancellation.clone();
    let operation = isolate_future(operation, settlement_cancellation)
        .map_err(|()| HarnessError::CapabilityPanicked { phase })?;
    tokio::pin!(operation);
    let control = match deadline {
        Some(deadline) => {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(HarnessError::Cancelled { phase }),
                () = sleep_until(deadline) => Err(HarnessError::TimedOut { phase }),
                result = &mut operation => Ok(result),
            }
        }
        None => {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(HarnessError::Cancelled { phase }),
                result = &mut operation => Ok(result),
            }
        }
    };
    match control {
        Ok(settlement) => settle_panic(settlement, phase),
        Err(control_error) => {
            settle_after_control(
                operation.as_mut(),
                settlement_signal.as_ref(),
                settlement_timeout,
                control_error,
                phase,
            )
            .await
        }
    }
}

async fn settle_after_control<O, T>(
    mut operation: Pin<&mut O>,
    settlement_cancellation: Option<&CancellationToken>,
    settlement_timeout: Duration,
    control_error: HarnessError,
    phase: ExecutionPhase,
) -> Result<T, HarnessError>
where
    O: Future<Output = Result<Result<T, HarnessError>, ()>>,
{
    if let Some(cancellation) = settlement_cancellation {
        cancellation.cancel();
    }
    if settlement_timeout.is_zero() {
        return Err(control_error);
    }
    match timeout(settlement_timeout, operation.as_mut()).await {
        Ok(settlement) => match settle_panic(settlement, phase) {
            Err(HarnessError::Cancelled { .. } | HarnessError::TimedOut { .. }) | Ok(_) => {
                Err(control_error)
            }
            Err(error) => Err(error),
        },
        Err(_) => Err(control_error),
    }
}

fn settle_panic<T>(
    result: Result<Result<T, HarnessError>, ()>,
    phase: ExecutionPhase,
) -> Result<T, HarnessError> {
    result.unwrap_or(Err(HarnessError::CapabilityPanicked { phase }))
}

#[cfg(test)]
mod tests {
    use std::{
        future::{Future, Ready, pending},
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    use super::{
        controlled, controlled_with_settlement_cancellation, controlled_with_settlement_grace,
        deadline,
    };
    use crate::{CancellationToken, ExecutionPhase, HarnessError};

    struct PanickingFuture;

    struct DropPanickingFuture;

    struct CancellationObservingFuture {
        cancellation: CancellationToken,
        dropped_after_cancellation: Arc<AtomicBool>,
    }

    impl Future for PanickingFuture {
        type Output = Result<(), HarnessError>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("sensitive poll panic")
        }
    }

    impl Future for DropPanickingFuture {
        type Output = Result<(), HarnessError>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for DropPanickingFuture {
        fn drop(&mut self) {
            panic!("sensitive drop panic")
        }
    }

    impl Future for CancellationObservingFuture {
        type Output = Result<(), HarnessError>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for CancellationObservingFuture {
        fn drop(&mut self) {
            self.dropped_after_cancellation
                .store(self.cancellation.is_cancelled(), Ordering::SeqCst);
        }
    }

    fn panicking_constructor() -> Ready<Result<(), HarnessError>> {
        panic!("sensitive constructor panic")
    }

    #[tokio::test]
    async fn pre_cancelled_signal_wins_over_ready_operation() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let result = controlled(&cancellation, None, ExecutionPhase::Model, || async {
            Ok::<_, HarnessError>("ready")
        })
        .await;
        assert_eq!(
            result,
            Err(HarnessError::Cancelled {
                phase: ExecutionPhase::Model
            })
        );
    }

    #[tokio::test]
    async fn deadline_stops_a_pending_operation() {
        let result = controlled(
            &CancellationToken::new(),
            deadline(Some(Duration::from_millis(1))).expect("valid deadline"),
            ExecutionPhase::Tool,
            pending::<Result<(), HarnessError>>,
        )
        .await;
        assert_eq!(
            result,
            Err(HarnessError::TimedOut {
                phase: ExecutionPhase::Tool
            })
        );
    }

    #[tokio::test]
    async fn settlement_cancellation_precedes_provider_future_drop() {
        let settlement_cancellation = CancellationToken::new();
        let provider_cancellation = settlement_cancellation.clone();
        let dropped_after_cancellation = Arc::new(AtomicBool::new(false));
        let dropped = dropped_after_cancellation.clone();
        let result = controlled_with_settlement_cancellation(
            &CancellationToken::new(),
            settlement_cancellation,
            deadline(Some(Duration::from_millis(1))).expect("valid deadline"),
            ExecutionPhase::Model,
            || CancellationObservingFuture {
                cancellation: provider_cancellation,
                dropped_after_cancellation: dropped,
            },
        )
        .await;

        assert_eq!(
            result,
            Err(HarnessError::TimedOut {
                phase: ExecutionPhase::Model
            })
        );
        assert!(dropped_after_cancellation.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn settlement_grace_waits_and_preserves_cleanup_failure() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let settlement_cancellation = CancellationToken::new();
        let provider_cancellation = settlement_cancellation.clone();
        let settled = Arc::new(AtomicBool::new(false));
        let did_settle = settled.clone();

        let result = controlled_with_settlement_grace(
            &cancellation,
            settlement_cancellation,
            None,
            ExecutionPhase::Tool,
            Duration::from_millis(100),
            || async move {
                provider_cancellation.cancelled().await;
                did_settle.store(true, Ordering::SeqCst);
                Err::<(), _>(HarnessError::Mcp("cleanup failed".to_owned()))
            },
        )
        .await;

        assert_eq!(result, Err(HarnessError::Mcp("cleanup failed".to_owned())));
        assert!(settled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn capability_constructor_and_poll_panics_are_sanitized() {
        for result in [
            controlled(
                &CancellationToken::new(),
                None,
                ExecutionPhase::Policy,
                panicking_constructor,
            )
            .await,
            controlled(
                &CancellationToken::new(),
                None,
                ExecutionPhase::Policy,
                || PanickingFuture,
            )
            .await,
            controlled(
                &CancellationToken::new(),
                None,
                ExecutionPhase::Policy,
                || DropPanickingFuture,
            )
            .await,
        ] {
            let error = result.expect_err("panic must become an error");
            assert_eq!(
                error,
                HarnessError::CapabilityPanicked {
                    phase: ExecutionPhase::Policy
                }
            );
            assert!(!error.to_string().contains("sensitive"));
        }
    }

    #[test]
    fn rejects_a_timeout_outside_the_runtime_clock_range() {
        assert!(matches!(
            deadline(Some(Duration::MAX)),
            Err(HarnessError::InvalidConfiguration(_))
        ));
    }
}
