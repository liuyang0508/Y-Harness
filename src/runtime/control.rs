//! Turn execution options and bounded external-operation waits.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::time::{Instant, sleep_until, timeout};

use crate::{
    ActorIdentity, AuthorityContext, CancellationToken, ExecutionBinding, ExecutionPhase,
    HarnessError, MemoryScope, ModelEventSink, TurnContextInput, isolation::isolate_future,
};

/// Caller-controlled isolation, cancellation, and deadline for one Turn.
#[derive(Clone, Default)]
pub struct TurnExecutionOptions {
    /// Trusted identity and optional tenant boundary for this Turn.
    ///
    /// Embedding hosts must derive this from their authenticated caller
    /// boundary rather than accepting request-authored identity claims.
    pub authority: AuthorityContext,
    /// Scope applied to long-term memory operations.
    pub memory_scope: MemoryScope,
    /// Bounded non-authoritative reference context supplied for this Turn.
    pub context: Vec<TurnContextInput>,
    /// Optional immutable deployment and environment evidence.
    ///
    /// Embedding hosts must obtain this from a governed control plane. Remote
    /// Protocol clients cannot author this field.
    pub execution_binding: Option<ExecutionBinding>,
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

impl TurnExecutionOptions {
    pub(super) fn validated_execution_binding(
        &self,
    ) -> Result<Option<ExecutionBinding>, HarnessError> {
        let Some(binding) = &self.execution_binding else {
            return Ok(None);
        };
        binding.validate()?;
        if binding.tenant_id() != self.authority.tenant_id() {
            return Err(HarnessError::InvalidConfiguration(
                "execution binding tenant does not match the trusted Turn authority".to_owned(),
            ));
        }
        Ok(Some(binding.clone()))
    }

    pub(super) fn validated_memory_scope(&self) -> Result<MemoryScope, HarnessError> {
        self.authority.validate_current("Turn authority")?;
        let mut scope = self.memory_scope.clone();
        if let Some(tenant_id) = scope.tenant_id.as_deref() {
            AuthorityContext::validate_tenant(tenant_id)?;
        }
        match (self.authority.tenant_id(), scope.tenant_id.as_deref()) {
            (Some(authority), Some(requested)) if authority != requested => {
                return Err(HarnessError::InvalidConfiguration(
                    "memory tenant does not match the trusted Turn authority".to_owned(),
                ));
            }
            (Some(authority), None) => scope.tenant_id = Some(authority.to_owned()),
            (None, Some(_))
                if matches!(self.authority.actor(), ActorIdentity::Authenticated { .. }) =>
            {
                return Err(HarnessError::InvalidConfiguration(
                    "an unscoped authenticated actor cannot select a memory tenant".to_owned(),
                ));
            }
            _ => {}
        }
        Ok(scope)
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
        TurnExecutionOptions, controlled, controlled_with_settlement_cancellation,
        controlled_with_settlement_grace, deadline,
    };
    use crate::{
        ActorIdentity, AuthorityContext, CancellationToken, ExecutionPhase, HarnessError,
        MemoryScope,
    };

    struct PanickingFuture;

    struct DropPanickingFuture;

    struct CancellationObservingFuture {
        cancellation: CancellationToken,
        dropped_after_cancellation: Arc<AtomicBool>,
    }

    fn remote_authority(tenant_id: Option<&str>) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test-auth".to_owned(),
                subject: "user-1".to_owned(),
            },
            tenant_id.map(str::to_owned),
        )
        .expect("valid authority")
    }

    #[test]
    fn trusted_tenant_is_injected_and_cannot_be_overridden() {
        let options = TurnExecutionOptions {
            authority: remote_authority(Some("tenant-a")),
            ..TurnExecutionOptions::default()
        };
        assert_eq!(
            options
                .validated_memory_scope()
                .expect("derived scope")
                .tenant_id
                .as_deref(),
            Some("tenant-a")
        );

        let mismatch = TurnExecutionOptions {
            authority: remote_authority(Some("tenant-a")),
            memory_scope: MemoryScope {
                tenant_id: Some("tenant-b".to_owned()),
                ..MemoryScope::default()
            },
            ..TurnExecutionOptions::default()
        };
        assert!(matches!(
            mismatch.validated_memory_scope(),
            Err(HarnessError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn unscoped_remote_actor_cannot_select_a_memory_tenant() {
        let options = TurnExecutionOptions {
            authority: remote_authority(None),
            memory_scope: MemoryScope {
                tenant_id: Some("tenant-a".to_owned()),
                ..MemoryScope::default()
            },
            ..TurnExecutionOptions::default()
        };
        assert!(matches!(
            options.validated_memory_scope(),
            Err(HarnessError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn trusted_local_process_retains_explicit_embedded_scope() {
        let options = TurnExecutionOptions {
            memory_scope: MemoryScope {
                tenant_id: Some("tenant-a".to_owned()),
                ..MemoryScope::default()
            },
            ..TurnExecutionOptions::default()
        };
        assert_eq!(
            options
                .validated_memory_scope()
                .expect("trusted embedded scope")
                .tenant_id
                .as_deref(),
            Some("tenant-a")
        );
    }

    #[test]
    fn embedded_memory_tenant_still_requires_a_canonical_identity() {
        let options = TurnExecutionOptions {
            memory_scope: MemoryScope {
                tenant_id: Some("../tenant".to_owned()),
                ..MemoryScope::default()
            },
            ..TurnExecutionOptions::default()
        };
        assert!(matches!(
            options.validated_memory_scope(),
            Err(HarnessError::InvalidConfiguration(_))
        ));
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
