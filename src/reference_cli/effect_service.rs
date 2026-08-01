//! Optional lifecycle owner for governed durable Effect consumption.
//!
//! This module belongs to the reference service, not the embedded Engine. It
//! supplies polling cadence, process-local cursors, bounded diagnostics,
//! failure backoff, and shutdown. The Executor and Reconciler remain
//! host-driven, task-free Core primitives.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::{MissedTickBehavior, interval, timeout},
};
use y_harness::{
    AllowListEffectExecutionPolicy, AllowListEffectReconciliationPolicy, AuthorityContext,
    CancellationToken, CapabilityOrigin, EFFECT_EXECUTOR_API_VERSION,
    EFFECT_RECONCILER_API_VERSION, EffectConnectorDescriptor, EffectConnectorRegistry,
    EffectDispatchGovernorPolicy, EffectEngine, EffectExecutor, EffectExecutorAttemptOutcome,
    EffectExecutorConfig, EffectExecutorRunRequest, EffectIdempotencyContract, EffectPageCursor,
    EffectReconciler, EffectReconcilerAttemptOutcome, EffectReconcilerConfig,
    EffectReconcilerRunRequest, EffectReconciliationConnectorDescriptor,
    EffectReconciliationConnectorRegistry, EffectReconciliationContract, HarnessError,
    JsonCommandEffectConnector, JsonCommandEffectReconciliationConnector,
    SqliteEffectDispatchGovernor,
};

use super::{
    CliResult,
    service::{LoadedConfig, ServiceJsonProcessConfig, build_json_effect_process},
};

const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_FAILURE_BACKOFF_MS: u64 = 5_000;
const MIN_POLL_INTERVAL_MS: u64 = 100;
const MAX_POLL_INTERVAL_MS: u64 = 86_400_000;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Explicit opt-in reference-service Effect consumption policy.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ServiceEffectConsumerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution: Option<ServiceEffectExecutionConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reconciliation: Option<ServiceEffectReconciliationConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceEffectExecutionConfig {
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default = "default_failure_backoff_ms")]
    failure_backoff_ms: u64,
    #[serde(default)]
    executor: EffectExecutorConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    governor: Option<EffectDispatchGovernorPolicy>,
    allow: Vec<ServiceEffectOperationConfig>,
    connectors: Vec<ServiceEffectExecutionConnectorConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceEffectReconciliationConfig {
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default = "default_failure_backoff_ms")]
    failure_backoff_ms: u64,
    #[serde(default)]
    reconciler: EffectReconcilerConfig,
    allow: Vec<ServiceEffectOperationConfig>,
    connectors: Vec<ServiceEffectReconciliationConnectorConfig>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceEffectOperationConfig {
    capability: String,
    operation: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceEffectExecutionConnectorConfig {
    origin_id: String,
    capability: String,
    operations: BTreeSet<String>,
    idempotency: EffectIdempotencyContract,
    process: ServiceJsonProcessConfig,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceEffectReconciliationConnectorConfig {
    origin_id: String,
    capability: String,
    operations: BTreeSet<String>,
    contract: EffectReconciliationContract,
    process: ServiceJsonProcessConfig,
}

impl ServiceEffectConsumerConfig {
    pub(super) fn validate(&self) -> Result<(), HarnessError> {
        if self.execution.is_none() && self.reconciliation.is_none() {
            return Err(HarnessError::InvalidConfiguration(
                "effect_consumer requires execution or reconciliation".to_owned(),
            ));
        }
        if let Some(execution) = &self.execution {
            validate_cadence(
                "effect_consumer.execution",
                execution.poll_interval_ms,
                execution.failure_backoff_ms,
            )?;
            execution.executor.validate()?;
            if let Some(governor) = &execution.governor {
                governor.validate()?;
            }
            for connector in &execution.connectors {
                require_command_lock("execution", &connector.capability, &connector.process)?;
            }
        }
        if let Some(reconciliation) = &self.reconciliation {
            validate_cadence(
                "effect_consumer.reconciliation",
                reconciliation.poll_interval_ms,
                reconciliation.failure_backoff_ms,
            )?;
            reconciliation.reconciler.validate()?;
            for connector in &reconciliation.connectors {
                require_command_lock("reconciliation", &connector.capability, &connector.process)?;
            }
        }
        Ok(())
    }
}

fn require_command_lock(
    mode: &str,
    capability: &str,
    process: &ServiceJsonProcessConfig,
) -> Result<(), HarnessError> {
    if process.command_sha256.is_none() {
        return Err(HarnessError::InvalidConfiguration(format!(
            "Effect {mode} Connector {capability} requires command_sha256"
        )));
    }
    Ok(())
}

fn validate_cadence(
    path: &str,
    poll_interval_ms: u64,
    failure_backoff_ms: u64,
) -> Result<(), HarnessError> {
    if !(MIN_POLL_INTERVAL_MS..=MAX_POLL_INTERVAL_MS).contains(&poll_interval_ms) {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{path}.poll_interval_ms must be {MIN_POLL_INTERVAL_MS}-{MAX_POLL_INTERVAL_MS}"
        )));
    }
    if !(MIN_POLL_INTERVAL_MS..=MAX_POLL_INTERVAL_MS).contains(&failure_backoff_ms) {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{path}.failure_backoff_ms must be {MIN_POLL_INTERVAL_MS}-{MAX_POLL_INTERVAL_MS}"
        )));
    }
    Ok(())
}

/// Fully assembled execution loop with no remaining configuration authority.
pub(super) struct EffectExecutionLoop {
    pub(super) executor: EffectExecutor,
    pub(super) poll_interval: Duration,
    pub(super) failure_backoff: Duration,
}

/// Fully assembled reconciliation loop with no remaining configuration authority.
pub(super) struct EffectReconciliationLoop {
    pub(super) reconciler: EffectReconciler,
    pub(super) poll_interval: Duration,
    pub(super) failure_backoff: Duration,
}

/// Optional execution and reconciliation loops owned by one service process.
pub(super) struct ConfiguredEffectConsumer {
    pub(super) execution: Option<EffectExecutionLoop>,
    pub(super) reconciliation: Option<EffectReconciliationLoop>,
}

pub(super) struct ConfiguredEffectConsumerAssembly {
    execution: Option<ConfiguredEffectExecutionAssembly>,
    reconciliation: Option<ConfiguredEffectReconciliationAssembly>,
}

struct ConfiguredEffectExecutionAssembly {
    connectors: EffectConnectorRegistry,
    policy: AllowListEffectExecutionPolicy,
    executor: EffectExecutorConfig,
    governor: Option<EffectDispatchGovernorPolicy>,
    poll_interval: Duration,
    failure_backoff: Duration,
    connector_count: usize,
    credential_connector_count: usize,
    secret_variable_count: usize,
    allow_count: usize,
}

struct ConfiguredEffectReconciliationAssembly {
    connectors: EffectReconciliationConnectorRegistry,
    policy: AllowListEffectReconciliationPolicy,
    reconciler: EffectReconcilerConfig,
    poll_interval: Duration,
    failure_backoff: Duration,
    connector_count: usize,
    credential_connector_count: usize,
    secret_variable_count: usize,
    allow_count: usize,
}

impl ConfiguredEffectConsumerAssembly {
    pub(super) async fn assemble(
        self,
        engine: EffectEngine,
        data_directory: &Path,
    ) -> Result<ConfiguredEffectConsumer, HarnessError> {
        let execution = match self.execution {
            Some(configured) => {
                let mut executor = EffectExecutor::new(engine.clone(), configured.connectors)?
                    .with_policy(Arc::new(configured.policy))
                    .with_config(configured.executor)?;
                if let Some(policy) = configured.governor {
                    let governor = Arc::new(
                        SqliteEffectDispatchGovernor::open(
                            data_directory.join("effect-governance.db"),
                        )
                        .await?,
                    );
                    executor = executor.with_dispatch_governor(governor, policy)?;
                }
                Some(EffectExecutionLoop {
                    executor,
                    poll_interval: configured.poll_interval,
                    failure_backoff: configured.failure_backoff,
                })
            }
            None => None,
        };
        let reconciliation = self
            .reconciliation
            .map(|configured| {
                let reconciler = EffectReconciler::new(engine, configured.connectors)?
                    .with_policy(Arc::new(configured.policy))
                    .with_config(configured.reconciler)?;
                Ok(EffectReconciliationLoop {
                    reconciler,
                    poll_interval: configured.poll_interval,
                    failure_backoff: configured.failure_backoff,
                })
            })
            .transpose()?;
        Ok(ConfiguredEffectConsumer {
            execution,
            reconciliation,
        })
    }

    pub(super) fn doctor_summary(&self) -> String {
        let execution = self.execution.as_ref().map_or_else(
            || "execution disabled".to_owned(),
            |configured| {
                let governor = configured.governor.as_ref().map_or_else(String::new, |policy| {
                    format!(
                        " / governor {}: {}/{} ms, {} failures/{} ms, {} ms probe",
                        policy.policy_id,
                        policy.max_dispatches_per_window,
                        policy.window_ms,
                        policy.failure_threshold,
                        policy.open_duration_ms,
                        policy.probe_lease_ms,
                    )
                });
                format!(
                    "execution {} dispatch-locked connector(s) / {} credential-scoped / {} secret variable(s) / {} allow(s){} / {} ms poll / {} ms backoff",
                    configured.connector_count,
                    configured.credential_connector_count,
                    configured.secret_variable_count,
                    configured.allow_count,
                    governor,
                    configured.poll_interval.as_millis(),
                    configured.failure_backoff.as_millis()
                )
            },
        );
        let reconciliation = self.reconciliation.as_ref().map_or_else(
            || "reconciliation disabled".to_owned(),
            |configured| {
                format!(
                    "reconciliation {} dispatch-locked connector(s) / {} credential-scoped / {} secret variable(s) / {} allow(s) / {} ms poll / {} ms backoff",
                    configured.connector_count,
                    configured.credential_connector_count,
                    configured.secret_variable_count,
                    configured.allow_count,
                    configured.poll_interval.as_millis(),
                    configured.failure_backoff.as_millis()
                )
            },
        );
        format!("{execution}; {reconciliation}")
    }
}

pub(super) async fn build(
    loaded: &LoadedConfig,
) -> CliResult<Option<ConfiguredEffectConsumerAssembly>> {
    let Some(configured) = loaded.effect_consumer() else {
        return Ok(None);
    };
    configured.validate()?;

    let execution = match &configured.execution {
        Some(configured) => Some(build_execution(loaded, configured).await?),
        None => None,
    };
    let reconciliation = match &configured.reconciliation {
        Some(configured) => Some(build_reconciliation(loaded, configured).await?),
        None => None,
    };
    Ok(Some(ConfiguredEffectConsumerAssembly {
        execution,
        reconciliation,
    }))
}

async fn build_execution(
    loaded: &LoadedConfig,
    configured: &ServiceEffectExecutionConfig,
) -> CliResult<ConfiguredEffectExecutionAssembly> {
    configured.executor.validate()?;
    if let Some(governor) = &configured.governor {
        governor.validate()?;
    }
    let supported = execution_capabilities(&configured.connectors)?;
    validate_allowlist("execution", &configured.allow, &supported)?;

    let mut connectors = EffectConnectorRegistry::new();
    for connector in &configured.connectors {
        if connector.process.timeout_ms > configured.executor.execution_timeout_ms {
            return Err(format!(
                "Effect execution Connector {} process timeout {} ms exceeds \
                 executor execution timeout {} ms",
                connector.capability,
                connector.process.timeout_ms,
                configured.executor.execution_timeout_ms
            )
            .into());
        }
        let (process, broker, secret_environment) = build_json_effect_process(
            loaded,
            &connector.process,
            &format!("Effect execution Connector {}", connector.capability),
            &connector.capability,
        )
        .await?;
        let mut adapter = JsonCommandEffectConnector::new(
            EffectConnectorDescriptor {
                capability: connector.capability.clone(),
                api_version: EFFECT_EXECUTOR_API_VERSION,
                operations: connector.operations.clone(),
                idempotency: connector.idempotency,
            },
            process,
            broker,
        )?;
        if let Some(secret_environment) = secret_environment {
            adapter = adapter.with_secret_environment(secret_environment)?;
        }
        connectors.register(
            CapabilityOrigin::External {
                id: connector.origin_id.clone(),
            },
            Arc::new(adapter),
        )?;
    }

    let mut policy = AllowListEffectExecutionPolicy::deny_by_default();
    for allowed in &configured.allow {
        policy = policy.allow(&allowed.capability, &allowed.operation)?;
    }
    Ok(ConfiguredEffectExecutionAssembly {
        connectors,
        policy,
        executor: configured.executor.clone(),
        governor: configured.governor.clone(),
        poll_interval: Duration::from_millis(configured.poll_interval_ms),
        failure_backoff: Duration::from_millis(configured.failure_backoff_ms),
        connector_count: configured.connectors.len(),
        credential_connector_count: configured
            .connectors
            .iter()
            .filter(|connector| !connector.process.secret_environment.is_empty())
            .count(),
        secret_variable_count: configured
            .connectors
            .iter()
            .map(|connector| connector.process.secret_environment.len())
            .sum(),
        allow_count: configured.allow.len(),
    })
}

async fn build_reconciliation(
    loaded: &LoadedConfig,
    configured: &ServiceEffectReconciliationConfig,
) -> CliResult<ConfiguredEffectReconciliationAssembly> {
    configured.reconciler.validate()?;
    let supported = reconciliation_capabilities(&configured.connectors)?;
    validate_allowlist("reconciliation", &configured.allow, &supported)?;

    let mut connectors = EffectReconciliationConnectorRegistry::new();
    for connector in &configured.connectors {
        if connector.process.timeout_ms > configured.reconciler.lookup_timeout_ms {
            return Err(format!(
                "Effect reconciliation Connector {} process timeout {} ms exceeds \
                 reconciler lookup timeout {} ms",
                connector.capability,
                connector.process.timeout_ms,
                configured.reconciler.lookup_timeout_ms
            )
            .into());
        }
        let (process, broker, secret_environment) = build_json_effect_process(
            loaded,
            &connector.process,
            &format!("Effect reconciliation Connector {}", connector.capability),
            &connector.capability,
        )
        .await?;
        let mut adapter = JsonCommandEffectReconciliationConnector::new(
            EffectReconciliationConnectorDescriptor {
                capability: connector.capability.clone(),
                api_version: EFFECT_RECONCILER_API_VERSION,
                operations: connector.operations.clone(),
                contract: connector.contract,
            },
            process,
            broker,
        )?;
        if let Some(secret_environment) = secret_environment {
            adapter = adapter.with_secret_environment(secret_environment)?;
        }
        connectors.register(
            CapabilityOrigin::External {
                id: connector.origin_id.clone(),
            },
            Arc::new(adapter),
        )?;
    }

    let mut policy = AllowListEffectReconciliationPolicy::deny_by_default();
    for allowed in &configured.allow {
        policy = policy.allow(&allowed.capability, &allowed.operation)?;
    }
    Ok(ConfiguredEffectReconciliationAssembly {
        connectors,
        policy,
        reconciler: configured.reconciler.clone(),
        poll_interval: Duration::from_millis(configured.poll_interval_ms),
        failure_backoff: Duration::from_millis(configured.failure_backoff_ms),
        connector_count: configured.connectors.len(),
        credential_connector_count: configured
            .connectors
            .iter()
            .filter(|connector| !connector.process.secret_environment.is_empty())
            .count(),
        secret_variable_count: configured
            .connectors
            .iter()
            .map(|connector| connector.process.secret_environment.len())
            .sum(),
        allow_count: configured.allow.len(),
    })
}

fn execution_capabilities(
    connectors: &[ServiceEffectExecutionConnectorConfig],
) -> CliResult<BTreeMap<String, BTreeSet<String>>> {
    let mut supported = BTreeMap::new();
    for connector in connectors {
        if supported
            .insert(connector.capability.clone(), connector.operations.clone())
            .is_some()
        {
            return Err(format!(
                "duplicate Effect execution Connector {}",
                connector.capability
            )
            .into());
        }
    }
    Ok(supported)
}

fn reconciliation_capabilities(
    connectors: &[ServiceEffectReconciliationConnectorConfig],
) -> CliResult<BTreeMap<String, BTreeSet<String>>> {
    let mut supported = BTreeMap::new();
    for connector in connectors {
        if supported
            .insert(connector.capability.clone(), connector.operations.clone())
            .is_some()
        {
            return Err(format!(
                "duplicate Effect reconciliation Connector {}",
                connector.capability
            )
            .into());
        }
    }
    Ok(supported)
}

fn validate_allowlist(
    mode: &str,
    allow: &[ServiceEffectOperationConfig],
    supported: &BTreeMap<String, BTreeSet<String>>,
) -> CliResult<()> {
    if supported.is_empty() {
        return Err(format!("Effect {mode} requires at least one Connector").into());
    }
    if allow.is_empty() {
        return Err(format!("Effect {mode} requires an explicit non-empty exact allowlist").into());
    }
    let mut unique = BTreeSet::new();
    for allowed in allow {
        if !unique.insert(allowed.clone()) {
            return Err(format!(
                "duplicate Effect {mode} allow {}/{}",
                allowed.capability, allowed.operation
            )
            .into());
        }
        let exact = supported
            .get(&allowed.capability)
            .is_some_and(|operations| operations.contains(&allowed.operation));
        if !exact {
            return Err(format!(
                "Effect {mode} allow {}/{} has no exact configured Connector operation",
                allowed.capability, allowed.operation
            )
            .into());
        }
    }
    Ok(())
}

/// Joinable ownership of every configured Effect consumer loop.
pub(super) struct EffectServiceHandle {
    cancellation: CancellationToken,
    task: JoinHandle<Result<(), HarnessError>>,
}

impl EffectServiceHandle {
    /// Stops new sweeps and waits a bounded time for in-flight work.
    pub(super) async fn shutdown(self) -> Result<(), HarnessError> {
        self.cancellation.cancel();
        let mut task = self.task;
        match timeout(SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(HarnessError::Effect(
                "reference-service Effect supervisor terminated unexpectedly".to_owned(),
            )),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(HarnessError::Effect(
                    "reference-service Effect shutdown timed out".to_owned(),
                ))
            }
        }
    }
}

/// Starts only the explicitly configured reference-service loops.
pub(super) fn start(
    configured: ConfiguredEffectConsumer,
    authority: AuthorityContext,
) -> Result<EffectServiceHandle, HarnessError> {
    if configured.execution.is_none() && configured.reconciliation.is_none() {
        return Err(HarnessError::InvalidConfiguration(
            "Effect consumer requires execution or reconciliation".to_owned(),
        ));
    }
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task =
        tokio::spawn(async move { supervise(configured, authority, task_cancellation).await });
    Ok(EffectServiceHandle { cancellation, task })
}

async fn supervise(
    configured: ConfiguredEffectConsumer,
    authority: AuthorityContext,
    cancellation: CancellationToken,
) -> Result<(), HarnessError> {
    let mut tasks = JoinSet::new();
    if let Some(execution) = configured.execution {
        let authority = authority.clone();
        let cancellation = cancellation.clone();
        tasks.spawn(async move {
            run_execution(execution, authority, cancellation).await;
        });
    }
    if let Some(reconciliation) = configured.reconciliation {
        let cancellation = cancellation.clone();
        tasks.spawn(async move {
            run_reconciliation(reconciliation, authority, cancellation).await;
        });
    }

    while let Some(joined) = tasks.join_next().await {
        if joined.is_err() || !cancellation.is_cancelled() {
            cancellation.cancel();
            while tasks.join_next().await.is_some() {}
            return Err(HarnessError::Effect(
                "reference-service Effect loop terminated unexpectedly".to_owned(),
            ));
        }
    }
    Ok(())
}

async fn run_execution(
    configured: EffectExecutionLoop,
    authority: AuthorityContext,
    cancellation: CancellationToken,
) {
    let mut cadence = interval(configured.poll_interval);
    cadence.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut cursor: Option<EffectPageCursor> = None;
    let mut cycle = 0_u64;
    let mut retry_at: Option<Instant> = None;
    let mut degraded = false;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = cadence.tick() => {
                if retry_at.is_some_and(|deadline| Instant::now() < deadline) {
                    continue;
                }
                cycle = match cycle.checked_add(1) {
                    Some(cycle) => cycle,
                    None => {
                        if !degraded {
                            eprintln!(
                                "Y-Harness Effect execution degraded: cycle identity exhausted"
                            );
                        }
                        return;
                    }
                };
                let result = configured
                    .executor
                    .run_once_as(
                        EffectExecutorRunRequest {
                            cycle_id: format!("service-effect-execution-{cycle}"),
                            after: cursor.clone(),
                        },
                        &authority,
                        cancellation.clone(),
                    )
                    .await;
                match result {
                    Ok(report) => {
                        cursor = report.next_after;
                        let failures = report
                            .attempts
                            .iter()
                            .filter(|attempt| {
                                attempt.governor_settlement_failed
                                    || execution_failure(&attempt.outcome)
                            })
                            .count();
                        let attempted = report.attempts.len();
                        let unavailable = attempted > 0 && failures == attempted;
                        retry_at = unavailable
                            .then(|| Instant::now() + configured.failure_backoff);
                        if failures == 0 {
                            if degraded {
                                eprintln!("Y-Harness Effect execution recovered");
                                degraded = false;
                            }
                        } else if !degraded {
                            eprintln!(
                                "Y-Harness Effect execution degraded: \
                                 {failures} attempt(s) unavailable"
                            );
                            degraded = true;
                        }
                    }
                    Err(_) => {
                        retry_at = Some(Instant::now() + configured.failure_backoff);
                        if !degraded {
                            eprintln!(
                                "Y-Harness Effect execution degraded: sweep unavailable"
                            );
                            degraded = true;
                        }
                    }
                }
            }
        }
    }
}

async fn run_reconciliation(
    configured: EffectReconciliationLoop,
    authority: AuthorityContext,
    cancellation: CancellationToken,
) {
    let mut cadence = interval(configured.poll_interval);
    cadence.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut cursor: Option<EffectPageCursor> = None;
    let mut cycle = 0_u64;
    let mut retry_at: Option<Instant> = None;
    let mut degraded = false;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = cadence.tick() => {
                if retry_at.is_some_and(|deadline| Instant::now() < deadline) {
                    continue;
                }
                cycle = match cycle.checked_add(1) {
                    Some(cycle) => cycle,
                    None => {
                        if !degraded {
                            eprintln!(
                                "Y-Harness Effect reconciliation degraded: cycle identity exhausted"
                            );
                        }
                        return;
                    }
                };
                let result = configured
                    .reconciler
                    .run_once_as(
                        EffectReconcilerRunRequest {
                            cycle_id: format!("service-effect-reconciliation-{cycle}"),
                            after: cursor.clone(),
                        },
                        &authority,
                        cancellation.clone(),
                    )
                    .await;
                match result {
                    Ok(report) => {
                        cursor = report.next_after;
                        let failures = report
                            .attempts
                            .iter()
                            .filter(|attempt| reconciliation_failure(&attempt.outcome))
                            .count();
                        let attempted = report.attempts.len();
                        let unavailable = attempted > 0 && failures == attempted;
                        retry_at = unavailable
                            .then(|| Instant::now() + configured.failure_backoff);
                        if failures == 0 {
                            if degraded {
                                eprintln!("Y-Harness Effect reconciliation recovered");
                                degraded = false;
                            }
                        } else if !degraded {
                            eprintln!(
                                "Y-Harness Effect reconciliation degraded: \
                                 {failures} attempt(s) unavailable"
                            );
                            degraded = true;
                        }
                    }
                    Err(_) => {
                        retry_at = Some(Instant::now() + configured.failure_backoff);
                        if !degraded {
                            eprintln!(
                                "Y-Harness Effect reconciliation degraded: sweep unavailable"
                            );
                            degraded = true;
                        }
                    }
                }
            }
        }
    }
}

fn execution_failure(outcome: &EffectExecutorAttemptOutcome) -> bool {
    matches!(
        outcome,
        EffectExecutorAttemptOutcome::ConnectorUnavailable
            | EffectExecutorAttemptOutcome::PolicyDenied { .. }
            | EffectExecutorAttemptOutcome::PolicyUnavailable
            | EffectExecutorAttemptOutcome::ClaimFailed
            | EffectExecutorAttemptOutcome::ClockUnavailableAfterClaim
            | EffectExecutorAttemptOutcome::GovernorUnavailable { .. }
            | EffectExecutorAttemptOutcome::SettlementFailed
            | EffectExecutorAttemptOutcome::AttemptFailed
    )
}

fn reconciliation_failure(outcome: &EffectReconcilerAttemptOutcome) -> bool {
    matches!(
        outcome,
        EffectReconcilerAttemptOutcome::ConnectorUnavailable
            | EffectReconcilerAttemptOutcome::PolicyDenied { .. }
            | EffectReconcilerAttemptOutcome::PolicyUnavailable
            | EffectReconcilerAttemptOutcome::LookupUnavailable
            | EffectReconcilerAttemptOutcome::InvalidEvidence
            | EffectReconcilerAttemptOutcome::ClockUnavailable
            | EffectReconcilerAttemptOutcome::SettlementFailed
            | EffectReconcilerAttemptOutcome::AttemptFailed
    )
}

const fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}

const fn default_failure_backoff_ms() -> u64 {
    DEFAULT_FAILURE_BACKOFF_MS
}

#[cfg(test)]
mod tests {
    use super::{execution_failure, reconciliation_failure};
    use y_harness::{EffectExecutorAttemptOutcome, EffectReconcilerAttemptOutcome};

    #[test]
    fn health_classification_distinguishes_domain_outcomes_from_unavailability() {
        assert!(!execution_failure(&EffectExecutorAttemptOutcome::Applied));
        assert!(!execution_failure(&EffectExecutorAttemptOutcome::Unknown {
            reason_code: "target.uncertain".to_owned(),
        }));
        assert!(execution_failure(
            &EffectExecutorAttemptOutcome::PolicyUnavailable
        ));

        assert!(!reconciliation_failure(
            &EffectReconcilerAttemptOutcome::StillUnknown {
                reason_code: "target.pending".to_owned(),
            }
        ));
        assert!(reconciliation_failure(
            &EffectReconcilerAttemptOutcome::LookupUnavailable
        ));
    }
}
