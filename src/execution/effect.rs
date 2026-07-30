//! Versioned shell-free JSON-command adapters for external Effect Connectors.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    JSON_COMMAND_MAX_INPUT_BYTES, JsonProcessConfig, ProcessBroker, ProcessBrokerDescriptor,
    validate_broker_descriptor, validate_process_success,
};
use crate::{
    AuthorityContext, EffectConnector, EffectConnectorDescriptor, EffectExecutionOutcome,
    EffectExecutionRequest, EffectId, EffectLeaseId, EffectOperation,
    EffectReconciliationConnector, EffectReconciliationConnectorDescriptor,
    EffectReconciliationOutcome, EffectReconciliationRequest, ExecutionPhase, HarnessError,
    HarnessFuture, SecretEffectPhase, SecretProvider, SecretReference, SecretRequest,
    SecretServiceUse, SecretUseContext, SecretValue, kernel::capture_capability_metadata,
};

/// Exact stdin/stdout envelope coordinate for JSON-command Effect adapters.
pub const JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION: u32 = 1;

/// Maximum secret environment variables resolved for one Effect process.
pub const MAX_EFFECT_SECRET_ENVIRONMENT_ENTRIES: usize = 64;

/// On-demand credential projection for one JSON-command Effect Connector.
///
/// Configuration retains only opaque references. Values are resolved under
/// the request's trusted [`AuthorityContext`] immediately before dispatch and
/// remain in non-serializable, zeroizing [`SecretValue`] buffers until the
/// process request is dropped.
#[derive(Clone)]
pub struct EffectSecretEnvironment {
    provider: Arc<dyn SecretProvider>,
    variables: BTreeMap<String, SecretReference>,
}

impl EffectSecretEnvironment {
    /// Validates one exact child-variable to Secret-reference projection.
    pub fn new(
        provider: Arc<dyn SecretProvider>,
        variables: BTreeMap<String, SecretReference>,
    ) -> Result<Self, HarnessError> {
        if variables.is_empty() || variables.len() > MAX_EFFECT_SECRET_ENVIRONMENT_ENTRIES {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Effect secret environment must contain 1-{MAX_EFFECT_SECRET_ENVIRONMENT_ENTRIES} entries"
            )));
        }
        if variables
            .keys()
            .any(|name| !super::valid_environment_name(name))
        {
            return Err(HarnessError::InvalidConfiguration(
                "Effect secret environment contains an invalid child variable name".to_owned(),
            ));
        }
        Ok(Self {
            provider,
            variables,
        })
    }

    /// Probes every unique reference under deployment authority.
    ///
    /// Values are dropped immediately and never enter Connector configuration.
    pub async fn probe(
        &self,
        consumer: &str,
        authority: &AuthorityContext,
    ) -> Result<(), HarnessError> {
        authority.validate_current("Effect Secret probe authority")?;
        let references = self.variables.values().collect::<BTreeSet<_>>();
        for reference in references {
            self.provider
                .resolve_as(
                    SecretRequest {
                        reference: reference.clone(),
                        consumer: consumer.to_owned(),
                        use_context: SecretUseContext::Service {
                            use_case: SecretServiceUse::StartupProbe,
                        },
                    },
                    authority,
                )
                .await
                .map_err(|_| {
                    HarnessError::Secret(
                        "Effect Connector credential availability probe failed".to_owned(),
                    )
                })?;
        }
        Ok(())
    }

    async fn resolve(
        &self,
        consumer: &str,
        authority: &AuthorityContext,
        use_context: SecretUseContext,
        cancellation: &crate::CancellationToken,
    ) -> Result<BTreeMap<String, SecretValue>, HarnessError> {
        authority.validate_current("Effect Secret resolution authority")?;
        let mut resolved = BTreeMap::new();
        for (child_name, reference) in &self.variables {
            let resolution = self.provider.resolve_as(
                SecretRequest {
                    reference: reference.clone(),
                    consumer: consumer.to_owned(),
                    use_context: use_context.clone(),
                },
                authority,
            );
            let value = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return Err(HarnessError::Cancelled {
                        phase: ExecutionPhase::Effect,
                    });
                }
                result = resolution => result.map_err(|_| {
                    HarnessError::Effect(
                        "Effect Connector credential resolution failed".to_owned(),
                    )
                })?,
            };
            resolved.insert(child_name.clone(), value);
        }
        Ok(resolved)
    }

    fn validate_plain_environment(
        &self,
        plain: &BTreeMap<String, String>,
    ) -> Result<(), HarnessError> {
        if self.variables.keys().any(|name| plain.contains_key(name)) {
            return Err(HarnessError::InvalidConfiguration(
                "Effect plain and secret environment names must not overlap".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Cancellation-free execution request delivered to an external command.
///
/// The live cancellation token remains inside Y-Harness and is propagated
/// separately through the selected [`ProcessBroker`].
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonEffectExecutionRequest {
    /// Exact JSON-command Effect protocol coordinate.
    pub protocol_version: u32,
    /// Stable durable Effect identity.
    pub effect_id: EffectId,
    /// Trusted execution identity and tenant boundary.
    pub authority: AuthorityContext,
    /// Immutable external operation coordinate.
    pub operation: EffectOperation,
    /// Target-system duplicate-suppression key.
    pub idempotency_key: String,
    /// Immutable bounded external request.
    pub input: Value,
    /// SHA-256 of `input`.
    pub input_sha256: String,
    /// Positive durable attempt.
    pub attempt: u32,
    /// Exact execution fence.
    pub lease_id: EffectLeaseId,
    /// Exclusive server-clock lease boundary.
    pub lease_expires_at_ms: u64,
}

/// Strict stdout settlement returned by an execution command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonEffectExecutionResponse {
    /// Exact JSON-command Effect protocol coordinate.
    pub protocol_version: u32,
    /// Authoritative Connector assertion.
    pub outcome: EffectExecutionOutcome,
}

/// Cancellation-free reconciliation request delivered to an external command.
///
/// The command is still bound by the registered authoritative read-only
/// contract. Process isolation does not make a dishonest query safe.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonEffectReconciliationRequest {
    /// Exact JSON-command Effect protocol coordinate.
    pub protocol_version: u32,
    /// Stable durable Effect identity.
    pub effect_id: EffectId,
    /// Trusted lookup identity and tenant boundary.
    pub authority: AuthorityContext,
    /// Immutable external operation coordinate.
    pub operation: EffectOperation,
    /// Target-system duplicate-suppression key used only for lookup.
    pub idempotency_key: String,
    /// Immutable bounded external request.
    pub input: Value,
    /// SHA-256 of `input`.
    pub input_sha256: String,
    /// Exact uncertain attempt.
    pub attempt: u32,
    /// Exact uncertain execution fence.
    pub lease_id: EffectLeaseId,
}

/// Strict stdout observation returned by a reconciliation command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonEffectReconciliationResponse {
    /// Exact JSON-command Effect protocol coordinate.
    pub protocol_version: u32,
    /// Authoritative read-only target observation.
    pub outcome: EffectReconciliationOutcome,
}

/// External Effect executor backed by one shell-free JSON command.
pub struct JsonCommandEffectConnector {
    descriptor: EffectConnectorDescriptor,
    config: JsonProcessConfig,
    broker: Arc<dyn ProcessBroker>,
    broker_descriptor: ProcessBrokerDescriptor,
    secret_environment: Option<EffectSecretEnvironment>,
}

impl JsonCommandEffectConnector {
    /// Captures broker trust and validates static process configuration.
    ///
    /// The Effect registry independently validates and freezes the Connector
    /// descriptor before routing any durable Effect.
    pub fn new(
        descriptor: EffectConnectorDescriptor,
        config: JsonProcessConfig,
        broker: Arc<dyn ProcessBroker>,
    ) -> Result<Self, HarnessError> {
        config.validate()?;
        let broker_descriptor =
            capture_capability_metadata("Effect process broker descriptor", || {
                broker.descriptor()
            })?;
        validate_broker_descriptor(&broker_descriptor)?;
        Ok(Self {
            descriptor,
            config,
            broker,
            broker_descriptor,
            secret_environment: None,
        })
    }

    /// Adds an on-demand, authority-aware credential environment.
    pub fn with_secret_environment(
        mut self,
        secret_environment: EffectSecretEnvironment,
    ) -> Result<Self, HarnessError> {
        secret_environment.validate_plain_environment(&self.config.environment)?;
        self.secret_environment = Some(secret_environment);
        Ok(self)
    }

    /// Returns the broker isolation visible to embedding Policy and operators.
    #[must_use]
    pub fn broker_descriptor(&self) -> ProcessBrokerDescriptor {
        self.broker_descriptor.clone()
    }
}

impl EffectConnector for JsonCommandEffectConnector {
    fn descriptor(&self) -> EffectConnectorDescriptor {
        self.descriptor.clone()
    }

    fn execute<'a>(
        &'a self,
        request: EffectExecutionRequest,
    ) -> HarnessFuture<'a, EffectExecutionOutcome> {
        Box::pin(async move {
            let EffectExecutionRequest {
                effect_id,
                authority,
                operation,
                idempotency_key,
                input,
                input_sha256,
                attempt,
                lease_id,
                lease_expires_at_ms,
                cancellation,
            } = request;
            let secret_environment = match &self.secret_environment {
                Some(environment) => {
                    environment
                        .resolve(
                            &self.descriptor.capability,
                            &authority,
                            SecretUseContext::GovernedEffect {
                                effect_id: effect_id.clone(),
                                operation: operation.clone(),
                                phase: SecretEffectPhase::Execution,
                                attempt,
                                lease_id: lease_id.clone(),
                            },
                            &cancellation,
                        )
                        .await?
                }
                None => BTreeMap::new(),
            };
            let request = JsonEffectExecutionRequest {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                effect_id,
                authority,
                operation,
                idempotency_key,
                input,
                input_sha256,
                attempt,
                lease_id,
                lease_expires_at_ms,
            };
            let stdin =
                crate::json::to_bounded_json_vec(&request, JSON_COMMAND_MAX_INPUT_BYTES).map_err(
                    |error| match error {
                        crate::json::BoundedJsonError::LimitExceeded => {
                            HarnessError::Effect(format!(
                                "Effect execution command request exceeds {JSON_COMMAND_MAX_INPUT_BYTES} bytes"
                            ))
                        }
                        crate::json::BoundedJsonError::CannotEncode => HarnessError::Effect(
                            "cannot encode Effect execution command request".to_owned(),
                        ),
                    },
                )?;
            let mut process = self.config.request(stdin, ExecutionPhase::Effect);
            process.secret_environment = secret_environment;
            let output = self
                .broker
                .execute(process, cancellation)
                .await
                .map_err(map_effect_process_error)?;
            validate_process_success(&output, "Effect execution command")
                .map_err(HarnessError::Effect)?;
            let response: JsonEffectExecutionResponse = serde_json::from_slice(&output.stdout)
                .map_err(|_| {
                    HarnessError::Effect(
                        "Effect execution command returned invalid JSON settlement".to_owned(),
                    )
                })?;
            validate_protocol(response.protocol_version, "execution")?;
            Ok(response.outcome)
        })
    }
}

/// External Effect reconciler backed by one shell-free JSON command.
pub struct JsonCommandEffectReconciliationConnector {
    descriptor: EffectReconciliationConnectorDescriptor,
    config: JsonProcessConfig,
    broker: Arc<dyn ProcessBroker>,
    broker_descriptor: ProcessBrokerDescriptor,
    secret_environment: Option<EffectSecretEnvironment>,
}

impl JsonCommandEffectReconciliationConnector {
    /// Captures broker trust and validates static process configuration.
    ///
    /// The reconciliation registry independently validates and freezes the
    /// authoritative read-only descriptor before routing an unknown Effect.
    pub fn new(
        descriptor: EffectReconciliationConnectorDescriptor,
        config: JsonProcessConfig,
        broker: Arc<dyn ProcessBroker>,
    ) -> Result<Self, HarnessError> {
        config.validate()?;
        let broker_descriptor =
            capture_capability_metadata("Effect reconciliation process broker descriptor", || {
                broker.descriptor()
            })?;
        validate_broker_descriptor(&broker_descriptor)?;
        Ok(Self {
            descriptor,
            config,
            broker,
            broker_descriptor,
            secret_environment: None,
        })
    }

    /// Adds an on-demand, authority-aware credential environment.
    pub fn with_secret_environment(
        mut self,
        secret_environment: EffectSecretEnvironment,
    ) -> Result<Self, HarnessError> {
        secret_environment.validate_plain_environment(&self.config.environment)?;
        self.secret_environment = Some(secret_environment);
        Ok(self)
    }

    /// Returns the broker isolation visible to embedding Policy and operators.
    #[must_use]
    pub fn broker_descriptor(&self) -> ProcessBrokerDescriptor {
        self.broker_descriptor.clone()
    }
}

impl EffectReconciliationConnector for JsonCommandEffectReconciliationConnector {
    fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
        self.descriptor.clone()
    }

    fn query<'a>(
        &'a self,
        request: EffectReconciliationRequest,
    ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
        Box::pin(async move {
            let EffectReconciliationRequest {
                effect_id,
                authority,
                operation,
                idempotency_key,
                input,
                input_sha256,
                attempt,
                lease_id,
                cancellation,
            } = request;
            let secret_environment = match &self.secret_environment {
                Some(environment) => {
                    environment
                        .resolve(
                            &self.descriptor.capability,
                            &authority,
                            SecretUseContext::GovernedEffect {
                                effect_id: effect_id.clone(),
                                operation: operation.clone(),
                                phase: SecretEffectPhase::Reconciliation,
                                attempt,
                                lease_id: lease_id.clone(),
                            },
                            &cancellation,
                        )
                        .await?
                }
                None => BTreeMap::new(),
            };
            let request = JsonEffectReconciliationRequest {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                effect_id,
                authority,
                operation,
                idempotency_key,
                input,
                input_sha256,
                attempt,
                lease_id,
            };
            let stdin =
                crate::json::to_bounded_json_vec(&request, JSON_COMMAND_MAX_INPUT_BYTES).map_err(
                    |error| match error {
                        crate::json::BoundedJsonError::LimitExceeded => {
                            HarnessError::Effect(format!(
                                "Effect reconciliation command request exceeds {JSON_COMMAND_MAX_INPUT_BYTES} bytes"
                            ))
                        }
                        crate::json::BoundedJsonError::CannotEncode => HarnessError::Effect(
                            "cannot encode Effect reconciliation command request".to_owned(),
                        ),
                    },
                )?;
            let mut process = self.config.request(stdin, ExecutionPhase::Effect);
            process.secret_environment = secret_environment;
            let output = self
                .broker
                .execute(process, cancellation)
                .await
                .map_err(map_effect_process_error)?;
            validate_process_success(&output, "Effect reconciliation command")
                .map_err(HarnessError::Effect)?;
            let response: JsonEffectReconciliationResponse = serde_json::from_slice(&output.stdout)
                .map_err(|_| {
                    HarnessError::Effect(
                        "Effect reconciliation command returned invalid JSON observation"
                            .to_owned(),
                    )
                })?;
            validate_protocol(response.protocol_version, "reconciliation")?;
            Ok(response.outcome)
        })
    }
}

fn validate_protocol(protocol_version: u32, kind: &str) -> Result<(), HarnessError> {
    if protocol_version != JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION {
        return Err(HarnessError::Effect(format!(
            "Effect {kind} command requires JSON protocol \
             {JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION}, received {protocol_version}"
        )));
    }
    Ok(())
}

fn map_effect_process_error(error: HarnessError) -> HarnessError {
    match error {
        HarnessError::Cancelled { .. } | HarnessError::TimedOut { .. } => error,
        _ => HarnessError::Effect("Effect command process failed".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::*;
    use crate::{
        ActorIdentity, CapabilityOrigin, EffectConnectorRegistry, EffectIdempotencyContract,
        EffectReceipt, EffectReconciliationConnectorRegistry, EffectReconciliationContract,
        ProcessIsolation, ProcessOutput, ProcessRequest, SECRET_API_VERSION,
        SecretProviderDescriptor,
    };

    struct RecordingBroker {
        output: ProcessOutput,
        inputs: Mutex<Vec<Vec<u8>>>,
        phases: Mutex<Vec<ExecutionPhase>>,
    }

    impl ProcessBroker for RecordingBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            ProcessBrokerDescriptor {
                name: "effect-recording".to_owned(),
                isolation: ProcessIsolation::Sandboxed {
                    mechanism: "test".to_owned(),
                },
                executable_integrity: crate::ProcessExecutableIntegrity::Unmeasured,
            }
        }

        fn execute<'a>(
            &'a self,
            request: ProcessRequest,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async move {
                self.inputs.lock().expect("inputs").push(request.stdin);
                self.phases
                    .lock()
                    .expect("phases")
                    .push(request.cancellation_phase);
                Ok(self.output.clone())
            })
        }
    }

    struct ErrorBroker;

    impl ProcessBroker for ErrorBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            ProcessBrokerDescriptor {
                name: "effect-error".to_owned(),
                isolation: ProcessIsolation::Denied,
                executable_integrity: crate::ProcessExecutableIntegrity::Unmeasured,
            }
        }

        fn execute<'a>(
            &'a self,
            _request: ProcessRequest,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async {
                Err(HarnessError::Execution(
                    "provider-secret-diagnostic".to_owned(),
                ))
            })
        }
    }

    struct CancelledBroker;

    impl ProcessBroker for CancelledBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            ProcessBrokerDescriptor {
                name: "effect-cancelled".to_owned(),
                isolation: ProcessIsolation::Sandboxed {
                    mechanism: "test".to_owned(),
                },
                executable_integrity: crate::ProcessExecutableIntegrity::Unmeasured,
            }
        }

        fn execute<'a>(
            &'a self,
            request: ProcessRequest,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async move {
                Err(HarnessError::Cancelled {
                    phase: request.cancellation_phase,
                })
            })
        }
    }

    struct PanickingDescriptorBroker;

    impl ProcessBroker for PanickingDescriptorBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            panic!("broker-secret")
        }

        fn execute<'a>(
            &'a self,
            _request: ProcessRequest,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async {
                Err(HarnessError::Execution(
                    "unreachable broker execution".to_owned(),
                ))
            })
        }
    }

    struct RecordingSecretProvider {
        fail: bool,
        requests: Mutex<Vec<(SecretRequest, AuthorityContext)>>,
    }

    impl SecretProvider for RecordingSecretProvider {
        fn descriptor(&self) -> SecretProviderDescriptor {
            SecretProviderDescriptor {
                name: "effect-secret-fixture".to_owned(),
                description: "Records typed Effect Secret requests".to_owned(),
                api_version: SECRET_API_VERSION,
            }
        }

        fn resolve<'a>(&'a self, _request: SecretRequest) -> HarnessFuture<'a, SecretValue> {
            Box::pin(async {
                Err(HarnessError::Secret(
                    "unscoped fixture resolution is forbidden".to_owned(),
                ))
            })
        }

        fn resolve_as<'a>(
            &'a self,
            request: SecretRequest,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, SecretValue> {
            Box::pin(async move {
                self.requests
                    .lock()
                    .expect("Secret requests")
                    .push((request, authority.clone()));
                if self.fail {
                    return Err(HarnessError::Secret(
                        "provider-secret-diagnostic".to_owned(),
                    ));
                }
                SecretValue::new(b"short-lived-token".to_vec())
            })
        }
    }

    struct SecretAwareBroker {
        output: ProcessOutput,
        calls: AtomicUsize,
    }

    impl ProcessBroker for SecretAwareBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            ProcessBrokerDescriptor {
                name: "effect-secret-aware".to_owned(),
                isolation: ProcessIsolation::Sandboxed {
                    mechanism: "test".to_owned(),
                },
                executable_integrity: crate::ProcessExecutableIntegrity::Unmeasured,
            }
        }

        fn execute<'a>(
            &'a self,
            request: ProcessRequest,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async move {
                assert!(!request.environment.contains_key("EFFECT_TOKEN"));
                assert_eq!(
                    request
                        .secret_environment
                        .get("EFFECT_TOKEN")
                        .expect("resolved Secret")
                        .expose_bytes(),
                    b"short-lived-token"
                );
                assert!(
                    !request
                        .stdin
                        .windows(b"short-lived-token".len())
                        .any(|window| window == b"short-lived-token")
                );
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(self.output.clone())
            })
        }
    }

    fn config() -> JsonProcessConfig {
        JsonProcessConfig {
            program: std::env::temp_dir().join("effect-adapter"),
            args: Vec::new(),
            current_dir: std::env::temp_dir(),
            environment: BTreeMap::new(),
            timeout: Duration::from_secs(1),
            max_output_bytes: 65_536,
        }
    }

    fn execution_descriptor() -> EffectConnectorDescriptor {
        EffectConnectorDescriptor {
            capability: "channel.email".to_owned(),
            api_version: crate::EFFECT_EXECUTOR_API_VERSION,
            operations: BTreeSet::from(["send".to_owned()]),
            idempotency: EffectIdempotencyContract::TargetEnforced,
        }
    }

    fn reconciliation_descriptor() -> EffectReconciliationConnectorDescriptor {
        EffectReconciliationConnectorDescriptor {
            capability: "channel.email".to_owned(),
            api_version: crate::EFFECT_RECONCILER_API_VERSION,
            operations: BTreeSet::from(["send".to_owned()]),
            contract: EffectReconciliationContract::AuthoritativeReadOnly,
        }
    }

    fn authority() -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "effect-adapter".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("authority")
    }

    fn operation() -> EffectOperation {
        EffectOperation {
            capability: "channel.email".to_owned(),
            operation: "send".to_owned(),
        }
    }

    fn execution_request(input: Value) -> EffectExecutionRequest {
        EffectExecutionRequest {
            effect_id: EffectId::from_static("effect-json"),
            authority: authority(),
            operation: operation(),
            idempotency_key: "idempotency-secret".to_owned(),
            input,
            input_sha256: "a".repeat(64),
            attempt: 2,
            lease_id: EffectLeaseId::from_static("lease-json"),
            lease_expires_at_ms: 10_000,
            cancellation: crate::CancellationToken::new(),
        }
    }

    fn reconciliation_request(input: Value) -> EffectReconciliationRequest {
        EffectReconciliationRequest {
            effect_id: EffectId::from_static("effect-json"),
            authority: authority(),
            operation: operation(),
            idempotency_key: "idempotency-secret".to_owned(),
            input,
            input_sha256: "a".repeat(64),
            attempt: 2,
            lease_id: EffectLeaseId::from_static("lease-json"),
            cancellation: crate::CancellationToken::new(),
        }
    }

    fn receipt() -> EffectReceipt {
        EffectReceipt {
            source: "mail.provider".to_owned(),
            external_id: "provider-42".to_owned(),
            observed_at_ms: 100,
            response_sha256: "b".repeat(64),
        }
    }

    fn output<T: Serialize>(value: &T) -> ProcessOutput {
        ProcessOutput {
            success: true,
            code: Some(0),
            stdout: serde_json::to_vec(value).expect("encode output"),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }

    #[tokio::test]
    async fn execution_adapter_emits_exact_v1_envelope_and_returns_settlement() {
        let expected = EffectExecutionOutcome::Applied { receipt: receipt() };
        let broker = Arc::new(RecordingBroker {
            output: output(&JsonEffectExecutionResponse {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                outcome: expected.clone(),
            }),
            inputs: Mutex::new(Vec::new()),
            phases: Mutex::new(Vec::new()),
        });
        let connector =
            JsonCommandEffectConnector::new(execution_descriptor(), config(), broker.clone())
                .expect("Connector");

        let outcome = connector
            .execute(execution_request(serde_json::json!({"message":"private"})))
            .await
            .expect("execute");

        assert_eq!(outcome, expected);
        assert_eq!(
            connector.broker_descriptor().isolation,
            ProcessIsolation::Sandboxed {
                mechanism: "test".to_owned()
            }
        );
        assert_eq!(
            *broker.phases.lock().expect("phases"),
            vec![ExecutionPhase::Effect]
        );
        let inputs = broker.inputs.lock().expect("inputs");
        let request: JsonEffectExecutionRequest =
            serde_json::from_slice(&inputs[0]).expect("request");
        assert_eq!(
            request.protocol_version,
            JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION
        );
        assert_eq!(request.effect_id, EffectId::from_static("effect-json"));
        assert_eq!(request.attempt, 2);
        assert_eq!(request.lease_expires_at_ms, 10_000);
        assert_eq!(request.authority.tenant_id(), Some("tenant-a"));
        assert_eq!(request.idempotency_key, "idempotency-secret");
        assert!(
            !serde_json::to_string(&request)
                .expect("request JSON")
                .contains("cancellation")
        );
    }

    #[tokio::test]
    async fn reconciliation_adapter_emits_exact_v1_envelope_and_returns_observation() {
        let expected = EffectReconciliationOutcome::NotApplied {
            reason_code: "provider.absent".to_owned(),
            retry_after_ms: Some(25),
        };
        let broker = Arc::new(RecordingBroker {
            output: output(&JsonEffectReconciliationResponse {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                outcome: expected.clone(),
            }),
            inputs: Mutex::new(Vec::new()),
            phases: Mutex::new(Vec::new()),
        });
        let connector = JsonCommandEffectReconciliationConnector::new(
            reconciliation_descriptor(),
            config(),
            broker.clone(),
        )
        .expect("Connector");

        let outcome = connector
            .query(reconciliation_request(
                serde_json::json!({"message":"private"}),
            ))
            .await
            .expect("query");

        assert_eq!(outcome, expected);
        assert_eq!(
            *broker.phases.lock().expect("phases"),
            vec![ExecutionPhase::Effect]
        );
        let inputs = broker.inputs.lock().expect("inputs");
        let request: JsonEffectReconciliationRequest =
            serde_json::from_slice(&inputs[0]).expect("request");
        assert_eq!(
            request.protocol_version,
            JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION
        );
        assert_eq!(request.attempt, 2);
        assert_eq!(request.lease_id, EffectLeaseId::from_static("lease-json"));
        assert!(
            !serde_json::to_string(&request)
                .expect("request JSON")
                .contains("cancellation")
        );
    }

    #[tokio::test]
    async fn secret_environment_uses_typed_effect_context_and_never_enters_json() {
        let provider = Arc::new(RecordingSecretProvider {
            fail: false,
            requests: Mutex::new(Vec::new()),
        });
        let secret_environment = EffectSecretEnvironment::new(
            provider.clone(),
            BTreeMap::from([(
                "EFFECT_TOKEN".to_owned(),
                SecretReference::new("effect/channel-email").expect("reference"),
            )]),
        )
        .expect("Secret environment");
        secret_environment
            .probe("channel.email", &authority())
            .await
            .expect("probe");

        let execution_broker = Arc::new(SecretAwareBroker {
            output: output(&JsonEffectExecutionResponse {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                outcome: EffectExecutionOutcome::Applied { receipt: receipt() },
            }),
            calls: AtomicUsize::new(0),
        });
        let execution = JsonCommandEffectConnector::new(
            execution_descriptor(),
            config(),
            execution_broker.clone(),
        )
        .expect("execution Connector")
        .with_secret_environment(secret_environment.clone())
        .expect("execution Secret environment");
        execution
            .execute(execution_request(serde_json::json!({"message":"private"})))
            .await
            .expect("execution");

        let reconciliation_broker = Arc::new(SecretAwareBroker {
            output: output(&JsonEffectReconciliationResponse {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                outcome: EffectReconciliationOutcome::StillUnknown {
                    reason_code: "provider.pending".to_owned(),
                },
            }),
            calls: AtomicUsize::new(0),
        });
        let reconciliation = JsonCommandEffectReconciliationConnector::new(
            reconciliation_descriptor(),
            config(),
            reconciliation_broker.clone(),
        )
        .expect("reconciliation Connector")
        .with_secret_environment(secret_environment)
        .expect("reconciliation Secret environment");
        reconciliation
            .query(reconciliation_request(serde_json::json!({
                "message": "private"
            })))
            .await
            .expect("reconciliation");

        assert_eq!(execution_broker.calls.load(Ordering::SeqCst), 1);
        assert_eq!(reconciliation_broker.calls.load(Ordering::SeqCst), 1);
        let requests = provider.requests.lock().expect("Secret requests");
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[0].0.use_context,
            SecretUseContext::Service {
                use_case: SecretServiceUse::StartupProbe
            }
        );
        assert_eq!(requests[0].1.tenant_id(), Some("tenant-a"));
        assert_eq!(
            requests[1].0.use_context,
            SecretUseContext::GovernedEffect {
                effect_id: EffectId::from_static("effect-json"),
                operation: operation(),
                phase: SecretEffectPhase::Execution,
                attempt: 2,
                lease_id: EffectLeaseId::from_static("lease-json"),
            }
        );
        assert_eq!(requests[1].0.consumer, "channel.email");
        assert_eq!(
            requests[2].0.use_context,
            SecretUseContext::GovernedEffect {
                effect_id: EffectId::from_static("effect-json"),
                operation: operation(),
                phase: SecretEffectPhase::Reconciliation,
                attempt: 2,
                lease_id: EffectLeaseId::from_static("lease-json"),
            }
        );
    }

    #[tokio::test]
    async fn secret_resolution_failure_and_precancellation_block_process_entry() {
        let provider = Arc::new(RecordingSecretProvider {
            fail: true,
            requests: Mutex::new(Vec::new()),
        });
        let environment = EffectSecretEnvironment::new(
            provider.clone(),
            BTreeMap::from([(
                "EFFECT_TOKEN".to_owned(),
                SecretReference::new("effect/channel-email").expect("reference"),
            )]),
        )
        .expect("Secret environment");
        let broker = Arc::new(SecretAwareBroker {
            output: output(&JsonEffectExecutionResponse {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                outcome: EffectExecutionOutcome::Applied { receipt: receipt() },
            }),
            calls: AtomicUsize::new(0),
        });
        let connector =
            JsonCommandEffectConnector::new(execution_descriptor(), config(), broker.clone())
                .expect("Connector")
                .with_secret_environment(environment)
                .expect("Secret environment");

        let error = connector
            .execute(execution_request(serde_json::json!({})))
            .await
            .expect_err("Secret failure");
        assert_eq!(
            error.to_string(),
            "effect error: Effect Connector credential resolution failed"
        );
        assert!(!error.to_string().contains("provider-secret"));
        assert_eq!(broker.calls.load(Ordering::SeqCst), 0);

        let provider = Arc::new(RecordingSecretProvider {
            fail: false,
            requests: Mutex::new(Vec::new()),
        });
        let environment = EffectSecretEnvironment::new(
            provider.clone(),
            BTreeMap::from([(
                "EFFECT_TOKEN".to_owned(),
                SecretReference::new("effect/channel-email").expect("reference"),
            )]),
        )
        .expect("Secret environment");
        let connector =
            JsonCommandEffectConnector::new(execution_descriptor(), config(), broker.clone())
                .expect("Connector")
                .with_secret_environment(environment)
                .expect("Secret environment");
        let request = execution_request(serde_json::json!({}));
        request.cancellation.cancel();
        assert!(matches!(
            connector.execute(request).await,
            Err(HarnessError::Cancelled {
                phase: ExecutionPhase::Effect
            })
        ));
        assert!(
            provider
                .requests
                .lock()
                .expect("Secret requests")
                .is_empty()
        );
        assert_eq!(broker.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn secret_environment_rejects_invalid_or_overlapping_child_names() {
        let provider = Arc::new(RecordingSecretProvider {
            fail: false,
            requests: Mutex::new(Vec::new()),
        });
        assert!(
            EffectSecretEnvironment::new(
                provider.clone(),
                BTreeMap::from([(
                    "INVALID-NAME".to_owned(),
                    SecretReference::new("effect/channel-email").expect("reference"),
                )]),
            )
            .is_err()
        );

        let environment = EffectSecretEnvironment::new(
            provider,
            BTreeMap::from([(
                "EFFECT_TOKEN".to_owned(),
                SecretReference::new("effect/channel-email").expect("reference"),
            )]),
        )
        .expect("Secret environment");
        let mut process = config();
        process
            .environment
            .insert("EFFECT_TOKEN".to_owned(), "plain".to_owned());
        let connector =
            JsonCommandEffectConnector::new(execution_descriptor(), process, Arc::new(ErrorBroker))
                .expect("Connector");
        assert!(connector.with_secret_environment(environment).is_err());
    }

    #[test]
    fn adapters_capture_broker_panic_and_register_through_effect_registries() {
        let error = match JsonCommandEffectConnector::new(
            execution_descriptor(),
            config(),
            Arc::new(PanickingDescriptorBroker),
        ) {
            Ok(_) => panic!("descriptor panic must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("descriptor"));
        assert!(!error.to_string().contains("broker-secret"));

        let execution = Arc::new(
            JsonCommandEffectConnector::new(
                execution_descriptor(),
                config(),
                Arc::new(ErrorBroker),
            )
            .expect("execution Connector"),
        );
        let mut execution_registry = EffectConnectorRegistry::new();
        execution_registry
            .register(
                CapabilityOrigin::External {
                    id: "json-effect/execution".to_owned(),
                },
                execution,
            )
            .expect("register execution");

        let reconciliation = Arc::new(
            JsonCommandEffectReconciliationConnector::new(
                reconciliation_descriptor(),
                config(),
                Arc::new(ErrorBroker),
            )
            .expect("reconciliation Connector"),
        );
        let mut reconciliation_registry = EffectReconciliationConnectorRegistry::new();
        reconciliation_registry
            .register(
                CapabilityOrigin::External {
                    id: "json-effect/reconciliation".to_owned(),
                },
                reconciliation,
            )
            .expect("register reconciliation");
    }

    #[tokio::test]
    async fn protocol_mismatch_invalid_json_and_truncation_fail_closed() {
        let mismatch = JsonCommandEffectConnector::new(
            execution_descriptor(),
            config(),
            Arc::new(RecordingBroker {
                output: output(&JsonEffectExecutionResponse {
                    protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION + 1,
                    outcome: EffectExecutionOutcome::Applied { receipt: receipt() },
                }),
                inputs: Mutex::new(Vec::new()),
                phases: Mutex::new(Vec::new()),
            }),
        )
        .expect("Connector");
        assert!(
            mismatch
                .execute(execution_request(serde_json::json!({})))
                .await
                .expect_err("protocol mismatch")
                .to_string()
                .contains("requires JSON protocol")
        );

        for output in [
            ProcessOutput {
                success: true,
                code: Some(0),
                stdout: b"{invalid".to_vec(),
                stderr: b"provider-secret".to_vec(),
                stdout_truncated: false,
                stderr_truncated: false,
            },
            ProcessOutput {
                success: true,
                code: Some(0),
                stdout: Vec::new(),
                stderr: b"provider-secret".to_vec(),
                stdout_truncated: true,
                stderr_truncated: false,
            },
        ] {
            let connector = JsonCommandEffectConnector::new(
                execution_descriptor(),
                config(),
                Arc::new(RecordingBroker {
                    output,
                    inputs: Mutex::new(Vec::new()),
                    phases: Mutex::new(Vec::new()),
                }),
            )
            .expect("Connector");
            let error = connector
                .execute(execution_request(serde_json::json!({})))
                .await
                .expect_err("invalid output");
            assert!(!error.to_string().contains("provider-secret"));
        }
    }

    #[tokio::test]
    async fn process_errors_are_redacted_and_cancellation_keeps_effect_phase() {
        let failed = JsonCommandEffectReconciliationConnector::new(
            reconciliation_descriptor(),
            config(),
            Arc::new(ErrorBroker),
        )
        .expect("Connector");
        let error = failed
            .query(reconciliation_request(serde_json::json!({})))
            .await
            .expect_err("process failure");
        assert_eq!(
            error.to_string(),
            "effect error: Effect command process failed"
        );
        assert!(!error.to_string().contains("provider-secret"));

        let cancelled = JsonCommandEffectConnector::new(
            execution_descriptor(),
            config(),
            Arc::new(CancelledBroker),
        )
        .expect("Connector");
        assert!(matches!(
            cancelled
                .execute(execution_request(serde_json::json!({})))
                .await,
            Err(HarnessError::Cancelled {
                phase: ExecutionPhase::Effect
            })
        ));
    }

    #[tokio::test]
    async fn oversized_request_is_rejected_before_process_entry() {
        let broker = Arc::new(RecordingBroker {
            output: output(&JsonEffectExecutionResponse {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                outcome: EffectExecutionOutcome::Applied { receipt: receipt() },
            }),
            inputs: Mutex::new(Vec::new()),
            phases: Mutex::new(Vec::new()),
        });
        let connector =
            JsonCommandEffectConnector::new(execution_descriptor(), config(), broker.clone())
                .expect("Connector");
        let error = connector
            .execute(execution_request(serde_json::json!({
                "oversized": "x".repeat(JSON_COMMAND_MAX_INPUT_BYTES)
            })))
            .await
            .expect_err("oversized request");

        assert!(error.to_string().contains("exceeds"));
        assert!(broker.inputs.lock().expect("inputs").is_empty());
    }
}
