//! Typed capability registration with validation and collision rejection.

use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use super::{HarnessError, LanguageModel, Tool, ToolDescriptor};
use crate::json::{BoundedJsonError, bounded_serialized_size, validate_value_shape};

pub(crate) const MAX_CAPABILITY_REGISTRY_ENTRIES: usize = 4_096;
const MAX_CAPABILITY_ORIGIN_ID_BYTES: usize = 256;
const MAX_TOOL_DESCRIPTOR_BYTES: usize = 1_048_576;
const MAX_TOOL_REGISTRY_METADATA_BYTES: usize = 8_388_608;
const MAX_TOOL_SCHEMA_DEPTH: usize = crate::json::MAX_JSON_DEPTH;
const MAX_TOOL_SCHEMA_NODES: usize = crate::json::MAX_JSON_NODES;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
/// Trust-bearing origin retained with a registered capability.
pub enum CapabilityOrigin {
    /// Implementation compiled into the runtime distribution.
    BuiltIn,
    /// Operator-approved in-process extension.
    TrustedExtension {
        /// Stable extension package identity.
        id: String,
    },
    /// Separately launched executable extension.
    External {
        /// Stable extension package or operator registration identity.
        id: String,
    },
}

/// Language-model implementation paired with validated identity and origin.
#[derive(Clone)]
pub struct RegisteredModel {
    /// Stable provider/model registry identity.
    pub id: String,
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Executable implementation.
    pub model: Arc<dyn LanguageModel>,
}

/// Deterministic collision-safe language-model registry.
#[derive(Default)]
pub struct ModelRegistry {
    models: BTreeMap<String, RegisteredModel>,
}

impl ModelRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers a model without allowing identity replacement.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        model: Arc<dyn LanguageModel>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("model", self.models.len(), 1)?;
        let id = capture_capability_metadata("model identity", || model.id().to_owned())?;
        validate_model_id(&id)?;
        if self.models.contains_key(&id) {
            return Err(HarnessError::DuplicateCapability(id));
        }
        self.models
            .insert(id.clone(), RegisteredModel { id, origin, model });
        Ok(())
    }

    /// Looks up a model by its stable provider/model identity.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&RegisteredModel> {
        self.models.get(id)
    }

    /// Returns registered model identities in deterministic order.
    #[must_use]
    pub fn ids(&self) -> Vec<String> {
        self.models.keys().cloned().collect()
    }
}

/// Tool implementation paired with validated metadata and origin.
pub struct RegisteredTool {
    /// Validated model-visible descriptor.
    pub descriptor: ToolDescriptor,
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Frozen same-response scheduling guarantee.
    pub batch_execution: crate::ToolBatchExecution,
    /// Executable implementation.
    pub tool: Arc<dyn Tool>,
}

#[derive(Default)]
/// Deterministic registry for tool capabilities.
pub struct ToolRegistry {
    tools: BTreeMap<String, RegisteredTool>,
    metadata_bytes: usize,
}

impl ToolRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers a tool without allowing name replacement.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        tool: Arc<dyn Tool>,
    ) -> Result<(), HarnessError> {
        self.register_batch([(origin, tool)])
    }

    /// Validates and atomically registers a batch without partial mutation.
    pub fn register_batch(
        &mut self,
        tools: impl IntoIterator<Item = (CapabilityOrigin, Arc<dyn Tool>)>,
    ) -> Result<(), HarnessError> {
        let mut staged = BTreeMap::new();
        let mut staged_metadata_bytes = 0usize;
        for (origin, tool) in tools {
            validate_capability_origin(&origin)?;
            validate_registry_growth("tool", self.tools.len(), staged.len().saturating_add(1))?;
            let descriptor = capture_capability_metadata("tool descriptor", || tool.descriptor())?;
            validate_descriptor(&descriptor)?;
            let batch_execution =
                capture_capability_metadata("tool batch execution", || tool.batch_execution())?;
            if self.tools.contains_key(&descriptor.name) || staged.contains_key(&descriptor.name) {
                return Err(HarnessError::DuplicateCapability(descriptor.name));
            }
            let descriptor_bytes = bounded_capability_metadata_bytes(
                "tool descriptor",
                &descriptor,
                MAX_TOOL_DESCRIPTOR_BYTES,
            )?;
            staged_metadata_bytes = staged_metadata_bytes
                .checked_add(descriptor_bytes)
                .ok_or_else(|| {
                    HarnessError::InvalidCapability(
                        "tool registry metadata byte count overflow".to_owned(),
                    )
                })?;
            let next_metadata_bytes = self
                .metadata_bytes
                .checked_add(staged_metadata_bytes)
                .ok_or_else(|| {
                    HarnessError::InvalidCapability(
                        "tool registry metadata byte count overflow".to_owned(),
                    )
                })?;
            if next_metadata_bytes > MAX_TOOL_REGISTRY_METADATA_BYTES {
                return Err(HarnessError::InvalidCapability(format!(
                    "tool registry metadata exceeds {MAX_TOOL_REGISTRY_METADATA_BYTES} bytes"
                )));
            }
            staged.insert(
                descriptor.name.clone(),
                RegisteredTool {
                    descriptor,
                    origin,
                    batch_execution,
                    tool,
                },
            );
        }
        self.tools.extend(staged);
        self.metadata_bytes += staged_metadata_bytes;
        Ok(())
    }

    #[must_use]
    /// Looks up a tool by its stable registry name.
    pub fn get(&self, name: &str) -> Option<&RegisteredTool> {
        self.tools.get(name)
    }

    #[must_use]
    /// Returns model-visible descriptors in deterministic name order.
    pub fn descriptors(&self) -> Vec<ToolDescriptor> {
        self.tools
            .values()
            .map(|registered| registered.descriptor.clone())
            .collect()
    }
}

pub(crate) fn capture_capability_metadata<T>(
    label: &str,
    operation: impl FnOnce() -> T,
) -> Result<T, HarnessError> {
    catch_unwind(AssertUnwindSafe(operation))
        .map_err(|_| HarnessError::InvalidCapability(format!("{label} provider panicked")))
}

pub(crate) fn validate_capability_origin(origin: &CapabilityOrigin) -> Result<(), HarnessError> {
    let id = match origin {
        CapabilityOrigin::BuiltIn => return Ok(()),
        CapabilityOrigin::TrustedExtension { id } | CapabilityOrigin::External { id } => id,
    };
    if id.trim().is_empty()
        || id.len() > MAX_CAPABILITY_ORIGIN_ID_BYTES
        || id.chars().any(char::is_control)
    {
        return Err(HarnessError::InvalidCapability(format!(
            "capability origin id must be 1-{MAX_CAPABILITY_ORIGIN_ID_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_registry_growth(
    kind: &str,
    current: usize,
    additional: usize,
) -> Result<(), HarnessError> {
    let next = current.checked_add(additional).ok_or_else(|| {
        HarnessError::InvalidCapability(format!("{kind} registry entry count overflow"))
    })?;
    if next > MAX_CAPABILITY_REGISTRY_ENTRIES {
        return Err(HarnessError::InvalidCapability(format!(
            "{kind} registry exceeds {MAX_CAPABILITY_REGISTRY_ENTRIES} entries"
        )));
    }
    Ok(())
}

fn bounded_capability_metadata_bytes<T: Serialize>(
    kind: &str,
    value: &T,
    maximum: usize,
) -> Result<usize, HarnessError> {
    match bounded_serialized_size(value, maximum) {
        Ok(bytes) => Ok(bytes),
        Err(BoundedJsonError::LimitExceeded) => Err(HarnessError::InvalidCapability(format!(
            "{kind} exceeds {maximum} bytes"
        ))),
        Err(BoundedJsonError::CannotEncode) => Err(HarnessError::InvalidCapability(format!(
            "{kind} cannot be encoded"
        ))),
    }
}

fn validate_descriptor(descriptor: &ToolDescriptor) -> Result<(), HarnessError> {
    validate_capability_name("tool", &descriptor.name)?;
    if descriptor.description.trim().is_empty() {
        return Err(HarnessError::InvalidCapability(format!(
            "tool {} has an empty description",
            descriptor.name
        )));
    }
    if !descriptor.input_schema.is_object() {
        return Err(HarnessError::InvalidCapability(format!(
            "tool {} input schema must be a JSON object",
            descriptor.name
        )));
    }
    validate_tool_schema_shape(&descriptor.name, &descriptor.input_schema)?;
    Ok(())
}

fn validate_tool_schema_shape(name: &str, schema: &serde_json::Value) -> Result<(), HarnessError> {
    validate_value_shape(schema).map_err(|_| {
        HarnessError::InvalidCapability(format!(
            "tool {name} input schema exceeds depth {MAX_TOOL_SCHEMA_DEPTH} or {MAX_TOOL_SCHEMA_NODES} nodes"
        ))
    })
}

pub(crate) fn validate_capability_name(kind: &str, name: &str) -> Result<(), HarnessError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '_' | '-' | '.')
        });

    if valid {
        Ok(())
    } else {
        Err(HarnessError::InvalidCapability(format!(
            "{kind} name {name:?} must be 1-64 lowercase ASCII characters, digits, '.', '_' or '-'"
        )))
    }
}

pub(crate) fn validate_model_id(id: &str) -> Result<(), HarnessError> {
    let valid = !id.is_empty()
        && id.len() <= 128
        && id.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
        });
    if valid {
        Ok(())
    } else {
        Err(HarnessError::InvalidCapability(format!(
            "model id {id:?} must be 1-128 portable ASCII identity characters"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use super::{CapabilityOrigin, ModelRegistry, ToolRegistry};
    use crate::{
        HarnessError, HarnessFuture, LanguageModel, ModelOutput, ModelRequest, Tool,
        ToolBatchExecution, ToolContext, ToolDescriptor,
    };

    struct TestModel(&'static str);

    struct TestTool(&'static str);

    struct LargeTool {
        name: &'static str,
        description_bytes: usize,
    }

    struct PanickingTool;
    struct ParallelTool;
    struct PanickingBatchExecutionTool;

    impl LanguageModel for TestModel {
        fn id(&self) -> &str {
            self.0
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::Message {
                    content: "ok".to_owned(),
                })
            })
        }
    }

    impl Tool for TestTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: self.0.to_owned(),
                description: "test tool".to_owned(),
                input_schema: json!({"type": "object"}),
            }
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move { Ok(input) })
        }
    }

    impl Tool for LargeTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: self.name.to_owned(),
                description: "x".repeat(self.description_bytes),
                input_schema: json!({"type": "object"}),
            }
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move { Ok(input) })
        }
    }

    impl Tool for PanickingTool {
        fn descriptor(&self) -> ToolDescriptor {
            panic!("sensitive descriptor panic")
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move { Ok(input) })
        }
    }

    impl Tool for ParallelTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "parallel".to_owned(),
                description: "parallel-safe test tool".to_owned(),
                input_schema: json!({"type": "object"}),
            }
        }

        fn batch_execution(&self) -> ToolBatchExecution {
            ToolBatchExecution::ParallelSafe
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move { Ok(input) })
        }
    }

    impl Tool for PanickingBatchExecutionTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "panic-batch-execution".to_owned(),
                description: "panics while declaring batch execution".to_owned(),
                input_schema: json!({"type": "object"}),
            }
        }

        fn batch_execution(&self) -> ToolBatchExecution {
            panic!("sensitive batch execution panic")
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move { Ok(input) })
        }
    }

    #[test]
    fn model_registry_validates_identity_and_rejects_replacement() {
        let mut registry = ModelRegistry::new();
        registry
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(TestModel("provider/model")),
            )
            .expect("register model");
        assert_eq!(registry.ids(), vec!["provider/model"]);
        let error = registry
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(TestModel("provider/model")),
            )
            .expect_err("duplicate model");
        assert_eq!(
            error,
            HarnessError::DuplicateCapability("provider/model".to_owned())
        );

        let invalid = registry
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(TestModel("provider model")),
            )
            .expect_err("invalid model id");
        assert!(matches!(invalid, HarnessError::InvalidCapability(_)));
    }

    #[test]
    fn tool_batch_registration_is_atomic_and_sanitizes_descriptor_panics() {
        let mut registry = ToolRegistry::new();
        let tools: Vec<(CapabilityOrigin, Arc<dyn Tool>)> = vec![
            (CapabilityOrigin::BuiltIn, Arc::new(TestTool("valid"))),
            (CapabilityOrigin::BuiltIn, Arc::new(TestTool("INVALID"))),
        ];
        assert!(matches!(
            registry.register_batch(tools),
            Err(HarnessError::InvalidCapability(_))
        ));
        assert!(registry.descriptors().is_empty());

        let error = registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(PanickingTool))
            .expect_err("descriptor panic");
        assert!(matches!(error, HarnessError::InvalidCapability(_)));
        assert!(!error.to_string().contains("sensitive"));
        assert!(registry.descriptors().is_empty());
    }

    #[test]
    fn tool_batch_execution_is_frozen_and_panic_isolated() {
        let mut registry = ToolRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(TestTool("sequential")))
            .expect("sequential Tool");
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(ParallelTool))
            .expect("parallel Tool");
        assert_eq!(
            registry.get("sequential").map(|tool| tool.batch_execution),
            Some(ToolBatchExecution::Sequential)
        );
        assert_eq!(
            registry.get("parallel").map(|tool| tool.batch_execution),
            Some(ToolBatchExecution::ParallelSafe)
        );

        let error = registry
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(PanickingBatchExecutionTool),
            )
            .expect_err("batch execution panic");
        assert!(matches!(error, HarnessError::InvalidCapability(_)));
        assert!(!error.to_string().contains("sensitive"));
        assert!(registry.get("panic-batch-execution").is_none());
    }

    #[test]
    fn registries_reject_unbounded_origins_counts_and_tool_metadata_atomically() {
        let origin = CapabilityOrigin::External {
            id: "x".repeat(super::MAX_CAPABILITY_ORIGIN_ID_BYTES + 1),
        };
        assert!(super::validate_capability_origin(&origin).is_err());
        assert!(
            super::validate_registry_growth("fixture", super::MAX_CAPABILITY_REGISTRY_ENTRIES, 1)
                .is_err()
        );

        let mut registry = ToolRegistry::new();
        let tools: Vec<(CapabilityOrigin, Arc<dyn Tool>)> = vec![
            (CapabilityOrigin::BuiltIn, Arc::new(TestTool("valid"))),
            (
                CapabilityOrigin::BuiltIn,
                Arc::new(LargeTool {
                    name: "oversized",
                    description_bytes: super::MAX_TOOL_DESCRIPTOR_BYTES,
                }),
            ),
        ];
        assert!(matches!(
            registry.register_batch(tools),
            Err(HarnessError::InvalidCapability(_))
        ));
        assert!(registry.descriptors().is_empty());
        assert_eq!(registry.metadata_bytes, 0);

        registry.metadata_bytes = super::MAX_TOOL_REGISTRY_METADATA_BYTES;
        assert!(
            registry
                .register(CapabilityOrigin::BuiltIn, Arc::new(TestTool("bounded")))
                .is_err()
        );
        assert!(registry.descriptors().is_empty());
        assert_eq!(
            registry.metadata_bytes,
            super::MAX_TOOL_REGISTRY_METADATA_BYTES
        );

        let mut nested = Value::Null;
        for _ in 0..=super::MAX_TOOL_SCHEMA_DEPTH {
            nested = json!({"nested": nested});
        }
        let descriptor = ToolDescriptor {
            name: "deep".to_owned(),
            description: "deep schema".to_owned(),
            input_schema: nested,
        };
        assert!(super::validate_descriptor(&descriptor).is_err());
    }
}
