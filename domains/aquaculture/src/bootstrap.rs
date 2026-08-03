//! Embedding-host assembly for the executable POC capabilities.

use std::sync::Arc;

use y_harness::{CapabilityOrigin, HarnessError, ToolRegistry, VerificationRegistry};

use crate::{
    tools::{MockErpQueryTool, MockIotQueryTool},
    verification::AquacultureOutputVerifier,
};

/// Registers the executable AQ-JR-001 tools and completion gate.
///
/// Authentication, model selection, policy, approval, memory, and persistence
/// remain embedding-host responsibilities and are intentionally not hidden by
/// this helper.
pub fn register_poc_capabilities(
    tools: &mut ToolRegistry,
    verifiers: &mut VerificationRegistry,
) -> Result<(), HarnessError> {
    let origin = || CapabilityOrigin::TrustedExtension {
        id: "domain-pack:aquaculture-agent@0.1.0".to_owned(),
    };
    tools.register(origin(), Arc::new(MockIotQueryTool))?;
    tools.register(origin(), Arc::new(MockErpQueryTool))?;
    verifiers.register(origin(), Arc::new(AquacultureOutputVerifier))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_exact_poc_surface() {
        let mut tools = ToolRegistry::new();
        let mut verifiers = VerificationRegistry::new();
        register_poc_capabilities(&mut tools, &mut verifiers).expect("register capabilities");
        assert!(tools.get("aquaculture.iot.query").is_some());
        assert!(tools.get("aquaculture.erp.query").is_some());
        assert!(verifiers.get("aquaculture.output-contract").is_some());
    }
}
