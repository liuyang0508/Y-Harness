//! Policy evaluation and approval settlement ports.

use std::collections::BTreeSet;

use crate::{
    ApprovalDecision, ApprovalRequest, HarnessFuture, PolicyDecision, ThreadId, ToolAuthorization,
    TurnId,
};

/// Authorization boundary evaluated before tool execution.
pub trait PolicyEngine: Send + Sync {
    /// Returns allow, deny, or ask for one proposed tool call.
    fn authorize<'a>(&'a self, request: &'a ToolAuthorization)
    -> HarnessFuture<'a, PolicyDecision>;
}

/// Settlement boundary for policy decisions that require approval.
pub trait ApprovalHandler: Send + Sync {
    /// Approves or denies one fully correlated request.
    fn decide<'a>(&'a self, request: &'a ApprovalRequest) -> HarnessFuture<'a, ApprovalDecision>;

    /// Marks pending requests from a Turn that can no longer consume settlement.
    fn abandon_turn<'a>(
        &'a self,
        _thread_id: &'a ThreadId,
        _turn_id: &'a TurnId,
        _reason: &'a str,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
/// Deny-by-default tool policy backed by an explicit name allow-list.
pub struct AllowListPolicy {
    allowed: BTreeSet<String>,
}

impl AllowListPolicy {
    #[must_use]
    /// Creates a policy that initially denies every tool.
    pub fn deny_by_default() -> Self {
        Self::default()
    }

    #[must_use]
    /// Adds one allowed tool name.
    pub fn allow(mut self, tool_name: impl Into<String>) -> Self {
        self.allowed.insert(tool_name.into());
        self
    }
}

impl PolicyEngine for AllowListPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a ToolAuthorization,
    ) -> HarnessFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if self.allowed.contains(&request.descriptor.name) {
                Ok(PolicyDecision::Allow)
            } else {
                Ok(PolicyDecision::Deny {
                    reason: "tool is not present in the runtime allow list".to_owned(),
                })
            }
        })
    }
}

/// Safe default used when no interactive or delegated approver is installed.
pub struct DenyAllApprovals;

impl ApprovalHandler for DenyAllApprovals {
    fn decide<'a>(&'a self, _request: &'a ApprovalRequest) -> HarnessFuture<'a, ApprovalDecision> {
        Box::pin(async {
            Ok(ApprovalDecision::Deny {
                reason: "no approval handler is configured".to_owned(),
            })
        })
    }
}
