//! Policy evaluation and approval settlement ports.

use std::collections::BTreeSet;

use crate::{
    ApprovalDecision, ApprovalRequest, AuthorityContext, HarnessFuture, PolicyDecision, ThreadId,
    ToolAuthorization, TurnId,
};

/// Authorization boundary evaluated before tool execution.
pub trait PolicyEngine: Send + Sync {
    /// Returns allow, deny, or ask for one proposed tool call.
    fn authorize<'a>(
        &'a self,
        request: &'a ToolAuthorization,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, PolicyDecision>;
}

/// Settlement boundary for policy decisions that require approval.
pub trait ApprovalHandler: Send + Sync {
    /// Approves or denies one fully correlated request.
    fn decide<'a>(&'a self, request: &'a ApprovalRequest) -> HarnessFuture<'a, ApprovalDecision>;

    /// Decides one request under trusted tenant authority.
    ///
    /// Legacy handlers are safe only for unscoped execution. Tenant-aware
    /// handlers must override this method and durably preserve the boundary.
    fn decide_as<'a>(
        &'a self,
        request: &'a ApprovalRequest,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, ApprovalDecision> {
        Box::pin(async move {
            if authority.tenant_id().is_some() {
                return Err(crate::HarnessError::InvalidConfiguration(
                    "tenant-scoped approvals require a tenant-aware approval handler".to_owned(),
                ));
            }
            self.decide(request).await
        })
    }

    /// Marks pending requests from a Turn that can no longer consume settlement.
    fn abandon_turn<'a>(
        &'a self,
        _thread_id: &'a ThreadId,
        _turn_id: &'a TurnId,
        _reason: &'a str,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Marks abandoned requests under trusted tenant authority.
    fn abandon_turn_as<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        turn_id: &'a TurnId,
        reason: &'a str,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async move {
            if authority.tenant_id().is_some() {
                return Err(crate::HarnessError::InvalidConfiguration(
                    "tenant-scoped approvals require a tenant-aware approval handler".to_owned(),
                ));
            }
            self.abandon_turn(thread_id, turn_id, reason).await
        })
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
        _authority: &'a AuthorityContext,
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

    fn decide_as<'a>(
        &'a self,
        request: &'a ApprovalRequest,
        _authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, ApprovalDecision> {
        self.decide(request)
    }
}
