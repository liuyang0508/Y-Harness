//! Policy evaluation and approval settlement ports.

use std::collections::BTreeSet;

use crate::{
    ApprovalDecision, ApprovalId, ApprovalRecord, ApprovalRequest, AuthorityContext, HarnessFuture,
    PolicyDecision, ThreadId, ToolAuthorization, TurnId,
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

    /// Returns whether this handler can submit and reload a durable wait
    /// without retaining a polling worker.
    #[must_use]
    fn supports_durable_wait(&self) -> bool {
        false
    }

    /// Idempotently submits one durable wait under trusted tenant authority.
    ///
    /// Handlers must override this together with [`Self::supports_durable_wait`]
    /// and [`Self::get_wait_as`]. The default fails closed and never fabricates
    /// an [`ApprovalRecord`].
    fn submit_wait_as<'a>(
        &'a self,
        _request: &'a ApprovalRequest,
        _authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, ApprovalRecord> {
        Box::pin(async {
            Err(crate::HarnessError::InvalidConfiguration(
                "approval handler does not support durable waits".to_owned(),
            ))
        })
    }

    /// Reloads one durable wait inside the exact trusted tenant boundary.
    ///
    /// The default fails closed rather than treating an unsupported handler as
    /// an empty Inbox.
    fn get_wait_as<'a>(
        &'a self,
        _approval_id: &'a ApprovalId,
        _authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<ApprovalRecord>> {
        Box::pin(async {
            Err(crate::HarnessError::InvalidConfiguration(
                "approval handler does not support durable waits".to_owned(),
            ))
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ApprovalHandler, DenyAllApprovals};
    use crate::{
        ApprovalActor, ApprovalDecision, ApprovalId, ApprovalRequest, AuthorityContext,
        CapabilityOrigin, HarnessError, RiskLevel, ThreadId, ToolAuthorization, ToolDescriptor,
        TurnId,
    };

    fn request() -> ApprovalRequest {
        ApprovalRequest {
            id: ApprovalId::from_static("approval"),
            requested_by: ApprovalActor::LocalProcess,
            authorization: ToolAuthorization {
                thread_id: ThreadId::from_static("thread"),
                turn_id: TurnId::from_static("turn"),
                call_id: "call".to_owned(),
                descriptor: ToolDescriptor {
                    name: "fixture.tool".to_owned(),
                    description: "fixture tool".to_owned(),
                    input_schema: json!({"type": "object"}),
                },
                origin: CapabilityOrigin::BuiltIn,
                input: json!({}),
            },
            reason: "fixture approval".to_owned(),
            risk: RiskLevel::Low,
        }
    }

    #[tokio::test]
    async fn default_durable_wait_port_fails_closed_without_changing_blocking_decisions() {
        let handler = DenyAllApprovals;
        let request = request();
        let authority = AuthorityContext::local_process();

        assert!(!handler.supports_durable_wait());
        assert!(matches!(
            handler.submit_wait_as(&request, &authority).await,
            Err(HarnessError::InvalidConfiguration(message))
                if message.contains("does not support durable waits")
        ));
        assert!(matches!(
            handler.get_wait_as(&request.id, &authority).await,
            Err(HarnessError::InvalidConfiguration(message))
                if message.contains("does not support durable waits")
        ));
        assert!(matches!(
            handler.decide_as(&request, &authority).await,
            Ok(ApprovalDecision::Deny { .. })
        ));
    }
}
