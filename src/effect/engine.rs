//! Composition boundary for the durable Effect lifecycle.

use std::sync::Arc;

use crate::{AuthorityContext, EffectId, HarnessError};

use super::{
    EffectCommand, EffectCommandResult, EffectCoordinator, EffectCreateRequest, EffectDueScanPage,
    EffectPage, EffectPageCursor, EffectSnapshot,
};

/// Governed durable external-effect lifecycle service.
#[derive(Clone)]
pub struct EffectEngine {
    coordinator: Arc<dyn EffectCoordinator>,
}

impl EffectEngine {
    /// Installs one authoritative Effect Coordinator.
    #[must_use]
    pub fn new(coordinator: Arc<dyn EffectCoordinator>) -> Self {
        Self { coordinator }
    }

    /// Creates or recognizes one unscoped durable intent.
    pub async fn create(
        &self,
        effect_id: EffectId,
        request: EffectCreateRequest,
        applied_at_ms: u64,
    ) -> Result<EffectSnapshot, HarnessError> {
        self.create_as(
            effect_id,
            request,
            applied_at_ms,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Creates or recognizes one intent in the exact authority boundary.
    pub async fn create_as(
        &self,
        effect_id: EffectId,
        request: EffectCreateRequest,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<EffectSnapshot, HarnessError> {
        self.coordinator
            .create_as(effect_id, request, applied_at_ms, authority)
            .await
    }

    /// Loads one unscoped Effect.
    pub async fn load(&self, effect_id: &EffectId) -> Result<Option<EffectSnapshot>, HarnessError> {
        self.load_as(effect_id, &AuthorityContext::local_process())
            .await
    }

    /// Loads one Effect only in the exact authority boundary.
    pub async fn load_as(
        &self,
        effect_id: &EffectId,
        authority: &AuthorityContext,
    ) -> Result<Option<EffectSnapshot>, HarnessError> {
        self.coordinator.load_as(effect_id, authority).await
    }

    /// Lists one bounded unscoped lifecycle page.
    pub async fn list(
        &self,
        status: Option<&str>,
        after: Option<&EffectPageCursor>,
        limit: usize,
    ) -> Result<EffectPage, HarnessError> {
        self.list_as(status, after, limit, &AuthorityContext::local_process())
            .await
    }

    /// Lists one bounded lifecycle page in the exact authority boundary.
    pub async fn list_as(
        &self,
        status: Option<&str>,
        after: Option<&EffectPageCursor>,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<EffectPage, HarnessError> {
        self.coordinator
            .list_as(status, after, limit, authority)
            .await
    }

    /// Scans one bounded unscoped page for expired execution leases.
    pub async fn scan_due(
        &self,
        at_ms: u64,
        after_effect_id: Option<&EffectId>,
        scan_limit: usize,
    ) -> Result<EffectDueScanPage, HarnessError> {
        self.scan_due_as(
            at_ms,
            after_effect_id,
            scan_limit,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Scans expired leases in the exact authority boundary.
    pub async fn scan_due_as(
        &self,
        at_ms: u64,
        after_effect_id: Option<&EffectId>,
        scan_limit: usize,
        authority: &AuthorityContext,
    ) -> Result<EffectDueScanPage, HarnessError> {
        self.coordinator
            .scan_due_as(at_ms, after_effect_id, scan_limit, authority)
            .await
    }

    /// Applies one unscoped actor-bound command.
    pub async fn apply(
        &self,
        effect_id: &EffectId,
        expected_revision: u64,
        command: EffectCommand,
        applied_at_ms: u64,
    ) -> Result<EffectCommandResult, HarnessError> {
        self.apply_as(
            effect_id,
            expected_revision,
            command,
            applied_at_ms,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Applies one command with exact revision and tenant fencing.
    pub async fn apply_as(
        &self,
        effect_id: &EffectId,
        expected_revision: u64,
        command: EffectCommand,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<EffectCommandResult, HarnessError> {
        self.coordinator
            .apply_as(
                effect_id,
                expected_revision,
                command,
                applied_at_ms,
                authority,
            )
            .await
    }
}
