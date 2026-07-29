//! Composition boundary between Human Handoff ownership and subject authority.

use std::sync::Arc;

use crate::{AuthorityContext, HarnessError, HarnessFuture, HumanHandoffId};

use super::{
    HumanHandoffCommand, HumanHandoffCommandResult, HumanHandoffCoordinator,
    HumanHandoffCreateRequest, HumanHandoffCursor, HumanHandoffDueScanPage, HumanHandoffPage,
    HumanHandoffSnapshot, HumanHandoffSubject,
};

/// Trusted subject-existence boundary used before a Human Handoff is created.
pub trait HumanHandoffSubjectResolver: Send + Sync {
    /// Returns whether the subject exists in the exact authority boundary.
    fn exists<'a>(
        &'a self,
        subject: &'a HumanHandoffSubject,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, bool>;
}

/// Governed Human Handoff lifecycle service.
#[derive(Clone)]
pub struct HumanHandoffEngine {
    coordinator: Arc<dyn HumanHandoffCoordinator>,
    subjects: Arc<dyn HumanHandoffSubjectResolver>,
}

impl HumanHandoffEngine {
    /// Composes durable ownership state with authoritative subject lookup.
    #[must_use]
    pub fn new(
        coordinator: Arc<dyn HumanHandoffCoordinator>,
        subjects: Arc<dyn HumanHandoffSubjectResolver>,
    ) -> Self {
        Self {
            coordinator,
            subjects,
        }
    }

    /// Creates one unscoped case only for an existing subject.
    pub async fn create(
        &self,
        handoff_id: HumanHandoffId,
        request: HumanHandoffCreateRequest,
        applied_at_ms: u64,
    ) -> Result<HumanHandoffSnapshot, HarnessError> {
        self.create_as(
            handoff_id,
            request,
            applied_at_ms,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Creates one case only when its subject exists in the exact trusted
    /// tenant boundary.
    pub async fn create_as(
        &self,
        handoff_id: HumanHandoffId,
        request: HumanHandoffCreateRequest,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<HumanHandoffSnapshot, HarnessError> {
        if !self.subjects.exists(&request.subject, authority).await? {
            return Err(HarnessError::HumanHandoff(
                "Human Handoff subject does not exist in the authority boundary".to_owned(),
            ));
        }
        self.coordinator
            .create_as(handoff_id, request, applied_at_ms, authority)
            .await
    }

    /// Loads one unscoped case.
    pub async fn load(
        &self,
        handoff_id: &HumanHandoffId,
    ) -> Result<Option<HumanHandoffSnapshot>, HarnessError> {
        self.load_as(handoff_id, &AuthorityContext::local_process())
            .await
    }

    /// Loads one case only inside the exact trusted tenant boundary.
    pub async fn load_as(
        &self,
        handoff_id: &HumanHandoffId,
        authority: &AuthorityContext,
    ) -> Result<Option<HumanHandoffSnapshot>, HarnessError> {
        self.coordinator.load_as(handoff_id, authority).await
    }

    /// Lists one unscoped queued work page.
    pub async fn list_queued(
        &self,
        queue: &str,
        after: Option<&HumanHandoffCursor>,
        limit: usize,
    ) -> Result<HumanHandoffPage, HarnessError> {
        self.list_queued_as(queue, after, limit, &AuthorityContext::local_process())
            .await
    }

    /// Lists queued work inside the exact trusted tenant boundary.
    pub async fn list_queued_as(
        &self,
        queue: &str,
        after: Option<&HumanHandoffCursor>,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<HumanHandoffPage, HarnessError> {
        self.coordinator
            .list_queued_as(queue, after, limit, authority)
            .await
    }

    /// Scans one bounded unscoped page for expired claims.
    pub async fn scan_due(
        &self,
        at_ms: u64,
        after_handoff_id: Option<&HumanHandoffId>,
        scan_limit: usize,
    ) -> Result<HumanHandoffDueScanPage, HarnessError> {
        self.scan_due_as(
            at_ms,
            after_handoff_id,
            scan_limit,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Scans one bounded page for expired claims in the exact tenant boundary.
    pub async fn scan_due_as(
        &self,
        at_ms: u64,
        after_handoff_id: Option<&HumanHandoffId>,
        scan_limit: usize,
        authority: &AuthorityContext,
    ) -> Result<HumanHandoffDueScanPage, HarnessError> {
        self.coordinator
            .scan_due_as(at_ms, after_handoff_id, scan_limit, authority)
            .await
    }

    /// Applies one actor-bound command to an unscoped case.
    pub async fn apply(
        &self,
        handoff_id: &HumanHandoffId,
        expected_revision: u64,
        command: HumanHandoffCommand,
        applied_at_ms: u64,
    ) -> Result<HumanHandoffCommandResult, HarnessError> {
        self.apply_as(
            handoff_id,
            expected_revision,
            command,
            applied_at_ms,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Applies one actor-bound command with exact revision and tenant fencing.
    pub async fn apply_as(
        &self,
        handoff_id: &HumanHandoffId,
        expected_revision: u64,
        command: HumanHandoffCommand,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<HumanHandoffCommandResult, HarnessError> {
        self.coordinator
            .apply_as(
                handoff_id,
                expected_revision,
                command,
                applied_at_ms,
                authority,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HumanHandoffCommandId, HumanHandoffSubject, MemoryHumanHandoffCoordinator, ThreadId,
    };

    struct FixedResolver(bool);

    impl HumanHandoffSubjectResolver for FixedResolver {
        fn exists<'a>(
            &'a self,
            _subject: &'a HumanHandoffSubject,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, bool> {
            Box::pin(async move { Ok(self.0) })
        }
    }

    fn request() -> HumanHandoffCreateRequest {
        HumanHandoffCreateRequest {
            command_id: HumanHandoffCommandId::from_static("create"),
            subject: HumanHandoffSubject::Thread {
                thread_id: ThreadId::from_static("thread"),
            },
            queue: "support.primary".to_owned(),
            reason_code: "agent.escalation".to_owned(),
            priority: 1,
        }
    }

    #[tokio::test]
    async fn creation_requires_an_authoritative_subject() {
        let missing = HumanHandoffEngine::new(
            Arc::new(MemoryHumanHandoffCoordinator::new()),
            Arc::new(FixedResolver(false)),
        );
        let error = missing
            .create(HumanHandoffId::from_static("handoff"), request(), 10)
            .await
            .expect_err("missing subject");
        assert!(error.to_string().contains("subject does not exist"));

        let existing = HumanHandoffEngine::new(
            Arc::new(MemoryHumanHandoffCoordinator::new()),
            Arc::new(FixedResolver(true)),
        );
        let created = existing
            .create(HumanHandoffId::from_static("handoff"), request(), 10)
            .await
            .expect("existing subject");
        assert_eq!(created.revision(), 1);
    }
}
