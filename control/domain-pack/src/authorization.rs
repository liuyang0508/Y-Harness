use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use y_harness::{ActorIdentity, AuthorityContext};

use crate::{
    DomainPackActivation, DomainPackError, DomainPackExecutionBinding, DomainPackFuture,
    DomainPackRelease, DomainPackReleaseId, DomainPackSnapshot, DomainPackStore,
    VerifiedDomainPack, model::validate_release_id, store::validate_pack_name,
};

const MAX_ROLE_GRANTS: usize = 4_096;

/// One exact governed Domain Pack operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainPackAction {
    /// Read one immutable release record.
    InspectRelease,
    /// Read one Pack activation record.
    InspectActivation,
    /// Install one immutable release.
    Install,
    /// Settle evaluation evidence.
    Evaluate,
    /// Approve one passing evaluation.
    Approve,
    /// Activate one approved release.
    Activate,
    /// Deactivate one active release.
    Deactivate,
    /// Restore one bounded rollback target.
    Rollback,
    /// Bind an approved active release to execution.
    Bind,
}

impl DomainPackAction {
    /// Returns the stable permission name used in denial evidence.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InspectRelease => "inspect_release",
            Self::InspectActivation => "inspect_activation",
            Self::Install => "install",
            Self::Evaluate => "evaluate",
            Self::Approve => "approve",
            Self::Activate => "activate",
            Self::Deactivate => "deactivate",
            Self::Rollback => "rollback",
            Self::Bind => "bind",
        }
    }
}

/// Trusted, bounded authorization input for one exact Domain Pack action.
///
/// The store wrapper constructs this value from an authenticated
/// [`AuthorityContext`]. Authorizers inspect it but cannot forge a different
/// action or resource for the in-flight store call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomainPackAuthorization {
    actor: ActorIdentity,
    tenant_id: Option<String>,
    action: DomainPackAction,
    pack_name: String,
    release: Option<DomainPackReleaseId>,
}

impl DomainPackAuthorization {
    /// Returns the trusted actor requesting the operation.
    #[must_use]
    pub fn actor(&self) -> &ActorIdentity {
        &self.actor
    }

    /// Returns the exact tenant partition, or none for an explicitly unscoped host.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Returns the exact requested operation.
    #[must_use]
    pub const fn action(&self) -> DomainPackAction {
        self.action
    }

    /// Returns the stable Pack name.
    #[must_use]
    pub fn pack_name(&self) -> &str {
        &self.pack_name
    }

    /// Returns the exact release when the operation targets one.
    #[must_use]
    pub fn release(&self) -> Option<&DomainPackReleaseId> {
        self.release.as_ref()
    }

    fn for_release(
        action: DomainPackAction,
        release: &DomainPackReleaseId,
        authority: &AuthorityContext,
    ) -> Result<Self, DomainPackError> {
        validate_release_id(release)?;
        let (actor, tenant_id) = validated_authority(authority)?;
        Ok(Self {
            actor,
            tenant_id,
            action,
            pack_name: release.name.clone(),
            release: Some(release.clone()),
        })
    }

    fn for_pack(
        action: DomainPackAction,
        name: &str,
        authority: &AuthorityContext,
    ) -> Result<Self, DomainPackError> {
        validate_pack_name(name)?;
        let (actor, tenant_id) = validated_authority(authority)?;
        Ok(Self {
            actor,
            tenant_id,
            action,
            pack_name: name.to_owned(),
            release: None,
        })
    }
}

/// Synchronous, non-blocking, fail-closed Domain Pack authorization policy.
///
/// The authorized store catches policy panics and denies the operation before
/// delegating to persistence.
pub trait DomainPackAuthorizer: Send + Sync {
    /// Returns whether one trusted request has the exact requested permission.
    fn allows(&self, request: &DomainPackAuthorization) -> bool;
}

/// Authorizer that denies every Domain Pack operation.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyDomainPackAuthorizer;

impl DomainPackAuthorizer for DenyDomainPackAuthorizer {
    fn allows(&self, _request: &DomainPackAuthorization) -> bool {
        false
    }
}

impl<T> DomainPackAuthorizer for Arc<T>
where
    T: DomainPackAuthorizer + ?Sized,
{
    fn allows(&self, request: &DomainPackAuthorization) -> bool {
        self.as_ref().allows(request)
    }
}

/// Reference least-privilege role for Domain Pack lifecycle operations.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainPackRole {
    /// Read release and activation state.
    Auditor,
    /// Install immutable release snapshots.
    Installer,
    /// Settle evaluation evidence.
    Evaluator,
    /// Approve passing evaluation evidence.
    Approver,
    /// Activate, deactivate, and roll back releases.
    Operator,
    /// Bind one governed release to an Engine execution.
    Executor,
    /// Perform every current Domain Pack action.
    Administrator,
}

impl DomainPackRole {
    const fn allows(self, action: DomainPackAction) -> bool {
        match self {
            Self::Auditor => matches!(
                action,
                DomainPackAction::InspectRelease | DomainPackAction::InspectActivation
            ),
            Self::Installer => matches!(action, DomainPackAction::Install),
            Self::Evaluator => matches!(action, DomainPackAction::Evaluate),
            Self::Approver => matches!(action, DomainPackAction::Approve),
            Self::Operator => matches!(
                action,
                DomainPackAction::Activate
                    | DomainPackAction::Deactivate
                    | DomainPackAction::Rollback
            ),
            Self::Executor => matches!(action, DomainPackAction::Bind),
            Self::Administrator => true,
        }
    }
}

/// One exact actor-and-tenant role grant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackRoleGrant {
    actor: ActorIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    roles: BTreeSet<DomainPackRole>,
}

impl DomainPackRoleGrant {
    /// Creates one validated exact grant.
    pub fn new(
        actor: ActorIdentity,
        tenant_id: Option<String>,
        roles: BTreeSet<DomainPackRole>,
    ) -> Result<Self, DomainPackError> {
        validate_grant(&actor, tenant_id.as_deref(), &roles)?;
        Ok(Self {
            actor,
            tenant_id,
            roles,
        })
    }

    /// Returns the exact granted actor.
    #[must_use]
    pub fn actor(&self) -> &ActorIdentity {
        &self.actor
    }

    /// Returns the exact granted tenant.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Returns the granted roles.
    #[must_use]
    pub fn roles(&self) -> &BTreeSet<DomainPackRole> {
        &self.roles
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum PrincipalKey {
    LocalProcess,
    Authenticated { authority: String, subject: String },
}

impl PrincipalKey {
    fn from_actor(actor: &ActorIdentity) -> Result<Self, DomainPackError> {
        match actor {
            ActorIdentity::LocalProcess => Ok(Self::LocalProcess),
            ActorIdentity::Authenticated { authority, subject } => Ok(Self::Authenticated {
                authority: authority.clone(),
                subject: subject.clone(),
            }),
            ActorIdentity::UnattributedLegacy => Err(DomainPackError::Invalid(
                "Domain Pack role grant actor is invalid".to_owned(),
            )),
        }
    }
}

type GrantKey = (Option<String>, PrincipalKey);

/// Exact actor-and-tenant RBAC reference authorizer.
///
/// This implementation has no wildcard, tenant fallback, or implicit local
/// privilege. Embedding hosts may replace it with an external policy engine by
/// implementing [`DomainPackAuthorizer`].
#[derive(Debug)]
pub struct DomainPackRoleAuthorizer {
    grants: BTreeMap<GrantKey, BTreeSet<DomainPackRole>>,
}

impl DomainPackRoleAuthorizer {
    /// Validates and indexes a bounded set of exact grants.
    pub fn new(grants: Vec<DomainPackRoleGrant>) -> Result<Self, DomainPackError> {
        if grants.is_empty() || grants.len() > MAX_ROLE_GRANTS {
            return Err(DomainPackError::Invalid(format!(
                "Domain Pack role grants must contain 1-{MAX_ROLE_GRANTS} entries"
            )));
        }
        let mut indexed = BTreeMap::new();
        for grant in grants {
            validate_grant(&grant.actor, grant.tenant_id.as_deref(), &grant.roles)?;
            let key = (grant.tenant_id, PrincipalKey::from_actor(&grant.actor)?);
            if indexed.insert(key, grant.roles).is_some() {
                return Err(DomainPackError::Invalid(
                    "duplicate exact Domain Pack role grant".to_owned(),
                ));
            }
        }
        Ok(Self { grants: indexed })
    }
}

impl DomainPackAuthorizer for DomainPackRoleAuthorizer {
    fn allows(&self, request: &DomainPackAuthorization) -> bool {
        let Ok(principal) = PrincipalKey::from_actor(request.actor()) else {
            return false;
        };
        self.grants
            .get(&(request.tenant_id.clone(), principal))
            .is_some_and(|roles| roles.iter().any(|role| role.allows(request.action())))
    }
}

/// Domain Pack store adapter that authorizes every operation before storage.
pub struct AuthorizedDomainPackStore<S, A> {
    inner: S,
    authorizer: A,
}

impl<S, A> AuthorizedDomainPackStore<S, A> {
    /// Wraps one store with one mandatory authorizer.
    #[must_use]
    pub const fn new(inner: S, authorizer: A) -> Self {
        Self { inner, authorizer }
    }
}

impl<S, A> AuthorizedDomainPackStore<S, A>
where
    S: DomainPackStore,
    A: DomainPackAuthorizer,
{
    fn authorize(&self, request: DomainPackAuthorization) -> Result<(), DomainPackError> {
        let allowed =
            catch_unwind(AssertUnwindSafe(|| self.authorizer.allows(&request))).unwrap_or(false);
        if allowed {
            Ok(())
        } else {
            Err(DomainPackError::Forbidden {
                action: request.action.as_str(),
                pack: request.pack_name,
            })
        }
    }
}

impl<S, A> DomainPackStore for AuthorizedDomainPackStore<S, A>
where
    S: DomainPackStore,
    A: DomainPackAuthorizer,
{
    fn install<'a>(
        &'a self,
        snapshot: DomainPackSnapshot,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        let authorization = DomainPackAuthorization::for_release(
            DomainPackAction::Install,
            &snapshot.release,
            authority,
        )
        .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self.inner.install(snapshot, authority),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn get<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, Option<DomainPackRelease>> {
        let authorization = DomainPackAuthorization::for_release(
            DomainPackAction::InspectRelease,
            release,
            authority,
        )
        .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self.inner.get(release, authority),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn evaluate<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        expected_revision: u64,
        suite_sha256: String,
        report_sha256: String,
        passed: bool,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        let authorization =
            DomainPackAuthorization::for_release(DomainPackAction::Evaluate, release, authority)
                .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self.inner.evaluate(
                release,
                expected_revision,
                suite_sha256,
                report_sha256,
                passed,
                authority,
            ),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn approve<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        expected_revision: u64,
        evidence_sha256: String,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        let authorization =
            DomainPackAuthorization::for_release(DomainPackAction::Approve, release, authority)
                .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self
                .inner
                .approve(release, expected_revision, evidence_sha256, authority),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn activation<'a>(
        &'a self,
        name: &'a str,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, Option<DomainPackActivation>> {
        let authorization =
            DomainPackAuthorization::for_pack(DomainPackAction::InspectActivation, name, authority)
                .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self.inner.activation(name, authority),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn bind<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_activation_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackExecutionBinding> {
        let authorization = DomainPackAuthorization::for_release(
            DomainPackAction::Bind,
            &verified.snapshot().release,
            authority,
        )
        .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self
                .inner
                .bind(verified, expected_activation_revision, authority),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn activate<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        let authorization = DomainPackAuthorization::for_release(
            DomainPackAction::Activate,
            &verified.snapshot().release,
            authority,
        )
        .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self.inner.activate(verified, expected_revision, authority),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn deactivate<'a>(
        &'a self,
        name: &'a str,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        let authorization =
            DomainPackAuthorization::for_pack(DomainPackAction::Deactivate, name, authority)
                .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self.inner.deactivate(name, expected_revision, authority),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn rollback<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        let authorization = DomainPackAuthorization::for_release(
            DomainPackAction::Rollback,
            &verified.snapshot().release,
            authority,
        )
        .and_then(|request| self.authorize(request));
        match authorization {
            Ok(()) => self.inner.rollback(verified, expected_revision, authority),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
}

fn validated_authority(
    authority: &AuthorityContext,
) -> Result<(ActorIdentity, Option<String>), DomainPackError> {
    let actor = authority.actor().clone();
    let tenant_id = authority.tenant_id().map(str::to_owned);
    AuthorityContext::new(actor.clone(), tenant_id.clone())
        .map_err(|_| DomainPackError::Invalid("trusted authority is invalid".to_owned()))?;
    Ok((actor, tenant_id))
}

fn validate_grant(
    actor: &ActorIdentity,
    tenant_id: Option<&str>,
    roles: &BTreeSet<DomainPackRole>,
) -> Result<(), DomainPackError> {
    AuthorityContext::new(actor.clone(), tenant_id.map(str::to_owned))
        .map_err(|_| DomainPackError::Invalid("Domain Pack role grant is invalid".to_owned()))?;
    if roles.is_empty() {
        return Err(DomainPackError::Invalid(
            "Domain Pack role grant must contain at least one role".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    };

    use semver::Version;

    use super::*;
    use crate::{
        DomainPackComponentKind, DomainPackComponentPin, DomainPackInventory, MemoryDomainPackStore,
    };

    #[tokio::test]
    async fn exact_roles_cover_the_complete_lifecycle() {
        let store = authorized_store(vec![
            grant("installer", "tenant-a", DomainPackRole::Installer),
            grant("evaluator", "tenant-a", DomainPackRole::Evaluator),
            grant("approver", "tenant-a", DomainPackRole::Approver),
            grant("operator", "tenant-a", DomainPackRole::Operator),
            grant("executor", "tenant-a", DomainPackRole::Executor),
            grant("auditor", "tenant-a", DomainPackRole::Auditor),
        ]);
        let first_snapshot = snapshot(1);
        let release = first_snapshot.release.clone();
        let first_verified = verified(&first_snapshot);

        let installed = store
            .install(first_snapshot, &authority("tenant-a", "installer"))
            .await
            .expect("installer");
        let evaluated = store
            .evaluate(
                &release,
                installed.revision,
                digest('a'),
                digest('c'),
                true,
                &authority("tenant-a", "evaluator"),
            )
            .await
            .expect("evaluator");
        let approved = store
            .approve(
                &release,
                evaluated.revision,
                digest('d'),
                &authority("tenant-a", "approver"),
            )
            .await
            .expect("approver");
        assert_eq!(approved.revision, 3);
        let active = store
            .activate(
                first_verified.clone(),
                0,
                &authority("tenant-a", "operator"),
            )
            .await
            .expect("operator");
        assert_eq!(active.revision, 1);
        let binding = store
            .bind(
                first_verified.clone(),
                1,
                &authority("tenant-a", "executor"),
            )
            .await
            .expect("executor");
        assert_eq!(binding.activation_revision(), 1);

        let second_snapshot = snapshot(2);
        let second_release = second_snapshot.release.clone();
        let second_verified = verified(&second_snapshot);
        store
            .install(second_snapshot, &authority("tenant-a", "installer"))
            .await
            .expect("install second");
        store
            .evaluate(
                &second_release,
                1,
                digest('a'),
                digest('e'),
                true,
                &authority("tenant-a", "evaluator"),
            )
            .await
            .expect("evaluate second");
        store
            .approve(
                &second_release,
                2,
                digest('f'),
                &authority("tenant-a", "approver"),
            )
            .await
            .expect("approve second");
        store
            .activate(second_verified, 1, &authority("tenant-a", "operator"))
            .await
            .expect("activate second");
        store
            .rollback(first_verified, 2, &authority("tenant-a", "operator"))
            .await
            .expect("rollback");
        store
            .deactivate("assistant", 3, &authority("tenant-a", "operator"))
            .await
            .expect("deactivate");

        assert!(
            store
                .get(&release, &authority("tenant-a", "auditor"))
                .await
                .expect("auditor release")
                .is_some()
        );
        assert!(
            store
                .activation("assistant", &authority("tenant-a", "auditor"))
                .await
                .expect("auditor activation")
                .is_some_and(|record| record.active.is_none())
        );
    }

    #[tokio::test]
    async fn exact_tenant_grants_have_no_fallback_and_denial_does_not_mutate() {
        let actor = actor("shared-installer");
        let store = AuthorizedDomainPackStore::new(
            MemoryDomainPackStore::new(),
            DomainPackRoleAuthorizer::new(vec![
                DomainPackRoleGrant::new(
                    actor.clone(),
                    Some("tenant-a".to_owned()),
                    BTreeSet::from([DomainPackRole::Installer]),
                )
                .expect("tenant-a installer"),
                DomainPackRoleGrant::new(
                    actor,
                    Some("tenant-b".to_owned()),
                    BTreeSet::from([DomainPackRole::Auditor]),
                )
                .expect("tenant-b auditor"),
            ])
            .expect("authorizer"),
        );
        let snapshot = snapshot(1);
        let release = snapshot.release.clone();

        let denied = store
            .install(snapshot, &authority("tenant-b", "shared-installer"))
            .await
            .expect_err("tenant role must not fall back");
        assert_eq!(
            denied,
            DomainPackError::Forbidden {
                action: "install",
                pack: "assistant".to_owned(),
            }
        );
        assert!(
            store
                .get(&release, &authority("tenant-b", "shared-installer"))
                .await
                .expect("inspect after denial")
                .is_none()
        );
    }

    #[tokio::test]
    async fn authorizer_panic_fails_closed_before_store_mutation() {
        let mode = Arc::new(AtomicU8::new(0));
        let store = AuthorizedDomainPackStore::new(
            MemoryDomainPackStore::new(),
            SwitchingAuthorizer { mode: mode.clone() },
        );
        let snapshot = snapshot(1);
        let release = snapshot.release.clone();
        let authority = authority("tenant-a", "actor");

        let denied = store
            .install(snapshot, &authority)
            .await
            .expect_err("panic must deny");
        assert!(matches!(
            denied,
            DomainPackError::Forbidden {
                action: "install",
                ..
            }
        ));
        mode.store(1, Ordering::SeqCst);
        assert!(
            store
                .get(&release, &authority)
                .await
                .expect("inspect after panic")
                .is_none()
        );
    }

    #[tokio::test]
    async fn store_separation_of_duty_still_applies_to_administrators() {
        let store = authorized_store(vec![
            grant("administrator-a", "tenant-a", DomainPackRole::Administrator),
            grant("administrator-b", "tenant-a", DomainPackRole::Administrator),
        ]);
        let snapshot = snapshot(1);
        let release = snapshot.release.clone();
        store
            .install(snapshot, &authority("tenant-a", "administrator-a"))
            .await
            .expect("install");
        store
            .evaluate(
                &release,
                1,
                digest('a'),
                digest('c'),
                true,
                &authority("tenant-a", "administrator-a"),
            )
            .await
            .expect("evaluate");

        let self_approval = store
            .approve(
                &release,
                2,
                digest('d'),
                &authority("tenant-a", "administrator-a"),
            )
            .await
            .expect_err("same actor cannot self-approve");
        assert!(matches!(self_approval, DomainPackError::Invalid(_)));
        store
            .approve(
                &release,
                2,
                digest('d'),
                &authority("tenant-a", "administrator-b"),
            )
            .await
            .expect("independent administrator");
    }

    #[test]
    fn role_grants_reject_empty_invalid_and_duplicate_entries() {
        let empty =
            DomainPackRoleGrant::new(actor("actor"), Some("tenant-a".to_owned()), BTreeSet::new())
                .expect_err("empty roles");
        assert!(matches!(empty, DomainPackError::Invalid(_)));

        let legacy = DomainPackRoleGrant::new(
            ActorIdentity::UnattributedLegacy,
            Some("tenant-a".to_owned()),
            BTreeSet::from([DomainPackRole::Auditor]),
        )
        .expect_err("legacy actor");
        assert!(matches!(legacy, DomainPackError::Invalid(_)));

        let grant = grant("actor", "tenant-a", DomainPackRole::Auditor);
        let duplicate = DomainPackRoleAuthorizer::new(vec![grant.clone(), grant])
            .expect_err("duplicate exact grant");
        assert!(matches!(duplicate, DomainPackError::Invalid(_)));
    }

    struct SwitchingAuthorizer {
        mode: Arc<AtomicU8>,
    }

    impl DomainPackAuthorizer for SwitchingAuthorizer {
        fn allows(&self, _request: &DomainPackAuthorization) -> bool {
            match self.mode.load(Ordering::SeqCst) {
                0 => panic!("authorization fixture panic"),
                1 => true,
                _ => false,
            }
        }
    }

    fn authorized_store(
        grants: Vec<DomainPackRoleGrant>,
    ) -> AuthorizedDomainPackStore<MemoryDomainPackStore, DomainPackRoleAuthorizer> {
        AuthorizedDomainPackStore::new(
            MemoryDomainPackStore::new(),
            DomainPackRoleAuthorizer::new(grants).expect("role authorizer"),
        )
    }

    fn grant(subject: &str, tenant: &str, role: DomainPackRole) -> DomainPackRoleGrant {
        DomainPackRoleGrant::new(
            actor(subject),
            Some(tenant.to_owned()),
            BTreeSet::from([role]),
        )
        .expect("role grant")
    }

    fn actor(subject: &str) -> ActorIdentity {
        ActorIdentity::Authenticated {
            authority: "domain-pack-tests".to_owned(),
            subject: subject.to_owned(),
        }
    }

    fn authority(tenant: &str, subject: &str) -> AuthorityContext {
        AuthorityContext::new(actor(subject), Some(tenant.to_owned())).expect("authority")
    }

    fn snapshot(major: u64) -> DomainPackSnapshot {
        DomainPackSnapshot::seal(
            DomainPackReleaseId {
                name: "assistant".to_owned(),
                version: Version::new(major, 0, 0),
            },
            format!("Assistant release {major}"),
            vec![
                DomainPackComponentPin {
                    kind: DomainPackComponentKind::Evaluation,
                    name: "promotion".to_owned(),
                    version: format!("eval:v{major}"),
                    content_sha256: digest('a'),
                },
                DomainPackComponentPin {
                    kind: DomainPackComponentKind::Tool,
                    name: "orders.read".to_owned(),
                    version: format!("tool:v{major}"),
                    content_sha256: digest('b'),
                },
            ],
        )
        .expect("snapshot")
    }

    fn verified(snapshot: &DomainPackSnapshot) -> VerifiedDomainPack {
        snapshot
            .verify(&DomainPackInventory::new(snapshot.components.clone()).expect("inventory"))
            .expect("verified")
    }

    fn digest(character: char) -> String {
        std::iter::repeat_n(character, 64).collect()
    }
}
