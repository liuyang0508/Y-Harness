use std::{collections::BTreeSet, error::Error};

use semver::Version;
use y_harness::{ActorIdentity, AuthorityContext};
use y_harness_domain_pack::{
    AuthorizedDomainPackStore, DomainPackComponentKind, DomainPackComponentPin,
    DomainPackInventory, DomainPackReleaseId, DomainPackRole, DomainPackRoleAuthorizer,
    DomainPackRoleGrant, DomainPackSnapshot, DomainPackStore, MemoryDomainPackStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let snapshot = DomainPackSnapshot::seal(
        DomainPackReleaseId {
            name: "course-assistant".to_owned(),
            version: Version::new(1, 0, 0),
        },
        "Course assistant deployment",
        vec![
            pin(
                DomainPackComponentKind::Workflow,
                "course-assistant",
                "workflow:v1",
                'a',
            ),
            pin(
                DomainPackComponentKind::Policy,
                "enterprise-default",
                "policy:v1",
                'b',
            ),
            pin(
                DomainPackComponentKind::Evaluation,
                "promotion",
                "eval:v1",
                'c',
            ),
        ],
    )?;
    let inventory = DomainPackInventory::new(snapshot.components.clone())?;
    let verified = snapshot.verify(&inventory)?;
    let release = snapshot.release.clone();
    let store = AuthorizedDomainPackStore::new(
        MemoryDomainPackStore::new(),
        DomainPackRoleAuthorizer::new(vec![
            grant("installer", DomainPackRole::Installer)?,
            grant("evaluator", DomainPackRole::Evaluator)?,
            grant("approver", DomainPackRole::Approver)?,
            grant("operator", DomainPackRole::Operator)?,
            grant("executor", DomainPackRole::Executor)?,
        ])?,
    );

    store.install(snapshot, &authority("installer")?).await?;
    store
        .evaluate(
            &release,
            1,
            digest('c'),
            digest('d'),
            true,
            &authority("evaluator")?,
        )
        .await?;
    store
        .approve(&release, 2, digest('e'), &authority("approver")?)
        .await?;
    let activation = store
        .activate(verified.clone(), 0, &authority("operator")?)
        .await?;
    let binding = store
        .bind(verified, activation.revision, &authority("executor")?)
        .await?;
    let engine_binding = binding.to_execution_binding()?;

    println!(
        "bound {}:{}@{} for tenant {} at activation revision {}",
        engine_binding.issuer(),
        engine_binding.name(),
        engine_binding.version(),
        engine_binding.tenant_id().unwrap_or("unscoped"),
        engine_binding.revision()
    );
    Ok(())
}

fn authority(subject: &str) -> Result<AuthorityContext, Box<dyn Error>> {
    Ok(AuthorityContext::new(
        actor(subject),
        Some("tenant-a".to_owned()),
    )?)
}

fn grant(subject: &str, role: DomainPackRole) -> Result<DomainPackRoleGrant, Box<dyn Error>> {
    Ok(DomainPackRoleGrant::new(
        actor(subject),
        Some("tenant-a".to_owned()),
        BTreeSet::from([role]),
    )?)
}

fn actor(subject: &str) -> ActorIdentity {
    ActorIdentity::Authenticated {
        authority: "example".to_owned(),
        subject: subject.to_owned(),
    }
}

fn pin(
    kind: DomainPackComponentKind,
    name: &str,
    version: &str,
    digest_character: char,
) -> DomainPackComponentPin {
    DomainPackComponentPin {
        kind,
        name: name.to_owned(),
        version: version.to_owned(),
        content_sha256: digest(digest_character),
    }
}

fn digest(character: char) -> String {
    std::iter::repeat_n(character, 64).collect()
}
