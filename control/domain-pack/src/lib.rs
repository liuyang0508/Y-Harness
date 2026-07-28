//! Optional Domain Pack control plane above the Y-Harness semantic Core.
//!
//! A Domain Pack is immutable deployment metadata. It pins the Workflow,
//! Skill, Tool, Policy, Evaluation, and Schema components used to specialize a
//! general Harness. This crate governs promotion and activation; it does not
//! add domain behavior to the Agent Loop.

#![warn(missing_docs)]

mod model;
mod store;

pub use model::{
    DOMAIN_PACK_FORMAT_VERSION, DomainPackComponentKind, DomainPackComponentPin,
    DomainPackInventory, DomainPackReleaseId, DomainPackSnapshot, VerifiedDomainPack,
};
pub use store::{
    DOMAIN_PACK_STORE_SCHEMA_VERSION, DomainPackActivation, DomainPackApproval, DomainPackError,
    DomainPackEvaluation, DomainPackExecutionBinding, DomainPackFuture, DomainPackRelease,
    DomainPackReleaseStage, DomainPackStore, MemoryDomainPackStore, SqliteDomainPackStore,
};
