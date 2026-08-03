//! Aquaculture specialization above the provider-neutral Y-Harness Core.
//!
//! This crate owns domain intent routing, trusted scope resolution, structured
//! context, connector contracts, output verification, and evaluation fixtures.
//! It deliberately does not change the Y-Harness Agent Loop.

pub mod bootstrap;
pub mod contracts;
pub mod evaluation;
pub mod evidence;
pub mod journey;
pub mod pack;
pub mod tools;
pub mod verification;

pub use bootstrap::register_poc_capabilities;
pub use contracts::{
    AgentRequest, ContextPackage, ContextPackageBuilder, DataOrigin, InteractionContext,
    PondResolution, ResolvedPondScope, TimeWindow,
};
pub use evaluation::poc_evaluation_suite;
pub use evidence::{EvidenceAssessment, EvidenceKind, EvidenceScore};
pub use journey::{JourneyId, JourneyResolution, JourneyRouter, JourneySpec, journey_registry};
pub use pack::{AQUACULTURE_PACK_VERSION, build_domain_pack_snapshot};
pub use tools::{MockErpQueryTool, MockIotQueryTool};
pub use verification::{AquacultureAnswerEnvelope, AquacultureOutputVerifier};
