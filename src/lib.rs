mod agent;
mod boundary;
mod budget;
mod capabilities;
mod command;
mod engine;
mod error;
mod isolation;
mod json_extract;
mod model;
mod process_tree;
mod run_summary;
mod scheduler;
mod script;
mod store;

pub use agent::{ServitorTransport, Transport};
pub use boundary::{
    BoundaryEnvelope, BoundaryEvent, BoundaryPolicy, EnvironmentPolicy, IsolationLevel,
    NetworkPolicy,
};
pub use budget::Budget;
pub use capabilities::{
    CAPABILITY_SCHEMA_VERSION, CapabilityEnvelope, CapabilityEvent, CapabilityPolicy, Effort,
    ModelChoice, ProviderCapability, RoleContract,
};
pub use engine::{Engine, Inspection};
pub use error::{ErrorPayload, WorkflowError};
pub use model::{
    BudgetEnvelope, BudgetEvent, BudgetLedger, CallKind, CallState, JournalEntry, PublicRun,
    ReconstructedState, RunState, RunStatus, WorkflowEvent, WorkflowEventEnvelope,
};
pub use store::WorkflowStore;

pub fn default_engine() -> Engine {
    Engine::new(
        WorkflowStore::from_environment(),
        std::sync::Arc::new(ServitorTransport::from_environment()),
    )
}
