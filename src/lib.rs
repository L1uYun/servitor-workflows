mod agent;
mod command;
mod engine;
mod error;
mod json_extract;
mod model;
mod run_summary;
mod scheduler;
mod script;
mod store;

pub use agent::{ServitorTransport, Transport};
pub use engine::{Engine, Inspection};
pub use error::{ErrorPayload, WorkflowError};
pub use model::{PublicRun, RunState, RunStatus};
pub use store::WorkflowStore;

pub fn default_engine() -> Engine {
    Engine::new(
        WorkflowStore::from_environment(),
        std::sync::Arc::new(ServitorTransport::from_environment()),
    )
}
