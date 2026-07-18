mod agent;
mod command;
mod engine;
mod error;
mod model;
mod report;
mod scheduler;
mod script;
mod store;

pub use agent::{ServitorTransport, Transport};
pub use engine::{Engine, Inspection};
pub use error::WorkflowError;
pub use model::{PublicRun, RunState, RunStatus};
pub use store::WorkflowStore;

use std::sync::Arc;

pub fn default_engine() -> Engine {
    Engine::new(
        WorkflowStore::from_environment(),
        Arc::new(ServitorTransport::from_environment()),
    )
}
