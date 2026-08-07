use super::{AuthorizedWriterOperation, Store};
use crate::sync::store::acknowledgements::StoreAckError;
use coven_protocol::circle_activation::CircleAuthoringState;
#[cfg(test)]
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::circle_journal::{
    CircleOperationIntent, CircleOperationJournal, CircleOperationPolicy, CircleOperationProgress,
    CircleTransitionHistory, PreparedCircleOperation,
};
pub use error::CircleOperationError;
use exact_object::read_exact_circle_object;

pub(super) mod activation;
mod authorized_writer;
pub(super) mod bootstrap_blobs;
mod commands;
mod error;
mod exact_object;
mod history;
pub(super) mod packages;
mod preparation;
mod publication;

pub(super) use authorized_writer::AuthorizedCircleWriter;
pub use commands::StoreCircleCommands;
pub(crate) use history::VerifiedCircleHistory;
pub use packages::CirclePackageReadError;

#[cfg(test)]
mod tests;
