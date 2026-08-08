use super::{AuthorizedWriterOperation, Store};
use crate::sync::store::acknowledgements::StoreAckError;
use coven_protocol::circle_activation::CircleAuthoringState;
#[cfg(test)]
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::circle_journal::{
    CircleOperationIntent, CircleOperationJournal, CircleOperationPolicy, CircleTransitionHistory,
    PreparedCircleOperation,
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
pub(crate) use preparation::PreparedCircleJournal;

#[cfg(test)]
mod tests;

/// Delete the payload behind every spool cleanup obligation this store has
/// committed, and clear each obligation with its file.
///
/// An operation's objects are enqueued for deletion inside the transaction that
/// drops the row naming them, so the obligation is durable the moment that
/// transaction commits. This is the other half: the flow that committed it
/// discharges it immediately, and calls this again when it resumes, so a crash
/// in between is finished by the flow that owns the work rather than left for a
/// sweeper.
pub(super) async fn drain_payload_spool(
    database: &coven_database::StoreDatabase,
    store_dir: &coven_foundation::store_dir::StoreDir,
) -> Result<(), CircleOperationError> {
    coven_database::payload_spool::PayloadSpool::new(store_dir)
        .drain_cleanup(database)
        .await
        .map_err(|error| {
            CircleOperationError::InvalidState(format!("Circle payload spool cleanup: {error}"))
        })
}
