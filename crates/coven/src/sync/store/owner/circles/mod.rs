use super::{
    history, writer::StoreWriterAuthorizationError, AuthorizedStore, AuthorizedWriterOperation,
    Store,
};
#[cfg(test)]
use crate::sync::store::circle_controls::VerifiedCircleActivations;
use crate::sync::store::circle_controls::{
    read_exact_circle_object, CircleAuthoringState, CircleOperationError, CircleOperationIntent,
    CircleOperationJournal, CircleOperationPolicy, CircleOperationProgress,
    CircleTransitionHistory, PreparedCircleOperation,
};

mod acknowledgements;
pub(super) mod activation;
mod close_responses;
mod commands;
mod packages;
mod preparation;
mod publication;

pub(super) use activation::VerifiedCircleHistory;
pub(crate) use packages::CirclePackageReadError;

pub(crate) struct StoreCircleCommands<'store> {
    store: &'store Store,
}

impl<'store> StoreCircleCommands<'store> {
    pub(super) fn new(store: &'store Store) -> Self {
        Self { store }
    }
}

pub(crate) struct AuthorizedCircleWriter<'writer, 'storage> {
    writer: &'writer mut AuthorizedWriterOperation<'storage>,
}

impl<'writer, 'storage> AuthorizedCircleWriter<'writer, 'storage> {
    pub(super) fn new(writer: &'writer mut AuthorizedWriterOperation<'storage>) -> Self {
        Self { writer }
    }

    fn publisher(&mut self) -> publication::CircleCandidatePublisher<'_, 'storage> {
        publication::CircleCandidatePublisher::new(&mut self.writer.store)
    }

    fn preparer(&mut self) -> preparation::CircleCandidatePreparer<'_, 'storage> {
        preparation::CircleCandidatePreparer::new(self.writer)
    }

    pub(crate) fn close(&mut self) -> close_responses::CircleCloseCoordinator<'_, 'storage> {
        close_responses::CircleCloseCoordinator::new(self.writer)
    }

    pub(crate) fn acknowledgements(
        &mut self,
    ) -> acknowledgements::CircleAcknowledgementWriter<'_, 'storage> {
        acknowledgements::CircleAcknowledgementWriter::new(self.writer)
    }
}

#[cfg(test)]
use commands::*;
#[cfg(test)]
use preparation::{
    prepare_circle_activation_objects, prepare_circle_object, prepare_circle_object_at,
    signed_circle_commit,
};
#[cfg(test)]
mod tests;
