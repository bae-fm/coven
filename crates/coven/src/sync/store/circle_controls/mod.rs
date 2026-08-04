//! Durable creation and activation of circles through the Store commit stream.

pub(crate) mod activation;
mod error;

pub(crate) use crate::protocol::circle_journal::{
    CircleOperationIntent, CircleOperationJournal, CircleOperationPolicy, CircleOperationProgress,
    CircleTransitionHistory, PreparedCircleOperation,
};
pub(crate) use activation::{
    read_exact_circle_object, verify_control_context_for_verified_commit, CircleAuthoringState,
    CircleEpochAccess, VerifiedCircleAccess, VerifiedCircleActivations, VerifiedCircleActive,
    VerifiedCircleImage, VerifiedCircleReference,
};
pub(crate) use error::CircleOperationError;
