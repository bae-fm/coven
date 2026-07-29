//! Durable creation and activation of circles through the Store commit stream.

pub(crate) mod activation;
mod error;
mod journal;

pub(crate) use activation::{
    read_exact_circle_object, verify_control_context_for_verified_commit, CircleAuthoringState,
    CircleCurrentState, CirclePackageAccess, LocalCircleExclusion, VerifiedCircleAccess,
    VerifiedCircleActivations, VerifiedCircleActive, VerifiedCircleImage, VerifiedCircleReference,
    VerifiedStreamActivations,
};
pub use error::CircleOperationError;
pub(crate) use journal::{
    CircleOperationIntent, CircleOperationJournal, CircleOperationPolicy, CircleOperationProgress,
    CircleTransitionHistory, PreparedCircleOperation,
};
