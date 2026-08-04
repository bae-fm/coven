//! Closed Circle activation values and exact Circle object verification.

mod context;
mod state;

pub(crate) use context::{read_exact_circle_object, verify_control_context_for_verified_commit};
#[cfg(test)]
pub(crate) use state::CircleCurrentControl;
pub(crate) use state::{
    CircleAuthoringState, CircleCurrentState, CircleEpochAccess, LocalCircleExclusion,
    VerifiedCircleAccess, VerifiedCircleActivations, VerifiedCircleActive, VerifiedCircleImage,
    VerifiedCircleReference, VerifiedStreamActivationPrefix, VerifiedStreamActivations,
};
