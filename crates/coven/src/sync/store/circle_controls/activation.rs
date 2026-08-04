//! Closed Circle activation values and exact Circle object verification.

mod context;

pub(crate) use crate::protocol::circle_activation::verify_control_context_for_verified_commit;
#[cfg(test)]
pub(crate) use crate::protocol::circle_activation::CircleCurrentControl;
pub(crate) use crate::protocol::circle_activation::{
    CircleAuthoringState, CircleEpochAccess, LocalCircleExclusion, VerifiedCircleAccess,
    VerifiedCircleActivations, VerifiedCircleActive, VerifiedCircleImage, VerifiedCircleReference,
    VerifiedStreamActivationPrefix, VerifiedStreamActivations,
};
pub(crate) use context::read_exact_circle_object;
