//! Durable creation and activation of circles through the Store commit stream.

pub(crate) mod activation;
mod commands;
mod error;
mod journal;
mod preparation;
mod publication;

pub(crate) use activation::{
    load_circle_activations, load_exact_slot_bytes, verify_control_context, CircleAuthoringState,
    VerifiedCircleAccess, VerifiedCircleActivations, VerifiedCircleActive, VerifiedCircleReference,
};
pub(crate) use commands::{create_circle, rename_circle, resume_circle_operations};
pub use error::CircleOperationError;
pub(crate) use journal::{
    CircleOperationIntent, CircleOperationJournal, CircleOperationPolicy, CircleOperationProgress,
    CircleTransitionHistory, PreparedCircleOperation,
};
#[cfg(test)]
use preparation::{
    prepare_circle_activation_objects, prepare_circle_object, prepare_circle_object_at,
    signed_circle_commit,
};
use preparation::{
    prepare_circle_operation, prepare_circle_operation_request, verify_prepared_objects_are_signed,
};
use publication::publish_circle_operation;

#[cfg(test)]
mod tests;
