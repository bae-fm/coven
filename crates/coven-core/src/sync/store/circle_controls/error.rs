use crate::sync::circle::{CircleId, CircleTransitionError};
use crate::sync::store::{StoreError, StoreRegistrationError};
use crate::sync::store_objects::StoreObjectError;

#[derive(Debug, thiserror::Error)]
pub enum CircleOperationError {
    #[error("database: {0}")]
    Database(String),
    #[error("circle protocol state is absent: {0}")]
    MissingState(&'static str),
    #[error("circle protocol state is invalid: {0}")]
    InvalidState(String),
    #[error("circle construction: {0}")]
    Construction(#[from] CircleTransitionError),
    #[error("circle object: {0}")]
    Object(#[from] StoreObjectError),
    #[error("Store publication: {0}")]
    StoreOutbound(#[from] StoreError),
    #[error("Store device registration: {0}")]
    StoreRegistration(#[from] StoreRegistrationError),
    #[error("circles require opaque cloud storage")]
    BrowsableStorage,
    #[error("circle operation journal: {0}")]
    Journal(String),
    #[error("circle operation {circle_id} is blocked: {block}")]
    Blocked {
        circle_id: CircleId,
        block: crate::sync::circle::CircleOperationBlock,
    },
    #[error("circle {circle_id} requires rotation: its roster names removed Store members {removed_members:?}")]
    RotationRequired {
        circle_id: CircleId,
        removed_members: Vec<String>,
    },
    #[error("circle {circle_id} has no retained control conflict to resolve")]
    NotConflicted { circle_id: CircleId },
    #[error("circle {circle_id} control conflict does not retain the chosen branch")]
    ChosenBranchNotRetained { circle_id: CircleId },
    #[error("circle {circle_id} has no local epoch close waiting for cancellation")]
    NoCloseToCancel { circle_id: CircleId },
    #[error("circle {circle_id} has no local epoch close waiting for device exclusion")]
    NoCloseToExclude { circle_id: CircleId },
    #[error("device {device_id} is not a participant in circle {circle_id}'s epoch close")]
    DeviceNotACloseParticipant {
        circle_id: CircleId,
        device_id: crate::sync::store_commit::StoreDeviceId,
    },
    #[error("circle operation {operation_id} is not blocked")]
    NotBlocked {
        operation_id: crate::sync::circle::CircleOperationId,
    },
    #[error("circle operation {operation_id} discard requires verified permanent nonactivation; it never assumes an unseen candidate failed to activate")]
    DiscardRequiresNonactivation {
        operation_id: crate::sync::circle::CircleOperationId,
    },
    #[error("circle {circle_id} has an unresolved control conflict")]
    Conflicted { circle_id: CircleId },
    #[error("circle {circle_id} is deleted")]
    Deleted { circle_id: CircleId },
    #[error("circle command channel is closed")]
    CommandChannelClosed,
    #[error("circle command ended without a reply")]
    ReplyChannelClosed,
}

impl From<crate::database::DbError> for CircleOperationError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}
