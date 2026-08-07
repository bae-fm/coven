use crate::sync::store::{StoreError, StoreRegistrationError};
use coven_protocol::circle::{CircleId, CircleTransitionError};
use coven_protocol::objects::StoreObjectError;

#[derive(Debug, thiserror::Error)]
pub enum CircleOperationError {
    #[error("database: {0}")]
    Database(coven_database::DbError),
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
        block: coven_protocol::circle::CircleOperationBlock,
    },
    #[error("circle {circle_id} requires rotation: its roster names removed Store members {removed_members:?}")]
    RotationRequired {
        circle_id: CircleId,
        removed_members: Vec<String>,
    },
    #[error("device excluded from circle {circle_id} close {close_id} must reset from the successor bootstrap before publishing")]
    ExcludedDeviceMustReset {
        circle_id: CircleId,
        close_id: coven_protocol::circle::CircleEpochCloseId,
    },
    #[error("circle {circle_id} has no retained control conflict to resolve")]
    NotConflicted { circle_id: CircleId },
    #[error("circle {circle_id} control conflict does not retain the chosen branch")]
    ChosenBranchNotRetained { circle_id: CircleId },
    #[error("circle {circle_id} control conflict cannot be resolved to its closing branch: an epoch close binds its participant responses to the closing control, so a resolution successor under a new control coordinate would strand them; resolve to a non-closing branch to discard the close instead")]
    ResolveToClosingBranch { circle_id: CircleId },
    #[error("circle {circle_id} has no local epoch close waiting for cancellation")]
    NoCloseToCancel { circle_id: CircleId },
    #[error("circle {circle_id} has no local epoch close waiting for device exclusion")]
    NoCloseToExclude { circle_id: CircleId },
    #[error("device {device_id} is not a participant in circle {circle_id}'s epoch close")]
    DeviceNotACloseParticipant {
        circle_id: CircleId,
        device_id: coven_protocol::store_commit::StoreDeviceId,
    },
    #[error("circle operation {operation_id} is not blocked")]
    NotBlocked {
        operation_id: coven_protocol::circle::CircleOperationId,
    },
    #[error("circle operation {operation_id} discard requires verified permanent nonactivation; it never assumes an unseen candidate failed to activate")]
    DiscardRequiresNonactivation {
        operation_id: coven_protocol::circle::CircleOperationId,
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

impl From<coven_database::DbError> for CircleOperationError {
    fn from(error: coven_database::DbError) -> Self {
        match error {
            coven_database::DbError::ExcludedDeviceMustReset {
                circle_id,
                close_id,
            } => Self::ExcludedDeviceMustReset {
                circle_id,
                close_id,
            },
            other => Self::Database(other),
        }
    }
}

impl From<coven_protocol::circle_journal::CircleJournalError> for CircleOperationError {
    fn from(error: coven_protocol::circle_journal::CircleJournalError) -> Self {
        CircleOperationError::Journal(error.to_string())
    }
}

impl From<coven_protocol::circle_activation::CircleStateError> for CircleOperationError {
    fn from(error: coven_protocol::circle_activation::CircleStateError) -> Self {
        CircleOperationError::InvalidState(error.to_string())
    }
}
