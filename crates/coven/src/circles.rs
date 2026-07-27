//! The `coven.circles()` application surface: create, lifecycle, inspection, and
//! typed public errors. A [`Circles`] is a borrowed namespace over a
//! [`CovenHandle`](crate::CovenHandle) with no state of its own; every method
//! delegates to the sync manager and maps internal refusals to [`CircleError`].

use coven_core::{
    Circle, CircleCloseStatus, CircleControlCoord, CircleEpochCloseId, CircleId, CircleMemberInfo,
    CircleOperationBlock, CircleOperationId, CircleOperationInfo, CircleRole, StoreDeviceId,
};

use crate::handle::CovenHandle;
use crate::sync::store::CircleOperationError;
use crate::sync::sync_manager::SyncError;

/// Why a Circle command or query failed. Maps the internal typed refusals 1:1 with
/// stable identifiers and carries the ids a caller needs to display or retry.
/// Write-path outcomes (a durable write's local/published/blocked/conflicted
/// status) are not here — those stay on
/// [`WriteStatus`](crate::WriteStatus)/[`WriteBlock`](crate::WriteBlock).
#[derive(Debug, thiserror::Error)]
pub enum CircleError {
    /// No sync provider is configured, so there is no Store to command.
    #[error("sync is not configured")]
    NotConfigured,
    /// The sync loop is not running, so a Circle write cannot be dispatched.
    #[error("the sync loop is not running")]
    LoopNotRunning,
    /// Circles require opaque object storage; a browsable provider cannot hold
    /// them.
    #[error("circles require opaque (non-browsable) cloud storage")]
    BrowsableStorage,
    /// The Circle's resolved roster names Store identities that are no longer
    /// active Store members. New content is refused until an Owner closes the
    /// epoch and activates a successor roster without them.
    #[error("circle {circle_id} requires rotation: its roster names removed Store members {removed_members:?}")]
    RotationRequired {
        circle_id: CircleId,
        removed_members: Vec<String>,
    },
    /// The Circle's control history has forked and awaits Owner resolution.
    #[error("circle {circle_id} has an unresolved control conflict")]
    Conflicted { circle_id: CircleId },
    /// The Circle's control history terminated in an Owner-signed deletion.
    #[error("circle {circle_id} is deleted")]
    Deleted { circle_id: CircleId },
    /// Resolution was requested for a Circle that holds no retained control
    /// conflict.
    #[error("circle {circle_id} has no retained control conflict to resolve")]
    NotConflicted { circle_id: CircleId },
    /// The resolution's chosen branch is not among the Circle's retained
    /// conflicting branches.
    #[error("circle {circle_id} control conflict does not retain the chosen branch")]
    ChosenBranchNotRetained { circle_id: CircleId },
    /// Cancellation was requested for a Circle with no in-flight epoch close.
    #[error("circle {circle_id} has no in-flight epoch close to cancel")]
    NoCloseToCancel { circle_id: CircleId },
    /// Device exclusion was requested for a Circle with no in-flight epoch close.
    #[error("circle {circle_id} has no in-flight epoch close for device exclusion")]
    NoCloseToExclude { circle_id: CircleId },
    /// The named device is not a participant in the Circle's in-flight epoch
    /// close.
    #[error("device {device_id} is not a participant in circle {circle_id}'s epoch close")]
    DeviceNotACloseParticipant {
        circle_id: CircleId,
        device_id: StoreDeviceId,
    },
    /// The chosen control branch starts an epoch close. Resolve the conflict to
    /// an active branch before starting a close.
    #[error("circle {circle_id} control conflict must resolve to an active branch")]
    ResolveToClosingBranch { circle_id: CircleId },
    /// This device was excluded from an epoch close and must reset from a
    /// successor bootstrap before it can continue.
    #[error("device was excluded from circle {circle_id} close {close_id} and must reset")]
    ExcludedDeviceMustReset {
        circle_id: CircleId,
        close_id: CircleEpochCloseId,
    },
    /// Retry was requested for a durable operation that is not blocked.
    #[error("circle operation {operation_id} is not blocked")]
    NotBlocked { operation_id: CircleOperationId },
    /// Discard was requested without proof the candidate can never activate. The
    /// operation stays durable; it never assumes an unseen candidate failed to
    /// activate.
    #[error("circle operation {operation_id} discard requires verified permanent nonactivation")]
    DiscardRequiresNonactivation { operation_id: CircleOperationId },
    /// A durable operation cannot publish because its author lost signed
    /// authority; the initiator may retry it once authority is restored.
    #[error("circle operation for {circle_id} is blocked: {block}")]
    Blocked {
        circle_id: CircleId,
        block: CircleOperationBlock,
    },
    /// The local signing identity is not established.
    #[error("the local identity is not established: {0}")]
    Identity(String),
    /// An internal protocol or database failure with no distinct public category.
    #[error("circle protocol error: {0}")]
    Protocol(String),
}

impl From<CircleOperationError> for CircleError {
    fn from(error: CircleOperationError) -> Self {
        match error {
            CircleOperationError::BrowsableStorage => Self::BrowsableStorage,
            CircleOperationError::RotationRequired {
                circle_id,
                removed_members,
            } => Self::RotationRequired {
                circle_id,
                removed_members,
            },
            CircleOperationError::Conflicted { circle_id } => Self::Conflicted { circle_id },
            CircleOperationError::Deleted { circle_id } => Self::Deleted { circle_id },
            CircleOperationError::NotConflicted { circle_id } => Self::NotConflicted { circle_id },
            CircleOperationError::ChosenBranchNotRetained { circle_id } => {
                Self::ChosenBranchNotRetained { circle_id }
            }
            CircleOperationError::NoCloseToCancel { circle_id } => {
                Self::NoCloseToCancel { circle_id }
            }
            CircleOperationError::NoCloseToExclude { circle_id } => {
                Self::NoCloseToExclude { circle_id }
            }
            CircleOperationError::DeviceNotACloseParticipant {
                circle_id,
                device_id,
            } => Self::DeviceNotACloseParticipant {
                circle_id,
                device_id,
            },
            CircleOperationError::ResolveToClosingBranch { circle_id } => {
                Self::ResolveToClosingBranch { circle_id }
            }
            CircleOperationError::ExcludedDeviceMustReset {
                circle_id,
                close_id,
            } => Self::ExcludedDeviceMustReset {
                circle_id,
                close_id,
            },
            CircleOperationError::NotBlocked { operation_id } => Self::NotBlocked { operation_id },
            CircleOperationError::DiscardRequiresNonactivation { operation_id } => {
                Self::DiscardRequiresNonactivation { operation_id }
            }
            CircleOperationError::Blocked { circle_id, block } => {
                Self::Blocked { circle_id, block }
            }
            CircleOperationError::CommandChannelClosed
            | CircleOperationError::ReplyChannelClosed => Self::LoopNotRunning,
            // Internal protocol and database failures carry no distinct public
            // category; surface their message under the catch-all.
            other => Self::Protocol(other.to_string()),
        }
    }
}

impl From<SyncError> for CircleError {
    fn from(error: SyncError) -> Self {
        match error {
            SyncError::NotConfigured => Self::NotConfigured,
            SyncError::LoopNotRunning => Self::LoopNotRunning,
            SyncError::Circle(error) => error.into(),
            SyncError::Key(error) => Self::Identity(error.to_string()),
            other => Self::Protocol(other.to_string()),
        }
    }
}

/// The `coven.circles()` namespace. Borrowed from a [`CovenHandle`]; holds no
/// state of its own.
pub struct Circles<'a> {
    handle: &'a CovenHandle,
}

impl<'a> Circles<'a> {
    pub(crate) fn new(handle: &'a CovenHandle) -> Self {
        Self { handle }
    }

    fn manager(
        &self,
    ) -> Result<std::sync::Arc<crate::sync::sync_manager::SyncManager>, CircleError> {
        self.handle.sync_manager().ok_or(CircleError::NotConfigured)
    }

    /// Create and activate a Circle whose founder is this Store identity. Returns
    /// only after the signed roster, metadata, access set, control, Store commit,
    /// activation head, and local materialization are durable.
    pub async fn create(&self, name: &str) -> Result<CircleId, CircleError> {
        self.manager()?.create_circle(name).await
    }

    /// Rename a Circle without changing its epoch key, membership, rows, or
    /// package history.
    pub async fn rename(&self, circle_id: CircleId, name: &str) -> Result<(), CircleError> {
        self.manager()?.rename_circle(circle_id, name).await
    }

    /// Add (or re-add) a Store identity to the Circle's roster, sealing it a fresh
    /// active access leaf and current bootstrap.
    pub async fn add_member(
        &self,
        circle_id: CircleId,
        member_pubkey: &str,
    ) -> Result<(), CircleError> {
        self.manager()?
            .add_circle_member(circle_id, member_pubkey.to_string(), CircleRole::Member)
            .await
    }

    /// Remove a Store identity from the Circle, closing the old epoch. Returns the
    /// durable operation id tracking the close.
    pub async fn remove_member(
        &self,
        circle_id: CircleId,
        member_pubkey: &str,
    ) -> Result<CircleOperationId, CircleError> {
        self.manager()?
            .remove_circle_member(circle_id, member_pubkey.to_string())
            .await
    }

    /// Resolve a forked Circle control by authoring a successor of the chosen
    /// branch. Callable regardless of rotation state — it is the exit path out of
    /// the conflict.
    pub async fn resolve(
        &self,
        circle_id: CircleId,
        chosen: CircleControlCoord,
    ) -> Result<(), CircleError> {
        self.manager()?
            .resolve_circle_control(circle_id, chosen)
            .await
    }

    /// Cancel an in-flight epoch close, restoring the frozen epoch. Returns the
    /// durable operation id the cancellation settles.
    pub async fn cancel_close(
        &self,
        circle_id: CircleId,
    ) -> Result<CircleOperationId, CircleError> {
        self.manager()?.cancel_circle_epoch_close(circle_id).await
    }

    /// Exclude an unavailable participant device from the Circle's in-flight epoch
    /// close. The excluded device must reset from the successor bootstrap before it
    /// can write or acknowledge again.
    pub async fn exclude_close_device(
        &self,
        circle_id: CircleId,
        device_id: StoreDeviceId,
    ) -> Result<(), CircleError> {
        self.manager()?
            .exclude_circle_close_device(circle_id, device_id)
            .await
    }

    /// Delete a Circle with an Owner-signed terminal control transition.
    pub async fn delete(&self, circle_id: CircleId) -> Result<(), CircleError> {
        self.manager()?.delete_circle(circle_id).await
    }

    /// Retry a blocked durable operation from its captured phase, idempotently.
    pub async fn retry_operation(
        &self,
        operation_id: CircleOperationId,
    ) -> Result<(), CircleError> {
        self.manager()?.retry_circle_operation(operation_id).await
    }

    /// Discard a durable operation that can provably never activate, deleting its
    /// candidate-exclusive objects and clearing its journal row. Refused typed
    /// without a verified permanent-nonactivation proof.
    pub async fn discard_operation(
        &self,
        operation_id: CircleOperationId,
    ) -> Result<(), CircleError> {
        self.manager()?.discard_circle_operation(operation_id).await
    }

    /// Every Circle the local identity can see, with its derived
    /// [`CircleState`](crate::CircleState).
    pub async fn list(&self) -> Result<Vec<Circle>, CircleError> {
        self.manager()?.list_circles().await
    }

    /// The Circle's current members who remain current Store members, with roles.
    pub async fn members(&self, circle_id: CircleId) -> Result<Vec<CircleMemberInfo>, CircleError> {
        self.manager()?.circle_members(circle_id).await
    }

    /// Every durable Circle operation that has not activated, with its typed
    /// progress and block reason.
    pub async fn operations(&self) -> Result<Vec<CircleOperationInfo>, CircleError> {
        self.manager()?.circle_operations().await
    }

    /// The read-only settlement status of a Circle's in-flight epoch close.
    pub async fn close_status(
        &self,
        circle_id: CircleId,
    ) -> Result<CircleCloseStatus, CircleError> {
        self.manager()?.circle_close_status(circle_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn circle_id(byte: u8) -> CircleId {
        CircleId::from_bytes([byte; 16])
    }

    fn device_id(byte: u8) -> StoreDeviceId {
        format!("{byte:02x}")
            .repeat(32)
            .parse()
            .expect("a 64-character hexadecimal device id")
    }

    fn close_id(byte: u8) -> CircleEpochCloseId {
        serde_json::from_str(&format!("\"{}\"", format!("{byte:02x}").repeat(32)))
            .expect("a 64-character hexadecimal close id")
    }

    fn authority_lost_block() -> CircleOperationBlock {
        serde_json::from_str(&format!(
            r#"{{"authority_lost":{{"grant_id":"{}"}}}}"#,
            "cd".repeat(32)
        ))
        .expect("an authority-lost block with a 64-character hexadecimal grant id")
    }

    /// Each internal refusal maps to its public variant, carrying the identifiers a
    /// caller needs. Covers the named refusal set: rename-on-deleted,
    /// resolve-on-nonconflicted, cancel-without-close, exclude-non-participant, and
    /// delete-on-conflicted, plus the browsable-storage and rotation refusals.
    #[test]
    fn internal_refusals_map_to_public_variants() {
        let circle = circle_id(1);

        let deleted: CircleError = CircleOperationError::Deleted { circle_id: circle }.into();
        assert!(matches!(deleted, CircleError::Deleted { circle_id } if circle_id == circle));

        let not_conflicted: CircleError =
            CircleOperationError::NotConflicted { circle_id: circle }.into();
        assert!(
            matches!(not_conflicted, CircleError::NotConflicted { circle_id } if circle_id == circle)
        );

        let no_close: CircleError =
            CircleOperationError::NoCloseToCancel { circle_id: circle }.into();
        assert!(
            matches!(no_close, CircleError::NoCloseToCancel { circle_id } if circle_id == circle)
        );

        let device = device_id(7);
        let not_participant: CircleError = CircleOperationError::DeviceNotACloseParticipant {
            circle_id: circle,
            device_id: device,
        }
        .into();
        assert!(matches!(
            not_participant,
            CircleError::DeviceNotACloseParticipant { circle_id, device_id }
                if circle_id == circle && device_id == device
        ));

        let conflicted: CircleError = CircleOperationError::Conflicted { circle_id: circle }.into();
        assert!(matches!(conflicted, CircleError::Conflicted { circle_id } if circle_id == circle));

        let browsable: CircleError = CircleOperationError::BrowsableStorage.into();
        assert!(matches!(browsable, CircleError::BrowsableStorage));

        let rotation: CircleError = CircleOperationError::RotationRequired {
            circle_id: circle,
            removed_members: vec!["pk".to_string()],
        }
        .into();
        assert!(matches!(
            rotation,
            CircleError::RotationRequired { circle_id, removed_members }
                if circle_id == circle && removed_members == vec!["pk".to_string()]
        ));

        let no_exclude: CircleError =
            CircleOperationError::NoCloseToExclude { circle_id: circle }.into();
        assert!(
            matches!(no_exclude, CircleError::NoCloseToExclude { circle_id } if circle_id == circle)
        );

        let chosen: CircleError =
            CircleOperationError::ChosenBranchNotRetained { circle_id: circle }.into();
        assert!(
            matches!(chosen, CircleError::ChosenBranchNotRetained { circle_id } if circle_id == circle)
        );

        let operation_id = CircleOperationId::placeholder("discard-map-seed");
        let not_blocked: CircleError = CircleOperationError::NotBlocked {
            operation_id: operation_id.clone(),
        }
        .into();
        assert!(matches!(
            not_blocked,
            CircleError::NotBlocked {
                operation_id: mapped
            } if mapped == operation_id
        ));

        let discard: CircleError = CircleOperationError::DiscardRequiresNonactivation {
            operation_id: operation_id.clone(),
        }
        .into();
        assert!(matches!(
            discard,
            CircleError::DiscardRequiresNonactivation { operation_id: mapped }
                if mapped == operation_id
        ));

        let block = authority_lost_block();
        let blocked: CircleError = CircleOperationError::Blocked {
            circle_id: circle,
            block: block.clone(),
        }
        .into();
        assert!(matches!(
            blocked,
            CircleError::Blocked {
                circle_id,
                block: mapped
            } if circle_id == circle && mapped == block
        ));

        let close_id = close_id(0xab);
        let excluded_reset: CircleError = CircleOperationError::ExcludedDeviceMustReset {
            circle_id: circle,
            close_id,
        }
        .into();
        assert!(matches!(
            excluded_reset,
            CircleError::ExcludedDeviceMustReset {
                circle_id,
                close_id: mapped
            } if circle_id == circle && mapped == close_id
        ));

        let closing_resolution: CircleError =
            CircleOperationError::ResolveToClosingBranch { circle_id: circle }.into();
        assert!(matches!(
            closing_resolution,
            CircleError::ResolveToClosingBranch { circle_id } if circle_id == circle
        ));

        // The channel-closed plumbing variants collapse to LoopNotRunning; other
        // internal failures collapse to the Protocol catch-all.
        let closed: CircleError = CircleOperationError::CommandChannelClosed.into();
        assert!(matches!(closed, CircleError::LoopNotRunning));
        let internal: CircleError = CircleOperationError::InvalidState("bad".to_string()).into();
        assert!(matches!(internal, CircleError::Protocol(_)));
    }

    /// No public Circle error's `Display` names a removed coordinated-protocol
    /// shape. The vocabulary of the deleted protocol must never surface to a host.
    #[test]
    fn no_public_error_display_names_removed_protocol_vocabulary() {
        let circle = circle_id(2);
        let close_id = close_id(0xab);
        let displays = [
            CircleError::NotConfigured.to_string(),
            CircleError::LoopNotRunning.to_string(),
            CircleError::BrowsableStorage.to_string(),
            CircleError::RotationRequired {
                circle_id: circle,
                removed_members: vec!["pk".to_string()],
            }
            .to_string(),
            CircleError::Conflicted { circle_id: circle }.to_string(),
            CircleError::Deleted { circle_id: circle }.to_string(),
            CircleError::NotConflicted { circle_id: circle }.to_string(),
            CircleError::ChosenBranchNotRetained { circle_id: circle }.to_string(),
            CircleError::NoCloseToCancel { circle_id: circle }.to_string(),
            CircleError::NoCloseToExclude { circle_id: circle }.to_string(),
            CircleError::DeviceNotACloseParticipant {
                circle_id: circle,
                device_id: device_id(3),
            }
            .to_string(),
            CircleError::ResolveToClosingBranch { circle_id: circle }.to_string(),
            CircleError::ExcludedDeviceMustReset {
                circle_id: circle,
                close_id,
            }
            .to_string(),
            CircleError::NotBlocked {
                operation_id: CircleOperationId::placeholder("not-blocked-display"),
            }
            .to_string(),
            CircleError::DiscardRequiresNonactivation {
                operation_id: CircleOperationId::placeholder("discard-display"),
            }
            .to_string(),
            CircleError::Blocked {
                circle_id: circle,
                block: authority_lost_block(),
            }
            .to_string(),
            CircleError::Identity("locked".to_string()).to_string(),
            CircleError::Protocol("state invalid".to_string()).to_string(),
        ];
        for display in displays {
            let lowered = display.to_lowercase();
            for forbidden in ["serial", "policy", "engine", "coordination"] {
                assert!(
                    !lowered.contains(forbidden),
                    "public Circle error names removed protocol vocabulary {forbidden:?}: {display}"
                );
            }
        }
    }
}
