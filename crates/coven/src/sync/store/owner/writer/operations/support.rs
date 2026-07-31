use super::*;

pub(crate) fn blocked_status(error: &StoreError) -> Option<crate::WriteBlock> {
    match error {
        StoreError::Database(_)
        | StoreError::BlobStorage { .. }
        // Nothing was persisted and the caller re-runs the operation, so this
        // blocks no writer.
        | StoreError::ActivationConflict
        | StoreError::CandidateCleanup(_) => None,
        StoreError::MergeAnnouncementOccupied { .. } => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: error.to_string(),
            })
        }
        StoreError::SequenceExhausted { .. } => Some(crate::WriteBlock::InvalidProtocolState {
            reason: error.to_string(),
        }),
        StoreError::AuthorExcluded { .. } => Some(crate::WriteBlock::InvalidProtocolState {
            reason: error.to_string(),
        }),
        StoreError::CirclePublicationBlocked(
            crate::protocol::circle::CirclePublicationBlocked::RotationRequired {
                circle_id,
                removed_members,
            },
        ) => Some(crate::WriteBlock::RotationRequired {
            circle_id: *circle_id,
            removed_members: removed_members.clone(),
        }),
        StoreError::Object(StoreObjectError::Storage(_)) => None,
        StoreError::MissingBlob { namespace, id } => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreError::LocalUserBlob { namespace, id } => Some(crate::WriteBlock::LocalUserBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreError::MissingState { key } => Some(crate::WriteBlock::InvalidProtocolState {
            reason: format!("Store protocol state {key:?} is absent"),
        }),
        StoreError::InvalidState { key, reason } => Some(crate::WriteBlock::InvalidProtocolState {
            reason: format!("Store protocol state {key:?} is invalid: {reason}"),
        }),
        StoreError::InvalidOutbound(_) | StoreError::Object(_) => {
            Some(crate::WriteBlock::InvalidPackage {
                reason: error.to_string(),
            })
        }
        StoreError::Preparation(StorePreparationError::LocalUserBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::LocalUserBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreError::Preparation(StorePreparationError::MissingPreparedBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreError::Preparation(StorePreparationError::Gate(_))
        | StoreError::Preparation(StorePreparationError::AssetScan(_))
        | StoreError::Preparation(StorePreparationError::Database(_)) => {
            Some(crate::WriteBlock::InvalidPackage {
                reason: error.to_string(),
            })
        }
        StoreError::Preparation(StorePreparationError::AssetUpload(_))
        | StoreError::Preparation(StorePreparationError::Storage { .. }) => None,
    }
}
