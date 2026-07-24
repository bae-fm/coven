use super::{
    prepare_circle_operation, prepare_circle_operation_request, publish_circle_operation,
    CircleAuthoringState, CircleOperationError, CircleOperationIntent, CircleTransitionHistory,
};
use crate::keys::{self, UserKeypair};
use crate::sync::circle::{CircleId, CirclePublicationBlocked, CircleRole, CircleRosterChain};
use crate::sync::cloud_storage::BlobPathScheme;
use crate::sync::storage::SyncStorage;
use crate::sync::store::database::StoreDatabase;
use crate::sync::store::Store;
use crate::sync::store_commit::CircleControlRef;

/// Refuse a transition that would distribute or rename the current key while a
/// Store-removed identity still holds it. Renaming or adding a member before the
/// epoch closes contradicts the close-first rotation rule; removing a member and
/// finishing an in-flight close remain the paths out.
async fn ensure_not_rotation_required(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    circle_id: CircleId,
) -> Result<(), CircleOperationError> {
    let membership = crate::sync::store::pull::load_cycle_membership(storage, database)
        .await
        .map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "load Store membership for Circle rotation check: {error}"
            ))
        })?;
    let chain = membership
        .chain
        .ok_or(CircleOperationError::MissingState("Store membership chain"))?;
    let active_store_members = chain
        .current_members()
        .into_iter()
        .map(|(pubkey, _)| pubkey)
        .collect();
    if let Some(CirclePublicationBlocked::RotationRequired {
        circle_id,
        removed_members,
    }) = database
        .circle_publication_rotation_block(circle_id, active_store_members)
        .await?
    {
        return Err(CircleOperationError::RotationRequired {
            circle_id,
            removed_members,
        });
    }
    Ok(())
}

impl Store {
    pub(crate) async fn create_circle(
        &self,
        device_id: &str,
        metadata_stamp: &str,
        name: &str,
        signer: &UserKeypair,
    ) -> Result<CircleId, CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        let storage = &**self.storage();
        crate::sync::store::ensure_active_registration(database, storage).await?;
        let journal = Box::pin(prepare_circle_operation(
            database,
            storage,
            device_id,
            metadata_stamp,
            name,
            signer,
        ))
        .await?;
        let circle_id = journal.circle_id();
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        Box::pin(publish_circle_operation(
            database,
            storage,
            &operation_id,
            signer,
            None,
        ))
        .await?;
        Ok(circle_id)
    }

    pub(crate) async fn rename_circle(
        &self,
        device_id: &str,
        metadata_stamp: &str,
        circle_id: CircleId,
        name: &str,
        signer: &UserKeypair,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        let storage = &**self.storage();
        crate::sync::store::ensure_active_registration(database, storage).await?;
        ensure_not_rotation_required(database, storage, circle_id).await?;
        let identity_pubkey = keys::public_key_hex(signer);
        let (current, activation_commit_ref) = database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let root = database
            .local_store_root_ref()
            .await?
            .ok_or(CircleOperationError::MissingState("Store root reference"))?;
        let (activation_commit, _) = crate::sync::store::pull::load_commit_with_author(
            storage,
            &root,
            &activation_commit_ref,
        )
        .await?;
        if activation_commit.candidate_family() != current.candidate_family {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current state differs from its activating Store commit"
            )));
        }
        let reference = activation_commit
            .circle_controls()
            .iter()
            .find(|reference| {
                reference.circle_id() == circle_id && reference.control() == &current.control.coord
            })
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        let journal = Box::pin(prepare_circle_operation_request(
            database,
            storage,
            device_id,
            CircleOperationRequest::Rename(Box::new(CircleRenameRequest {
                circle_id,
                name: name.to_string(),
                metadata_stamp: metadata_stamp.to_string(),
                current,
                previous_control: reference.clone(),
            })),
            signer,
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle rename changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        Box::pin(publish_circle_operation(
            database,
            storage,
            &operation_id,
            signer,
            None,
        ))
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn add_circle_member(
        &self,
        device_id: &str,
        circle_id: CircleId,
        member_pubkey: String,
        role: CircleRole,
        bootstrap: crate::sync::store::snapshot::SnapshotCut,
        routing_key: &crate::sync::circle::RowRoutingKey,
        signer: &UserKeypair,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        let storage = &**self.storage();
        crate::sync::store::ensure_active_registration(database, storage).await?;
        ensure_not_rotation_required(database, storage, circle_id).await?;
        let identity_pubkey = keys::public_key_hex(signer);
        let (current, activation_commit_ref) = database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let root = database
            .local_store_root_ref()
            .await?
            .ok_or(CircleOperationError::MissingState("Store root reference"))?;
        let (activation_commit, activation_author) =
            crate::sync::store::pull::load_commit_with_author(
                storage,
                &root,
                &activation_commit_ref,
            )
            .await?;
        if activation_commit.candidate_family() != current.candidate_family {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current state differs from its activating Store commit"
            )));
        }
        let reference = activation_commit
            .circle_controls()
            .iter()
            .find(|reference| {
                reference.circle_id() == circle_id && reference.control() == &current.control.coord
            })
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        let keyring = match &current.access.disposition {
            crate::sync::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
            crate::sync::circle::CircleAccessDisposition::Inactive => {
                return Err(CircleOperationError::InvalidState(
                    "Circle member addition requires active local access".to_string(),
                ));
            }
        };
        let roster_chain = super::activation::load_circle_control_roster_chain(
            database,
            storage,
            &root,
            &activation_commit_ref,
            &activation_commit,
            &activation_author,
            reference,
            &current.control,
            keyring,
        )
        .await?;
        let journal = Box::pin(prepare_circle_operation_request(
            database,
            storage,
            device_id,
            CircleOperationRequest::AddMember(Box::new(CircleAddMemberRequest {
                circle_id,
                member_pubkey,
                role,
                bootstrap,
                current,
                previous_control: reference.clone(),
                roster_chain,
            })),
            signer,
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle member addition changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        Box::pin(publish_circle_operation(
            database,
            storage,
            &operation_id,
            signer,
            Some(routing_key),
        ))
        .await
    }

    pub(crate) async fn remove_circle_member(
        &self,
        device_id: &str,
        circle_id: CircleId,
        member_pubkey: String,
        signer: &UserKeypair,
    ) -> Result<crate::sync::circle::CircleOperationId, CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        let storage = &**self.storage();
        crate::sync::store::ensure_active_registration(database, storage).await?;
        let identity_pubkey = keys::public_key_hex(signer);
        let (current, activation_commit_ref) = database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let root = database
            .local_store_root_ref()
            .await?
            .ok_or(CircleOperationError::MissingState("Store root reference"))?;
        let (activation_commit, activation_author) =
            crate::sync::store::pull::load_commit_with_author(
                storage,
                &root,
                &activation_commit_ref,
            )
            .await?;
        if activation_commit.candidate_family() != current.candidate_family {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current state differs from its activating Store commit"
            )));
        }
        let reference = activation_commit
            .circle_controls()
            .iter()
            .find(|reference| {
                reference.circle_id() == circle_id && reference.control() == &current.control.coord
            })
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        let keyring = match &current.access.disposition {
            crate::sync::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
            crate::sync::circle::CircleAccessDisposition::Inactive => {
                return Err(CircleOperationError::InvalidState(
                    "Circle member removal requires active local access".to_string(),
                ));
            }
        };
        let roster_chain = super::activation::load_circle_control_roster_chain(
            database,
            storage,
            &root,
            &activation_commit_ref,
            &activation_commit,
            &activation_author,
            reference,
            &current.control,
            keyring,
        )
        .await?;
        let journal = Box::pin(prepare_circle_operation_request(
            database,
            storage,
            device_id,
            CircleOperationRequest::RemoveMember(Box::new(CircleRemoveMemberRequest {
                circle_id,
                member_pubkey,
                current,
                previous_control: reference.clone(),
                roster_chain,
            })),
            signer,
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle member removal changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        database.insert_circle_operation(journal).await?;
        Box::pin(publish_circle_operation(
            database,
            storage,
            &operation_id,
            signer,
            None,
        ))
        .await?;
        Ok(operation_id)
    }

    pub(crate) async fn resume_circle_operations(
        &self,
        identity: &UserKeypair,
        routing_key: Option<&crate::sync::circle::RowRoutingKey>,
    ) -> Result<(), CircleOperationError> {
        let database = self.database();
        let storage = &**self.storage();
        while let Some(journal) = database.oldest_pending_circle_operation().await? {
            if !journal.is_publishable() {
                return Err(CircleOperationError::Journal(format!(
                    "pending circle operation {} contains a blocked payload",
                    journal.circle_id()
                )));
            }
            match Box::pin(publish_circle_operation(
                database,
                storage,
                &journal.operation_id,
                identity,
                routing_key,
            ))
            .await
            {
                Ok(()) | Err(CircleOperationError::Blocked { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

pub(super) struct CircleRenameRequest {
    pub(super) circle_id: CircleId,
    pub(super) name: String,
    pub(super) metadata_stamp: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
}

pub(super) struct CircleAddMemberRequest {
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) role: CircleRole,
    pub(super) bootstrap: crate::sync::store::snapshot::SnapshotCut,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
}

pub(super) struct CircleRemoveMemberRequest {
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
}

pub(super) struct CircleFinalizeEpochCloseRequest {
    pub(super) operation_id: crate::sync::circle::CircleOperationId,
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) metadata_stamp: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
    pub(super) intent: crate::sync::circle::CircleEpochCloseIntent,
    pub(super) responses: Vec<crate::sync::circle::CircleEpochCloseResponseRef>,
    pub(super) bootstrap: crate::sync::store::snapshot::SnapshotCut,
}

pub(super) enum CircleOperationRequest {
    Create {
        name: String,
        metadata_stamp: String,
    },
    Rename(Box<CircleRenameRequest>),
    AddMember(Box<CircleAddMemberRequest>),
    RemoveMember(Box<CircleRemoveMemberRequest>),
    FinalizeEpochClose(Box<CircleFinalizeEpochCloseRequest>),
}

impl CircleOperationRequest {
    pub(super) fn intent(&self) -> CircleOperationIntent {
        match self {
            Self::Create { name, .. } => CircleOperationIntent::Create { name: name.clone() },
            Self::Rename(request) => CircleOperationIntent::Rename {
                name: request.name.clone(),
            },
            Self::AddMember(request) => CircleOperationIntent::AddMember {
                member_pubkey: request.member_pubkey.clone(),
                role: request.role,
            },
            Self::RemoveMember(request) => CircleOperationIntent::RemoveMember {
                member_pubkey: request.member_pubkey.clone(),
            },
            Self::FinalizeEpochClose(request) => CircleOperationIntent::RemoveMember {
                member_pubkey: request.member_pubkey.clone(),
            },
        }
    }

    pub(super) fn history(&self) -> CircleTransitionHistory {
        match self {
            Self::Create { .. } => CircleTransitionHistory::Founder,
            Self::Rename(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::AddMember(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::RemoveMember(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
            Self::FinalizeEpochClose(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
        }
    }

    pub(super) fn operation_id(&self) -> Option<&crate::sync::circle::CircleOperationId> {
        match self {
            Self::FinalizeEpochClose(request) => Some(&request.operation_id),
            Self::Create { .. } | Self::Rename(_) | Self::AddMember(_) | Self::RemoveMember(_) => {
                None
            }
        }
    }
}
