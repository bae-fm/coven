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

    /// Resolve a Circle whose control history forked into concurrent valid
    /// successors by authoring a covering successor of the chosen branch. This
    /// is callable on a conflicted Circle regardless of rotation state — it is
    /// deliberately not gated by `ensure_not_rotation_required`, because
    /// resolution is the exit path out of the conflict and a conflicted Circle
    /// has no single resolved roster to evaluate rotation against. A
    /// rotation-required Circle re-derives that state from the resolved
    /// successor and blocks new content afterward.
    pub(crate) async fn resolve_circle_control(
        &self,
        device_id: &str,
        circle_id: CircleId,
        chosen: crate::sync::circle::CircleControlCoord,
        signer: &UserKeypair,
    ) -> Result<(), CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        let database = self.database();
        let storage = &**self.storage();
        crate::sync::store::ensure_active_registration(database, storage).await?;
        let branches = database
            .circle_control_conflict_branches(circle_id)
            .await?
            .ok_or(CircleOperationError::NotConflicted { circle_id })?;
        if !branches.contains(&chosen) {
            return Err(CircleOperationError::ChosenBranchNotRetained { circle_id });
        }
        let identity_pubkey = keys::public_key_hex(signer);
        let chosen_activation = database
            .verified_circle_activation(circle_id, chosen.clone())
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} conflict omits retained authority for the chosen branch"
                ))
            })?;
        let chosen_state =
            chosen_branch_authoring_state(circle_id, &identity_pubkey, &chosen_activation)?;
        let previous_control = chosen_activation.reference.clone();
        let mut losing_heads = Vec::new();
        for branch in &branches {
            if *branch == chosen {
                continue;
            }
            let activation = database
                .verified_circle_activation(circle_id, branch.clone())
                .await?
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle {circle_id} conflict omits retained authority for a losing branch"
                    ))
                })?;
            losing_heads.push(crate::sync::circle::MergeCircleControlHeadRef {
                coord: activation.reference.control.clone(),
                head_hash: activation.reference.head_hash,
                object: activation.reference.head_object.clone(),
            });
        }
        let journal = Box::pin(prepare_circle_operation_request(
            database,
            storage,
            device_id,
            CircleOperationRequest::ResolveControl(Box::new(CircleResolveControlRequest {
                circle_id,
                chosen: chosen_state,
                previous_control,
                losing_heads,
                conflicting_branches: branches,
            })),
            signer,
        ))
        .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle control resolution changed Circle identity".to_string(),
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

/// Materialize the chosen conflicting branch's authoring inputs from its
/// retained verified activation. A conflicted Circle exposes no single
/// `authoring_state`, so the resolution reads the chosen branch's roster,
/// metadata, keyring, and access leaf directly from that branch's retained
/// activation.
fn chosen_branch_authoring_state(
    circle_id: CircleId,
    identity_pubkey: &str,
    activation: &crate::sync::store::circle_controls::VerifiedCircleReference,
) -> Result<CircleAuthoringState, CircleOperationError> {
    let access = activation.local_access.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "Circle {circle_id} chosen branch has no retained local access"
        ))
    })?;
    let active = access.active.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "Circle {circle_id} control resolution requires active access to the chosen branch"
        ))
    })?;
    if access.leaf.value.recipient_pubkey != identity_pubkey {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle {circle_id} chosen branch belongs to another local identity"
        )));
    }
    Ok(CircleAuthoringState {
        candidate_family: access.leaf.value.candidate_family,
        control: activation.control.clone(),
        access: access.leaf.value.clone(),
        roster: active.roster.clone(),
        metadata: active.metadata.clone(),
    })
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

pub(super) struct CircleResolveControlRequest {
    pub(super) circle_id: CircleId,
    pub(super) chosen: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) losing_heads: Vec<crate::sync::circle::MergeCircleControlHeadRef>,
    /// Every retained branch coordinate, in canonical order, as captured when
    /// the command ran. Preparation verifies this still equals the currently
    /// retained conflict set inside the journal transaction, so a branch
    /// discovered between command and activation resurfaces as a new conflict
    /// rather than being silently swallowed.
    pub(super) conflicting_branches: Vec<crate::sync::circle::CircleControlCoord>,
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
    ResolveControl(Box<CircleResolveControlRequest>),
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
            Self::ResolveControl(request) => CircleOperationIntent::ResolveControl {
                chosen: request.chosen.control.coord.clone(),
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
            Self::ResolveControl(request) => {
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
            Self::Create { .. }
            | Self::Rename(_)
            | Self::AddMember(_)
            | Self::RemoveMember(_)
            | Self::ResolveControl(_) => None,
        }
    }
}
