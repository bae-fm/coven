use super::{
    prepare_circle_operation, prepare_circle_operation_request, publish_circle_operation,
    CircleAuthoringState, CircleOperationError, CircleOperationIntent, CircleTransitionHistory,
};
use crate::database::Database;
use crate::keys::{self, UserKeypair};
use crate::sync::circle::{CircleId, CircleOperationState};
use crate::sync::storage::SyncStorage;
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::CircleControlRef;

pub(crate) async fn create_circle(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleId, CircleOperationError> {
    crate::sync::store::ensure_active_registration(db, storage).await?;
    let journal = Box::pin(prepare_circle_operation(
        db,
        storage,
        device_id,
        metadata_stamp,
        name,
        signer,
    ))
    .await?;
    let circle_id = journal.circle_id();
    let operation_id = journal.operation_id.clone();
    StoreDatabase::new(db)
        .insert_circle_operation(journal)
        .await?;
    Box::pin(publish_circle_operation(db, storage, &operation_id, signer)).await?;
    Ok(circle_id)
}

pub(crate) async fn rename_circle(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    metadata_stamp: &str,
    circle_id: CircleId,
    name: &str,
    signer: &UserKeypair,
) -> Result<(), CircleOperationError> {
    crate::sync::store::ensure_active_registration(db, storage).await?;
    let identity_pubkey = keys::public_key_hex(signer);
    let (current, activation_commit_ref) = db
        .circle_authoring_context(circle_id, &identity_pubkey)
        .await?;
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or(CircleOperationError::MissingState("Store root reference"))?;
    let (activation_commit, _) =
        crate::sync::store::pull::load_commit_with_author(storage, &root, &activation_commit_ref)
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
        db,
        storage,
        device_id,
        metadata_stamp,
        CircleOperationRequest::Rename(Box::new(CircleRenameRequest {
            circle_id,
            name: name.to_string(),
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
    StoreDatabase::new(db)
        .insert_circle_operation(journal)
        .await?;
    Box::pin(publish_circle_operation(db, storage, &operation_id, signer)).await
}

pub(super) struct CircleRenameRequest {
    pub(super) circle_id: CircleId,
    pub(super) name: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
}

pub(super) enum CircleOperationRequest {
    Create { name: String },
    Rename(Box<CircleRenameRequest>),
}

impl CircleOperationRequest {
    pub(super) fn name(&self) -> &str {
        match self {
            Self::Create { name } => name,
            Self::Rename(request) => &request.name,
        }
    }

    pub(super) fn intent(&self) -> CircleOperationIntent {
        match self {
            Self::Create { name } => CircleOperationIntent::Create { name: name.clone() },
            Self::Rename(request) => CircleOperationIntent::Rename {
                name: request.name.clone(),
            },
        }
    }

    pub(super) fn history(&self) -> CircleTransitionHistory {
        match self {
            Self::Create { .. } => CircleTransitionHistory::Founder,
            Self::Rename(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
        }
    }
}

pub(crate) async fn resume_circle_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    identity: &UserKeypair,
) -> Result<(), CircleOperationError> {
    while let Some(journal) = StoreDatabase::new(db)
        .oldest_pending_circle_operation()
        .await?
    {
        if !matches!(journal.state(), CircleOperationState::Pending) {
            return Err(CircleOperationError::Journal(format!(
                "pending circle operation {} contains a blocked payload",
                journal.circle_id()
            )));
        }
        match Box::pin(publish_circle_operation(
            db,
            storage,
            &journal.operation_id,
            identity,
        ))
        .await
        {
            Ok(()) | Err(CircleOperationError::Blocked { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
