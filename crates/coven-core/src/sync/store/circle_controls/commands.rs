use super::{
    prepare_circle_operation, prepare_circle_operation_request, publish_circle_operation,
    CircleAuthoringState, CircleOperationError, CircleOperationIntent, CircleTransitionHistory,
};
use crate::keys::{self, UserKeypair};
use crate::sync::circle::{CircleId, CircleOperationState};
use crate::sync::cloud_storage::BlobPathScheme;
use crate::sync::store::Store;
use crate::sync::store_commit::CircleControlRef;

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
        database.insert_circle_operation(journal).await?;
        Box::pin(publish_circle_operation(
            database,
            storage,
            &operation_id,
            signer,
        ))
        .await
    }

    pub(crate) async fn resume_circle_operations(
        &self,
        identity: &UserKeypair,
    ) -> Result<(), CircleOperationError> {
        let database = self.database();
        let storage = &**self.storage();
        while let Some(journal) = database.oldest_pending_circle_operation().await? {
            if !matches!(journal.state(), CircleOperationState::Pending) {
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
