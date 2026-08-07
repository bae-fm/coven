use super::commands::{
    CircleAddMemberRequest, CircleCancelEpochCloseRequest, CircleDeleteRequest,
    CircleFinalizeEpochCloseRequest, CircleOperationRequest, CircleRemoveMemberRequest,
    CircleRenameRequest, CircleResolveControlRequest, CircleResolveLosingBranch,
};
use super::*;
use coven_protocol::circle::{
    circle_epoch_close_response_semantic_prefix, CircleControlState, CircleEpochCloseExclusionRef,
    CircleEpochCloseResponseRef, CircleEpochCloseResponseSlotValue, CircleEpochCloseSettlement,
    CircleId, CircleRole, PreparedCircleControl,
};
use coven_protocol::objects::{
    ProtocolObjectContext, ProtocolObjectDomain, StorageError, StoreObjectError,
};
use coven_protocol::store_commit::CommitFrontier;

mod acknowledgements;
mod commands;
mod epoch_close;
mod writer_test_support;

pub(crate) struct AuthorizedCircleWriter<'writer, 'storage> {
    writer: &'writer mut AuthorizedWriterOperation<'storage>,
    database: coven_database::StoreDatabase,
    storage: std::sync::Arc<dyn coven_storage::SyncStorage>,
    store_dir: &'storage coven_foundation::store_dir::StoreDir,
    root: coven_protocol::store_commit::StoreRootRef,
    membership: coven_protocol::membership::MembershipChain,
    local_writer: std::sync::Arc<crate::sync::store::owner::writer::LocalStoreWriter>,
}

impl<'writer, 'storage> AuthorizedCircleWriter<'writer, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        writer: &'writer mut AuthorizedWriterOperation<'storage>,
        database: coven_database::StoreDatabase,
        storage: std::sync::Arc<dyn coven_storage::SyncStorage>,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
        root: coven_protocol::store_commit::StoreRootRef,
        membership: coven_protocol::membership::MembershipChain,
        local_writer: std::sync::Arc<crate::sync::store::owner::writer::LocalStoreWriter>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            store_dir,
            root,
            membership,
            local_writer,
        }
    }

    pub(super) fn publisher(&mut self) -> publication::CircleCandidatePublisher<'_, 'storage> {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let membership = self.membership.clone();
        let history = self.writer.circle_history();
        publication::CircleCandidatePublisher::new(
            database,
            storage,
            membership,
            std::sync::Arc::clone(&self.local_writer),
            history,
        )
    }

    pub(super) fn preparer(&mut self) -> preparation::CircleCandidatePreparer<'_, 'storage> {
        let announcement_stream_id = self.writer.announcement_stream_id();
        let database = self.database.clone();
        let membership = self.membership.clone();
        let root = self.root.clone();
        let storage = self.storage.clone();
        let history = self.writer.circle_history();
        preparation::CircleCandidatePreparer::new(
            announcement_stream_id,
            database,
            membership,
            root,
            storage,
            std::sync::Arc::clone(&self.local_writer),
            history,
        )
    }

    pub(crate) fn snapshots(
        &mut self,
    ) -> crate::sync::store::snapshots::CircleSnapshotWriter<'_, 'storage> {
        crate::sync::store::snapshots::CircleSnapshotWriter::new(
            self.writer,
            self.database.clone(),
            self.storage.clone(),
            self.store_dir,
            self.root.clone(),
            std::sync::Arc::clone(&self.local_writer),
        )
    }

    fn history(&mut self) -> VerifiedCircleHistory<'_, 'storage> {
        self.writer.circle_history()
    }

    /// A deleted Circle is terminal: every lifecycle command refuses it with a
    /// typed reason rather than a generic missing-authoring-state error.
    async fn ensure_not_deleted(&self, circle_id: CircleId) -> Result<(), CircleOperationError> {
        if self.database.circle_is_deleted(circle_id).await? {
            return Err(CircleOperationError::Deleted { circle_id });
        }
        Ok(())
    }

    async fn current_authoring_context(
        &mut self,
        circle_id: CircleId,
    ) -> Result<
        (
            CircleAuthoringState,
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
            coven_protocol::store_commit::CircleControlRef,
        ),
        CircleOperationError,
    > {
        self.ensure_not_deleted(circle_id).await?;
        let identity_pubkey = self.local_writer.author_pubkey();
        let (current, activation_commit_ref) = self
            .database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let (activation_commit, reference) = self
            .verified_activation_control(circle_id, &current, &activation_commit_ref)
            .await?;
        Ok((current, activation_commit, reference))
    }

    async fn current_delete_context(
        &mut self,
        circle_id: CircleId,
    ) -> Result<
        (
            CircleAuthoringState,
            coven_protocol::store_commit::CircleControlRef,
        ),
        CircleOperationError,
    > {
        let identity_pubkey = self.local_writer.author_pubkey();
        let (current, activation_commit_ref) = self
            .database
            .circle_delete_context(circle_id, &identity_pubkey)
            .await?;
        let (_, reference) = self
            .verified_activation_control(circle_id, &current, &activation_commit_ref)
            .await?;
        Ok((current, reference))
    }

    /// Load the Circle's activating Store commit and pin down the control
    /// reference it carries for the current state, rejecting a commit whose
    /// candidate family or control set disagrees with that state.
    async fn verified_activation_control(
        &mut self,
        circle_id: CircleId,
        current: &CircleAuthoringState,
        activation_commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<
        (
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
            coven_protocol::store_commit::CircleControlRef,
        ),
        CircleOperationError,
    > {
        let activation_commit = self
            .history()
            .load_commit(activation_commit_ref)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if activation_commit.value().candidate_family() != current.candidate_family {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current state differs from its activating Store commit"
            )));
        }
        let reference = activation_commit
            .value()
            .circle_controls()
            .iter()
            .find(|reference| {
                reference.circle_id() == circle_id && reference.control() == &current.control.coord
            })
            .cloned()
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        Ok((activation_commit, reference))
    }
}

fn retained_branch_authoring_state(
    circle_id: CircleId,
    identity_pubkey: &str,
    activation: &coven_protocol::circle_activation::VerifiedCircleReference,
) -> Result<CircleAuthoringState, CircleOperationError> {
    let access = activation.local_access.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch has no local access"
        ))
    })?;
    let active = access.active.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch has no active access"
        ))
    })?;
    if access.leaf.value.recipient_pubkey != identity_pubkey {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch belongs to another local identity"
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

fn losing_branch_selected_metadata(
    circle_id: CircleId,
    activation: &coven_protocol::circle_activation::VerifiedCircleReference,
) -> Result<coven_protocol::circle::CircleMetadata, CircleOperationError> {
    activation
        .local_access
        .as_ref()
        .and_then(|access| access.active.as_ref())
        .map(|active| active.metadata.clone())
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle {circle_id} control resolution requires active access to every branch"
            ))
        })
}
