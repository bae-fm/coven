use super::{
    CircleAuthoringState, CircleOperationError, CircleOperationIntent, CircleTransitionHistory,
};
use coven_protocol::circle::{CircleCloseStatus, CircleId, CircleRole, CircleRosterChain};
use coven_protocol::store_commit::CircleControlRef;
use coven_storage::BlobPathScheme;

pub struct StoreCircleCommands<'store> {
    store: &'store super::Store,
}

impl<'store> StoreCircleCommands<'store> {
    pub(crate) fn new(store: &'store super::Store) -> Self {
        Self { store }
    }

    /// Every Circle mutation requires protected blob paths: a browsable layout
    /// would leak Circle structure through object names.
    fn require_protected_paths(&self) -> Result<(), CircleOperationError> {
        if matches!(self.store.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(CircleOperationError::BrowsableStorage);
        }
        Ok(())
    }

    async fn writer(
        &self,
    ) -> Result<
        crate::sync::store::commit_publication::AuthorizedWriterOperation<'store>,
        CircleOperationError,
    > {
        self.require_protected_paths()?;
        self.store
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))
    }

    /// The read-only settlement status of a Circle's in-flight epoch close: for
    /// each participant device, whether its create-once response slot holds a
    /// response, an Owner exclusion, or is still empty. Reports each slot's
    /// declared settlement; the finalize path verifies each slot before acting on
    /// it. A read, so it does not require Owner authorization — any participant
    /// resolving the closing control can inspect it.
    pub(crate) async fn circle_close_status(
        &self,
        circle_id: CircleId,
    ) -> Result<CircleCloseStatus, CircleOperationError> {
        self.store.circle_close_status(circle_id).await
    }

    pub(crate) async fn create_circle(
        &self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<CircleId, CircleOperationError> {
        let mut writer = self.writer().await?;
        writer.circles().create_circle(metadata_stamp, name).await
    }

    pub(crate) async fn rename_circle(
        &self,
        metadata_stamp: &str,
        circle_id: CircleId,
        name: &str,
    ) -> Result<(), CircleOperationError> {
        let mut writer = self.writer().await?;
        writer
            .circles()
            .rename_circle(metadata_stamp, circle_id, name)
            .await
    }

    pub(crate) async fn remove_circle_member(
        &self,
        circle_id: CircleId,
        member_pubkey: String,
    ) -> Result<coven_protocol::circle::CircleOperationId, CircleOperationError> {
        let mut writer = self.writer().await?;
        writer
            .circles()
            .remove_circle_member(circle_id, member_pubkey)
            .await
    }

    /// Resolve a Circle whose control history forked into concurrent valid
    /// successors by authoring a covering successor of the chosen branch. This
    /// is callable on a conflicted Circle regardless of rotation state — it is
    /// deliberately allowed during required rotation, because resolution is the
    /// exit path out of the conflict and a conflicted Circle
    /// has no single resolved roster to evaluate rotation against. A
    /// rotation-required Circle re-derives that state from the resolved
    /// successor and blocks new content afterward.
    pub(crate) async fn resolve_circle_control(
        &self,
        circle_id: CircleId,
        chosen: coven_protocol::circle::CircleControlCoord,
    ) -> Result<(), CircleOperationError> {
        let mut writer = self.writer().await?;
        writer
            .circles()
            .resolve_circle_control(circle_id, chosen)
            .await
    }

    /// Cancel the local device's in-flight epoch close by settling its one outcome
    /// slot with an Owner-signed cancellation and activating a reopening control
    /// that restores the frozen epoch. When concurrent controls have made the
    /// Circle conflicted, the durable operation's close id selects its exact
    /// retained branch; reopening that branch leaves the other branches visible
    /// for an explicit control resolution. Refuses if no local close operation is
    /// waiting for responses — a close whose outcome already won the slot has
    /// moved out of the waiting state and cannot be cancelled.
    pub(crate) async fn cancel_circle_epoch_close(
        &self,
        circle_id: CircleId,
    ) -> Result<coven_protocol::circle::CircleOperationId, CircleOperationError> {
        let mut writer = self.writer().await?;
        writer.circles().cancel_circle_epoch_close(circle_id).await
    }

    /// Sign and publish an Owner exclusion of an unavailable participant device to
    /// that device's create-once close-response slot, letting a stalled close reach
    /// completion. Create-once decides the slot: if the device's own response
    /// landed first, the exclusion is a no-op and that response is adopted.
    pub(crate) async fn exclude_circle_close_device(
        &self,
        circle_id: CircleId,
        excluded_device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<(), CircleOperationError> {
        let mut writer = self.writer().await?;
        writer
            .circles()
            .exclude_circle_close_device(circle_id, excluded_device_id)
            .await
    }

    /// Return a blocked operation to its captured phase and re-enter the publish
    /// pipeline, which revalidates against refreshed signed state. Initiator-driven
    /// — the cycle never auto-unblocks. Refuses typed if the operation is not
    /// blocked; retrying twice converges because publication is per-step
    /// idempotent and re-blocks if authority is still absent.
    pub(crate) async fn retry_circle_operation(
        &self,
        operation_id: &coven_protocol::circle::CircleOperationId,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<(), CircleOperationError> {
        let mut writer = self.writer().await?;
        writer
            .circles()
            .retry_circle_operation(operation_id, routing_encryption)
            .await
    }

    /// Discard a durable Circle operation that can provably never activate,
    /// exact-deleting its candidate-exclusive objects and clearing its journal
    /// row. Legal only with one of the three direct nonactivation proofs — a
    /// different verified winner already occupies the candidate's successor slot,
    /// the author was permanently excluded, or a membership revocation forecloses
    /// activation. Without proof it refuses typed: it never assumes an unseen
    /// candidate failed to activate. Idempotent and restart-safe — a crash between
    /// the recorded proof and the cleared row resumes the same cleanup from the
    /// durable `Discarding` state.
    pub(crate) async fn discard_circle_operation(
        &self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), CircleOperationError> {
        self.require_protected_paths()?;
        let mut authorized = self
            .store
            .authorize()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        authorized.discard_circle_operation(operation_id).await
    }

    /// Author the terminal deletion of a Circle. It requires a resolved current
    /// state — a conflicted Circle is refused until the Owner resolves it,
    /// because the conflicting set may bury membership intent — and refuses a
    /// Circle that is already deleted. It is not gated by the rotation-required
    /// check: deletion distributes no key, so it is a terminal exit like member
    /// removal.
    pub(crate) async fn delete_circle(
        &self,
        circle_id: CircleId,
    ) -> Result<(), CircleOperationError> {
        let mut writer = self.writer().await?;
        writer.circles().delete_circle(circle_id).await
    }
}

pub(crate) struct CircleRenameRequest {
    pub(super) circle_id: CircleId,
    pub(super) name: String,
    pub(super) metadata_stamp: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
}

pub(crate) struct CircleAddMemberRequest {
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) role: CircleRole,
    bootstrap: crate::sync::store::SnapshotCut,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
}

impl CircleAddMemberRequest {
    pub(super) fn new(
        circle_id: CircleId,
        member_pubkey: String,
        role: CircleRole,
        bootstrap: crate::sync::store::SnapshotCut,
        current: CircleAuthoringState,
        previous_control: CircleControlRef,
        roster_chain: CircleRosterChain,
    ) -> Self {
        Self {
            circle_id,
            member_pubkey,
            role,
            bootstrap,
            current,
            previous_control,
            roster_chain,
        }
    }

    pub(super) fn bootstrap_blobs(&self) -> &[coven_database::SnapshotBlobFact] {
        self.bootstrap.blobs()
    }

    pub(super) async fn read_bootstrap_image(
        &self,
    ) -> Result<Vec<u8>, coven_database::SnapshotImageError> {
        self.bootstrap.read_image().await
    }

    pub(super) fn bootstrap_coverage(&self) -> &coven_protocol::store_commit::CommitFrontier {
        self.bootstrap.coverage()
    }
}

pub(crate) struct CircleRemoveMemberRequest {
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
}

pub(crate) struct CircleDeleteRequest {
    pub(super) circle_id: CircleId,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
}

pub(crate) struct CircleResolveControlRequest {
    pub(super) circle_id: CircleId,
    pub(super) chosen: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    /// The retained branches other than `chosen`. The resolution merges each
    /// one's control, metadata, and roster head frontiers into its own so no
    /// author-stream head slot is re-allocated once the conflict collapses.
    pub(super) losing_branches: Vec<CircleResolveLosingBranch>,
    /// Every retained branch coordinate, in canonical order, as captured when
    /// the command ran. Preparation verifies this still equals the currently
    /// retained conflict set inside the journal transaction, so a branch
    /// discovered between command and activation resurfaces as a new conflict
    /// rather than being silently swallowed.
    pub(super) conflicting_branches: Vec<coven_protocol::circle::CircleControlCoord>,
}

pub(crate) struct CircleResolveLosingBranch {
    /// The losing branch's exact activation reference: its control head plus the
    /// full activation objects (metadata and roster head frontiers and their
    /// entries) the resolution covers.
    pub(super) reference: CircleControlRef,
    /// The metadata entry this branch selected — one input to the resolution's
    /// deterministic name selection over the merged frontier.
    pub(super) selected_metadata: coven_protocol::circle::CircleMetadata,
}

pub(crate) struct CircleFinalizeEpochCloseRequest {
    pub(super) operation_id: coven_protocol::circle::CircleOperationId,
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) metadata_stamp: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
    pub(super) roster_chain: CircleRosterChain,
    pub(super) intent: coven_protocol::circle::CircleEpochCloseIntent,
    pub(super) responses: Vec<coven_protocol::circle::CircleEpochCloseSettlement>,
    bootstrap: crate::sync::store::SnapshotCut,
}

impl CircleFinalizeEpochCloseRequest {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        operation_id: coven_protocol::circle::CircleOperationId,
        circle_id: CircleId,
        member_pubkey: String,
        metadata_stamp: String,
        current: CircleAuthoringState,
        previous_control: CircleControlRef,
        roster_chain: CircleRosterChain,
        intent: coven_protocol::circle::CircleEpochCloseIntent,
        responses: Vec<coven_protocol::circle::CircleEpochCloseSettlement>,
        bootstrap: crate::sync::store::SnapshotCut,
    ) -> Self {
        Self {
            operation_id,
            circle_id,
            member_pubkey,
            metadata_stamp,
            current,
            previous_control,
            roster_chain,
            intent,
            responses,
            bootstrap,
        }
    }

    pub(super) fn bootstrap_blobs(&self) -> &[coven_database::SnapshotBlobFact] {
        self.bootstrap.blobs()
    }

    pub(super) async fn read_bootstrap_image(
        &self,
    ) -> Result<Vec<u8>, coven_database::SnapshotImageError> {
        self.bootstrap.read_image().await
    }

    pub(super) fn bootstrap_coverage(&self) -> &coven_protocol::store_commit::CommitFrontier {
        self.bootstrap.coverage()
    }
}

pub(crate) struct CircleCancelEpochCloseRequest {
    pub(super) operation_id: coven_protocol::circle::CircleOperationId,
    pub(super) circle_id: CircleId,
    pub(super) member_pubkey: String,
    pub(super) current: CircleAuthoringState,
    pub(super) previous_control: CircleControlRef,
}

pub(crate) enum CircleOperationRequest {
    Create {
        name: String,
        metadata_stamp: String,
    },
    Rename(Box<CircleRenameRequest>),
    AddMember(Box<CircleAddMemberRequest>),
    RemoveMember(Box<CircleRemoveMemberRequest>),
    ResolveControl(Box<CircleResolveControlRequest>),
    Delete(Box<CircleDeleteRequest>),
    FinalizeEpochClose(Box<CircleFinalizeEpochCloseRequest>),
    CancelEpochClose(Box<CircleCancelEpochCloseRequest>),
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
            Self::Delete(_) => CircleOperationIntent::Delete,
            Self::FinalizeEpochClose(request) => CircleOperationIntent::RemoveMember {
                member_pubkey: request.member_pubkey.clone(),
            },
            Self::CancelEpochClose(request) => CircleOperationIntent::RemoveMember {
                member_pubkey: request.member_pubkey.clone(),
            },
        }
    }

    pub(super) fn history(&self) -> CircleTransitionHistory {
        let previous_control = match self {
            Self::Create { .. } => return CircleTransitionHistory::Founder,
            Self::Rename(request) => &request.previous_control,
            Self::AddMember(request) => &request.previous_control,
            Self::RemoveMember(request) => &request.previous_control,
            Self::ResolveControl(request) => &request.previous_control,
            Self::Delete(request) => &request.previous_control,
            Self::FinalizeEpochClose(request) => &request.previous_control,
            Self::CancelEpochClose(request) => &request.previous_control,
        };
        CircleTransitionHistory::Successor(Box::new(previous_control.clone()))
    }

    /// The stable operation id and derived write identity for a close settlement.
    /// Finalize and cancel settle the same durable operation but derive distinct
    /// write identities, so a crashed settlement resumes as the kind it began as
    /// rather than being re-derived into the other.
    pub(super) fn settlement(
        &self,
    ) -> Option<(
        coven_protocol::circle::CircleOperationId,
        coven_protocol::write::WriteId,
    )> {
        match self {
            Self::FinalizeEpochClose(request) => Some((
                request.operation_id.clone(),
                request.operation_id.finalization_write_id(),
            )),
            Self::CancelEpochClose(request) => Some((
                request.operation_id.clone(),
                request.operation_id.cancellation_write_id(),
            )),
            Self::Create { .. }
            | Self::Rename(_)
            | Self::AddMember(_)
            | Self::RemoveMember(_)
            | Self::ResolveControl(_)
            | Self::Delete(_) => None,
        }
    }
}
