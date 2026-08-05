use super::*;

impl LocalStoreWriter {
    pub(crate) async fn pull(
        &self,
        history: &mut crate::sync::store::owner::writer::AuthorizedStoreHistory<'_>,
        membership: &crate::protocol::membership::MembershipChain,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<
        crate::sync::store::owner::writer::pull::StorePullExecution,
        crate::sync::store::owner::writer::pull::StorePullError,
    > {
        history
            .pull(membership, Some(&self.identity), routing_encryption)
            .await
    }

    pub(crate) fn sign_device_acknowledgement(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        sequence: u64,
        history_cut: crate::protocol::store_commit::StoreHistoryCut,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        snapshot: Option<crate::protocol::store_commit::StoreSnapshotLocator>,
        exclusions: crate::protocol::store_commit::StoreAckExclusionState,
        sync_time: String,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<
        crate::protocol::store_commit::StoreAck,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::StoreAck::signed(
            store_root_hash,
            self.registration.reference().clone(),
            sequence,
            history_cut,
            device_state,
            snapshot,
            exclusions,
            sync_time,
            successor,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_store_write_commit(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        write_id: crate::WriteId,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        order: crate::protocol::store_commit::StoreCommitOrder,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        membership_authority: crate::protocol::store_commit::StoreOperationMembershipAuthority,
        operations: crate::protocol::store_commit::StoreCommitOperationsInput<'_>,
    ) -> Result<
        crate::protocol::store_commit::StoreBatchCommit,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::StoreBatchCommit::signed_operations(
            store_root_hash,
            write_id,
            coord,
            self.registration.reference().clone(),
            self.registration.value(),
            order,
            membership_state,
            device_state,
            membership_authority,
            operations,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_candidate_abandonment(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        write_id: crate::WriteId,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        order: crate::protocol::store_commit::StoreCommitOrder,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        cleanup: Vec<crate::protocol::store_commit::CandidateCleanupManifest>,
    ) -> Result<
        crate::protocol::store_commit::StoreBatchCommit,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::StoreBatchCommit::signed_with_candidate_abandonment(
            store_root_hash,
            write_id,
            coord,
            self.registration.reference().clone(),
            self.registration.value(),
            order,
            membership_state,
            device_state,
            cleanup,
            &self.device_signer,
        )
    }

    pub(crate) fn verify_prepared_commit(
        &self,
        bytes: &[u8],
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        object: crate::protocol::objects::ExactObjectRef,
    ) -> Result<
        crate::protocol::store_commit::VerifiedStoreBatchCommit,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::VerifiedStoreBatchCommit::parse_prepared(
            bytes,
            store_root_hash,
            coord,
            object,
            self.registration.value(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_snapshot(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        generation: u64,
        predecessor: Option<crate::protocol::store_commit::StoreSnapshotRef>,
        image: crate::protocol::store_commit::SnapshotImageRef,
        coverage: crate::protocol::store_commit::CommitFrontier,
        state: crate::protocol::store_commit::StoreSnapshotState,
        history_summary: crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        schema_version: u32,
        created_at: String,
        successor: crate::protocol::store_commit::SnapshotSuccessorLink,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::SnapshotMeta::signed(
            store_root_hash,
            self.registration.reference().clone(),
            generation,
            predecessor,
            image,
            coverage,
            state,
            history_summary,
            schema_version,
            created_at,
            successor,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn drain_tombstones(
        &self,
        database: &crate::database::StoreDatabase,
        storage: &dyn crate::storage::SyncStorage,
        cipher: &dyn crate::storage::CloudCipherAccess,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
        store_id: &str,
        clock: &dyn crate::clock::Clock,
    ) -> Result<usize, String> {
        crate::blob::delete::TombstoneDrain::new(
            database,
            storage,
            cipher,
            pending_rotation,
            store_id,
            &self.identity,
            clock,
        )
        .drain()
        .await
    }

    pub(crate) fn sign_operation_batch(
        &self,
        write_id: crate::WriteId,
        context: StoreOperationSigningContext,
        batch: crate::sync::store::owner::writer::operation::operations::StoreOperationBatch,
    ) -> Result<
        (
            crate::protocol::store_commit::StoreBatchCommit,
            Option<crate::protocol::store_commit::ActivatedStoreDeviceRegistration>,
        ),
        crate::sync::store::StoreError,
    > {
        use crate::protocol::store_commit::{
            StoreBatchCommit, StoreCommitOperationsInput, StoreControl,
        };
        use crate::sync::store::owner::writer::operation::operations::StoreOperationBatch;

        let registration_activation = match &batch {
            StoreOperationBatch::Outcome { registration, .. } => registration.as_deref().cloned(),
            _ => None,
        };
        let registration_ref = self.registration.reference().clone();
        let registration = self.registration.value();
        let signer = &self.device_signer;
        let root_hash = context.root.store_root_hash;
        let commit = match batch {
            StoreOperationBatch::Acknowledgement {
                reference: acknowledgement,
                value: _,
                circle_acknowledgements,
            } => StoreBatchCommit::signed_operations(
                root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                StoreCommitOperationsInput {
                    acknowledgement: Some(acknowledgement),
                    circle_acknowledgements: circle_acknowledgements
                        .iter()
                        .map(|circle| circle.reference.clone())
                        .collect(),
                    control: None,
                    device_join_attempt_decisions: Vec::new(),
                    device_join_outcomes: Vec::new(),
                    device_join_cleanup_receipts: Vec::new(),
                    provider_access_grants: Vec::new(),
                    device_registrations: Vec::new(),
                    device_exclusion_proposals: Vec::new(),
                    device_exclusion_outcomes: Vec::new(),
                    stream_activations: Vec::new(),
                    circle_controls: Vec::new(),
                    store_package: None,
                    circle_packages: &[],
                },
                signer,
            ),
            StoreOperationBatch::ProviderAccessGrant(grant) => {
                StoreBatchCommit::signed_with_provider_access(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    vec![grant],
                    signer,
                )
            }
            StoreOperationBatch::Attempt(attempt) => StoreBatchCommit::signed_with_join_attempts(
                root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                vec![attempt],
                signer,
            ),
            StoreOperationBatch::Abandonment(abandonment) => {
                StoreBatchCommit::signed_with_join_abandonments(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    vec![abandonment],
                    signer,
                )
            }
            StoreOperationBatch::Outcome {
                outcome,
                registration: activation,
            } => StoreBatchCommit::signed_with_join_outcomes(
                root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                vec![outcome],
                activation
                    .into_iter()
                    .map(|activation| {
                        activation.activated_reference().map_err(|error| {
                            crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                signer,
            ),
            StoreOperationBatch::CleanupReceipt(receipt) => {
                StoreBatchCommit::signed_with_join_cleanup_receipts(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    vec![receipt],
                    signer,
                )
            }
            StoreOperationBatch::DeviceExclusionProposal(proposal) => {
                StoreBatchCommit::signed_with_device_exclusions(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    vec![proposal.reference().clone()],
                    Vec::new(),
                    signer,
                )
            }
            StoreOperationBatch::DeviceExclusionOutcome(outcome) => {
                StoreBatchCommit::signed_with_device_exclusions(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    Vec::new(),
                    vec![outcome.wire_reference()],
                    signer,
                )
            }
            StoreOperationBatch::ReclaimAuthorization(authorization) => {
                StoreBatchCommit::signed_reclaim_authorization(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    *authorization,
                    signer,
                )
            }
            StoreOperationBatch::ReclaimReceipt(receipt) => {
                StoreBatchCommit::signed_reclaim_receipt(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    *receipt,
                    signer,
                )
            }
            StoreOperationBatch::OwnerPromotionRequest(request) => {
                StoreBatchCommit::signed_with_owner_promotion_request(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    request,
                    signer,
                )
            }
            StoreOperationBatch::MergeMembershipActivation {
                transition,
                stream_activations,
            } => StoreBatchCommit::signed_operations(
                root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                StoreCommitOperationsInput {
                    acknowledgement: None,
                    circle_acknowledgements: Vec::new(),
                    control: Some(StoreControl { transition }),
                    device_join_attempt_decisions: Vec::new(),
                    device_join_outcomes: Vec::new(),
                    device_join_cleanup_receipts: Vec::new(),
                    provider_access_grants: Vec::new(),
                    device_registrations: Vec::new(),
                    device_exclusion_proposals: Vec::new(),
                    device_exclusion_outcomes: Vec::new(),
                    stream_activations,
                    circle_controls: Vec::new(),
                    store_package: None,
                    circle_packages: &[],
                },
                signer,
            ),
        }
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))?;
        Ok((commit, registration_activation))
    }
}
