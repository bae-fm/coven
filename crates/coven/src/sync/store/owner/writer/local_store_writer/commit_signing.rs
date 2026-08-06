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
        clock: &dyn coven_foundation::clock::Clock,
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
            DeviceJoinAttemptDecisionRef, StoreBatchCommit, StoreCommitOperationsInput,
            StoreControl,
        };
        use crate::sync::store::owner::writer::operation::operations::StoreOperationBatch;

        fn sign_ops(
            context: StoreOperationSigningContext,
            write_id: crate::WriteId,
            registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
            registration: &crate::protocol::store_commit::StoreDeviceRegistration,
            signer: &crate::keys::UserKeypair,
            input: crate::protocol::store_commit::StoreCommitOperationsInput<'_>,
        ) -> Result<
            crate::protocol::store_commit::StoreBatchCommit,
            crate::protocol::store_commit::StoreProtocolError,
        > {
            crate::protocol::store_commit::StoreBatchCommit::signed_operations(
                context.root.store_root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                input,
                signer,
            )
        }

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
            } => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    acknowledgement: Some(acknowledgement),
                    circle_acknowledgements: circle_acknowledgements
                        .iter()
                        .map(|circle| circle.reference.clone())
                        .collect(),
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::ProviderAccessGrant(grant) => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    provider_access_grants: vec![grant],
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::Attempt(attempt) => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    device_join_attempt_decisions: vec![DeviceJoinAttemptDecisionRef::Attempt(
                        attempt,
                    )],
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::Abandonment(abandonment) => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    device_join_attempt_decisions: vec![DeviceJoinAttemptDecisionRef::Abandoned(
                        abandonment,
                    )],
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::Outcome {
                outcome,
                registration: activation,
            } => {
                let device_registrations = activation
                    .into_iter()
                    .map(|activation| {
                        activation.activated_reference().map_err(|error| {
                            crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                sign_ops(
                    context,
                    write_id,
                    registration_ref,
                    registration,
                    signer,
                    StoreCommitOperationsInput {
                        device_join_outcomes: vec![outcome],
                        device_registrations,
                        ..StoreCommitOperationsInput::empty()
                    },
                )
            }
            StoreOperationBatch::CleanupReceipt(receipt) => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    device_join_cleanup_receipts: vec![receipt],
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::DeviceExclusionProposal(proposal) => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    device_exclusion_proposals: vec![proposal.reference().clone()],
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::DeviceExclusionOutcome(outcome) => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    device_exclusion_outcomes: vec![outcome.wire_reference()],
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::MergeMembershipActivation {
                transition,
                stream_activations,
            } => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    control: Some(StoreControl { transition }),
                    stream_activations,
                    ..StoreCommitOperationsInput::empty()
                },
            ),
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
        }
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))?;
        Ok((commit, registration_activation))
    }
}
