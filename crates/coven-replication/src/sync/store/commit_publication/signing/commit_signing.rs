use super::*;
use crate::sync::store::authorization::history::publication::StoreCommitPublicationAttemptOutcome;

impl LocalStoreWriter {
    pub(crate) async fn install_current_publication(
        &self,
        history: &mut crate::sync::store::authorization::history::AuthorizedStoreHistory<'_>,
        membership: &mut coven_protocol::membership::MembershipChain,
    ) -> Result<crate::sync::store::pull::StorePullResult, crate::sync::store::StoreError> {
        history
            .install_current_publication(membership, &self.identity)
            .await
    }

    pub(crate) async fn publish_store_commit(
        &self,
        history: &mut crate::sync::store::authorization::history::AuthorizedStoreHistory<'_>,
        membership: &mut coven_protocol::membership::MembershipChain,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<StoreCommitPublicationAttemptOutcome, crate::sync::store::StoreError> {
        history
            .publish_store_commit(membership, &self.identity, &self.device_signer, commit)
            .await
    }

    pub(crate) async fn publish_store_snapshot(
        &self,
        history: &mut crate::sync::store::authorization::history::AuthorizedStoreHistory<'_>,
        membership: &mut coven_protocol::membership::MembershipChain,
        pending: &coven_database::DurableSnapshotPublication,
        objects: &crate::sync::store::snapshots::AuthorizedSnapshotPublication<'_>,
    ) -> Result<
        crate::sync::store::authorization::history::publication::StoreSnapshotPublicationAttemptOutcome,
        crate::sync::store::snapshots::SnapshotError,
    >{
        history
            .publish_store_snapshot(membership, &self.identity, pending, objects)
            .await
    }

    pub(crate) async fn pull(
        &self,
        history: &mut crate::sync::store::authorization::history::AuthorizedStoreHistory<'_>,
        membership: &coven_protocol::membership::MembershipChain,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<
        crate::sync::store::pull::StorePullExecution,
        crate::sync::store::pull::StorePullError,
    > {
        history
            .pull(membership, Some(&self.identity), routing_encryption)
            .await
    }

    /// What this device would assert about `history_cut` right now, ahead of
    /// signing anything: the caller compares it against the acknowledgement it
    /// already stands behind and signs only if it says something new.
    pub(crate) fn device_acknowledgement_assertion(
        &self,
        history_cut: coven_protocol::store_commit::StoreHistoryCut,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
    ) -> coven_protocol::store_commit::StoreAckAssertion {
        coven_protocol::store_commit::StoreAckAssertion {
            registration: self.registration.reference().clone(),
            store_cut: history_cut,
            device_state,
        }
    }

    pub(crate) fn sign_device_acknowledgement(
        &self,
        sequence: u64,
        assertion: coven_protocol::store_commit::StoreAckAssertion,
        sync_time: String,
        successor: coven_protocol::store_commit::SuccessorLink,
    ) -> Result<
        coven_protocol::store_commit::StoreAck,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::StoreAck::signed(
            self.registration.value().store_root.store_root_hash,
            sequence,
            assertion,
            sync_time,
            successor,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_store_write_commit(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        write_id: coven_protocol::write::WriteId,
        coord: coven_protocol::store_commit::StoreCommitCoord,
        order: coven_protocol::store_commit::StoreCommitOrder,
        publication_base: coven_protocol::store_commit::StorePublicationBase,
        membership_state: coven_protocol::circle_control::StoreMembershipStateRef,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        membership_authority: coven_protocol::membership::MembershipCoord,
        operations: coven_protocol::store_commit::StoreCommitOperationsInput<'_>,
    ) -> Result<
        coven_protocol::store_commit::StoreBatchCommit,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::StoreBatchCommit::signed_operations(
            store_root_hash,
            write_id,
            coord,
            self.registration.reference().clone(),
            self.registration.value(),
            order,
            publication_base,
            membership_state,
            device_state,
            membership_authority,
            operations,
            &self.device_signer,
        )
    }

    pub(crate) fn sign_store_publication_entry(
        &self,
        previous: &coven_database::ObservedStorePublication,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<
        coven_protocol::store_commit::StorePublicationEntry,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::StorePublicationEntry::signed_commit(
            previous.record(),
            commit,
            &self.device_signer,
        )
    }

    pub(crate) fn advance_store_publication(
        &self,
        previous: &coven_database::ObservedStorePublication,
        entry: &coven_protocol::store_commit::StorePublicationEntry,
        prepared: &coven_protocol::objects::PreparedExactObject,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<
        coven_protocol::store_commit::StoreCurrentPublicationRecord,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        let reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
            entry,
            prepared.reference().clone(),
        )?;
        coven_protocol::store_commit::StoreCurrentPublicationRecord::advance_commit(
            previous.record(),
            entry,
            reference,
            commit,
            &self.device_signer,
        )
    }

    pub(crate) fn sign_store_snapshot_publication_entry(
        &self,
        previous: &coven_database::ObservedStorePublication,
        snapshot: coven_protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        coven_protocol::store_commit::StorePublicationEntry,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::StorePublicationEntry::signed_snapshot(
            previous.record(),
            self.registration.reference().clone(),
            snapshot,
            &self.device_signer,
        )
    }

    pub(crate) fn advance_store_snapshot_publication(
        &self,
        previous: &coven_database::ObservedStorePublication,
        entry: &coven_protocol::store_commit::StorePublicationEntry,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<
        coven_protocol::store_commit::StoreCurrentPublicationRecord,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        let reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
            entry,
            prepared.reference().clone(),
        )?;
        coven_protocol::store_commit::StoreCurrentPublicationRecord::advance_snapshot(
            previous.record(),
            entry,
            reference,
            &self.device_signer,
        )
    }

    pub(crate) fn verify_prepared_commit(
        &self,
        bytes: &[u8],
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        coord: coven_protocol::store_commit::StoreCommitCoord,
        object: coven_protocol::objects::ExactObjectRef,
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::VerifiedStoreBatchCommit::parse_prepared(
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
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        publication_predecessor: coven_protocol::store_commit::StoreCurrentPublicationRecord,
        image: coven_protocol::store_commit::SnapshotImageRef,
        membership_rollup: coven_protocol::store_commit::MembershipRollupRef,
        coverage: coven_protocol::store_commit::CommitFrontier,
        state: coven_protocol::store_commit::StoreSnapshotState,
        history_summary: coven_protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        schema_version: u32,
        created_at: String,
    ) -> Result<
        coven_protocol::store_commit::SnapshotMeta,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::SnapshotMeta::signed(
            store_root_hash,
            self.registration.reference().clone(),
            publication_predecessor,
            image,
            membership_rollup,
            coverage,
            state,
            history_summary,
            schema_version,
            created_at,
            &self.device_signer,
        )
    }

    /// Sign the membership rollup this device is about to publish beside a
    /// snapshot. Signed by the same device that signs the snapshot naming it,
    /// so a reader that opens the rollup knows who stands behind the objects in
    /// it before it checks any of them.
    pub(crate) fn sign_membership_rollup(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        streams: Vec<coven_protocol::store_commit::MembershipRollupStream>,
    ) -> Result<
        coven_protocol::store_commit::MembershipRollup,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::MembershipRollup::signed(
            store_root_hash,
            self.registration.reference().clone(),
            streams,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn drain_tombstones(
        &self,
        database: &coven_database::StoreDatabase,
        storage: &dyn coven_storage::CloudSyncObjectStorage,
        store_id: &str,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<usize, crate::blob::delete::TombstoneDrainError> {
        crate::blob::delete::TombstoneDrain::new(database, storage, store_id, &self.identity, clock)
            .drain()
            .await
    }

    pub(crate) fn sign_operation_batch(
        &self,
        write_id: coven_protocol::write::WriteId,
        context: StoreOperationSigningContext,
        batch: crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch,
    ) -> Result<
        (
            coven_protocol::store_commit::StoreBatchCommit,
            Option<coven_protocol::store_commit::ActivatedStoreDeviceRegistration>,
        ),
        crate::sync::store::StoreError,
    > {
        use crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch;
        use coven_protocol::store_commit::{
            DeviceJoinAttemptDecisionRef, StoreBatchCommit, StoreCommitOperationsInput,
            StoreControl,
        };

        fn sign_ops(
            context: StoreOperationSigningContext,
            write_id: coven_protocol::write::WriteId,
            registration_ref: coven_protocol::store_commit::StoreDeviceRegistrationRef,
            registration: &coven_protocol::store_commit::StoreDeviceRegistration,
            signer: &coven_keys::keys::UserKeypair,
            input: coven_protocol::store_commit::StoreCommitOperationsInput<'_>,
        ) -> Result<
            coven_protocol::store_commit::StoreBatchCommit,
            coven_protocol::store_commit::StoreProtocolError,
        > {
            coven_protocol::store_commit::StoreBatchCommit::signed_operations(
                registration.store_root.store_root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.publication_base,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                input,
                signer,
            )
        }

        let registration_activation = match &batch {
            StoreOperationBatch::JoinActivation { registration, .. } => Some(*registration.clone()),
            StoreOperationBatch::SamePrincipalDeviceJoin { registration, .. } => {
                Some(*registration.clone())
            }
            _ => None,
        };
        let registration_ref = self.registration.reference().clone();
        let registration = self.registration.value();
        let signer = &self.device_signer;
        let root_hash = registration.store_root.store_root_hash;
        let commit = match batch {
            StoreOperationBatch::AbandonCandidates(manifests) => {
                StoreBatchCommit::signed_with_candidate_abandonment(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.publication_base,
                    context.membership_state,
                    context.device_state,
                    manifests,
                    signer,
                )
            }

            StoreOperationBatch::Circle {
                reference,
                stream_activations,
            } => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    circle_controls: vec![reference],
                    stream_activations,
                    ..StoreCommitOperationsInput::empty()
                },
            ),
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
            StoreOperationBatch::SamePrincipalDeviceJoin {
                attempt_id,
                registration: activated_registration,
                transition,
            } => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    device_join_attempt_decisions: vec![DeviceJoinAttemptDecisionRef::Attempt(
                        attempt_id,
                    )],
                    control: Some(StoreControl { transition }),
                    device_registrations: vec![activated_registration.activated_reference()?],
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
            StoreOperationBatch::JoinActivation {
                registration: activation,
                transition,
            } => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    control: Some(StoreControl { transition }),
                    device_registrations: vec![activation.activated_reference()?],
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::DeviceExclusionProposal {
                proposal,
                transition,
            } => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    control: Some(StoreControl { transition }),
                    device_exclusion_proposals: vec![proposal.reference().clone()],
                    ..StoreCommitOperationsInput::empty()
                },
            ),
            StoreOperationBatch::DeviceExclusionOutcome {
                outcome,
                transition,
            } => sign_ops(
                context,
                write_id,
                registration_ref,
                registration,
                signer,
                StoreCommitOperationsInput {
                    control: Some(StoreControl { transition }),
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
                    context.publication_base,
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
                    context.publication_base,
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
                    context.publication_base,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    request,
                    signer,
                )
            }
        }
        .map_err(crate::sync::store::StoreError::from)?;
        Ok((commit, registration_activation))
    }
}
