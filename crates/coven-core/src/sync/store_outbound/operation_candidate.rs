use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreOperationPublicationOutcome {
    Activated(StoreBatchCommitRef),
    Nonactivated(StoreBatchCommitRef),
    Reprepared,
    RepreparedCandidate(Box<PreparedStoreOperationCommit>),
    NonactivatedCandidate {
        candidate: Box<PreparedStoreOperationCommit>,
        nonactivation: Box<super::remote_object::VerifiedCandidateNonactivation>,
    },
}

pub(crate) async fn prepare_store_operation_candidate_common(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: &StoreOperationPlanCommon,
    batch: StoreOperationBatch,
) -> Result<PreparedStoreOperationCommon, StoreOutboundError> {
    let store_root_hash = plan.root.store_root_hash;
    let registration_activation = match &batch {
        StoreOperationBatch::Outcome { registration, .. } => registration.as_deref().cloned(),
        _ => None,
    };
    let commit = match batch {
        StoreOperationBatch::Acknowledgement {
            reference: acknowledgement,
            value: _,
        } => StoreBatchCommit::signed_operations(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            plan.membership_authority.clone(),
            StoreCommitOperationsInput {
                acknowledgement: Some(acknowledgement),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            &plan.device_signer,
        ),
        StoreOperationBatch::ProviderAccessGrant(grant) => {
            StoreBatchCommit::signed_with_provider_access(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                plan.membership_authority.clone(),
                vec![grant],
                Vec::new(),
                &plan.device_signer,
            )
        }
        StoreOperationBatch::Attempt(attempt) => StoreBatchCommit::signed_with_join_attempts(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            plan.membership_authority.clone(),
            vec![attempt],
            &plan.device_signer,
        ),
        StoreOperationBatch::Abandonment(abandonment) => {
            StoreBatchCommit::signed_with_join_abandonments(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                plan.membership_authority.clone(),
                vec![abandonment],
                &plan.device_signer,
            )
        }
        StoreOperationBatch::Outcome {
            outcome,
            registration,
        } => StoreBatchCommit::signed_with_join_outcomes(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            plan.membership_authority.clone(),
            vec![outcome],
            registration
                .into_iter()
                .map(|activation| activation.reference.clone())
                .collect(),
            &plan.device_signer,
        ),
        StoreOperationBatch::CleanupReceipt(receipt) => {
            StoreBatchCommit::signed_with_join_cleanup_receipts(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                plan.membership_authority.clone(),
                vec![receipt],
                &plan.device_signer,
            )
        }
        StoreOperationBatch::DeviceExclusionProposal(proposal) => {
            StoreBatchCommit::signed_with_device_exclusions(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                plan.membership_authority.clone(),
                vec![proposal.reference().clone()],
                Vec::new(),
                &plan.device_signer,
            )
        }
        StoreOperationBatch::DeviceExclusionOutcome(outcome) => {
            StoreBatchCommit::signed_with_device_exclusions(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                plan.membership_authority.clone(),
                Vec::new(),
                vec![outcome.wire_reference()],
                &plan.device_signer,
            )
        }
        StoreOperationBatch::ReclaimAuthorization(authorization) => {
            StoreBatchCommit::signed_reclaim_authorization(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                *authorization,
                &plan.device_signer,
            )
        }
        StoreOperationBatch::ReclaimReceipt(receipt) => StoreBatchCommit::signed_reclaim_receipt(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            *receipt,
            &plan.device_signer,
        ),
        StoreOperationBatch::OwnerPromotionRequest(request) => {
            StoreBatchCommit::signed_with_owner_promotion_request(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                plan.membership_authority.clone(),
                request,
                &plan.device_signer,
            )
        }
        StoreOperationBatch::MergeMembershipActivation {
            transition,
            stream_activations,
        } => StoreBatchCommit::signed_operations(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            plan.membership_authority.clone(),
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: Some(StoreControl { transition }),
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations,
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            &plan.device_signer,
        ),
    }
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreCommit);
    let stream_id = plan.coord.stream_id.to_string();
    let prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &stream_id,
        commit.seq(),
        commit.commit_hash(),
    );
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let prepared = storage
        .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
        .map_err(StoreObjectError::from)?;
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, plan.coord.clone(), prepared.reference().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    Ok(PreparedStoreOperationCommon {
        commit,
        prepared,
        reference: commit_ref,
        registration_activation,
    })
}

pub(crate) async fn prepare_store_operation_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: StoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<PreparedStoreOperationCommit, StoreOutboundError> {
    crate::sync::store_engine::engine::operations::prepare_candidate(db, storage, plan, batch).await
}
