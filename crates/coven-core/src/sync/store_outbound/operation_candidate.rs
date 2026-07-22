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

pub(crate) async fn prepare_store_operation_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: StoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<PreparedStoreOperationCommit, StoreOutboundError> {
    let store_root_hash = plan.root.store_root_hash;
    let registration_activation = match &batch {
        StoreOperationBatch::Outcome { registration, .. } => registration.as_deref().cloned(),
        _ => None,
    };
    let acknowledgement_evidence = match &batch {
        StoreOperationBatch::Acknowledgement { reference, value } => {
            Some((reference.clone(), value.clone()))
        }
        _ => None,
    };
    let retained_registration_evidence = match &batch {
        StoreOperationBatch::Outcome {
            registration: Some(registration),
            ..
        } => vec![super::store_commit::RetainedVerifiedRegistration {
            reference: registration.reference.registration.clone(),
            value: registration.registration.clone(),
        }],
        _ => Vec::new(),
    };
    let retained_device_operations = match &batch {
        StoreOperationBatch::DeviceExclusionProposal(proposal) => Some(
            super::store_commit::RetainedStoreDeviceOperations::from_sources(
                vec![proposal.clone()],
                Vec::new(),
            ),
        ),
        StoreOperationBatch::DeviceExclusionOutcome(outcome) => Some(
            super::store_commit::RetainedStoreDeviceOperations::from_sources(
                Vec::new(),
                vec![outcome.clone()],
            ),
        ),
        _ => None,
    };
    let commit = match batch {
        StoreOperationBatch::Control(control) => StoreBatchCommit::signed_with_control(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            plan.membership_authority.clone(),
            Some(control),
            None,
            &plan.device_signer,
        ),
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
                control: Some(StoreControl::MergeMembership { transition }),
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
    let stream_id = match plan.coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    };
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
    let candidate = match plan {
        StoreOperationCommitPlan::Serial(plan) => {
            let authorization_after = plan
                .authorization
                .authorize_and_apply(&commit_ref, &commit, &plan.registration)
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let head = StoreSerialHead::signed(
                store_root_hash,
                StoreSerialHeadState::Commit {
                    author_registration: plan.registration_ref.clone(),
                    commit: commit_ref.clone(),
                },
                &plan.device_signer,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            PreparedStoreOperationCommit::Serial(PreparedSerialStoreOperationCommit {
                common: PreparedStoreOperationCommon {
                    commit,
                    prepared,
                    reference: commit_ref,
                    registration_activation,
                },
                base_head: plan.base_head,
                head,
                authorization_after,
            })
        }
        StoreOperationCommitPlan::MergeConcurrent(plan) => {
            let acknowledgement = match acknowledgement_evidence {
                Some((reference, value)) => Some(
                    super::store_pull::retain_activated_acknowledgement(
                        storage,
                        &plan.root,
                        &commit_ref,
                        &commit,
                        &plan.registration,
                        reference,
                        value,
                    )
                    .await
                    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
                ),
                None => None,
            };
            let merge_history_evidence = super::store_pull::MergeHistorySuccessorEvidence {
                registrations: retained_registration_evidence,
                acknowledgement,
                membership_proof: None,
            };
            let registrations = registration_activation
                .as_ref()
                .map(|activation| {
                    vec![(
                        activation.registration.clone(),
                        activation.authority.clone(),
                    )]
                })
                .unwrap_or_default();
            let device_operations = match retained_device_operations {
                Some(retained) => retained
                    .verify_for(&plan.root, &commit)
                    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
                None => {
                    super::store_commit::VerifiedStoreDeviceOperations::without_exclusions(&commit)
                        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
                }
            };
            let state_after = Box::pin(super::store_pull::derive_local_merge_post_device_state(
                storage,
                &plan.root,
                &commit,
                plan.predecessor_state.clone(),
                &registrations,
                device_operations,
            ))
            .await
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let head_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let device_id = plan.registration_ref.device_id.to_string();
            let successor = super::store_pull::prepare_merge_history_successor(
                db,
                &plan.root,
                &commit,
                &commit_ref,
                &plan.membership,
                &plan.registration,
                None,
                state_after,
                merge_history_evidence,
            )
            .await
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let next_prefix = head_slot_prefix(&device_id, successor_store_sequence(commit.seq())?);
            let next_slot = storage
                .allocate_protocol_slot(&head_context, &next_prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;
            let head = StoreDeviceHead::signed(
                store_root_hash,
                plan.registration_ref.clone(),
                commit_ref.clone(),
                successor.summary.digest(),
                SuccessorLink {
                    activation: plan
                        .registration
                        .store_announcement_activation(&plan.registration_ref)
                        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
                        .activation_id(),
                    predecessor: successor.predecessor_head.map(|reference| reference.object),
                    next_slot,
                },
                &plan.device_signer,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let head_prefix = head_slot_prefix(&device_id, commit.seq());
            let prepared_head = storage
                .prepare_protocol_object(
                    &head_context,
                    successor.head_slot,
                    &head_prefix,
                    head.to_bytes(),
                )
                .map_err(StoreObjectError::from)?;
            PreparedStoreOperationCommit::MergeConcurrent(PreparedMergeStoreOperationCommit {
                common: PreparedStoreOperationCommon {
                    commit,
                    prepared,
                    reference: commit_ref,
                    registration_activation,
                },
                head,
                prepared_head,
                history_summary: successor.summary,
            })
        }
    };
    Ok(candidate)
}
