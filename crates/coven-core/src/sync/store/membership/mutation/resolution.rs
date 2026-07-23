use crate::database::Database;
use crate::keys::{self, UserKeypair};
use crate::sync::membership::{
    self, MembershipChain, MembershipChange, MembershipError, MembershipHeadRef,
};
use crate::sync::remote_object;
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use crate::sync::store::operations;
use crate::sync::store_commit;
use crate::sync::store_commit::membership_head_slot_prefix;
use crate::sync::store_objects;

use super::{
    decode_membership_mutation, encode_membership_mutation, encode_membership_progress,
    exact_owned_remote, finish_membership_transition, prepare_membership_transition,
    publish_prepared_merge_membership_activation, publish_prepared_merge_membership_authority,
    validate_prepared_publication, validate_prepared_transition, InviteError,
    MembershipMutationPlan, MembershipMutationProgress, MutationPersistence, ResolveMutationPlan,
};

impl ResolveMutationPlan {
    fn remote_objects(&self) -> Result<Vec<remote_object::RemoteObjectRecord>, InviteError> {
        self.candidate
            .merge_membership_resolution_remote_objects(
                &self.transition,
                &self.publication,
                &self.resolution,
                &self.reference,
                &self.resolution_object,
            )
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))
    }

    fn validate_closed_shape(&self) -> Result<(), InviteError> {
        validate_prepared_transition(&self.transition)?;
        validate_prepared_publication(&self.publication)?;
        if !self.resolution.verify_signature()
            || self.reference
                != self
                    .resolution
                    .resolution_ref(self.resolution_object.reference().clone())
            || self.resolution_object.stored_bytes()
                != serde_json::to_vec(&self.resolution).map_err(|error| {
                    InviteError::InvalidDurableMutation(format!(
                        "serialize membership resolution: {error}"
                    ))
                })?
            || self.transition.entry != self.publication.entry
            || self.transition.entry_ref != self.publication.entry_ref
            || self.transition.entry_object != self.publication.entry_object
            || self.candidate.commit.control()
                != Some(&store_commit::StoreControl {
                    transition: self.transition.transition.clone(),
                })
            || !matches!(
                &self.publication.entry.change,
                MembershipChange::ResolutionActivation { resolution }
                    if resolution == &self.reference
            )
            || !matches!(
                &self.publication.head.activation,
                membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.candidate.reference
            )
        {
            return Err(InviteError::InvalidDurableMutation(
                "membership resolution plan violates its exact activation graph".to_string(),
            ));
        }
        self.remote_objects()?;
        Ok(())
    }
}

async fn build_resolution_mutation(
    storage: &dyn SyncStorage,
    db: &Database,
    chain: &MembershipChain,
    signer: &UserKeypair,
    device_id: &str,
    conflict_hash: store_commit::ObjectHash,
    resolver_branch_heads: Vec<MembershipHeadRef>,
    created_at: &str,
) -> Result<ResolveMutationPlan, InviteError> {
    let base = operations::prepare_merge_conflict_resolution_commit(
        db,
        storage,
        device_id,
        signer,
        chain.head_refs(),
    )
    .await
    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let chain = base.membership();
    let membership::MembershipStatus::Conflict(membership::MembershipConflict::RevocationCycle {
        conflict_hash: current,
        maximal_valid_branches,
        ..
    }) = chain.status()
    else {
        return Err(MembershipError::Conflict.into());
    };
    if *current != conflict_hash
        || !maximal_valid_branches
            .iter()
            .any(|branch| branch.heads == resolver_branch_heads)
    {
        return Err(MembershipError::InvalidConflictResolution.into());
    }
    let resolver_pubkey = keys::public_key_hex(signer);
    let replacement_grant =
        membership::derive_store_resolution_grant(&conflict_hash, &resolver_pubkey);
    let stream_id = store_commit::StreamActivation::grant_authorized_stream_id(
        base.root().store_root_hash,
        base.registration_ref(),
        &replacement_grant,
        store_commit::StreamAnchorDomain::StoreMembership,
    );
    let membership_context = ProtocolObjectContext::signed_plaintext(
        base.root().store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let membership_slot = storage
        .allocate_protocol_slot(
            &membership_context,
            &membership_head_slot_prefix(&resolver_pubkey, &replacement_grant, stream_id, 1),
            ".json",
        )
        .await?;
    let recovery_context = ProtocolObjectContext::signed_plaintext(
        base.root().store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let recovery_slot = storage
        .allocate_protocol_slot(
            &recovery_context,
            &store_commit::owner_recovery_semantic_prefix(
                &resolver_pubkey,
                replacement_grant.clone(),
                1,
            ),
            ".json",
        )
        .await?;
    let membership = store_commit::GrantStreamAnchor::StoreMembership {
        first_slot: membership_slot,
    };
    let recovery = store_commit::GrantStreamAnchor::OwnerRecovery {
        first_slot: recovery_slot,
    };
    let acceptance = store_commit::OwnerConflictResolutionAcceptance::signed(
        base.root().store_root_hash,
        replacement_grant,
        base.registration_ref().clone(),
        membership.clone(),
        recovery,
        base.device_state().clone(),
        base.registration(),
        signer,
    )
    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let resolution = chain.signed_cycle_resolution(
        base.root().store_root_hash,
        resolver_branch_heads,
        membership,
        acceptance,
        signer,
    )?;
    let resolution_bytes = serde_json::to_vec(&resolution).map_err(|error| {
        InviteError::InvalidDurableMutation(format!("serialize membership resolution: {error}"))
    })?;
    let resolution_context = ProtocolObjectContext::signed_plaintext(
        base.root().store_root_hash,
        ProtocolObjectDomain::StoreMembershipResolution,
    );
    let resolution_hash = resolution.resolution_hash();
    let resolution_prefix = store_commit::membership_resolution_semantic_prefix(
        conflict_hash,
        &resolver_pubkey,
        resolution_hash,
    );
    let resolution_slot = storage
        .allocate_protocol_slot(&resolution_context, &resolution_prefix, ".json")
        .await?;
    let resolution_object = storage.prepare_protocol_object(
        &resolution_context,
        resolution_slot,
        &resolution_prefix,
        resolution_bytes,
    )?;
    let reference = resolution.resolution_ref(resolution_object.reference().clone());
    let mut resolved_chain = chain.clone();
    resolved_chain.apply_resolutions(
        base.root().store_root_hash,
        &[(reference.clone(), resolution.clone())],
    )?;
    let entry = resolved_chain.signed_resolution_activation_in_stream(
        base.root().store_root_hash,
        signer,
        stream_id,
        reference.clone(),
        &resolution,
        created_at.to_string(),
    )?;
    let transition = prepare_membership_transition(
        storage,
        db,
        base.root().store_root_hash,
        &resolved_chain,
        entry,
        signer,
    )
    .await?;
    let operation = base
        .finish(&resolved_chain, &reference)
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let mut stream_activations = vec![
        store_commit::StreamActivation::grant_authorized(
            resolution.store_root_hash,
            resolution.replacement_acceptance.owner_registration.clone(),
            resolution.replacement_grant.clone(),
            resolution.replacement_acceptance.membership.clone(),
        ),
        store_commit::StreamActivation::grant_authorized(
            resolution.store_root_hash,
            resolution.replacement_acceptance.owner_registration.clone(),
            resolution.replacement_grant.clone(),
            resolution.replacement_acceptance.recovery.clone(),
        ),
    ];
    stream_activations.sort();
    let mut candidate = operations::prepare_candidate(
        db,
        storage,
        operation,
        operations::StoreOperationBatch::MergeMembershipActivation {
            transition: transition.transition.clone(),
            stream_activations,
        },
    )
    .await
    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let publication = finish_membership_transition(
        storage,
        db,
        resolution.store_root_hash,
        transition.clone(),
        membership::MembershipHeadActivation::StoreCommit {
            commit: candidate.reference.clone(),
        },
        signer,
    )
    .await?;
    candidate
        .attach_merge_membership_proof(storage, &publication, Some(&resolution), signer)
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let plan = ResolveMutationPlan {
        resolution,
        reference,
        resolution_object,
        transition: Box::new(transition),
        candidate: Box::new(candidate),
        publication: Box::new(publication),
    };
    plan.validate_closed_shape()?;
    Ok(plan)
}

async fn finish_nonactivating_resolution(
    plan: &ResolveMutationPlan,
    persistence: &MutationPersistence<'_>,
    storage: &dyn SyncStorage,
) -> Result<(), InviteError> {
    let candidate_objects = vec![
        plan.candidate.reference.object.clone(),
        plan.transition.entry_ref.object.clone(),
        plan.publication.head_ref.object.clone(),
    ];
    let mut retained = vec![plan.reference.object.clone()];
    retained.push(plan.candidate.head_ref().object);
    let cleanup = persistence
        .db
        .membership_candidate_cleanup_targets(
            persistence.intent_hash,
            plan.candidate.reference.clone(),
            candidate_objects.iter().chain(&retained).cloned().collect(),
        )
        .await?;
    for target in cleanup {
        store_objects::delete_exact_object(storage, &target.object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        persistence
            .db
            .mark_candidate_cleanup_absent(target.object)
            .await?;
    }
    persistence
        .db
        .complete_nonactivating_membership_candidate_mutation(
            persistence.intent_hash,
            plan.candidate.reference.clone(),
            candidate_objects,
            retained,
            None,
        )
        .await?;
    Ok(())
}

async fn execute_resolution_mutation(
    storage: &dyn SyncStorage,
    chain: &mut MembershipChain,
    mut plan: ResolveMutationPlan,
    progress: MembershipMutationProgress,
    mut persistence: MutationPersistence<'_>,
) -> Result<membership::StoreMembershipConflictResolutionRef, InviteError> {
    plan.validate_closed_shape()?;
    if let MembershipMutationProgress::ResolutionCandidateNonactivating { nonactivation } =
        &progress
    {
        if nonactivation
            .reference()
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
            != plan.candidate.reference
        {
            return Err(InviteError::InvalidDurableMutation(
                "resolution nonactivation names another candidate".to_string(),
            ));
        }
        finish_nonactivating_resolution(&plan, &persistence, storage).await?;
        return Err(InviteError::InvalidDurableMutation(
            "membership resolution candidate did not activate".to_string(),
        ));
    }
    if let MembershipMutationProgress::ResolutionActivated { candidate } = &progress {
        if candidate != &plan.candidate.reference {
            return Err(InviteError::InvalidDurableMutation(
                "resolution activation names another candidate".to_string(),
            ));
        }
        let mut successor = chain.clone();
        successor.apply_resolutions(
            plan.resolution.store_root_hash,
            &[(plan.reference.clone(), plan.resolution.clone())],
        )?;
        successor.add_entry(plan.publication.entry.clone())?;
        successor.activate_head_ref(plan.publication.head_ref.clone())?;
        *chain = successor;
        return Ok(plan.reference);
    }
    if !matches!(progress, MembershipMutationProgress::Pending) {
        return Err(InviteError::InvalidDurableMutation(
            "membership resolution carries another mutation's progress".to_string(),
        ));
    }
    let mut successor = chain.clone();
    successor.apply_resolutions(
        plan.resolution.store_root_hash,
        &[(plan.reference.clone(), plan.resolution.clone())],
    )?;
    successor.add_entry(plan.publication.entry.clone())?;
    let root = persistence
        .db
        .local_store_root_ref()
        .await?
        .ok_or_else(|| InviteError::InvalidDurableMutation("Store root is absent".to_string()))?;
    let author = store_objects::load_registration_ref(
        storage,
        &root,
        &plan.publication.head.body.author_registration,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?
    .value;
    let remotes = plan.remote_objects()?;
    store_objects::create_exact_object(storage, &plan.resolution_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    store_objects::load_membership_resolution_ref(storage, root.store_root_hash, &plan.reference)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    crate::sync::store::database::StoreDatabase::new(persistence.db)
        .mark_remote_object_uploaded(exact_owned_remote(&remotes, &plan.reference.object)?)
        .await?;
    publish_prepared_merge_membership_authority(
        storage,
        root.store_root_hash,
        &plan.transition,
        &[],
    )
    .await?;
    crate::sync::store::database::StoreDatabase::new(persistence.db)
        .mark_remote_object_uploaded(exact_owned_remote(
            &remotes,
            &plan.transition.entry_ref.object,
        )?)
        .await?;
    operations::upload_commit(storage, &plan.candidate)
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    crate::sync::store::database::StoreDatabase::new(persistence.db)
        .mark_remote_object_uploaded(exact_owned_remote(
            &remotes,
            &plan.candidate.reference.object,
        )?)
        .await?;
    loop {
        let previous = plan.candidate.as_ref().clone();
        let current_remotes = plan.remote_objects()?;
        let outcome = publish_prepared_merge_membership_activation(
            persistence.db,
            storage,
            &root,
            &author,
            &plan.transition,
            &plan.publication,
            plan.candidate.clone(),
            operations::StoreMembershipJournalCompletion::Mutation {
                intent_hash: persistence.intent_hash,
                progress_bytes: encode_membership_progress(
                    &MembershipMutationProgress::ResolutionActivated {
                        candidate: plan.candidate.reference.clone(),
                    },
                )?,
                remote_objects: current_remotes.clone(),
            },
        )
        .await?;
        match outcome {
            operations::StoreOperationPublicationOutcome::Activated(reference)
                if reference == plan.candidate.reference =>
            {
                successor.activate_head_ref(plan.publication.head_ref.clone())?;
                *chain = successor;
                return Ok(plan.reference);
            }
            operations::StoreOperationPublicationOutcome::RepreparedCandidate(replacement)
                if replacement.reference == plan.candidate.reference =>
            {
                let previous_remotes = plan.remote_objects()?;
                let previous_head = previous.head_ref();
                plan.candidate = replacement;
                let replacement_remotes = plan.remote_objects()?;
                let replacement_head = plan.candidate.head_ref();
                let bytes =
                    encode_membership_mutation(&MembershipMutationPlan::Resolve(plan.clone()))?;
                persistence.intent_hash = persistence
                    .db
                    .adopt_merge_membership_candidate_head(
                        persistence.intent_hash,
                        bytes,
                        exact_owned_remote(&previous_remotes, &previous_head.object)?,
                        exact_owned_remote(&replacement_remotes, &replacement_head.object)?,
                        None,
                    )
                    .await?;
            }
            operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                candidate,
                nonactivation,
            } if candidate.as_ref() == plan.candidate.as_ref() => {
                let progress = MembershipMutationProgress::ResolutionCandidateNonactivating {
                    nonactivation: nonactivation.clone().into_durable(),
                };
                let cleanup = persistence
                    .db
                    .begin_membership_candidate_nonactivation(
                        persistence.intent_hash,
                        plan.candidate.reference.clone(),
                        vec![
                            plan.candidate.reference.object.clone(),
                            plan.transition.entry_ref.object.clone(),
                            plan.publication.head_ref.object.clone(),
                        ],
                        vec![
                            plan.reference.object.clone(),
                            plan.candidate.head_ref().object,
                        ],
                        encode_membership_progress(&progress)?,
                        *nonactivation,
                    )
                    .await?;
                for target in cleanup {
                    store_objects::delete_exact_object(storage, &target.object)
                        .await
                        .map_err(|error| InviteError::Crypto(error.to_string()))?;
                    persistence
                        .db
                        .mark_candidate_cleanup_absent(target.object)
                        .await?;
                }
                finish_nonactivating_resolution(&plan, &persistence, storage).await?;
                return Err(InviteError::InvalidDurableMutation(
                    "membership resolution candidate did not activate".to_string(),
                ));
            }
            _ => {
                return Err(InviteError::InvalidDurableMutation(
                    "membership resolution returned an inapplicable publication outcome"
                        .to_string(),
                ))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_membership_conflict(
    storage: &dyn SyncStorage,
    chain: &mut MembershipChain,
    signer: &UserKeypair,
    device_id: &str,
    conflict_hash: store_commit::ObjectHash,
    resolver_branch_heads: Vec<MembershipHeadRef>,
    created_at: &str,
    db: &Database,
) -> Result<membership::StoreMembershipConflictResolutionRef, InviteError> {
    let _mutation = db.lock_membership_mutation().await;
    let (plan, progress, intent_hash) = match db.outbound_membership_mutation().await? {
        Some(row) => {
            let intent_hash = row.intent_hash;
            let (pending, progress) = decode_membership_mutation(row)?;
            let MembershipMutationPlan::Resolve(plan) = pending else {
                return Err(InviteError::PendingMutation(
                    "another membership mutation is pending".to_string(),
                ));
            };
            if plan.resolution.conflict_hash != conflict_hash
                || plan.resolution.resolver_pubkey != keys::public_key_hex(signer)
                || plan.resolution.resolver_branch_heads != resolver_branch_heads
            {
                return Err(InviteError::PendingMutation(
                    "the pending resolution has different immutable inputs".to_string(),
                ));
            }
            (plan, progress, intent_hash)
        }
        None => {
            let plan = build_resolution_mutation(
                storage,
                db,
                chain,
                signer,
                device_id,
                conflict_hash,
                resolver_branch_heads,
                created_at,
            )
            .await?;
            let bytes = encode_membership_mutation(&MembershipMutationPlan::Resolve(plan.clone()))?;
            let progress = MembershipMutationProgress::Pending;
            let intent_hash = db
                .stage_membership_candidate_mutation(
                    bytes,
                    encode_membership_progress(&progress)?,
                    plan.remote_objects()?,
                    None,
                )
                .await?;
            (plan, progress, intent_hash)
        }
    };
    execute_resolution_mutation(
        storage,
        chain,
        plan,
        progress,
        MutationPersistence { db, intent_hash },
    )
    .await
}
