use crate::encryption::{self, EncryptionService};
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{CloudAccessOutcome, CloudAccessState, CloudHome, RevokeOutcome};
use crate::sync::cloud_storage;
use crate::sync::membership::{
    self, AuthorStreamId, MemberRole, MembershipChain, MembershipChange,
};
use crate::sync::remote_object;
use crate::sync::storage::{self, SyncStorage};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit;
use crate::sync::store_objects;
use crate::sync::wrapped_store_key::{load_wrapped_store_key, prepare_wrapped_store_key};

use super::{
    chain_with_exact_entry, decode_membership_mutation, ed25519_hex_to_x25519,
    encode_membership_mutation, encode_membership_progress, exact_owned_remote,
    finish_membership_transition, load_authorized_owner_keyring, prepare_membership_publication,
    prepare_membership_transition, publish_prepared_merge_membership_activation,
    publish_prepared_merge_membership_authority, select_mutation_author_stream, signed_wrapped_key,
    unwrap_store_keyring_for_refs, validate_prepared_publication, validate_prepared_transition,
    InviteError, MembershipMutationPlan, MembershipMutationProgress, MutationPersistence,
    ReplacementWrappedKey, RevokeMembershipPublication, RevokeMutationPlan,
};

/// Build a removal that revokes provider access, rotates the Store key, and
/// publishes the signed membership change as one durable operation.
async fn build_revoke_mutation(
    storage: &dyn SyncStorage,
    database: &StoreDatabase,
    store_root_hash: store_commit::ObjectHash,
    chain: &MembershipChain,
    owner_keypair: &UserKeypair,
    stream_id: AuthorStreamId,
    revokee_pubkey: &str,
    store_id: &str,
    timestamp: &str,
    current_encryption: &EncryptionService,
) -> Result<RevokeMutationPlan, InviteError> {
    if chain.store_id() != Some(store_id) {
        return Err(InviteError::InvalidDurableMutation(format!(
            "membership chain store {:?} differs from requested store {store_id:?}",
            chain.store_id()
        )));
    }
    let members = chain.current_members();
    if !members.iter().any(|(pubkey, _)| pubkey == revokee_pubkey) {
        return Err(InviteError::NotAMember(revokee_pubkey.to_string()));
    }
    let current_owners = members
        .iter()
        .filter(|(pubkey, role)| pubkey != revokee_pubkey && *role == MemberRole::Owner)
        .map(|(pubkey, _)| pubkey.clone())
        .collect::<Vec<_>>();
    if current_owners.is_empty() {
        return Err(InviteError::LastOwner);
    }
    let owner_pubkey = keys::public_key_hex(owner_keypair);
    let authority_refs = chain.wrapped_key_authority_for(&owner_pubkey)?;
    let current_keyring = load_authorized_owner_keyring(
        storage,
        store_root_hash,
        owner_keypair,
        store_id,
        &authority_refs,
        current_encryption,
    )
    .await?;
    let new_keyring = current_keyring
        .with_appended_generation(
            current_keyring
                .current_generation()
                .checked_add(1)
                .ok_or_else(|| InviteError::Crypto("store key generation overflow".to_string()))?,
            encryption::generate_random_key(),
        )
        .map_err(|error| InviteError::Crypto(format!("append key generation: {error}")))?;
    let remaining_members = members
        .iter()
        .filter(|(pubkey, _)| pubkey != revokee_pubkey)
        .cloned()
        .collect::<Vec<_>>();
    let mut wraps = Vec::with_capacity(remaining_members.len());
    for (recipient, _) in remaining_members {
        let recipient_key = ed25519_hex_to_x25519(&recipient)?;
        wraps.push(ReplacementWrappedKey {
            prepared: prepare_wrapped_store_key(
                storage,
                store_root_hash,
                &recipient,
                signed_wrapped_key(
                    store_id,
                    &recipient,
                    &recipient_key,
                    &new_keyring,
                    owner_keypair,
                )?,
            )
            .await?,
        });
    }
    wraps.sort_by(|left, right| left.prepared.reference.cmp(&right.prepared.reference));
    let wrapped_keys = wraps
        .iter()
        .map(|wrap| wrap.prepared.reference.clone())
        .collect();
    let publication = if chain.is_owner_now(revokee_pubkey) {
        let device_id = database
            .sqlite()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or_else(|| {
                InviteError::InvalidDurableMutation(
                    "local Store device registration is absent".to_string(),
                )
            })?;
        let operation = Box::pin(crate::sync::store::operations::prepare_plan(
            database,
            storage,
            chain,
            &device_id,
            owner_keypair,
        ))
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let entry = chain.signed_remove_member_with_owner_barrier_state(
            owner_keypair,
            stream_id,
            revokee_pubkey.to_string(),
            wrapped_keys,
            operation.device_state().clone(),
            timestamp.to_string(),
        )?;
        let transition = prepare_membership_transition(
            storage,
            database,
            store_root_hash,
            chain,
            entry,
            owner_keypair,
        )
        .await?;
        let mut candidate = Box::pin(crate::sync::store::operations::prepare_candidate(
            database,
            storage,
            operation,
            crate::sync::store::operations::StoreOperationBatch::MergeMembershipActivation {
                transition: transition.transition.clone(),
                stream_activations: Vec::new(),
            },
        ))
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let head = finish_membership_transition(
            storage,
            database,
            store_root_hash,
            transition.clone(),
            membership::MembershipHeadActivation::StoreCommit {
                commit: candidate.reference.clone(),
            },
            owner_keypair,
        )
        .await?;
        candidate
            .attach_merge_membership_proof(storage, &head, None, owner_keypair)
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        RevokeMembershipPublication::StoreActivated {
            transition: Box::new(transition),
            candidate: Box::new(candidate),
            publication: Box::new(head),
        }
    } else {
        let entry = chain.signed_remove_member_with_wrapped_keys_in_stream(
            owner_keypair,
            stream_id,
            revokee_pubkey.to_string(),
            wrapped_keys,
            timestamp.to_string(),
        )?;
        RevokeMembershipPublication::Direct {
            publication: Box::new(
                prepare_membership_publication(
                    storage,
                    database,
                    store_root_hash,
                    chain,
                    entry,
                    owner_keypair,
                )
                .await?,
            ),
        }
    };
    let provider_account_email = chain
        .current_member_provider_email(revokee_pubkey)
        .map(str::to_string);
    Ok(RevokeMutationPlan {
        publication,
        revokee_pubkey: revokee_pubkey.to_string(),
        desired_access: CloudAccessState::Absent {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email: provider_account_email.clone(),
        },
        prior_access: CloudAccessState::Present {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email,
        },
        wraps,
        keyring_payload: new_keyring
            .to_keyring_payload()
            .map_err(|error| InviteError::Crypto(format!("serialize rotated keyring: {error}")))?,
    })
}

fn revoke_plan_matches_request(
    plan: &RevokeMutationPlan,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    store_id: &str,
) -> bool {
    plan.publication.entry().author_pubkey == hex::encode(owner_keypair.public_key())
        && plan.publication.entry().store_id == store_id
        && plan.revokee_pubkey == revokee_pubkey
        && matches!(
            &plan.publication.entry().change,
            MembershipChange::RemoveMember { user_pubkey, .. }
                if user_pubkey == revokee_pubkey
        )
        && matches!(
            &plan.desired_access,
            CloudAccessState::Absent { member_pubkey, .. }
                if member_pubkey == revokee_pubkey
        )
        && matches!(
            &plan.prior_access,
            CloudAccessState::Present { member_pubkey, .. }
                if member_pubkey == revokee_pubkey
        )
}

impl RevokeMutationPlan {
    fn validate_closed_shape(&self) -> Result<(), InviteError> {
        let publication = self.publication.publication();
        validate_prepared_publication(publication)?;
        let (desired_member, desired_email) = match &self.desired_access {
            CloudAccessState::Absent {
                member_pubkey,
                provider_account_email,
            } => (member_pubkey, provider_account_email),
            CloudAccessState::Present { .. } => {
                return Err(InviteError::InvalidDurableMutation(
                    "membership removal requests present provider access".to_string(),
                ))
            }
        };
        let (prior_member, prior_email) = match &self.prior_access {
            CloudAccessState::Present {
                member_pubkey,
                provider_account_email,
            } => (member_pubkey, provider_account_email),
            CloudAccessState::Absent { .. } => {
                return Err(InviteError::InvalidDurableMutation(
                    "membership removal compensation requests absent provider access".to_string(),
                ))
            }
        };
        if desired_member != &self.revokee_pubkey
            || prior_member != &self.revokee_pubkey
            || desired_email != prior_email
        {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal access and compensation intents disagree".to_string(),
            ));
        }
        let MembershipChange::RemoveMember {
            user_pubkey,
            wrapped_keys,
            retirement_device_state,
            retirement_barriers,
            ..
        } = &publication.entry.change
        else {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal plan contains another change".to_string(),
            ));
        };
        let planned_wraps = self
            .wraps
            .iter()
            .map(|wrap| wrap.prepared.reference.clone())
            .collect::<Vec<_>>();
        if user_pubkey != &self.revokee_pubkey || wrapped_keys != &planned_wraps {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal plan differs from its exact entry".to_string(),
            ));
        }
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => {
                if retirement_device_state.is_some()
                    || retirement_barriers.values().any(|barrier| {
                        matches!(
                            barrier,
                            membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                        )
                    })
                    || !matches!(
                        publication.head.activation,
                        membership::MembershipHeadActivation::Direct
                    )
                {
                    return Err(InviteError::InvalidDurableMutation(
                        "direct membership removal carries Owner retirement authority".to_string(),
                    ));
                }
            }
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                ..
            } => {
                validate_prepared_transition(transition)?;
                candidate
                    .validate_closed_shape()
                    .map_err(InviteError::InvalidDurableMutation)?;
                if transition.entry != publication.entry
                    || transition.entry_ref != publication.entry_ref
                    || transition.entry_object != publication.entry_object
                    || retirement_device_state.as_ref() != Some(&candidate.commit.device_state)
                    || !retirement_barriers.values().any(|barrier| {
                        matches!(
                            barrier,
                            membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                        )
                    })
                    || candidate.commit.control()
                        != Some(&store_commit::StoreControl {
                            transition: transition.transition.clone(),
                        })
                    || !transition
                        .transition
                        .matches_head(&publication.head, &publication.head_ref)
                    || !matches!(
                        &publication.head.activation,
                        membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &candidate.reference
                    )
                {
                    return Err(InviteError::InvalidDurableMutation(
                        "Owner retirement differs from its exact Store activation graph"
                            .to_string(),
                    ));
                }
                candidate
                    .merge_membership_activation_remote_objects(
                        transition,
                        publication,
                        &self
                            .wraps
                            .iter()
                            .map(|wrap| wrap.prepared.clone())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn candidate_remote_objects(
        &self,
    ) -> Result<Option<Vec<remote_object::RemoteObjectRecord>>, InviteError> {
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => Ok(None),
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                publication,
            } => candidate
                .merge_membership_activation_remote_objects(
                    transition,
                    publication,
                    &self
                        .wraps
                        .iter()
                        .map(|wrap| wrap.prepared.clone())
                        .collect::<Vec<_>>(),
                )
                .map(Some)
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string())),
        }
    }

    fn candidate_cleanup_objects(
        &self,
    ) -> (Vec<storage::ExactObjectRef>, Vec<storage::ExactObjectRef>) {
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => (Vec::new(), Vec::new()),
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                publication,
            } => {
                let candidate_objects = std::iter::once(candidate.reference.object.clone())
                    .chain(std::iter::once(transition.entry_ref.object.clone()))
                    .chain(std::iter::once(publication.head_ref.object.clone()))
                    .chain(
                        self.wraps
                            .iter()
                            .map(|wrap| wrap.prepared.reference.object.clone()),
                    )
                    .collect();
                let retained = vec![candidate.head_ref().object];
                (candidate_objects, retained)
            }
        }
    }
}

async fn finish_nonactivating_revoke(
    plan: &RevokeMutationPlan,
    persistence: &MutationPersistence<'_>,
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
) -> Result<(), InviteError> {
    let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication else {
        return Err(InviteError::InvalidDurableMutation(
            "direct membership removal has no candidate cleanup".to_string(),
        ));
    };
    let (candidate_objects, _) = plan.candidate_cleanup_objects();
    let cleanup = persistence
        .database
        .membership_candidate_cleanup_targets(
            persistence.intent_hash,
            candidate.reference.clone(),
            candidate_objects,
        )
        .await?;
    finish_nonactivating_revoke_with_targets(plan, persistence, storage, cloud_home, cleanup).await
}

async fn finish_nonactivating_revoke_with_targets(
    plan: &RevokeMutationPlan,
    persistence: &MutationPersistence<'_>,
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    cleanup: Vec<crate::database::CandidateCleanupObject>,
) -> Result<(), InviteError> {
    let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication else {
        return Err(InviteError::InvalidDurableMutation(
            "direct membership removal has no candidate terminalization".to_string(),
        ));
    };
    match cloud_home.set_access(plan.prior_access.clone()).await? {
        CloudAccessOutcome::Present(_) => {}
        CloudAccessOutcome::Absent(_) => {
            return Err(InviteError::InvalidDurableMutation(
                "provider returned absent while restoring a nonactivated removal".to_string(),
            ))
        }
    }
    for target in cleanup {
        store_objects::delete_exact_object(storage, &target.object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        persistence
            .database
            .mark_candidate_cleanup_absent(target.object)
            .await?;
    }
    let (candidate_objects, retained) = plan.candidate_cleanup_objects();
    persistence
        .database
        .complete_nonactivating_membership_candidate_mutation(
            persistence.intent_hash,
            candidate.reference.clone(),
            candidate_objects,
            retained,
            Some(
                EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                    .map_err(|error| {
                        InviteError::Crypto(format!("parse rotated keyring: {error}"))
                    })?
                    .current_generation(),
            ),
        )
        .await?;
    Ok(())
}

async fn execute_revoke_mutation(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: store_commit::ObjectHash,
    chain: &mut MembershipChain,
    mut plan: RevokeMutationPlan,
    mut progress: MembershipMutationProgress,
    mut persistence: MutationPersistence<'_>,
    pending_rotation: &cloud_storage::PendingRotation,
) -> Result<EncryptionService, InviteError> {
    plan.validate_closed_shape()?;
    if matches!(progress, MembershipMutationProgress::InviteGranted { .. }) {
        return Err(InviteError::InvalidDurableMutation(
            "removal carries invitation progress".to_string(),
        ));
    }
    let mut validated_chain = chain_with_exact_entry(chain, plan.publication.entry())?;
    if let MembershipMutationProgress::RevokeActivated { candidate } = &progress {
        let (expected, publication) = match &plan.publication {
            RevokeMembershipPublication::Direct { publication } => (None, publication),
            RevokeMembershipPublication::StoreActivated {
                candidate,
                publication,
                ..
            } => (Some(&candidate.reference), publication),
        };
        if candidate.as_ref() != expected {
            return Err(InviteError::InvalidDurableMutation(
                "membership activation names another candidate".to_string(),
            ));
        }
        validated_chain.activate_head_ref(publication.head_ref.clone())?;
        *chain = validated_chain;
        return EncryptionService::from_keyring_payload(plan.keyring_payload)
            .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")));
    }
    if let MembershipMutationProgress::RevokeCandidateNonactivating { nonactivation } = &progress {
        let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication
        else {
            return Err(InviteError::InvalidDurableMutation(
                "direct removal carries Store-candidate nonactivation".to_string(),
            ));
        };
        nonactivation
            .validate()
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        if nonactivation
            .reference()
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
            != candidate.reference
        {
            return Err(InviteError::InvalidDurableMutation(
                "membership nonactivation names another candidate".to_string(),
            ));
        }
        finish_nonactivating_revoke(&plan, &persistence, storage, cloud_home).await?;
        let generation = EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
            .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?
            .current_generation();
        pending_rotation
            .remove_candidate(generation, persistence.intent_hash)
            .map_err(InviteError::InvalidDurableMutation)?;
        return Err(InviteError::InvalidDurableMutation(
            "membership removal candidate did not activate".to_string(),
        ));
    }
    let publication = plan.publication.publication().clone();
    let root = persistence
        .database
        .local_store_root_ref()
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation("local Store root reference is absent".to_string())
        })?;
    let author = store_objects::load_registration_ref(
        storage,
        &root,
        &publication.head.body.author_registration,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?
    .value;
    if !publication.head.verify(&author) {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership head has an invalid certified-device signature".to_string(),
        ));
    }
    let keyring = EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
        .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?;
    let remaining = validated_chain.current_members();
    if remaining.len() != plan.wraps.len() {
        return Err(InviteError::InvalidDurableMutation(
            "planned replacement wraps do not cover every remaining member exactly once"
                .to_string(),
        ));
    }
    let mut planned_recipients = std::collections::BTreeSet::new();
    for wrapped in &plan.wraps {
        let reference = &wrapped.prepared.reference;
        if !planned_recipients.insert(reference.recipient_pubkey.clone())
            || !remaining
                .iter()
                .any(|(member_pubkey, _)| member_pubkey == &reference.recipient_pubkey)
        {
            return Err(InviteError::InvalidDurableMutation(format!(
                "planned replacement wrap has duplicate or non-member recipient {}",
                reference.recipient_pubkey
            )));
        }
        let envelope = wrapped.prepared.validate()?;
        if envelope.generation != keyring.current_generation()
            || envelope.author_pubkey != publication.entry.author_pubkey
            || envelope
                .verify_and_unwrap(
                    &publication.entry.store_id,
                    &reference.recipient_pubkey,
                    std::iter::once(publication.entry.author_pubkey.as_str()),
                )
                .is_err()
        {
            return Err(InviteError::InvalidDurableMutation(format!(
                "planned replacement wrap for {} is not bound to the exact removal, generation, recipient, and author",
                reference.recipient_pubkey
            )));
        }
    }
    let authority_refs = match &publication.entry.change {
        MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys,
        _ => {
            return Err(InviteError::InvalidDurableMutation(
                "planned removal publication is not a removal".to_string(),
            ))
        }
    };
    let planned_refs = plan
        .wraps
        .iter()
        .map(|wrap| wrap.prepared.reference.clone())
        .collect::<Vec<_>>();
    if authority_refs != &planned_refs {
        return Err(InviteError::InvalidDurableMutation(
            "planned removal authority differs from its exact wrapped keys".to_string(),
        ));
    }
    let remote_objects = plan.candidate_remote_objects()?;
    for wrapped in &plan.wraps {
        if let Some(remotes) = &remote_objects {
            let expected = exact_owned_remote(remotes, &wrapped.prepared.reference.object)?;
            persistence
                .database
                .mark_remote_object_uploaded(expected)
                .await?;
        }
    }
    match &plan.publication {
        RevokeMembershipPublication::Direct { .. } => {
            for wrapped in &plan.wraps {
                store_objects::create_exact_object(storage, &wrapped.prepared.object)
                    .await
                    .map_err(|error| InviteError::Crypto(error.to_string()))?;
                load_wrapped_store_key(storage, store_root_hash, &wrapped.prepared.reference)
                    .await?;
            }
            store_objects::create_exact_object(storage, &publication.entry_object)
                .await
                .map_err(|error| InviteError::Crypto(error.to_string()))?;
            store_objects::load_membership_entry_ref(
                storage,
                store_root_hash,
                &publication.entry_ref,
            )
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        }
        RevokeMembershipPublication::StoreActivated { transition, .. } => {
            publish_prepared_merge_membership_authority(
                storage,
                store_root_hash,
                transition,
                &plan
                    .wraps
                    .iter()
                    .map(|wrap| wrap.prepared.clone())
                    .collect::<Vec<_>>(),
            )
            .await?;
        }
    }
    if let Some(remotes) = &remote_objects {
        let expected = exact_owned_remote(remotes, &publication.entry_ref.object)?;
        persistence
            .database
            .mark_remote_object_uploaded(expected)
            .await?;
    }
    match cloud_home.set_access(plan.desired_access.clone()).await? {
        CloudAccessOutcome::Absent(_) => {}
        CloudAccessOutcome::Present(_) => {
            return Err(InviteError::InvalidDurableMutation(
                "provider returned present outcome for absent access request".to_string(),
            ))
        }
    }
    if matches!(progress, MembershipMutationProgress::Pending) {
        progress = MembershipMutationProgress::RevokeAccessRemoved;
        persistence.record_progress(&progress).await?;
    }
    match plan.publication.clone() {
        RevokeMembershipPublication::Direct { publication } => {
            store_objects::create_exact_object(storage, &publication.head_object)
                .await
                .map_err(|error| InviteError::Crypto(error.to_string()))?;
            store_objects::load_membership_head_ref(
                storage,
                store_root_hash,
                &publication.head_ref,
                &author,
            )
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
            validated_chain.activate_head_ref(publication.head_ref.clone())?;
            persistence
                .database
                .record_direct_revoke_activation(
                    persistence.intent_hash,
                    encode_membership_progress(&MembershipMutationProgress::RevokeActivated {
                        candidate: None,
                    })?,
                    keyring.current_generation(),
                )
                .await?;
            pending_rotation
                .mark_committed_mutation(keyring.current_generation(), persistence.intent_hash)
                .map_err(InviteError::InvalidDurableMutation)?;
            *chain = validated_chain;
            Ok(keyring)
        }
        RevokeMembershipPublication::StoreActivated {
            transition,
            mut candidate,
            publication,
        } => {
            let initial_remotes = candidate
                .merge_membership_activation_remote_objects(
                    &transition,
                    &publication,
                    &plan
                        .wraps
                        .iter()
                        .map(|wrap| wrap.prepared.clone())
                        .collect::<Vec<_>>(),
                )
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
            crate::sync::store::operations::upload_commit(storage, &candidate)
                .await
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
            persistence
                .database
                .mark_remote_object_uploaded(exact_owned_remote(
                    &initial_remotes,
                    &candidate.reference.object,
                )?)
                .await?;
            loop {
                let previous_candidate = candidate.as_ref().clone();
                let current_remotes = candidate
                    .merge_membership_activation_remote_objects(
                        &transition,
                        &publication,
                        &plan
                            .wraps
                            .iter()
                            .map(|wrap| wrap.prepared.clone())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
                let outcome = Box::pin(publish_prepared_merge_membership_activation(
                    persistence.database,
                    storage,
                    &root,
                    &author,
                    &transition,
                    &publication,
                    candidate.clone(),
                    crate::sync::store::operations::StoreMembershipJournalCompletion::RotationMutation {
                        intent_hash: persistence.intent_hash,
                        progress_bytes: encode_membership_progress(
                            &MembershipMutationProgress::RevokeActivated {
                                candidate: Some(candidate.reference.clone()),
                            },
                        )?,
                        generation: keyring.current_generation(),
                        remote_objects: current_remotes.clone(),
                    },
                ))
                .await?;
                match outcome {
                    crate::sync::store::operations::StoreOperationPublicationOutcome::Activated(reference) => {
                        if reference != candidate.reference {
                            return Err(InviteError::InvalidDurableMutation(
                                "membership removal activated another Store candidate".to_string(),
                            ));
                        }
                        validated_chain.activate_head_ref(publication.head_ref.clone())?;
                        pending_rotation
                            .mark_committed_mutation(
                                keyring.current_generation(),
                                persistence.intent_hash,
                            )
                            .map_err(InviteError::InvalidDurableMutation)?;
                        *chain = validated_chain;
                        return Ok(keyring);
                    }
                    crate::sync::store::operations::StoreOperationPublicationOutcome::RepreparedCandidate(
                        replacement,
                    ) => {
                        if replacement.reference != candidate.reference {
                            return Err(InviteError::InvalidDurableMutation(
                                "membership removal reprepare changed its signed candidate"
                                    .to_string(),
                            ));
                        }
                        let previous_remotes = previous_candidate
                            .merge_membership_activation_remote_objects(
                                &transition,
                                &publication,
                                &plan
                                    .wraps
                                    .iter()
                                    .map(|wrap| wrap.prepared.clone())
                                    .collect::<Vec<_>>(),
                            )
                            .map_err(|error| {
                                InviteError::InvalidDurableMutation(error.to_string())
                            })?;
                        candidate = replacement;
                        let replacement_remotes = candidate
                            .merge_membership_activation_remote_objects(
                                &transition,
                                &publication,
                                &plan
                                    .wraps
                                    .iter()
                                    .map(|wrap| wrap.prepared.clone())
                                    .collect::<Vec<_>>(),
                            )
                            .map_err(|error| {
                                InviteError::InvalidDurableMutation(error.to_string())
                            })?;
                        let previous_head = previous_candidate.head_ref();
                        let replacement_head = candidate.head_ref();
                        plan.publication = RevokeMembershipPublication::StoreActivated {
                            transition: transition.clone(),
                            candidate: candidate.clone(),
                            publication: publication.clone(),
                        };
                        let plan_bytes = encode_membership_mutation(
                            &MembershipMutationPlan::Revoke(plan.clone()),
                        )?;
                        let previous_intent_hash = persistence.intent_hash;
                        let replacement_intent_hash = persistence
                            .database
                            .adopt_merge_membership_candidate_head(
                                persistence.intent_hash,
                                plan_bytes,
                                exact_owned_remote(&previous_remotes, &previous_head.object)?,
                                exact_owned_remote(&replacement_remotes, &replacement_head.object)?,
                                Some(keyring.current_generation()),
                            )
                            .await?;
                        pending_rotation
                            .replace_candidate_mutation(
                                keyring.current_generation(),
                                previous_intent_hash,
                                replacement_intent_hash,
                            )
                            .map_err(InviteError::InvalidDurableMutation)?;
                        persistence.intent_hash = replacement_intent_hash;
                    }
                    crate::sync::store::operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                        candidate: returned,
                        nonactivation,
                    } => {
                        if *returned != *candidate {
                            return Err(InviteError::InvalidDurableMutation(
                                "membership removal nonactivation returned another candidate"
                                    .to_string(),
                            ));
                        }
                        let verified = *nonactivation;
                        let durable = verified.clone().into_durable();
                        progress = MembershipMutationProgress::RevokeCandidateNonactivating {
                            nonactivation: durable,
                        };
                        let progress_bytes = encode_membership_progress(&progress)?;
                        let (candidate_objects, retained) =
                            plan.candidate_cleanup_objects();
                        let cleanup = persistence
                            .database
                            .begin_membership_candidate_nonactivation(
                                persistence.intent_hash,
                                candidate.reference.clone(),
                                candidate_objects,
                                retained,
                                progress_bytes,
                                verified,
                            )
                            .await?;
                        finish_nonactivating_revoke_with_targets(
                            &plan,
                            &persistence,
                            storage,
                            cloud_home,
                            cleanup,
                        )
                        .await?;
                        pending_rotation
                            .remove_candidate(
                                keyring.current_generation(),
                                persistence.intent_hash,
                            )
                            .map_err(InviteError::InvalidDurableMutation)?;
                        return Err(InviteError::InvalidDurableMutation(
                            "membership removal candidate did not activate".to_string(),
                        ));
                    }
                    crate::sync::store::operations::StoreOperationPublicationOutcome::Nonactivated(
                        reference,
                    ) => {
                        return Err(InviteError::InvalidDurableMutation(format!(
                            "membership removal candidate {} lost without exact evidence",
                            reference.commit_hash
                        )))
                    }
                    crate::sync::store::operations::StoreOperationPublicationOutcome::Reprepared => {
                        return Err(InviteError::InvalidDurableMutation(
                            "membership removal returned acknowledgement-only reprepare state"
                                .to_string(),
                        ))
                    }
                }
            }
        }
    }
}

async fn complete_already_removed_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: store_commit::ObjectHash,
    chain: &MembershipChain,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    store_id: &str,
) -> Result<EncryptionService, InviteError> {
    if !chain
        .current_members()
        .into_iter()
        .any(|(_, role)| role == MemberRole::Owner)
    {
        return Err(InviteError::LastOwner);
    }
    let references = chain.wrapped_key_authority_for(&keys::public_key_hex(owner_keypair))?;
    let keyring = unwrap_store_keyring_for_refs(
        storage,
        store_root_hash,
        owner_keypair,
        store_id,
        &references,
    )
    .await?;
    revoke_provider_access(
        cloud_home,
        CloudAccessState::Absent {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email: None,
        },
    )
    .await?;
    Ok(keyring)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn revoke_member_durable(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: store_commit::ObjectHash,
    chain: &mut MembershipChain,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    store_id: &str,
    timestamp: &str,
    current_encryption: &EncryptionService,
    pending_rotation: &cloud_storage::PendingRotation,
    database: &StoreDatabase,
) -> Result<EncryptionService, InviteError> {
    let _mutation = database.lock_membership_mutation().await;
    let (plan, progress, intent_hash) = match database.outbound_membership_mutation().await? {
        Some(row) => {
            let intent_hash = row.intent_hash;
            let (pending, progress) = decode_membership_mutation(row)?;
            let MembershipMutationPlan::Revoke(plan) = pending else {
                return Err(InviteError::PendingMutation(
                    "an invitation is pending".to_string(),
                ));
            };
            if !revoke_plan_matches_request(&plan, owner_keypair, revokee_pubkey, store_id) {
                return Err(InviteError::PendingMutation(
                    "the pending removal has different immutable inputs".to_string(),
                ));
            }
            (plan, progress, intent_hash)
        }
        None => {
            let is_current = chain
                .current_members()
                .iter()
                .any(|(pubkey, _)| pubkey == revokee_pubkey);
            let was_removed = chain.entries().iter().any(|entry| {
                matches!(
                    &entry.change,
                    MembershipChange::RemoveMember { user_pubkey, .. }
                        if user_pubkey == revokee_pubkey
                )
            });
            if !is_current && was_removed {
                return complete_already_removed_member(
                    storage,
                    cloud_home,
                    store_root_hash,
                    chain,
                    owner_keypair,
                    revokee_pubkey,
                    store_id,
                )
                .await;
            }
            let stream_id = select_mutation_author_stream(database, chain, owner_keypair).await?;
            let plan = Box::pin(build_revoke_mutation(
                storage,
                database,
                store_root_hash,
                chain,
                owner_keypair,
                stream_id,
                revokee_pubkey,
                store_id,
                timestamp,
                current_encryption,
            ))
            .await?;
            plan.validate_closed_shape()?;
            let encoded =
                encode_membership_mutation(&MembershipMutationPlan::Revoke(plan.clone()))?;
            let progress = MembershipMutationProgress::Pending;
            let progress_bytes = encode_membership_progress(&progress)?;
            let pending_generation =
                EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                    .map_err(|error| {
                        InviteError::Crypto(format!("parse rotated keyring: {error}"))
                    })?
                    .current_generation();
            let intent_hash = match plan.candidate_remote_objects()? {
                Some(remotes) => {
                    database
                        .stage_membership_candidate_mutation(
                            encoded,
                            progress_bytes,
                            remotes,
                            Some(pending_generation),
                        )
                        .await?
                }
                None => {
                    database
                        .stage_membership_mutation(
                            encoded,
                            progress_bytes,
                            Some(pending_generation),
                        )
                        .await?
                }
            };
            (plan, progress, intent_hash)
        }
    };
    let pending_generation = EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
        .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?
        .current_generation();
    match &progress {
        MembershipMutationProgress::RevokeActivated { .. } => {
            pending_rotation.mark_committed_mutation(pending_generation, intent_hash)
        }
        _ => pending_rotation.mark_candidate(pending_generation, intent_hash),
    }
    .map_err(InviteError::InvalidDurableMutation)?;
    Box::pin(execute_revoke_mutation(
        storage,
        cloud_home,
        store_root_hash,
        chain,
        plan,
        progress,
        MutationPersistence {
            database,
            intent_hash,
        },
        pending_rotation,
    ))
    .await
}

pub(crate) async fn complete_revoke_rotation_adoption(
    database: &StoreDatabase,
    pending_rotation: &cloud_storage::PendingRotation,
    adopted_generation: u64,
) -> Result<(), InviteError> {
    let _mutation = database.lock_membership_mutation().await;
    let row = database
        .outbound_membership_mutation()
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(
                "activated removal journal is absent during key adoption".to_string(),
            )
        })?;
    let intent_hash = row.intent_hash;
    let (plan, progress) = decode_membership_mutation(row)?;
    let MembershipMutationPlan::Revoke(plan) = plan else {
        return Err(InviteError::InvalidDurableMutation(
            "key adoption found another membership mutation".to_string(),
        ));
    };
    if !matches!(progress, MembershipMutationProgress::RevokeActivated { .. }) {
        return Err(InviteError::InvalidDurableMutation(
            "key adoption found a removal that is not activated".to_string(),
        ));
    }
    let planned_generation = EncryptionService::from_keyring_payload(plan.keyring_payload)
        .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?
        .current_generation();
    if planned_generation != adopted_generation {
        return Err(InviteError::InvalidDurableMutation(format!(
            "adopted key generation {adopted_generation} differs from the activated removal generation {planned_generation}"
        )));
    }
    let gate = database
        .complete_local_rotation_adoption(intent_hash, adopted_generation)
        .await?;
    pending_rotation.install_durable_gate(gate);
    Ok(())
}

async fn revoke_provider_access(
    cloud_home: &dyn CloudHome,
    absent_access: CloudAccessState,
) -> Result<bool, InviteError> {
    match cloud_home.set_access(absent_access).await? {
        CloudAccessOutcome::Absent(RevokeOutcome::Revoked) => Ok(true),
        CloudAccessOutcome::Absent(RevokeOutcome::Unsupported) => {
            tracing::info!(
                "cloud provider offers no per-member credential revocation; chain revocation and store key rotation protect later content",
            );
            Ok(false)
        }
        CloudAccessOutcome::Present(_) => Err(InviteError::Crypto(
            "provider returned present outcome for absent access request".to_string(),
        )),
    }
}
