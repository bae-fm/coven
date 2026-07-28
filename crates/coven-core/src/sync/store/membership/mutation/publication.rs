use crate::database::{VerifiedMergeMembershipObjects, LOCAL_DEVICE_ID_STATE_KEY};
use crate::keys::UserKeypair;
use crate::sync::membership::{
    self, AuthorHead, MembershipChain, MembershipChange, MembershipEntry, MembershipHeadRef,
};
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store::operations;
use crate::sync::store_commit::{
    self, membership_entry_semantic_prefix, membership_head_slot_prefix, SuccessorLink,
};
use crate::sync::store_objects;
use crate::sync::wrapped_store_key::{
    load_wrapped_store_key, PreparedWrappedStoreKey, WrappedStoreKeyRef,
};

use super::{InviteError, PreparedMembershipPublication, PreparedMembershipTransition};

pub(super) fn chain_with_exact_entry(
    chain: &MembershipChain,
    entry: &MembershipEntry,
) -> Result<MembershipChain, InviteError> {
    let coord = entry.coord();
    if let Some((_, stored)) = chain
        .entries_with_coords()
        .find(|(stored_coord, _)| **stored_coord == coord)
    {
        if stored != entry {
            return Err(InviteError::InvalidDurableMutation(format!(
                "committed entry at {coord:?} differs from the durable plan"
            )));
        }
        return Ok(chain.clone());
    }
    let mut validated = chain.clone();
    validated.add_entry_at(coord, entry.clone())?;
    Ok(validated)
}

pub(crate) fn validate_prepared_publication(
    publication: &PreparedMembershipPublication,
) -> Result<(), InviteError> {
    validate_prepared_transition(&PreparedMembershipTransition {
        entry: publication.entry.clone(),
        entry_ref: publication.entry_ref.clone(),
        entry_object: publication.entry_object.clone(),
        transition: membership::MergeMembershipHeadTransition {
            body: publication.head.body.clone(),
            head_slot: publication.head_ref.object.slot().clone(),
        },
    })?;
    let coord = publication.entry.coord();
    if publication.entry_ref.coord != coord
        || publication.entry_ref.object != *publication.entry_object.reference()
        || publication.head.body.entry != publication.entry_ref
        || publication.head.entry_coord() != coord
        || publication.head_ref.coord != coord
        || publication.head_ref.head_hash != publication.head.head_hash()
        || publication.head_ref.object != *publication.head_object.reference()
        || publication.head_object.stored_bytes()
            != serde_json::to_vec(&publication.head)
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership publication does not bind one exact entry and head".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_prepared_transition(
    transition: &PreparedMembershipTransition,
) -> Result<(), InviteError> {
    let coord = transition.entry.coord();
    let entry_bytes = serde_json::to_vec(&transition.entry)
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
        InviteError::InvalidDurableMutation("membership sequence is exhausted".to_string())
    })?;
    let entry_key = format!(
        "{}.json",
        membership_entry_semantic_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
            coord.entry_hash,
        )
    );
    let head_key = format!(
        "{}.json",
        membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        )
    );
    let successor_key = format!(
        "{}.json",
        membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            next_sequence,
        )
    );
    if transition.entry_ref.coord != transition.entry.coord()
        || transition.entry_ref.object != *transition.entry_object.reference()
        || transition.entry_object.stored_bytes() != entry_bytes
        || transition.entry_ref.object.slot().logical_key() != entry_key
        || transition.transition.body.entry != transition.entry_ref
        || transition.transition.body.resolutions != transition.entry.resolution_dependencies
        || transition.transition.head_slot.logical_key() != head_key
        || transition.transition.body.successor.next_slot.logical_key() != successor_key
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership transition does not bind its exact entry".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn prepare_membership_publication(
    storage: &dyn SyncStorage,
    database: &StoreDatabase,
    store_root_hash: store_commit::ObjectHash,
    chain: &MembershipChain,
    entry: MembershipEntry,
    owner_keypair: &UserKeypair,
) -> Result<PreparedMembershipPublication, InviteError> {
    let prepared = prepare_membership_transition(
        storage,
        database,
        store_root_hash,
        chain,
        entry,
        owner_keypair,
    )
    .await?;
    finish_membership_transition(
        storage,
        database,
        store_root_hash,
        prepared,
        membership::MembershipHeadActivation::Direct,
        owner_keypair,
    )
    .await
}

pub(crate) async fn prepare_membership_transition(
    storage: &dyn SyncStorage,
    database: &StoreDatabase,
    store_root_hash: store_commit::ObjectHash,
    chain: &MembershipChain,
    entry: MembershipEntry,
    owner_keypair: &UserKeypair,
) -> Result<PreparedMembershipTransition, InviteError> {
    let device_id = database
        .sqlite()
        .get_protocol_state(LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(
                "local Store device registration is absent".to_string(),
            )
        })?;
    let (root, registration_ref, registration, _) =
        operations::load_local_store_authority(database, &device_id, owner_keypair)
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    if root.store_root_hash != store_root_hash
        || registration.author_pubkey != entry.author_pubkey
        || registration_ref.device_id != registration.device_id
    {
        return Err(InviteError::InvalidDurableMutation(
            "membership author differs from the active exact device registration".to_string(),
        ));
    }
    let (entry_object, entry_ref) =
        store_objects::prepare_membership_entry(storage, store_root_hash, &entry)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
    let coord = entry.coord();
    let predecessor = chain
        .head_ref_for_stream(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
        )
        .cloned();
    let current_slot = match predecessor.as_ref() {
        Some(reference) => {
            let loaded = store_objects::load_membership_head_ref(
                storage,
                store_root_hash,
                reference,
                &registration,
            )
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
            loaded.value.body.successor.next_slot.clone()
        }
        None => match chain.membership_anchor(&coord.author_owner_grant) {
            Some(store_commit::GrantStreamAnchor::StoreMembership { first_slot }) => {
                first_slot.clone()
            }
            Some(
                store_commit::GrantStreamAnchor::OwnerRecovery { .. }
                | store_commit::GrantStreamAnchor::CircleControl { .. }
                | store_commit::GrantStreamAnchor::CircleRoster { .. }
                | store_commit::GrantStreamAnchor::CircleMetadata { .. },
            ) => {
                return Err(InviteError::InvalidDurableMutation(format!(
                    "Owner grant {} uses another domain's anchor as its membership stream",
                    coord.author_owner_grant
                )))
            }
            None => {
                return Err(InviteError::InvalidDurableMutation(format!(
                    "Owner grant {} has no activated membership stream anchor",
                    coord.author_owner_grant
                )))
            }
        },
    };
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
        InviteError::InvalidDurableMutation("membership head sequence overflow".to_string())
    })?;
    let next_prefix = membership_head_slot_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        next_sequence,
    );
    let next_slot = storage
        .allocate_protocol_slot(&context, &next_prefix, ".json")
        .await?;
    let anchor = chain
        .membership_anchor(&coord.author_owner_grant)
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(format!(
                "Owner grant {} has no activated membership stream anchor",
                coord.author_owner_grant
            ))
        })?;
    let transition = membership::MergeMembershipHeadTransition {
        body: membership::MembershipHeadBody {
            author_registration: registration_ref.clone(),
            entry: entry_ref.clone(),
            predecessor: predecessor.clone(),
            resolutions: entry.resolution_dependencies.clone(),
            successor: SuccessorLink {
                activation: store_commit::StreamActivation::grant_authorized(
                    root.store_root_hash,
                    registration_ref.clone(),
                    coord.author_owner_grant.clone(),
                    anchor.clone(),
                )
                .activation_id(),
                predecessor: predecessor
                    .as_ref()
                    .map(|reference| reference.object.clone()),
                next_slot,
            },
        },
        head_slot: current_slot,
    };
    Ok(PreparedMembershipTransition {
        entry,
        entry_ref,
        entry_object,
        transition,
    })
}

pub(crate) async fn finish_membership_transition(
    storage: &dyn SyncStorage,
    database: &StoreDatabase,
    store_root_hash: store_commit::ObjectHash,
    prepared: PreparedMembershipTransition,
    activation: membership::MembershipHeadActivation,
    owner_keypair: &UserKeypair,
) -> Result<PreparedMembershipPublication, InviteError> {
    let device_id = database
        .sqlite()
        .get_protocol_state(LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(
                "local Store device registration is absent".to_string(),
            )
        })?;
    let (root, registration_ref, registration, device_signer) =
        operations::load_local_store_authority(database, &device_id, owner_keypair)
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    if root.store_root_hash != store_root_hash
        || registration.author_pubkey != prepared.entry.author_pubkey
        || registration_ref != prepared.transition.body.author_registration
    {
        return Err(InviteError::InvalidDurableMutation(
            "membership transition author differs from the active exact device registration"
                .to_string(),
        ));
    }
    let head = AuthorHead::signed(
        prepared.entry.store_id.clone(),
        prepared.transition.body.clone(),
        activation,
        &device_signer,
    );
    let coord = prepared.entry.coord();
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let head_prefix = membership_head_slot_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
    );
    let head_bytes = serde_json::to_vec(&head).map_err(|error| {
        InviteError::InvalidDurableMutation(format!("serialize membership head: {error}"))
    })?;
    let head_object = storage.prepare_protocol_object(
        &context,
        prepared.transition.head_slot.clone(),
        &head_prefix,
        head_bytes,
    )?;
    let head_ref = MembershipHeadRef {
        coord,
        head_hash: head.head_hash(),
        object: head_object.reference().clone(),
    };
    let publication = PreparedMembershipPublication {
        entry: prepared.entry,
        entry_ref: prepared.entry_ref,
        entry_object: prepared.entry_object,
        head,
        head_ref,
        head_object,
    };
    validate_prepared_publication(&publication)?;
    Ok(publication)
}

pub(crate) async fn publish_prepared_merge_membership_authority(
    storage: &dyn SyncStorage,
    store_root_hash: store_commit::ObjectHash,
    transition: &PreparedMembershipTransition,
    wraps: &[PreparedWrappedStoreKey],
) -> Result<(), InviteError> {
    validate_prepared_transition(transition)?;
    let expected_wraps: Vec<&WrappedStoreKeyRef> = match &transition.entry.change {
        MembershipChange::SetMember { wrapped_key, .. } => vec![wrapped_key],
        MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.iter().collect(),
        MembershipChange::Founder { .. }
        | MembershipChange::ProviderAdmin
        | MembershipChange::ResolutionActivation { .. } => Vec::new(),
    };
    if expected_wraps.len() != wraps.len()
        || expected_wraps
            .iter()
            .zip(wraps)
            .any(|(expected, prepared)| **expected != prepared.reference)
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared Merge membership wraps differ from their exact transition".to_string(),
        ));
    }
    for prepared in wraps {
        prepared.validate()?;
        store_objects::create_exact_object(storage, &prepared.object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        load_wrapped_store_key(storage, store_root_hash, &prepared.reference).await?;
    }
    store_objects::create_exact_object(storage, &transition.entry_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    store_objects::load_membership_entry_ref(storage, store_root_hash, &transition.entry_ref)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    Ok(())
}

pub(crate) async fn publish_prepared_merge_membership_activation(
    database: &crate::sync::store::StoreDatabase,
    storage: &dyn SyncStorage,
    root: &store_commit::StoreRootRef,
    author: &store_commit::StoreDeviceRegistration,
    transition: &PreparedMembershipTransition,
    publication: &PreparedMembershipPublication,
    candidate: Box<operations::PreparedStoreOperationCommit>,
    completion: operations::StoreMembershipJournalCompletion,
) -> Result<operations::StoreOperationPublicationOutcome, InviteError> {
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, root)
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    publish_prepared_merge_membership_activation_with_history(
        database,
        &mut history_verifier,
        author,
        transition,
        publication,
        candidate,
        completion,
    )
    .await
}

pub(crate) async fn publish_prepared_merge_membership_activation_with_history(
    database: &crate::sync::store::StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    author: &store_commit::StoreDeviceRegistration,
    transition: &PreparedMembershipTransition,
    publication: &PreparedMembershipPublication,
    candidate: Box<operations::PreparedStoreOperationCommit>,
    completion: operations::StoreMembershipJournalCompletion,
) -> Result<operations::StoreOperationPublicationOutcome, InviteError> {
    let storage = history_verifier.storage();
    let root = history_verifier.root();
    validate_prepared_transition(transition)?;
    validate_prepared_publication(publication)?;
    candidate
        .validate_closed_shape()
        .map_err(InviteError::InvalidDurableMutation)?;
    if candidate.commit.control()
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
        || !publication.head.verify(author)
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared Merge membership head differs from its exact Store activation".to_string(),
        ));
    }
    store_objects::create_exact_object(storage, &publication.head_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    store_objects::load_membership_head_ref(
        storage,
        root.store_root_hash,
        &publication.head_ref,
        author,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    database
        .mark_remote_object_uploaded(
            completion
                .remote_object(&publication.head_ref.object)
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?,
        )
        .await
        .map_err(|error| {
            InviteError::InvalidDurableMutation(format!(
                "record uploaded Merge membership head: {error}"
            ))
        })?;
    let membership_objects = VerifiedMergeMembershipObjects::verify(
        &candidate.commit,
        &candidate.reference,
        &transition.entry,
        &publication.head,
        publication.head_ref.clone(),
    )?;
    let _authorship = database.author_own_stream().await;
    operations::publish_prepared_with_history(
        database,
        history_verifier,
        candidate,
        Some(membership_objects),
        Some(completion),
    )
    .await
    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))
}
