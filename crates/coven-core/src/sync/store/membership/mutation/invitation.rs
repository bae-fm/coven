use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeJoinInfo};
use crate::sync::blocking;
use crate::sync::membership::{
    AuthorStreamId, MemberRole, MembershipChain, MembershipChange, MembershipError,
};
use crate::sync::storage::SyncStorage;
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::ObjectHash;
use crate::sync::store_objects;
use crate::sync::wrapped_store_key::{prepare_wrapped_store_key, WrappedStoreKeyRef};

use super::{
    chain_with_exact_entry, decode_membership_mutation, ed25519_hex_to_x25519,
    encode_membership_mutation, encode_membership_progress, load_authorized_owner_keyring,
    prepare_membership_publication, select_mutation_author_stream, signed_wrapped_key,
    validate_prepared_publication, InviteError, InviteMutationPlan, MembershipMutationPlan,
    MembershipMutationProgress, MutationPersistence,
};

async fn build_invite_mutation(
    storage: &dyn SyncStorage,
    database: &StoreDatabase,
    store_root_hash: ObjectHash,
    chain: &MembershipChain,
    owner_keypair: &UserKeypair,
    stream_id: AuthorStreamId,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    timestamp: &str,
) -> Result<InviteMutationPlan, InviteError> {
    if role == MemberRole::Owner {
        return Err(MembershipError::OwnerPromotionRequired.into());
    }
    if chain.store_id() != Some(store_id) {
        return Err(InviteError::InvalidDurableMutation(format!(
            "membership chain store {:?} differs from requested store {store_id:?}",
            chain.store_id()
        )));
    }
    let invitee_x25519_pk = ed25519_hex_to_x25519(invitee_ed25519_pubkey)?;
    let owner_refs = chain.wrapped_key_authority_for(&keys::public_key_hex(owner_keypair))?;
    let authorized_keyring = load_authorized_owner_keyring(
        storage,
        store_root_hash,
        owner_keypair,
        store_id,
        &owner_refs,
        encryption,
    )
    .await?;
    let signing_store_id = store_id.to_string();
    let signing_recipient = invitee_ed25519_pubkey.to_string();
    let signing_keyring = authorized_keyring.clone();
    let signing_owner = owner_keypair.clone();
    let signed = blocking::run(move || {
        signed_wrapped_key(
            &signing_store_id,
            &signing_recipient,
            &invitee_x25519_pk,
            &signing_keyring,
            &signing_owner,
        )
    })
    .await
    .map_err(|error| InviteError::Crypto(format!("seal invited member Store key: {error}")))??;
    let wrapped_key =
        prepare_wrapped_store_key(storage, store_root_hash, invitee_ed25519_pubkey, signed).await?;
    let entry = chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
        owner_keypair,
        stream_id,
        invitee_ed25519_pubkey.to_string(),
        invitee_email.map(str::to_string),
        role.clone(),
        None,
        wrapped_key.reference.clone(),
        timestamp.to_string(),
    )?;
    let publication = prepare_membership_publication(
        storage,
        database,
        store_root_hash,
        chain,
        entry,
        owner_keypair,
    )
    .await?;
    Ok(InviteMutationPlan {
        publication,
        invitee_pubkey: invitee_ed25519_pubkey.to_string(),
        invitee_email: invitee_email.map(str::to_string),
        role,
        desired_access: CloudAccessState::Present {
            member_pubkey: invitee_ed25519_pubkey.to_string(),
            provider_account_email: invitee_email.map(str::to_string),
        },
        wrapped_key,
    })
}

fn invite_plan_matches_request(
    plan: &InviteMutationPlan,
    owner_keypair: &UserKeypair,
    invitee_pubkey: &str,
    invitee_email: Option<&str>,
    role: &MemberRole,
    store_id: &str,
) -> bool {
    plan.publication.entry.author_pubkey == hex::encode(owner_keypair.public_key())
        && plan.publication.entry.store_id == store_id
        && plan.invitee_pubkey == invitee_pubkey
        && plan.invitee_email.as_deref() == invitee_email
        && &plan.role == role
        && plan.desired_access
            == (CloudAccessState::Present {
                member_pubkey: invitee_pubkey.to_string(),
                provider_account_email: invitee_email.map(str::to_string),
            })
        && matches!(
            &plan.publication.entry.change,
            MembershipChange::SetMember {
                user_pubkey,
                provider_account_email,
                role: entry_role,
                ..
            } if user_pubkey == invitee_pubkey
                && provider_account_email.as_deref() == invitee_email
                && entry_role.role() == *role
        )
}

async fn execute_invite_mutation(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: ObjectHash,
    chain: &mut MembershipChain,
    plan: InviteMutationPlan,
    mut progress: MembershipMutationProgress,
    persistence: MutationPersistence<'_>,
) -> Result<(CloudHomeJoinInfo, WrappedStoreKeyRef), InviteError> {
    validate_prepared_publication(&plan.publication)?;
    let mut validated_chain = chain_with_exact_entry(chain, &plan.publication.entry)?;
    let root = persistence
        .database
        .sqlite()
        .local_store_root_ref()
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation("local Store root reference is absent".to_string())
        })?;
    let author = store_objects::load_registration_ref(
        storage,
        &root,
        &plan.publication.head.body.author_registration,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?
    .value;
    if !plan.publication.head.verify(&author) {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership head has an invalid certified-device signature".to_string(),
        ));
    }
    let wrapped = plan.wrapped_key.validate()?;
    let authority_matches = matches!(
        &plan.publication.entry.change,
        MembershipChange::SetMember { wrapped_key, .. }
            if wrapped_key == &plan.wrapped_key.reference
    );
    if !authority_matches
        || wrapped.author_pubkey != plan.publication.entry.author_pubkey
        || wrapped
            .verify_and_unwrap(
                &plan.publication.entry.store_id,
                &plan.invitee_pubkey,
                std::iter::once(plan.publication.entry.author_pubkey.as_str()),
            )
            .is_err()
    {
        return Err(InviteError::InvalidDurableMutation(
            "planned invitation wrap is not bound to its exact entry, recipient, and author"
                .to_string(),
        ));
    }
    let outcome = cloud_home.set_access(plan.desired_access.clone()).await?;
    let CloudAccessOutcome::Present(observed_join_info) = outcome else {
        return Err(InviteError::InvalidDurableMutation(
            "provider returned absent outcome for present access request".to_string(),
        ));
    };
    let join_info = match progress {
        MembershipMutationProgress::Pending => {
            progress = MembershipMutationProgress::InviteGranted {
                join_info: observed_join_info.clone(),
            };
            persistence.record_progress(&progress).await?;
            observed_join_info
        }
        MembershipMutationProgress::InviteGranted { join_info } => {
            if join_info != observed_join_info {
                return Err(InviteError::InvalidDurableMutation(
                    "provider returned different join information while verifying persisted access"
                        .to_string(),
                ));
            }
            join_info
        }
        MembershipMutationProgress::RevokeAccessRemoved
        | MembershipMutationProgress::RevokeCandidateNonactivating { .. }
        | MembershipMutationProgress::ResolutionCandidateNonactivating { .. }
        | MembershipMutationProgress::RevokeActivated { .. }
        | MembershipMutationProgress::ResolutionActivated { .. } => {
            return Err(InviteError::InvalidDurableMutation(
                "invitation carries member-removal progress".to_string(),
            ))
        }
    };
    store_objects::create_exact_object(storage, &plan.wrapped_key.object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    store_objects::create_exact_object(storage, &plan.publication.entry_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    store_objects::load_membership_entry_ref(storage, store_root_hash, &plan.publication.entry_ref)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    store_objects::create_exact_object(storage, &plan.publication.head_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    store_objects::load_membership_head_ref(
        storage,
        store_root_hash,
        &plan.publication.head_ref,
        &author,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    validated_chain.activate_head_ref(plan.publication.head_ref.clone())?;
    *chain = validated_chain;
    persistence.complete().await?;
    Ok((join_info, plan.wrapped_key.reference))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_invitation_with_encryption_durable(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: ObjectHash,
    chain: &mut MembershipChain,
    owner_keypair: &UserKeypair,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    timestamp: &str,
    database: &StoreDatabase,
) -> Result<(CloudHomeJoinInfo, WrappedStoreKeyRef), InviteError> {
    let _mutation = database.sqlite().lock_membership_mutation().await;
    let (plan, progress, intent_hash) = match database.outbound_membership_mutation().await? {
        Some(row) => {
            let intent_hash = row.intent_hash;
            let (pending, progress) = decode_membership_mutation(row)?;
            let MembershipMutationPlan::Invite(plan) = pending else {
                return Err(InviteError::PendingMutation(
                    "a member removal is pending".to_string(),
                ));
            };
            if !invite_plan_matches_request(
                &plan,
                owner_keypair,
                invitee_ed25519_pubkey,
                invitee_email,
                &role,
                store_id,
            ) {
                return Err(InviteError::PendingMutation(
                    "the pending invitation has different immutable inputs".to_string(),
                ));
            }
            (plan, progress, intent_hash)
        }
        None => {
            let stream_id = select_mutation_author_stream(database, chain, owner_keypair).await?;
            let plan = Box::pin(build_invite_mutation(
                storage,
                database,
                store_root_hash,
                chain,
                owner_keypair,
                stream_id,
                invitee_ed25519_pubkey,
                invitee_email,
                role,
                encryption,
                store_id,
                timestamp,
            ))
            .await?;
            let encoded =
                encode_membership_mutation(&MembershipMutationPlan::Invite(plan.clone()))?;
            let progress = MembershipMutationProgress::Pending;
            let intent_hash = database
                .stage_membership_mutation(encoded, encode_membership_progress(&progress)?, None)
                .await?;
            (plan, progress, intent_hash)
        }
    };
    Box::pin(execute_invite_mutation(
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
    ))
    .await
}
