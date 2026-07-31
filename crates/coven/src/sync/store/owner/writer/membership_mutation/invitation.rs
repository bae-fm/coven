use crate::encryption::EncryptionService;
use crate::keys;
use crate::protocol::membership::{
    AuthorStreamId, MemberRole, MembershipChain, MembershipChange, MembershipError,
};
use crate::protocol::wrapped_store_key::WrappedStoreKey;
use crate::protocol::wrapped_store_key::WrappedStoreKeyRef;
use crate::storage as store_objects;
use crate::storage::cloud::{CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeJoinInfo};
use crate::sync::blocking;

use super::{
    chain_with_exact_entry, decode_membership_mutation, encode_membership_mutation,
    encode_membership_progress, select_mutation_author_stream, validate_prepared_publication,
    AuthorizedMembershipPublication, InviteError, InviteMutationPlan, MembershipMutationPlan,
    MembershipMutationProgress, MutationPersistence,
};

async fn build_invite_mutation(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    chain: &MembershipChain,
    stream_id: AuthorStreamId,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    timestamp: &str,
) -> Result<InviteMutationPlan, InviteError> {
    let owner_keypair = operation.writer.identity.clone();
    if role == MemberRole::Owner {
        return Err(MembershipError::OwnerPromotionRequired.into());
    }
    if chain.store_id() != Some(store_id) {
        return Err(InviteError::InvalidDurableMutation(format!(
            "membership chain store {:?} differs from requested store {store_id:?}",
            chain.store_id()
        )));
    }
    let invitee_x25519_pk = keys::ed25519_hex_to_x25519_public_key(invitee_ed25519_pubkey)?;
    let authorized_keyring = operation
        .open_keyring_or_for_membership(chain, encryption)
        .await?;
    let signing_store_id = store_id.to_string();
    let signing_recipient = invitee_ed25519_pubkey.to_string();
    let signing_keyring = authorized_keyring.clone();
    let signing_owner = owner_keypair.clone();
    let signed = blocking::run(move || {
        WrappedStoreKey::seal_keyring(
            &signing_store_id,
            &signing_recipient,
            &invitee_x25519_pk,
            &signing_keyring,
            &signing_owner,
        )
        .map_err(|error| InviteError::Crypto(format!("serialize invited member keyring: {error}")))
    })
    .await
    .map_err(|error| InviteError::Crypto(format!("seal invited member Store key: {error}")))??;
    let wrapped_key = operation
        .prepare_wrapped_key(invitee_ed25519_pubkey, signed)
        .await?;
    let entry = chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
        &owner_keypair,
        stream_id,
        invitee_ed25519_pubkey.to_string(),
        invitee_email.map(str::to_string),
        role.clone(),
        None,
        wrapped_key.reference.clone(),
        timestamp.to_string(),
    )?;
    let publication = AuthorizedMembershipPublication::new(operation)
        .prepare_publication(chain, entry)
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

async fn execute_invite_mutation(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    cloud_home: &dyn CloudHome,
    chain: &mut MembershipChain,
    plan: InviteMutationPlan,
    mut progress: MembershipMutationProgress,
    persistence: MutationPersistence<'_>,
) -> Result<(CloudHomeJoinInfo, WrappedStoreKeyRef), InviteError> {
    let store_root_hash = operation.store_root().store_root_hash;
    validate_prepared_publication(&plan.publication)?;
    let mut validated_chain = chain_with_exact_entry(chain, &plan.publication.entry)?;
    let author = operation
        .verify_membership_publication_author(&plan.publication)
        .await?;
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
    let storage = operation.storage.as_ref();
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
    storage
        .create_protocol_object(&plan.wrapped_key.object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    storage
        .create_protocol_object(&plan.publication.entry_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    store_objects::load_membership_entry_ref(storage, store_root_hash, &plan.publication.entry_ref)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    storage
        .create_protocol_object(&plan.publication.head_object)
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
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    cloud_home: &dyn CloudHome,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    timestamp: &str,
) -> Result<(CloudHomeJoinInfo, WrappedStoreKeyRef), InviteError> {
    let database = operation.database.clone();
    let owner_keypair = operation.writer.identity.clone();
    let mut chain = operation.membership.clone();
    let _mutation = database.membership_mutation_permit().await;
    let (plan, progress, intent_hash) = match database.outbound_membership_mutation().await? {
        Some(row) => {
            let intent_hash = row.intent_hash;
            let (pending, progress) = decode_membership_mutation(row)?;
            let MembershipMutationPlan::Invite(plan) = pending else {
                return Err(InviteError::PendingMutation(
                    "a member removal is pending".to_string(),
                ));
            };
            if !plan.matches_request(
                &owner_keypair,
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
            let stream_id =
                select_mutation_author_stream(&database, &chain, &owner_keypair).await?;
            let plan = Box::pin(build_invite_mutation(
                operation,
                &chain,
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
    let result = Box::pin(execute_invite_mutation(
        operation,
        cloud_home,
        &mut chain,
        plan,
        progress,
        MutationPersistence {
            database: &database,
            intent_hash,
        },
    ))
    .await?;
    operation.membership = chain;
    Ok(result)
}
