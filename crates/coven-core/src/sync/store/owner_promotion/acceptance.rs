use crate::keys::UserKeypair;
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{
    membership_head_slot_prefix, owner_recovery_semantic_prefix, GrantStreamAnchor,
    OwnerPromotionAcceptance, OwnerPromotionAnchors, OwnerPromotionRequest, StreamActivation,
    StreamAnchorDomain,
};

use super::journal::{OwnerPromotionJournal, OwnerPromotionJournalState};
use super::OwnerPromotionError;

pub async fn accept_owner_promotion(
    store_db: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity: &UserKeypair,
    request: OwnerPromotionRequest,
) -> Result<OwnerPromotionAcceptance, OwnerPromotionError> {
    if let Some(existing) = store_db
        .load_owner_promotion_journal(request.promotion_id)
        .await?
    {
        if let OwnerPromotionJournalState::AcceptanceReady { acceptance }
        | OwnerPromotionJournalState::MergeMembershipPrepared { acceptance, .. }
        | OwnerPromotionJournalState::MergeHeadPrepared { acceptance, .. }
        | OwnerPromotionJournalState::Finalized { acceptance, .. }
        | OwnerPromotionJournalState::Stale { acceptance, .. } = existing.state
        {
            if acceptance.request.as_ref() == &request {
                return Ok(acceptance);
            }
        }
        return Err(OwnerPromotionError::Protocol(
            "promotion id is already bound to another journal state".to_string(),
        ));
    }
    let (root, registration_ref, registration, _) =
        crate::sync::store::operations::load_local_store_authority(store_db, device_id, identity)
            .await?;
    if registration_ref != request.member_registration
        || registration.author_pubkey != request.member_pubkey
    {
        return Err(OwnerPromotionError::Protocol(
            "promotion request targets another local device".to_string(),
        ));
    }
    let protocol = crate::sync::store_objects::load_store_protocol_root(storage, &root)
        .await
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
    let live = storage.provider_binding().await?;
    if live.store != protocol.value.descriptor.provider || live.device != registration.provider {
        return Err(OwnerPromotionError::Protocol(
            "live storage principal differs from the Store and candidate registration".to_string(),
        ));
    }
    let activation =
        crate::sync::store::find_owner_promotion_request_activation(storage, &root, &request)
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let membership_stream = StreamActivation::grant_authorized_stream_id(
        root.store_root_hash,
        &registration_ref,
        &request.intended_owner_grant,
        StreamAnchorDomain::StoreMembership,
    );
    let membership_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let membership_prefix = membership_head_slot_prefix(
        &request.member_pubkey,
        &request.intended_owner_grant,
        membership_stream,
        1,
    );
    let membership_slot = storage
        .allocate_protocol_slot(&membership_context, &membership_prefix, ".json")
        .await?;
    let recovery_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let recovery_prefix = owner_recovery_semantic_prefix(
        &request.member_pubkey,
        request.intended_owner_grant.clone(),
        1,
    );
    let recovery_slot = storage
        .allocate_protocol_slot(&recovery_context, &recovery_prefix, ".json")
        .await?;
    let anchors = OwnerPromotionAnchors {
        membership: GrantStreamAnchor::StoreMembership {
            first_slot: membership_slot,
        },
        recovery: GrantStreamAnchor::OwnerRecovery {
            first_slot: recovery_slot,
        },
    };
    let acceptance = OwnerPromotionAcceptance::signed(
        request.clone(),
        activation,
        anchors,
        &registration,
        identity,
    )
    .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    crate::sync::store::verify_owner_promotion_acceptance(storage, &root, &acceptance)
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let journal = OwnerPromotionJournal {
        promotion_id: request.promotion_id,
        target: request.member_registration.clone(),
        state: OwnerPromotionJournalState::AcceptanceReady {
            acceptance: acceptance.clone(),
        },
    };
    store_db
        .begin_owner_promotion_acceptance_journal(journal)
        .await?;
    Ok(acceptance)
}
