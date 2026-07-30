use crate::protocol::store_commit::{
    membership_head_slot_prefix, owner_recovery_semantic_prefix, GrantStreamAnchor,
    OwnerPromotionAcceptance, OwnerPromotionAnchors, OwnerPromotionRequest, StreamActivation,
    StreamAnchorDomain,
};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain};

use super::journal::{OwnerPromotionJournal, OwnerPromotionJournalState};
use super::OwnerPromotionError;

/// Accept an activated promotion request on the exact member device it names.
pub(crate) async fn accept(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    request: OwnerPromotionRequest,
) -> Result<OwnerPromotionAcceptance, OwnerPromotionError> {
    let store_db = operation.database().clone();
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
    let (registration_ref, registration, _) = operation.registration();
    let registration_ref = registration_ref.clone();
    let registration = registration.clone();
    if registration_ref != request.member_registration
        || registration.author_pubkey != request.member_pubkey
    {
        return Err(OwnerPromotionError::Protocol(
            "promotion request targets another local device".to_string(),
        ));
    }
    let verified_activation = operation
        .history_verifier_mut()
        .find_owner_promotion_request_activation(&request)
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let root = operation.store_root().clone();
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
    let membership_slot = operation
        .storage()
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
    let recovery_slot = operation
        .storage()
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
        verified_activation.activation().clone(),
        anchors,
        &registration,
        operation.identity(),
    )
    .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    operation
        .history_verifier_mut()
        .verify_owner_promotion_acceptance_from_request_activation(&acceptance, verified_activation)
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
