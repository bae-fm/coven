use crate::sync::membership::{MembershipChain, MembershipGrantId, StoreMembershipRoleGrant};
use crate::sync::storage::SyncStorage;
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{ObjectHash, StoreDeviceRegistrationRef, StoreRootRef};

use super::OwnerPromotionError;

const TARGET_PREFIX: &str = "owner_promotion_target/";

pub(super) fn target_key(
    target: &StoreDeviceRegistrationRef,
) -> Result<String, OwnerPromotionError> {
    let bytes = serde_json::to_vec(target).map_err(|error| {
        OwnerPromotionError::Protocol(format!("serialize promotion target: {error}"))
    })?;
    Ok(format!("{TARGET_PREFIX}{}", ObjectHash::digest(&bytes)))
}

pub(super) async fn load_current_merge_membership(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<MembershipChain, OwnerPromotionError> {
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, root)
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    load_current_merge_membership_with_history(&mut history_verifier, database).await
}

pub(super) async fn load_current_merge_membership_with_history(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    database: &StoreDatabase,
) -> Result<MembershipChain, OwnerPromotionError> {
    let founder = database
        .sqlite()
        .get_protocol_state(crate::sync::store::membership::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or_else(|| OwnerPromotionError::Protocol("Store founder is absent".to_string()))?;
    let loaded = crate::sync::store::membership::load_current_exact_chain_with_history(
        history_verifier,
        Some(&founder),
        Some(database),
    )
    .await
    .map_err(|error| OwnerPromotionError::Protocol(error.to_string()));
    loaded
}

pub(super) fn exact_merge_member_grant(
    membership: &MembershipChain,
    member_pubkey: &str,
) -> Result<MembershipGrantId, OwnerPromotionError> {
    let grants = membership.active_grant_ids(member_pubkey);
    let Some(grant) = grants.iter().next() else {
        return Err(OwnerPromotionError::Protocol(
            "promotion target has no active Member grant".to_string(),
        ));
    };
    if grants.len() != 1
        || membership
            .active_grant(grant)
            .is_none_or(|record| record.role != StoreMembershipRoleGrant::Member)
    {
        return Err(OwnerPromotionError::Protocol(
            "promotion target does not have exactly one active Member grant".to_string(),
        ));
    }
    Ok(grant.clone())
}
