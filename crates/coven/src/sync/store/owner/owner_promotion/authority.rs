use crate::protocol::membership::{MembershipChain, MembershipGrantId, StoreMembershipRoleGrant};
use crate::protocol::store_commit::{ObjectHash, StoreDeviceRegistrationRef};

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
