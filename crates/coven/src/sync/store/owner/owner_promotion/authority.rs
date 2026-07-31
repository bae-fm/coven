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
