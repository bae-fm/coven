use coven_protocol::objects::{PreparedExactObject, ProtocolObjectDomain, StoreObjectError};
use coven_protocol::store_commit::StoreDeviceRegistration;
use coven_storage::CloudSyncObjectStorage;

use super::StoreRegistrationError;

pub(super) fn prepare_registration_object(
    storage: &dyn CloudSyncObjectStorage,
    registration: &StoreDeviceRegistration,
    slot: coven_protocol::objects::ObjectSlot,
) -> Result<PreparedExactObject, StoreRegistrationError> {
    let semantic_prefix = slot
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StoreRegistrationError::Invalid(
                "reserved registration slot has no .json suffix".to_string(),
            )
        })?
        .to_string();
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        registration.store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    storage
        .prepare_protocol_object(&context, slot, &semantic_prefix, registration.to_bytes())
        .map_err(StoreObjectError::from)
        .map_err(StoreRegistrationError::from)
}
