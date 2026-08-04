use crate::protocol::objects::{PreparedExactObject, ProtocolObjectDomain, StoreObjectError};
use crate::protocol::store_commit::StoreDeviceRegistration;
use crate::storage::SyncStorage;

use super::StoreRegistrationError;

pub(super) fn prepare_registration_object(
    storage: &dyn SyncStorage,
    registration: &StoreDeviceRegistration,
    slot: crate::protocol::objects::ObjectSlot,
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
    let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
        registration.store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    storage
        .prepare_protocol_object(&context, slot, &semantic_prefix, registration.to_bytes())
        .map_err(StoreObjectError::from)
        .map_err(StoreRegistrationError::from)
}
