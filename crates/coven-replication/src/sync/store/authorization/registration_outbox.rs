use super::registration::StoreRegistrationError;
use coven_database::StoreDatabase;
use coven_protocol::objects::ProtocolObjectDomain;
use coven_protocol::objects::StorageError;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::store_commit::{
    ack_slot_prefix, registration_semantic_prefix, StoreDeviceRegistration,
};
use coven_storage::{SyncStorage, VerifiedObjectWrites};

pub(crate) struct RegistrationOutbox<'storage> {
    database: StoreDatabase,
    storage: &'storage dyn SyncStorage,
}

impl<'storage> RegistrationOutbox<'storage> {
    pub(crate) fn new(database: StoreDatabase, storage: &'storage dyn SyncStorage) -> Self {
        Self { database, storage }
    }

    pub(crate) async fn drain(&self) -> Result<u64, StoreRegistrationError> {
        let store_root = self
            .database
            .local_store_root_ref()
            .await
            .map_err(database_error)?
            .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
        let mut published = 0_u64;
        while let Some(outbound) = self
            .database
            .oldest_unpublished_store_device_registration()
            .await
            .map_err(database_error)?
        {
            let registration = StoreDeviceRegistration::parse_at(
                &outbound.registration_bytes,
                &store_root,
                outbound.device_id,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if registration.registration_hash() != outbound.registration_hash {
                return Err(StoreRegistrationError::Invalid(
                    "durable registration columns differ from its exact signed bytes".to_string(),
                ));
            }
            let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                store_root.store_root_hash,
                ProtocolObjectDomain::StoreDeviceRegistration,
            );
            let semantic_prefix = registration_semantic_prefix(&outbound.device_id.to_string());
            self.storage
                .create_and_verify(
                    &context,
                    &outbound.prepared,
                    &semantic_prefix,
                    &outbound.registration_bytes,
                )
                .await
                .map_err(publication_error)?;
            let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                store_root.store_root_hash,
                ProtocolObjectDomain::StoreAck,
            );
            self.storage
                .create_protocol_object(&outbound.initial_ack.prepared)
                .await
                .map_err(StoreObjectError::from)?;
            self.storage
                .verify_readback(
                    &ack_context,
                    &outbound.initial_ack_ref.object,
                    &ack_slot_prefix(&outbound.device_id.to_string(), 1),
                    &outbound.initial_ack.bytes,
                )
                .await
                .map_err(publication_error)?;
            self.database
                .mark_local_store_device_registration_created(
                    coven_protocol::objects::ExactProtocolObject {
                        value: registration,
                        bytes: outbound.registration_bytes,
                        prepared: outbound.prepared,
                    },
                    outbound.initial_ack_ref,
                    outbound.initial_ack,
                )
                .await
                .map_err(database_error)?;
            published = published.checked_add(1).ok_or_else(|| {
                StoreRegistrationError::Database(
                    "registration publish count exceeded u64".to_string(),
                )
            })?;
        }
        Ok(published)
    }
}

fn database_error(error: coven_database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::Database(error.to_string())
}

/// An exact object that opened to bytes other than the durable ones is invalid
/// durable state, not a transport failure.
fn publication_error(error: StorageError) -> StoreRegistrationError {
    match error {
        StorageError::ReadbackMismatch(key) => StoreRegistrationError::Invalid(format!(
            "exact readback of {key} differs from its durable bytes"
        )),
        error => StoreObjectError::from(error).into(),
    }
}
