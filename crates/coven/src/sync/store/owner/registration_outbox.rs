use super::registration::StoreRegistrationError;
use crate::database::StoreDatabase;
use crate::protocol::objects::ProtocolObjectDomain;
use crate::protocol::objects::StoreObjectError;
use crate::protocol::store_commit::{
    ack_slot_prefix, registration_semantic_prefix, StoreDeviceRegistration,
};
use crate::storage::SyncStorage;

pub(super) struct RegistrationOutbox<'storage> {
    database: StoreDatabase,
    storage: &'storage dyn SyncStorage,
}

impl<'storage> RegistrationOutbox<'storage> {
    pub(super) fn new(database: StoreDatabase, storage: &'storage dyn SyncStorage) -> Self {
        Self { database, storage }
    }

    pub(super) async fn drain(&self) -> Result<u64, StoreRegistrationError> {
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
            let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
                store_root.store_root_hash,
                ProtocolObjectDomain::StoreDeviceRegistration,
            );
            let semantic_prefix = registration_semantic_prefix(&outbound.device_id.to_string());
            self.storage
                .create_protocol_object(&outbound.prepared)
                .await
                .map_err(StoreObjectError::from)?;
            let opened = self
                .storage
                .read_protocol_object(&context, outbound.prepared.reference(), &semantic_prefix)
                .await
                .map_err(StoreObjectError::from)?;
            if opened != outbound.registration_bytes {
                return Err(StoreRegistrationError::Invalid(
                    "Store registration exact readback differs from its durable bytes".to_string(),
                ));
            }
            let ack_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
                store_root.store_root_hash,
                ProtocolObjectDomain::StoreAck,
            );
            self.storage
                .create_protocol_object(&outbound.initial_ack.prepared)
                .await
                .map_err(StoreObjectError::from)?;
            let opened_ack = self
                .storage
                .read_protocol_object(
                    &ack_context,
                    &outbound.initial_ack_ref.object,
                    &ack_slot_prefix(&outbound.device_id.to_string(), 1),
                )
                .await
                .map_err(StoreObjectError::from)?;
            if opened_ack != outbound.initial_ack.bytes {
                return Err(StoreRegistrationError::Invalid(
                    "Store initial acknowledgement exact readback differs from its durable bytes"
                        .to_string(),
                ));
            }
            self.database
                .mark_local_store_device_registration_created(
                    crate::database::ExactProtocolObject {
                        value: registration,
                        bytes: outbound.registration_bytes,
                        object: outbound.prepared.reference().clone(),
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

fn database_error(error: crate::database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::Database(error.to_string())
}
