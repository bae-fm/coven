use super::registration::StoreRegistrationError;
use coven_database::StoreDatabase;
use coven_protocol::objects::ProtocolObjectDomain;
use coven_protocol::objects::StorageError;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::store_commit::{
    ack_slot_prefix, registration_semantic_prefix, StoreDeviceRegistration,
};
use coven_storage::CloudSyncObjectStorage;

pub(crate) struct RegistrationOutbox<'storage> {
    database: StoreDatabase,
    storage: &'storage dyn CloudSyncObjectStorage,
}

impl<'storage> RegistrationOutbox<'storage> {
    pub(crate) fn new(
        database: StoreDatabase,
        storage: &'storage dyn CloudSyncObjectStorage,
    ) -> Self {
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
            .map_err(StoreRegistrationError::from)?;
            if registration.registration_hash() != outbound.registration_hash {
                return Err(StoreRegistrationError::Invalid(
                    "durable registration columns differ from its exact signed bytes".to_string(),
                ));
            }
            let exact_registration = coven_protocol::objects::ExactProtocolObject {
                value: registration.clone(),
                bytes: outbound.registration_bytes.clone(),
                prepared: outbound.prepared.clone(),
            };
            match &outbound.state {
                coven_database::LocalDeviceRegistrationState::Prepared => {
                    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                        store_root.store_root_hash,
                        ProtocolObjectDomain::StoreDeviceRegistration,
                    );
                    let semantic_prefix =
                        registration_semantic_prefix(&outbound.device_id.to_string());
                    self.storage
                        .create_verified_protocol_object(
                            &context,
                            &outbound.prepared,
                            &semantic_prefix,
                            &outbound.registration_bytes,
                        )
                        .await
                        .map_err(publication_error)?;
                    self.database
                        .mark_local_store_device_registration_published(
                            exact_registration.clone(),
                            outbound.initial_ack_ref.clone(),
                            outbound.initial_ack.clone(),
                        )
                        .await
                        .map_err(database_error)?;
                }
                coven_database::LocalDeviceRegistrationState::RegistrationPublished
                | coven_database::LocalDeviceRegistrationState::RegistrationActivated { .. } => {}
                coven_database::LocalDeviceRegistrationState::Created
                | coven_database::LocalDeviceRegistrationState::Activated { .. } => {
                    return Err(StoreRegistrationError::Invalid(
                        "registration outbox selected a completed publication".to_string(),
                    ));
                }
            }
            let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                store_root.store_root_hash,
                ProtocolObjectDomain::StoreAck,
            );
            self.storage
                .create_verified_protocol_object(
                    &ack_context,
                    &outbound.initial_ack.prepared,
                    &ack_slot_prefix(&outbound.device_id.to_string(), 1),
                    &outbound.initial_ack.bytes,
                )
                .await
                .map_err(StoreObjectError::from)?;
            self.database
                .mark_local_store_device_ack_published(
                    exact_registration,
                    outbound.initial_ack_ref,
                    outbound.initial_ack,
                )
                .await
                .map_err(database_error)?;
            published = published
                .checked_add(1)
                .ok_or(StoreRegistrationError::PublishCountExhausted)?;
        }
        Ok(published)
    }
}

fn database_error(error: coven_database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::from(error)
}

/// An exact object that opened to bytes other than the durable ones is invalid
/// durable state, not a transport failure.
fn publication_error(error: StorageError) -> StoreRegistrationError {
    match error {
        StorageError::PreparedObjectMismatch(key) => StoreRegistrationError::Invalid(format!(
            "prepared exact object {key} differs from its durable bytes"
        )),
        error => StoreObjectError::from(error).into(),
    }
}
