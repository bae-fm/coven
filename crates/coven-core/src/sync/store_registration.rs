//! Durable append-only Store device registration and retirement.

use crate::database::Database;
use crate::keys::UserKeypair;

use super::storage::SyncStorage;
use super::store_commit::{
    registration_semantic_prefix, ObjectHash, StoreDeviceRegistration, StoreDeviceRegistrationState,
};
use super::store_objects::{append_and_verify, StoreObjectError};

#[derive(Debug, thiserror::Error)]
pub enum StoreRegistrationError {
    #[error("Store device registration database state: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("Store device registration is missing protocol state {key:?}")]
    MissingState { key: &'static str },
    #[error("Store device registration bytes are invalid: {0}")]
    Invalid(String),
    #[error("retired Store device {device_id:?} cannot become active again")]
    RetiredDevice { device_id: String },
}

pub async fn ensure_active_registration(
    db: &Database,
    storage: &dyn SyncStorage,
    signer: &UserKeypair,
) -> Result<(), StoreRegistrationError> {
    drain_registration_outbox(db, storage).await?;
    match db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
    {
        Some(registration)
            if registration.state == StoreDeviceRegistrationState::Active
                && registration.published =>
        {
            return Ok(())
        }
        Some(registration) if registration.state == StoreDeviceRegistrationState::Active => {
            return Err(StoreRegistrationError::Database(format!(
                "Store device registration revision {} remained unpublished after drain",
                registration.revision
            )))
        }
        Some(_) => {
            return Err(StoreRegistrationError::RetiredDevice {
                device_id: protocol_value(db, crate::database::LOCAL_DEVICE_ID_STATE_KEY).await?,
            })
        }
        None => {}
    }
    let registration = StoreDeviceRegistration::signed(
        protocol_hash(db).await?,
        protocol_value(db, crate::database::LOCAL_DEVICE_ID_STATE_KEY).await?,
        1,
        None,
        StoreDeviceRegistrationState::Active,
        signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    db.stage_store_device_registration(registration)
        .await
        .map_err(database_error)?;
    drain_registration_outbox(db, storage).await.map(|_| ())
}

pub async fn retire_registration(
    db: &Database,
    storage: &dyn SyncStorage,
    signer: &UserKeypair,
) -> Result<bool, StoreRegistrationError> {
    drain_registration_outbox(db, storage).await?;
    let Some(latest) = db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
    else {
        return Ok(false);
    };
    if latest.state == StoreDeviceRegistrationState::Retired && latest.published {
        return Ok(true);
    }
    if latest.state == StoreDeviceRegistrationState::Retired {
        return Err(StoreRegistrationError::Database(format!(
            "Store device retirement revision {} remained unpublished after drain",
            latest.revision
        )));
    }
    let registration = StoreDeviceRegistration::signed(
        protocol_hash(db).await?,
        protocol_value(db, crate::database::LOCAL_DEVICE_ID_STATE_KEY).await?,
        latest.revision + 1,
        Some(latest.registration_hash),
        StoreDeviceRegistrationState::Retired,
        signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    db.stage_store_device_registration(registration)
        .await
        .map_err(database_error)?;
    drain_registration_outbox(db, storage).await?;
    Ok(true)
}

pub async fn drain_registration_outbox(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<u64, StoreRegistrationError> {
    let store_root_hash = protocol_hash(db).await?;
    let device_id = protocol_value(db, crate::database::LOCAL_DEVICE_ID_STATE_KEY).await?;
    let mut published = 0_u64;
    while let Some(outbound) = db
        .oldest_unpublished_store_device_registration()
        .await
        .map_err(database_error)?
    {
        let registration = StoreDeviceRegistration::parse_at(
            &outbound.registration_bytes,
            store_root_hash,
            &device_id,
            outbound.revision,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        if registration.registration_hash() != outbound.registration_hash
            || registration.previous_registration_hash != outbound.previous_registration_hash
            || registration.state != outbound.state
        {
            return Err(StoreRegistrationError::Invalid(
                "durable registration columns differ from its exact signed bytes".to_string(),
            ));
        }
        append_and_verify(
            storage,
            &registration_semantic_prefix(
                &device_id,
                outbound.revision,
                outbound.registration_hash,
            ),
            ".json",
            &outbound.registration_bytes,
        )
        .await?;
        db.complete_store_device_registration(outbound.revision, outbound.registration_hash)
            .await
            .map_err(database_error)?;
        published = published.checked_add(1).ok_or_else(|| {
            StoreRegistrationError::Database("registration publish count exceeded u64".to_string())
        })?;
    }
    Ok(published)
}

async fn protocol_hash(db: &Database) -> Result<ObjectHash, StoreRegistrationError> {
    protocol_value(db, crate::database::STORE_ROOT_HASH_STATE_KEY)
        .await?
        .parse()
        .map_err(|error| {
            StoreRegistrationError::Invalid(format!("store protocol root hash: {error}"))
        })
}

async fn protocol_value(
    db: &Database,
    key: &'static str,
) -> Result<String, StoreRegistrationError> {
    db.get_protocol_state(key)
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::MissingState { key })
}

fn database_error(error: crate::database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store_commit::{registration_semantic_prefix, StoreDeviceRegistration};
    use crate::sync::store_objects::{
        append_and_verify, list_latest_registration_chains, StoreObjectError,
    };
    use crate::sync::test_helpers::{open_test_db, publish_test_store_protocol_root};

    async fn initialized(
        source: &str,
    ) -> (
        InMemoryCloudHome,
        CloudSyncStorage,
        Database,
        UserKeypair,
        ObjectHash,
    ) {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "registration-store-test",
            signer.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(source)));
        let db = open_test_db();
        let store_root_hash = publish_test_store_protocol_root(
            &db,
            &storage,
            "registration-store-test",
            "dev-reader",
            &signer,
        )
        .await;
        (home, storage, db, signer, store_root_hash)
    }

    #[tokio::test]
    async fn active_registration_and_retired_successor_form_one_verified_chain() {
        let (_home, storage, db, signer, store_root_hash) = initialized("active-retired").await;
        ensure_active_registration(&db, &storage, &signer)
            .await
            .unwrap();
        assert!(retire_registration(&db, &storage, &signer).await.unwrap());

        let chains = list_latest_registration_chains(&storage, store_root_hash)
            .await
            .unwrap();
        let latest = &chains.latest_by_device["dev-reader"].value;
        assert_eq!(latest.revision, 2);
        assert_eq!(latest.state, StoreDeviceRegistrationState::Retired);
        assert!(latest.previous_registration_hash.is_some());
    }

    #[tokio::test]
    async fn failed_active_append_retries_the_owned_exact_bytes() {
        let (home, storage, db, signer, store_root_hash) = initialized("active-retry").await;
        home.fail_append_before_call(1);
        assert!(ensure_active_registration(&db, &storage, &signer)
            .await
            .is_err());
        let pending = db
            .oldest_unpublished_store_device_registration()
            .await
            .unwrap()
            .expect("Active bytes remain owned");
        assert_eq!(pending.revision, 1);
        assert_eq!(pending.state, StoreDeviceRegistrationState::Active);

        ensure_active_registration(&db, &storage, &signer)
            .await
            .unwrap();
        let chains = list_latest_registration_chains(&storage, store_root_hash)
            .await
            .unwrap();
        assert_eq!(
            chains.latest_by_device["dev-reader"].semantic_hash,
            pending.registration_hash,
        );
    }

    #[tokio::test]
    async fn registration_slot_fork_is_rejected() {
        let (_home, storage, _db, signer, store_root_hash) = initialized("registration-fork").await;
        let outsider = UserKeypair::generate();
        for author in [&signer, &outsider] {
            let registration = StoreDeviceRegistration::signed(
                store_root_hash,
                "dev-reader".to_string(),
                1,
                None,
                StoreDeviceRegistrationState::Active,
                author,
            )
            .unwrap();
            append_and_verify(
                &storage,
                &registration_semantic_prefix("dev-reader", 1, registration.registration_hash()),
                ".json",
                &registration.to_bytes(),
            )
            .await
            .unwrap();
        }
        assert!(matches!(
            list_latest_registration_chains(&storage, store_root_hash).await,
            Err(StoreObjectError::SemanticFork { slot, .. })
                if slot == "store-v1/devices/dev-reader/1"
        ));
    }

    #[tokio::test]
    async fn registration_chain_missing_predecessor_is_rejected() {
        let (_home, storage, _db, signer, store_root_hash) = initialized("registration-gap").await;
        let registration = StoreDeviceRegistration::signed(
            store_root_hash,
            "dev-reader".to_string(),
            2,
            Some(ObjectHash::digest(b"missing registration")),
            StoreDeviceRegistrationState::Retired,
            &signer,
        )
        .unwrap();
        append_and_verify(
            &storage,
            &registration_semantic_prefix("dev-reader", 2, registration.registration_hash()),
            ".json",
            &registration.to_bytes(),
        )
        .await
        .unwrap();
        assert!(matches!(
            list_latest_registration_chains(&storage, store_root_hash).await,
            Err(StoreObjectError::MissingRegistrationRevision { device_id, revision })
                if device_id == "dev-reader" && revision == 1
        ));
    }
}
