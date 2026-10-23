//! Applications use api.md's declarations through the public crate root.

use coven::{
    Clock, CryptoError, DbError, ObjectPath, PathError, S3Detection, S3Probe, S3ProbeResult,
    SnapshotError, StorageTime, StoredObject, WriteWait,
};
use std::time::{Duration, SystemTime};

struct AppClock;
impl Clock for AppClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
    }
    fn elapsed(&self) -> Duration {
        Duration::ZERO
    }
}

#[test]
fn appendix_e_types_and_variants_are_available_at_the_crate_root() {
    let parsed: Result<ObjectPath, PathError> = ObjectPath::parse("invalid");
    assert!(parsed.is_err());
    let clock: &dyn Clock = &AppClock;
    assert_eq!(clock.elapsed(), Duration::ZERO);
    let stored_at: StorageTime = StorageTime::new(1, 2).unwrap();
    assert_eq!((stored_at.seconds(), stored_at.nanos()), (1, 2));
    let path = ObjectPath::floor(coven::StoreId(uuid::Uuid::from_u128(1)));
    let object = StoredObject {
        object_id: path.as_str().into(),
        path,
        size: 0,
        stored_at,
        time_resolution: Duration::from_nanos(1),
        revision: "revision".into(),
        replace_token: "condition".into(),
        checksum: None,
    };
    assert_eq!(object.replace_token, "condition");
    let _waits = [
        WriteWait::Slot(1),
        WriteWait::Sources(Vec::new()),
        WriteWait::SchemaPublication(2),
    ];
    let _exhausted: DbError = DbError::WriteNumberExhausted;
    let _identity: CryptoError = CryptoError::KeyIdentity;
    let _snapshot: SnapshotError = SnapshotError::KeyUnavailable {
        audience: coven::Audience::Store,
        key: coven::KeyId(uuid::Uuid::from_u128(2)),
    };
    let _results: fn(&S3Detection) -> &[S3ProbeResult] = |detection| &detection.results;
    let _probe: fn(&S3ProbeResult) -> &S3Probe = |result| &result.probe;
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn application_tests_use_memory_storage_through_the_facade() {
    use coven::{
        BodyFailure, Faults, MemoryRequest, MemoryStorage, MemoryStorageBuilder, Storage,
        StorageFailure, TestCoven,
    };
    use std::sync::Arc;
    let root = tempfile::tempdir().unwrap();
    let app = TestCoven::new();
    let layout = coven::StoreLayout::new(root.path().into());
    let builder: MemoryStorageBuilder = MemoryStorage::builder();
    let storage: Arc<MemoryStorage> = Arc::new(builder.build().unwrap());
    let handle = app
        .builder(layout)
        .synced_tables(vec![])
        .migrations(vec![])
        .storage_connector(storage.clone())
        .create_store("Household")
        .await
        .unwrap();
    handle
        .setup_s3_storage(
            storage.config(),
            "Ana's laptop",
            "key".into(),
            coven::SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    assert_eq!(
        handle.storage_config().await.unwrap(),
        Some(storage.config())
    );
    let path = coven::ObjectPath::slot(handle.store_id(), 99.try_into().unwrap());
    storage.plant(&path, b"provider bytes").await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"provider bytes");
    let mut faults = Faults::none();
    faults
        .body_failures
        .insert(path.clone(), BodyFailure::Interrupted(0));
    storage.set_faults(faults).await;
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    storage.set_faults(Faults::none()).await;
    assert_eq!(
        storage.for_device().read(&path).await.unwrap(),
        b"provider bytes"
    );
    assert!(storage
        .request_trace()
        .contains(&MemoryRequest::Read(path, None)));
    handle.close().await.unwrap();
}
