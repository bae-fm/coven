use super::*;
use crate::test_utils::{Conformance, Faults, MemoryStorage};
use coven_crypto::SecretText;
use std::collections::BTreeMap;
struct Bridge {
    memory: MemoryStorage,
    abort_failure: Option<StorageFailure>,
    non_owner: bool,
    listed: Option<Vec<StoredObject>>,
    uploads: tokio::sync::Mutex<BTreeMap<String, UploadSession>>,
}
fn config() -> StorageConfig {
    StorageConfig::CloudKit {
        container: "container".into(),
        owner: "owner".into(),
        zone: "zone".into(),
    }
}
impl Bridge {
    fn new(memory: MemoryStorage) -> Self {
        Self {
            memory,
            abort_failure: None,
            non_owner: false,
            listed: None,
            uploads: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }
}
#[async_trait]
impl CloudKitOps for Bridge {
    async fn is_owner(&self, location: &StorageConfig) -> Result<bool, StorageError> {
        assert_eq!(location, &config());
        Ok(!self.non_owner)
    }
    fn single_request_limit(&self) -> u64 {
        16
    }
    async fn create(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        assert_eq!(location, &config());
        assert!(bytes.len() <= 16);
        self.memory.create(path, bytes).await
    }
    async fn replace(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        assert_eq!(location, &config());
        self.memory.replace(path, bytes).await
    }
    async fn read(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, StorageError> {
        assert_eq!(location, &config());
        match range {
            Some(range) => self.memory.read_range(path, range).await,
            None => self.memory.read(path).await,
        }
    }
    async fn list(
        &self,
        location: &StorageConfig,
        prefix: &ObjectPrefix,
    ) -> Result<Vec<StoredObject>, StorageError> {
        assert_eq!(location, &config());
        if let Some(listed) = &self.listed {
            return Ok(listed.clone());
        }
        self.memory.list(prefix).await
    }
    async fn delete(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
    ) -> Result<(), StorageError> {
        assert_eq!(location, &config());
        self.memory.delete(path).await
    }
    async fn set_access(
        &self,
        location: &StorageConfig,
        email: &str,
        granted: bool,
    ) -> Result<(), StorageError> {
        assert_eq!(location, &config());
        assert!(!self.non_owner, "sharing must check ownership first");
        if granted {
            self.memory.grant_access(email).await?;
        } else {
            self.memory
                .revoke_access(&MemberAccess::ProviderAccount(email.into()))
                .await?;
        }
        Ok(())
    }
    async fn begin_upload(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        total: u64,
    ) -> Result<CloudKitUpload, StorageError> {
        assert_eq!(location, &config());
        let session = self.memory.begin_upload(path, total).await?;
        let mut uploads = self.uploads.lock().await;
        let id = uploads.len().to_string();
        let part_size = session.part_size();
        uploads.insert(id.clone(), session);
        Ok(CloudKitUpload {
            id: SecretText::new(id),
            part_size,
        })
    }
    async fn upload_status(
        &self,
        location: &StorageConfig,
        id: &SecretText,
    ) -> Result<CloudKitUploadStatus, StorageError> {
        assert_eq!(location, &config());
        let mut uploads = self.uploads.lock().await;
        let session = uploads
            .get_mut(id.as_str())
            .ok_or(StorageError::SessionExpired)?;
        self.memory.resume_upload(session).await?;
        Ok(if session.is_complete() {
            CloudKitUploadStatus::Complete
        } else {
            CloudKitUploadStatus::Uploading {
                confirmed: session.confirmed,
            }
        })
    }
    async fn upload_part(
        &self,
        location: &StorageConfig,
        id: &SecretText,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        assert_eq!(location, &config());
        let mut uploads = self.uploads.lock().await;
        let session = uploads
            .get_mut(id.as_str())
            .ok_or(StorageError::SessionExpired)?;
        assert_eq!(session.confirmed, offset);
        self.memory.upload_part(session, bytes).await
    }
    async fn finish_upload(
        &self,
        location: &StorageConfig,
        id: &SecretText,
    ) -> Result<(), StorageError> {
        assert_eq!(location, &config());
        self.memory
            .finish_upload(
                self.uploads
                    .lock()
                    .await
                    .get_mut(id.as_str())
                    .ok_or(StorageError::SessionExpired)?,
            )
            .await
    }
    async fn abort_upload(
        &self,
        location: &StorageConfig,
        id: &SecretText,
    ) -> Result<(), StorageError> {
        assert_eq!(location, &config());
        if let Some(failure) = self.abort_failure {
            return Err(StorageError::Injected(failure));
        }
        self.memory
            .abort_upload(
                self.uploads
                    .lock()
                    .await
                    .get(id.as_str())
                    .ok_or(StorageError::SessionExpired)?,
            )
            .await
    }
}
#[tokio::test]
async fn bridge_conforms_and_retains_parts_across_adapter_restart() {
    let bridge = Arc::new(Bridge::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    ));
    let storage = Arc::new(CloudKitStorage::new(config(), bridge.clone()).unwrap());
    Conformance::new(storage.clone()).run().await.unwrap();
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xff; 16],
    )));
    let mut session = storage.begin_upload(&path, 5).await.unwrap();
    let recorded = session.encode().unwrap();
    bridge
        .memory
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(storage.upload_part(&mut session, b"abcd").await.is_err());
    drop(storage);
    drop(session);
    let storage = CloudKitStorage::new(config(), bridge.clone()).unwrap();
    let mut session = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.confirmed, 4);
    storage.upload_part(&mut session, b"e").await.unwrap();
    storage.finish_upload(&mut session).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"abcde");
    assert_eq!(storage.sign_out(), ProviderSignOut::RemoveFromAppleAccount);
    bridge
        .memory
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::PermissionDenied,
            ..Faults::none()
        })
        .await;
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
}

#[tokio::test]
async fn expired_bridge_session_restarts_from_retained_bytes() {
    let bridge = Arc::new(Bridge::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    ));
    let storage = CloudKitStorage::new(config(), bridge).unwrap();
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut expired = storage.begin_upload(&path, 5).await.unwrap();
    storage.upload_part(&mut expired, b"abcd").await.unwrap();
    storage.abort_upload(&expired).await.unwrap();
    assert!(matches!(
        storage.resume_upload(&mut expired).await,
        Err(StorageError::SessionExpired)
    ));
    let replacement = storage.restart_upload(&expired).await.unwrap();
    assert_eq!(replacement.path(), &path);
    assert_eq!(replacement.confirmed_bytes(), 0);
    let mut replacement = UploadSession::decode(replacement.encode().unwrap().as_bytes()).unwrap();
    storage
        .upload_part(&mut replacement, b"abcd")
        .await
        .unwrap();
    storage.upload_part(&mut replacement, b"e").await.unwrap();
    storage.finish_upload(&mut replacement).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"abcde");
}

#[tokio::test]
async fn abort_accepts_a_forgotten_session_and_keeps_published_objects() {
    let bridge = Arc::new(Bridge::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    ));
    let storage = CloudKitStorage::new(config(), bridge.clone()).unwrap();
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.abort_upload(&upload).await.unwrap();
    bridge.uploads.lock().await.clear();
    storage.abort_upload(&upload).await.unwrap();
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    let unconfirmed = upload.clone();
    storage.finish_upload(&mut upload).await.unwrap();
    storage.abort_upload(&unconfirmed).await.unwrap();
    bridge.uploads.lock().await.clear();
    storage.abort_upload(&unconfirmed).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn create_respects_the_bridges_single_request_limit() {
    let bridge = Arc::new(Bridge::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    ));
    let storage = CloudKitStorage::new(config(), bridge.clone()).unwrap();
    assert_eq!(storage.single_request_limit(), 16);
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&path, &[1; 16]).await.unwrap();
    assert!(bridge.uploads.lock().await.is_empty());
    storage.delete(&path).await.unwrap();
    storage.create(&path, &[2; 17]).await.unwrap();
    assert_eq!(bridge.uploads.lock().await.len(), 1);
    assert_eq!(storage.read(&path).await.unwrap(), [2; 17]);
    let positions = ObjectPath::positions(coven_foundation::id_source::DeviceId(31));
    assert!(matches!(
        storage.replace(&positions, &[3; 17]).await,
        Err(StorageError::SingleRequestTooLarge {
            size: 17,
            limit: 16
        })
    ));
    assert_eq!(bridge.uploads.lock().await.len(), 1);
}

#[tokio::test]
async fn automatic_upload_keeps_both_transfer_and_abort_failures() {
    let mut bridge = Bridge::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    );
    bridge.abort_failure = Some(StorageFailure::PermissionDenied);
    bridge
        .memory
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    let bridge = Arc::new(bridge);
    let storage = CloudKitStorage::new(config(), bridge.clone()).unwrap();
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let error = storage.create(&path, &[1; 17]).await.unwrap_err();
    let StorageError::Cleanup { operation, cleanup } = error else {
        panic!("lost one of the failures");
    };
    assert_eq!(operation.failure(), StorageFailure::Network);
    assert_eq!(cleanup.failure(), StorageFailure::PermissionDenied);
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
}

#[tokio::test]
async fn sharing_requires_the_store_owners_account() {
    let mut bridge = Bridge::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    );
    bridge.non_owner = true;
    let storage = CloudKitStorage::new(config(), Arc::new(bridge)).unwrap();
    assert!(matches!(
        storage.grant_access("new@example.test").await,
        Err(StorageError::NotStoreOwner)
    ));
    assert!(matches!(
        storage
            .revoke_access(&MemberAccess::ProviderAccount("kept@example.test".into()))
            .await,
        Err(StorageError::NotStoreOwner)
    ));
}

#[tokio::test]
async fn listing_keeps_the_native_publication_metadata() {
    let bridge = Arc::new(Bridge::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    ));
    let storage = CloudKitStorage::new(config(), bridge.clone()).unwrap();
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&path, b"data").await.unwrap();
    let listed = storage.list(&ObjectPrefix::all()).await.unwrap();
    assert_eq!(
        listed,
        [StoredObject {
            path,
            size: 4,
            stored_at: std::time::SystemTime::UNIX_EPOCH
        }]
    );
}

#[tokio::test]
async fn listing_refuses_duplicate_paths_and_objects_outside_the_requested_prefix() {
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let object = StoredObject {
        path,
        size: 4,
        stored_at: std::time::SystemTime::UNIX_EPOCH,
    };
    for listed in [vec![object.clone()], vec![object.clone(), object.clone()]] {
        let mut bridge = Bridge::new(
            MemoryStorage::new(
                config(),
                Arc::new(coven_foundation::clock::FixedClock::new(
                    std::time::SystemTime::UNIX_EPOCH,
                )),
            )
            .unwrap(),
        );
        bridge.listed = Some(listed.clone());
        let storage = CloudKitStorage::new(config(), Arc::new(bridge)).unwrap();
        let prefix = if listed.len() == 1 {
            ObjectPrefix::positions()
        } else {
            ObjectPrefix::all()
        };
        assert_eq!(
            storage.list(&prefix).await.unwrap_err().failure(),
            StorageFailure::Protocol
        );
    }
}
