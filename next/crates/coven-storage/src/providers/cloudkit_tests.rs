use super::*;
use crate::test_utils::{Conformance, Faults, MemoryStorage};
use coven_crypto::SecretText;
use std::collections::BTreeMap;
struct Bridge {
    memory: MemoryStorage,
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
    fn new() -> Self {
        Self {
            memory: MemoryStorage::new(config()).unwrap(),
            uploads: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }
}
#[async_trait]
impl CloudKitOps for Bridge {
    async fn create(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        assert_eq!(location, &config());
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
    ) -> Result<Vec<ObjectPath>, StorageError> {
        assert_eq!(location, &config());
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
    let bridge = Arc::new(Bridge::new());
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
    let bridge = Arc::new(Bridge::new());
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
