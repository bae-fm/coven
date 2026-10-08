use crate::*;
use coven_foundation::id_source::DeviceId;
use std::sync::Arc;

/// Run identical object, range, prefix and immutable-create checks on any provider.
/// The caller supplies the capability at construction, like production composition.
pub struct Conformance {
    storage: Arc<dyn Storage>,
}
impl Conformance {
    /// The provider under test, at an otherwise empty location.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
    /// Exercise the real provider operations, returning the first violated contract.
    pub async fn run(&self) -> Result<(), StorageError> {
        use coven_foundation::id_source::DeviceId;
        let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
        let other = ObjectPath::device_log(DeviceId(32), std::num::NonZeroU64::MIN);
        let data = b"encrypted bytes";
        self.storage.create(&path, data).await?;
        self.storage.create(&other, b"other").await?;
        if self.storage.read(&path).await? != data {
            return Err(StorageFailure::Protocol.with_source("whole read"));
        }
        for (start, end) in [(0, 3), (4, 8), (12, 15)] {
            let range = ByteRange::new(start, end)?;
            if self.storage.read_range(&path, range).await? != range.select(data)? {
                return Err(StorageFailure::Protocol.with_source("range read"));
            }
        }
        if !matches!(
            self.storage.read_range(&path, ByteRange::new(15, 16)?).await,
            Err(error) if error.failure() == StorageFailure::InvalidRange
        ) {
            return Err(StorageFailure::Protocol.with_source("past-end range misclassified"));
        }
        if self
            .storage
            .create(&path, b"replacement")
            .await
            .err()
            .map(|e| e.failure())
            != Some(StorageFailure::AlreadyExists)
        {
            return Err(StorageFailure::Protocol.with_source("create-once refusal"));
        }
        self.storage.create_once(&path, data).await?;
        if self
            .storage
            .list(&ObjectPrefix::device_log(DeviceId(31)))
            .await?
            .into_iter()
            .map(|object| (object.path, object.size))
            .collect::<Vec<_>>()
            != [(path.clone(), data.len() as u64)]
        {
            return Err(StorageFailure::Protocol.with_source("prefix listing"));
        }
        self.storage.delete(&path).await?;
        self.storage.delete(&path).await?;
        self.storage.delete(&other).await?;
        if self.storage.read(&path).await.err().map(|e| e.failure())
            != Some(StorageFailure::NotFound)
        {
            return Err(StorageFailure::Protocol.with_source("deleted object read"));
        }
        let positions = ObjectPath::positions(DeviceId(31));
        if !matches!(
            self.storage.begin_upload(&positions, 1).await,
            Err(error) if error.failure() == StorageFailure::InvalidPath
        ) {
            return Err(
                StorageFailure::Protocol.with_source("positions accepted a recorded upload")
            );
        }
        if !matches!(
            self.storage.replace(&path, data).await,
            Err(error) if error.failure() == StorageFailure::InvalidPath
        ) {
            return Err(
                StorageFailure::Protocol.with_source("immutable object accepted replacement")
            );
        }
        self.storage.replace(&positions, b"first positions").await?;
        self.storage.replace(&positions, b"next positions").await?;
        if self.storage.read(&positions).await? != b"next positions" {
            return Err(StorageFailure::Protocol.with_source("posted positions replacement"));
        }
        self.storage.delete(&positions).await?;
        self.storage.create(&path, &[]).await?;
        self.storage.create_once(&path, &[]).await?;
        if !self.storage.read(&path).await?.is_empty() {
            return Err(StorageFailure::Protocol.with_source("empty object changed"));
        }
        self.storage.create_once(&path, b"different").await?;
        if !self.storage.read(&path).await?.is_empty() {
            return Err(StorageFailure::Protocol.with_source("empty immutable object replaced"));
        }
        self.storage.delete(&path).await?;
        let bytes: Vec<_> = (0..42).map(|index| (index % 251) as u8).collect();
        self.storage.create(&path, &bytes).await?;
        if self
            .storage
            .read_range(&path, ByteRange::new(5, 37)?)
            .await?
            != bytes[5..37]
        {
            return Err(StorageFailure::Protocol.with_source("range across upload parts changed"));
        }
        if self
            .storage
            .read_range(
                &path,
                ByteRange::new(bytes.len() as u64 - 1, bytes.len() as u64 + 1)?,
            )
            .await
            .err()
            .map(|error| error.failure())
            != Some(StorageFailure::InvalidRange)
        {
            return Err(
                StorageFailure::Protocol.with_source("clipped past-end range misclassified")
            );
        }
        self.storage.delete(&path).await?;
        let first = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
        self.storage
            .setup(&first, data)
            .await
            .map_err(|error| match error {
                StorageSetupError::Storage(error) => error,
                error => StorageFailure::Protocol.with_source(error),
            })?;
        if self.storage.read(&first).await? != data {
            return Err(
                StorageFailure::Protocol.with_source("setup did not create the first entry")
            );
        }
        self.storage.delete(&first).await?;
        Ok(())
    }
}

impl Conformance {
    /// Preserve provider publication times and sizes through listings and retries.
    /// Advance the fixture clock after publication to distinguish retry timestamps.
    pub async fn listing_times(
        &self,
        stored_at: std::time::SystemTime,
        advance: impl std::future::Future<Output = ()>,
    ) {
        let storage = &self.storage;
        let first = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
        let second = ObjectPath::device_log(DeviceId(32), std::num::NonZeroU64::MIN);
        storage.create_once(&first, b"first").await.unwrap();
        storage.create_once(&second, b"second").await.unwrap();
        let expected = vec![
            StoredObject {
                path: first.clone(),
                size: 5,
                stored_at,
            },
            StoredObject {
                path: second.clone(),
                size: 6,
                stored_at,
            },
        ];
        assert_eq!(
            storage.list(&ObjectPrefix::device_logs()).await.unwrap(),
            expected
        );
        advance.await;
        storage.create_once(&first, b"first").await.unwrap();
        assert_eq!(storage.list(&ObjectPrefix::all()).await.unwrap(), expected);
        storage.delete(&first).await.unwrap();
        storage.delete(&second).await.unwrap();
    }

    /// Restart an expired recording at its original destination with no progress.
    /// Return the expiry error so native fixtures can also inspect its provider cause.
    pub async fn expired_session(
        &self,
        expire: impl std::future::Future<Output = ()>,
    ) -> StorageError {
        let storage = &self.storage;
        let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
        let mut upload = storage.begin_upload(&path, 4).await.unwrap();
        expire.await;
        let error = storage.resume_upload(&mut upload).await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::SessionExpired);
        let replacement = storage.restart_upload(&upload).await.unwrap();
        assert_eq!(replacement.path(), &path);
        assert_eq!(replacement.total_bytes(), 4);
        assert_eq!(replacement.confirmed_bytes(), 0);
        assert_ne!(
            replacement.encode().unwrap().as_bytes(),
            upload.encode().unwrap().as_bytes()
        );
        let mut replacement =
            UploadSession::decode(replacement.encode().unwrap().as_bytes()).unwrap();
        storage.resume_upload(&mut replacement).await.unwrap();
        storage
            .upload_part(&mut replacement, b"data")
            .await
            .unwrap();
        storage.finish_upload(&mut replacement).await.unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), b"data");
        assert!(matches!(storage.restart_upload(&replacement).await,
            Err(error) if error.failure() == StorageFailure::InvalidPart));
        storage.delete(&path).await.unwrap();
        error
    }

    /// Non-owner accounts may neither add nor remove another account's access.
    pub async fn owner_only_sharing(&self) {
        for error in [
            self.storage
                .grant_access("new@example.test")
                .await
                .err()
                .unwrap(),
            self.storage
                .revoke_access(&MemberAccess::ProviderAccount("kept@example.test".into()))
                .await
                .err()
                .unwrap(),
        ] {
            assert_eq!(error.failure(), StorageFailure::NotStoreOwner);
        }
    }

    /// Restore a persisted recording after the provider accepts a part but loses its reply.
    pub async fn lost_part_reply(
        &self,
        part_size: usize,
        lose: impl std::future::Future<Output = ()>,
    ) {
        let storage = &self.storage;
        let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
        let mut upload = storage
            .begin_upload(&path, part_size as u64 + 1)
            .await
            .unwrap();
        assert_eq!(upload.part_size(), part_size);
        let recorded = upload.encode().unwrap();
        let bytes = vec![42; part_size];
        lose.await;
        assert_eq!(
            storage
                .upload_part(&mut upload, &bytes)
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Network
        );
        assert_eq!(upload.confirmed_bytes(), 0);
        let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
        storage.resume_upload(&mut upload).await.unwrap();
        assert_eq!(upload.confirmed_bytes(), part_size as u64);
        storage.upload_part(&mut upload, b"z").await.unwrap();
        storage.finish_upload(&mut upload).await.unwrap();
        storage.finish_upload(&mut upload).await.unwrap();
        let mut expected = bytes;
        expected.push(b'z');
        assert_eq!(storage.read(&path).await.unwrap(), expected);
        storage.delete(&path).await.unwrap();
    }

    /// Every object, upload and sharing operation preserves a permission failure.
    pub async fn permission_failures(
        &self,
        deny: impl std::future::Future<Output = ()>,
    ) -> Vec<StorageError> {
        let storage = &self.storage;
        let path = ObjectPath::device_log(
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        let positions = ObjectPath::positions(coven_foundation::id_source::DeviceId(31));
        let mut pending = storage.begin_upload(&path, 4).await.unwrap();
        let mut ready = storage.begin_upload(&path, 4).await.unwrap();
        storage.upload_part(&mut ready, b"data").await.unwrap();
        // Drive and OneDrive publish the final part immediately. Test finish by
        // recovering its still-incomplete recording after the reply was lost.
        if ready.is_complete() {
            ready = pending.clone();
        }
        deny.await;
        let mut errors = vec![
            storage.create(&path, b"data").await.err().unwrap(),
            storage.replace(&positions, b"data").await.err().unwrap(),
            storage.read(&path).await.err().unwrap(),
            storage
                .read_range(&path, ByteRange::new(0, 1).unwrap())
                .await
                .err()
                .unwrap(),
            storage.list(&ObjectPrefix::all()).await.err().unwrap(),
            storage.delete(&path).await.err().unwrap(),
            storage.begin_upload(&path, 4).await.err().unwrap(),
            storage.resume_upload(&mut pending).await.err().unwrap(),
            storage
                .upload_part(&mut pending, b"data")
                .await
                .err()
                .unwrap(),
            storage.finish_upload(&mut ready).await.err().unwrap(),
            storage.abort_upload(&pending).await.err().unwrap(),
        ];
        if storage.config().provider() != CloudProvider::S3 {
            errors.push(storage.grant_access("member").await.err().unwrap());
            errors.push(
                storage
                    .revoke_access(&MemberAccess::ProviderAccount("member".into()))
                    .await
                    .err()
                    .unwrap(),
            );
        }
        for error in &errors {
            assert_eq!(
                error.failure(),
                StorageFailure::PermissionDenied,
                "{error:?}"
            );
        }
        errors
    }
}
