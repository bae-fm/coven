use crate::*;
use coven_foundation::id_source::{DeviceId, StoreId};
use std::sync::Arc;

/// Run identical object, range, prefix and immutable-create checks on any provider.
/// The caller supplies the capability at construction, like production composition.
pub struct Conformance {
    storage: Arc<dyn Storage>,
    store: StoreId,
}
impl Conformance {
    /// The provider under test, with an unused store ID.
    pub fn new(storage: Arc<dyn Storage>, store: StoreId) -> Self {
        Self { storage, store }
    }
    /// Two stores may publish the same device and number in one provider location.
    pub async fn shared_location(&self, other: StoreId) {
        assert_ne!(self.store, other);
        let first = ObjectPath::store_log(self.store, DeviceId(31), std::num::NonZeroU64::MIN);
        let second = ObjectPath::store_log(other, DeviceId(31), std::num::NonZeroU64::MIN);
        let (left, right) = tokio::join!(
            self.storage.create(&first, b"first"),
            self.storage.create(&second, b"second")
        );
        left.unwrap();
        right.unwrap();
        for (store, path, bytes) in [
            (self.store, &first, b"first".as_slice()),
            (other, &second, b"second".as_slice()),
        ] {
            assert_eq!(self.storage.read(path).await.unwrap(), bytes);
            let listed = self.storage.list(&ObjectPrefix::all(store)).await.unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(&listed[0].path, path);
        }
        self.storage.delete(&first).await.unwrap();
        assert_eq!(self.storage.read(&second).await.unwrap(), b"second");
        self.storage.delete(&second).await.unwrap();
    }
    /// Exercise the real provider operations, returning the first violated contract.
    pub async fn run(&self) -> Result<(), StorageError> {
        use coven_foundation::id_source::DeviceId;
        let path = ObjectPath::device_log(self.store, DeviceId(31), std::num::NonZeroU64::MIN);
        let other = ObjectPath::device_log(self.store, DeviceId(32), std::num::NonZeroU64::MIN);
        let data = b"encrypted bytes";
        if self.storage.status(&path).await?.is_some() {
            return Err(StorageFailure::Protocol.with_source("absent exact status"));
        }
        self.storage.create(&path, data).await?;
        self.storage.create(&other, b"other").await?;
        let first = self
            .storage
            .status(&path)
            .await?
            .ok_or(StorageFailure::NotFound)?;
        let read = self.storage.read_object(&path, None).await?;
        if read.metadata != first {
            return Err(
                StorageFailure::Protocol.with_source("read metadata differs from exact status")
            );
        }
        if read.bytes != data {
            return Err(StorageFailure::Protocol.with_source("whole read"));
        }
        for (start, end) in [(0, 3), (4, 8), (12, 15)] {
            let range = ByteRange::new(start, end)?;
            let read = self.storage.read_object(&path, Some(range)).await?;
            if read.metadata.size != first.size || read.metadata.revision != first.revision {
                return Err(
                    StorageFailure::Protocol.with_source("range omitted complete-object metadata")
                );
            }
            if read.bytes != range.select(data)? {
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
        if self.storage.status(&path).await?.as_ref() != Some(&first) {
            return Err(StorageFailure::Protocol
                .with_source("immutable retry changed publication metadata"));
        }
        let listed = self
            .storage
            .list(&ObjectPrefix::device_log(self.store, DeviceId(31)))
            .await?
            .into_iter()
            .map(|object| {
                (
                    object.path,
                    object.size,
                    object.stored_at,
                    object.object_id,
                    object.revision,
                )
            })
            .collect::<Vec<_>>();
        if listed
            != [(
                path.clone(),
                first.size,
                first.stored_at,
                first.object_id.clone(),
                first.revision.clone(),
            )]
        {
            return Err(StorageFailure::Protocol.with_source(format!(
                "prefix listing metadata: listed={listed:?}, status={first:?}"
            )));
        }
        self.storage.delete(&path).await?;
        self.storage.delete(&path).await?;
        self.storage.delete(&other).await?;
        if self.storage.status(&path).await?.is_some() {
            return Err(StorageFailure::Protocol.with_source("deleted exact status"));
        }
        if self.storage.read(&path).await.err().map(|e| e.failure())
            != Some(StorageFailure::NotFound)
        {
            return Err(StorageFailure::Protocol.with_source("deleted object read"));
        }
        if !matches!(
            self.storage.replace(&path, data).await,
            Err(error) if error.failure() == StorageFailure::InvalidPath
        ) {
            return Err(
                StorageFailure::Protocol.with_source("immutable object accepted replacement")
            );
        }
        for path in [
            ObjectPath::positions(self.store, DeviceId(31)),
            ObjectPath::clock(self.store, DeviceId(31)),
        ] {
            if !matches!(
                self.storage.begin_upload(&path, 1).await,
                Err(error) if error.failure() == StorageFailure::InvalidPath
            ) {
                return Err(StorageFailure::Protocol
                    .with_source("replaceable object accepted a recorded upload"));
            }
            self.storage.replace(&path, b"last record").await?;
            let mut previous = self
                .storage
                .status(&path)
                .await?
                .ok_or(StorageFailure::NotFound)?;
            for bytes in [b"next record", b"last record"] {
                self.storage.replace(&path, bytes).await?;
                let next = self
                    .storage
                    .status(&path)
                    .await?
                    .ok_or(StorageFailure::NotFound)?;
                if previous.revision == next.revision {
                    return Err(
                        StorageFailure::Protocol.with_source("replacement did not change revision")
                    );
                }
                previous = next;
            }
            if self.storage.read(&path).await? != b"last record" {
                return Err(StorageFailure::Protocol.with_source("object replacement"));
            }
            if !self
                .storage
                .list(&ObjectPrefix::all(self.store))
                .await?
                .iter()
                .any(|object| {
                    object.path == path
                        && object.size == previous.size
                        && object.revision == previous.revision
                })
            {
                return Err(StorageFailure::Protocol
                    .with_source("replaced object metadata differs from listing"));
            }
            self.storage.delete(&path).await?;
        }
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
        let first = ObjectPath::device_log(self.store, DeviceId(31), std::num::NonZeroU64::MIN);
        let second = ObjectPath::device_log(self.store, DeviceId(32), std::num::NonZeroU64::MIN);
        storage.create_once(&first, b"first").await.unwrap();
        storage.create_once(&second, b"second").await.unwrap();
        let expected = storage
            .list(&ObjectPrefix::device_logs(self.store))
            .await
            .unwrap();
        assert_eq!(
            expected
                .iter()
                .map(|object| (&object.path, object.size, object.stored_at))
                .collect::<Vec<_>>(),
            [(&first, 5, stored_at), (&second, 6, stored_at)]
        );
        advance.await;
        storage.create_once(&first, b"first").await.unwrap();
        assert_eq!(
            storage.list(&ObjectPrefix::all(self.store)).await.unwrap(),
            expected
        );
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
        let path = ObjectPath::device_log(self.store, DeviceId(31), std::num::NonZeroU64::MIN);
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
                .revoke_access("kept@example.test")
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
        let path = ObjectPath::device_log(self.store, DeviceId(31), std::num::NonZeroU64::MIN);
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
            self.store,
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        let positions =
            ObjectPath::positions(self.store, coven_foundation::id_source::DeviceId(31));
        let mut pending = storage.begin_upload(&path, 4).await.unwrap();
        let mut ready = storage.begin_upload(&path, 4).await.unwrap();
        storage.upload_part(&mut ready, b"data").await.unwrap();
        // Drive publishes the final part immediately. Test finish by
        // recovering its still-incomplete recording after the reply was lost.
        if ready.is_complete() {
            ready = pending.clone();
        }
        deny.await;
        let mut errors = vec![
            storage.create(&path, b"data").await.err().unwrap(),
            storage.replace(&positions, b"data").await.err().unwrap(),
            storage.read(&path).await.err().unwrap(),
            storage.status(&path).await.err().unwrap(),
            storage
                .read_range(&path, ByteRange::new(0, 1).unwrap())
                .await
                .err()
                .unwrap(),
            storage
                .list(&ObjectPrefix::all(self.store))
                .await
                .err()
                .unwrap(),
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
            errors.push(storage.revoke_access("member").await.err().unwrap());
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
