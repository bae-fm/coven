use crate::*;
use coven_foundation::id_source::{DeviceId, FileId, StoreId};
use std::num::NonZeroU64;
use std::sync::Arc;

/// Run identical object, range, prefix and immutable-create checks on any provider.
/// The caller supplies the capability at construction, like production composition.
pub struct Conformance {
    storage: Arc<dyn Storage>,
    store: StoreId,
}
/// The provider refused an occupied immutable path and left it unchanged.
fn occupied<T>(result: Result<T, StorageError>) -> bool {
    result.is_err_and(|error| {
        error.failure() == StorageFailure::AlreadyExists
            && !matches!(error, StorageError::Cleanup { .. })
    })
}

impl Conformance {
    /// The provider under test, with an unused store ID.
    pub fn new(storage: Arc<dyn Storage>, store: StoreId) -> Self {
        Self { storage, store }
    }
    /// Two stores may publish the same device and number in one provider location.
    pub async fn shared_location(&self, other: StoreId) {
        assert_ne!(self.store, other);
        let first = ObjectPath::slot(self.store, NonZeroU64::MIN);
        let second = ObjectPath::slot(other, NonZeroU64::MIN);
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
        let path = ObjectPath::file(self.store, DeviceId(31), FileId(uuid::Uuid::from_u128(1)));
        let other = ObjectPath::file(self.store, DeviceId(32), FileId(uuid::Uuid::from_u128(1)));
        let data = b"encrypted bytes";
        if self.storage.status(&path).await?.is_some() {
            return Err(StorageFailure::Protocol.with_source("absent exact status"));
        }
        let created = self.storage.create(&path, data).await?;
        self.storage.create(&other, b"other").await?;
        let first = self
            .storage
            .status(&path)
            .await?
            .ok_or(StorageFailure::NotFound)?;
        if created.is_some_and(|created| created != first) {
            return Err(
                StorageFailure::Protocol.with_source("create reply differs from exact status")
            );
        }
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
        if !occupied(self.storage.create(&path, b"replacement").await) {
            return Err(StorageFailure::Protocol.with_source("create-once refusal"));
        }
        if !occupied(self.storage.create(&path, data).await) {
            return Err(StorageFailure::Protocol.with_source("create-once retry refusal"));
        }
        if self.storage.status(&path).await?.as_ref() != Some(&first) {
            return Err(StorageFailure::Protocol
                .with_source("immutable retry changed publication metadata"));
        }
        let listed = self
            .storage
            .list(&ObjectPrefix::device_files(self.store, DeviceId(31)))
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
            self.storage.replace(&first, data).await,
            Err(error) if error.failure() == StorageFailure::InvalidPath
        ) {
            return Err(
                StorageFailure::Protocol.with_source("immutable object accepted replacement")
            );
        }
        for path in [ObjectPath::floor(self.store)] {
            if !matches!(
                self.storage.begin_upload(&path, 1).await,
                Err(error) if error.failure() == StorageFailure::InvalidPath
            ) {
                return Err(StorageFailure::Protocol
                    .with_source("replaceable object accepted a recorded upload"));
            }
            self.storage.create(&path, b"last record").await?;
            let mut previous = self
                .storage
                .status(&path)
                .await?
                .ok_or(StorageFailure::NotFound)?;
            for bytes in [b"next record", b"last record"] {
                self.storage.replace(&previous, bytes).await?;
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
                if !matches!(self.storage.replace(&previous, b"stale floor").await, Err(error) if error.failure() == StorageFailure::Conflict)
                {
                    return Err(StorageFailure::Protocol
                        .with_source("stale floor replacement was accepted"));
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
        if !occupied(self.storage.create(&path, &[]).await) {
            return Err(StorageFailure::Protocol.with_source("empty create-once refusal"));
        }
        if !self.storage.read(&path).await?.is_empty() {
            return Err(StorageFailure::Protocol.with_source("empty object changed"));
        }
        if !occupied(self.storage.create(&path, b"different").await) {
            return Err(StorageFailure::Protocol.with_source("empty create-once refusal"));
        }
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

    /// Setups of one store race for its first sequence slot (§10.3, D10): of three
    /// concurrent creates, one per claimant connection, exactly one wins and
    /// keeps its bytes, and the others are refused as occupied. The winner's
    /// connection deletes the slot afterwards.
    pub async fn creation_race(
        &self,
        claimants: [Arc<dyn Storage>; 3],
    ) -> Result<(), StorageError> {
        let marker = ObjectPath::slot(self.store, NonZeroU64::MIN);
        let claims: [&[u8]; 3] = [b"claim 1", b"claim 2", b"claim 3"];
        let (first, second, third) = tokio::join!(
            claimants[0].create(&marker, claims[0]),
            claimants[1].create(&marker, claims[1]),
            claimants[2].create(&marker, claims[2]),
        );
        let mut winners = Vec::new();
        for (index, result) in [first, second, third].into_iter().enumerate() {
            match result {
                Ok(_) => winners.push(index),
                Err(error)
                    if error.failure() == StorageFailure::AlreadyExists
                        && !matches!(error, StorageError::Cleanup { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        let [winner] = winners.as_slice() else {
            return Err(StorageFailure::Protocol
                .with_source(format!("{} concurrent creates won one path", winners.len())));
        };
        if self.storage.read(&marker).await? != claims[*winner] {
            return Err(StorageFailure::Protocol.with_source("creation race kept another claim"));
        }
        claimants[*winner].delete(&marker).await
    }
}

impl Conformance {
    /// Preserve provider publication times and sizes through listings and retries.
    /// Advance the fixture clock after publication to distinguish retry timestamps.
    pub async fn listing_times(
        &self,
        stored_at: coven_format::value::StorageTime,
        advance: impl std::future::Future<Output = ()>,
    ) {
        let storage = &self.storage;
        let first = ObjectPath::file(self.store, DeviceId(31), FileId(uuid::Uuid::from_u128(1)));
        let second = ObjectPath::file(self.store, DeviceId(32), FileId(uuid::Uuid::from_u128(1)));
        storage.create(&first, b"first").await.unwrap();
        storage.create(&second, b"second").await.unwrap();
        let expected = storage
            .list(&ObjectPrefix::files(self.store))
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
        assert!(occupied(storage.create(&first, b"first").await));
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
        let path = ObjectPath::file(self.store, DeviceId(31), FileId(uuid::Uuid::from_u128(1)));
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
        let path = ObjectPath::file(self.store, DeviceId(31), FileId(uuid::Uuid::from_u128(1)));
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
        let path = ObjectPath::slot(self.store, NonZeroU64::MIN);
        let floor = ObjectPath::floor(self.store);
        storage.create(&floor, b"previous floor").await.unwrap();
        let previous = storage.status(&floor).await.unwrap().unwrap();
        let mut ready = storage.begin_upload(&path, 4).await.unwrap();
        let part = vec![1; ready.part_size()];
        let mut pending = storage
            .begin_upload(&path, part.len() as u64 + 1)
            .await
            .unwrap();
        storage.upload_part(&mut pending, &part).await.unwrap();
        storage.upload_part(&mut ready, b"data").await.unwrap();
        deny.await;
        let mut errors = vec![
            storage.create(&path, b"data").await.err().unwrap(),
            storage.replace(&previous, b"data").await.err().unwrap(),
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
            storage.resume_upload(&mut pending).await.err().unwrap(),
            storage.upload_part(&mut pending, b"z").await.err().unwrap(),
            storage.finish_upload(&mut ready).await.err().unwrap(),
            storage.abort_upload(&pending).await.err().unwrap(),
        ];
        if storage.config().provider() != CloudProvider::S3 {
            // S3 names its upload before any request (Appendix E.10).
            errors.push(storage.begin_upload(&path, 4).await.err().unwrap());
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
