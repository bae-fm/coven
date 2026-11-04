//! Finding and ending the uploads another device left (§17.10, Appendix
//! E.10). A copy profile stages every session and temporary in the device's
//! reserved folder; conditional completion puts a session at its destination,
//! whose path names the device only for its files and snapshots.
use super::*;
use crate::left_upload::{Left, LeftUpload};
use coven_foundation::id_source::{DeviceId, StoreId};

impl S3Storage {
    /// `device`'s unfinished multipart uploads in its reserved folder and at
    /// its file and snapshot destinations, then the objects it staged in its
    /// folder. Sessions come first, so ending them stops any from assembling
    /// another temporary before the temporaries go.
    pub(super) async fn left_uploads(
        &self,
        store: StoreId,
        device: DeviceId,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        let base = self.prefix.key_prefix();
        let folder = format!("{base}{}/", coven_format::path::device_uploads(store, device));
        let mut left = Vec::new();
        for prefix in [
            folder.clone(),
            self.prefix.key_prefix_of(&ObjectPrefix::device_files(store, device)),
            // Its snapshots sit in one folder per audience, and storage
            // doesn't know the store's circles: the whole snapshots folder is
            // listed, and a session kept when its path names the device.
            self.prefix.key_prefix_of(&ObjectPrefix::snapshots(store)),
        ] {
            for (key, id, _) in self.unfinished_uploads(&prefix, None).await? {
                let named = key.starts_with(&folder)
                    || key
                        .strip_prefix(&base)
                        .and_then(|path| ObjectPath::parse(path).ok())
                        .is_some_and(|path| path.device() == Some(device));
                if named {
                    left.push(LeftUpload(Left::S3Multipart { key, id: SecretText::new(id) }));
                }
            }
        }
        for key in self.keys_under(&folder, self.settings.listing).await? {
            left.push(LeftUpload(Left::Staged(key)));
        }
        Ok(left)
    }

    /// The unfinished multipart uploads at slots 1 to `through`. Only
    /// conditional completion puts a claim's session at its slot's path; a
    /// copy profile stages it in its device's folder, so nothing is listed.
    pub(super) async fn settled_claims(
        &self,
        store: StoreId,
        through: u64,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        if self.settings.copy_publication.multipart().is_some() {
            return Ok(Vec::new());
        }
        let base = self.prefix.key_prefix();
        let mut left = Vec::new();
        let slots = self.prefix.key_prefix_of(&ObjectPrefix::slots(store));
        for (key, id, _) in self.unfinished_uploads(&slots, None).await? {
            let settled = key
                .strip_prefix(&base)
                .and_then(|path| ObjectPath::parse(path).ok())
                .and_then(|path| path.slot_number())
                .is_some_and(|slot| slot.0 <= through);
            if settled {
                left.push(LeftUpload(Left::S3Multipart { key, id: SecretText::new(id) }));
            }
        }
        Ok(left)
    }

    /// The unfinished multipart uploads at the floor's key begun before
    /// `floor`, the current floor, was published, compared in whole seconds
    /// with a strictly earlier second: each observed an older revision, so
    /// none can publish. Only a profile certified to date an upload's start
    /// reaches this (Appendix E.6); a listing without the date there is a
    /// protocol failure.
    pub(super) async fn superseded_floor(
        &self,
        floor: &StoredObject,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        let key = self.prefix.key(&floor.path);
        let published = floor.stored_at.seconds();
        let mut left = Vec::new();
        for (listed, id, initiated) in self.unfinished_uploads(&key, None).await? {
            if listed != key {
                continue;
            }
            let initiated = initiated
                .ok_or(StorageFailure::Protocol.with_source("S3 listed no upload start"))?;
            let begun = u64::try_from(initiated.secs())
                .map_err(|error| StorageFailure::Protocol.with_source(error))?;
            if begun < published {
                left.push(LeftUpload(Left::S3Multipart { key: listed, id: SecretText::new(id) }));
            }
        }
        Ok(left)
    }

    /// Abort a left multipart upload, or delete a staged object. One
    /// already completed, aborted or deleted succeeds.
    pub(super) async fn end_left(&self, upload: &LeftUpload) -> Result<(), StorageError> {
        match &upload.0 {
            Left::S3Multipart { key, id } => self.abort_multipart(key, id.as_str(), None).await,
            Left::Staged(key) => self.delete_key(key).await,
            // Another provider's: not this one's to end.
            #[cfg(target_vendor = "apple")]
            Left::CloudKitRoot(_) => Err(StorageFailure::SessionMismatch.into()),
            #[cfg(any(test, feature = "test-utils"))]
            Left::Memory(_) => Err(StorageFailure::SessionMismatch.into()),
        }
    }

    /// Every unfinished multipart upload under `prefix`: its key, upload id
    /// and, where the service lists it, when it began. The requests count
    /// under `publication` when one is given. A page that doesn't say
    /// whether another follows is a protocol failure, never the last.
    pub(super) async fn unfinished_uploads(
        &self,
        prefix: &str,
        publication: Option<&PublicationRequest>,
    ) -> Result<Vec<(String, String, Option<aws_sdk_s3::primitives::DateTime>)>, StorageError> {
        let mut key_marker = None;
        let mut upload_marker = None;
        let mut uploads = Vec::new();
        let mut pages = Pagination::new();
        loop {
            let mut request = self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_key_marker(key_marker)
                .set_upload_id_marker(upload_marker)
                .customize();
            if let Some(publication) = publication {
                request = request.interceptor(publication.clone());
            }
            let page = request.send().await.map_err(s3_error)?;
            for upload in page.uploads() {
                let key = metadata::required(upload.key(), "S3 upload listing omitted key")?;
                within(key, prefix)?;
                let id = metadata::required(upload.upload_id(), "S3 upload listing omitted id")?;
                uploads.push((key.to_owned(), id.to_owned(), upload.initiated().copied()));
            }
            if !page
                .is_truncated()
                .ok_or(StorageFailure::Protocol.with_source("S3 omitted page completion"))?
            {
                return Ok(uploads);
            }
            let key = metadata::required(page.next_key_marker(), "S3 omitted upload key marker")?;
            let id = metadata::required(page.next_upload_id_marker(), "S3 omitted upload marker")?;
            pages.check(&format!("{key}/{id}"))?;
            key_marker = Some(key.to_owned());
            upload_marker = Some(id.to_owned());
        }
    }

    /// Every object key under `prefix`, across all pages.
    pub(super) async fn keys_under(
        &self,
        prefix: &str,
        listing: S3Listing,
    ) -> Result<Vec<String>, StorageError> {
        let mut marker = None;
        let mut keys = Vec::new();
        let mut pages = Pagination::new();
        loop {
            let page = self.list_page(listing, prefix, None, marker, None).await?;
            for object in page.contents {
                let key = metadata::required(object.key(), "S3 list entry omitted key")?;
                within(key, prefix)?;
                keys.push(key.to_owned());
            }
            let Some(next) = page.next else {
                return Ok(keys);
            };
            pages.check(&next)?;
            marker = Some(next);
        }
    }
}

/// Refuse a listed key outside the requested prefix rather than remove it.
pub(super) fn within(key: &str, prefix: &str) -> Result<(), StorageError> {
    if key.starts_with(prefix) {
        Ok(())
    } else {
        Err(StorageFailure::Protocol.with_source("S3 listed a key outside the requested prefix"))
    }
}
