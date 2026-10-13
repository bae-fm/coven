//! Built-in CloudKit storage. All payloads reaching this adapter are encrypted.

#[path = "cloudkit_access.rs"]
mod access;
#[path = "cloudkit_native.rs"]
mod native;
#[path = "cloudkit_query.rs"]
mod query;
#[path = "cloudkit_record.rs"]
mod record;
#[path = "cloudkit_upload.rs"]
mod upload;
#[path = "cloudkit_zone.rs"]
mod zone;
use crate::*;
use coven_foundation::id_source::{DeviceId, IdSourceRef, StoreId};
use objc2::{rc::Retained, AnyThread};
use objc2_cloud_kit::{CKContainer, CKDatabase, CKRecordID, CKRecordSavePolicy, CKRecordZoneID};
use objc2_foundation::NSString;
use record::{Record, PART_SIZE};
pub(super) use zone::prepare;

enum Body {
    Inline(Vec<u8>),
    Parts(String),
}

pub(crate) struct CloudKitStorage {
    container_id: String,
    container: Retained<CKContainer>,
    database: Retained<CKDatabase>,
    ids: IdSourceRef,
    files: std::sync::Arc<coven_foundation::files::AssetFiles>,
    owner: String,
    zone: String,
    #[cfg(test)]
    query_pages: tests::Pages,
}
impl CloudKitStorage {
    pub(crate) async fn new(config: StorageConfig, ids: IdSourceRef) -> Result<Self, StorageError> {
        native::check_available()?;
        config.validate()?;
        let StorageConfig::CloudKit {
            container: container_id,
            owner,
            zone,
        } = config
        else {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("expected CloudKit location")
            );
        };
        if zone == "_defaultZone" || owner == "__defaultOwner__" {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("CloudKit requires a custom zone and stable owner identity"));
        }
        // CloudKit owns this process's entitled container and account context.
        let container = objc2::rc::autoreleasepool(|_| unsafe {
            CKContainer::containerWithIdentifier(&NSString::from_str(&container_id))
        });
        let account = native::current_account(&container).wait().await?;
        let database = objc2::rc::autoreleasepool(|_| unsafe {
            if account == owner {
                container.privateCloudDatabase()
            } else {
                container.sharedCloudDatabase()
            }
        });
        Ok(Self {
            owner,
            zone,
            container_id,
            container,
            database,
            ids,
            #[cfg(test)]
            query_pages: Default::default(),
            files: std::sync::Arc::new(
                coven_foundation::files::AssetFiles::new()
                    .map_err(|error| StorageFailure::File.with_source(error))?,
            ),
        })
    }
    fn zone_id(&self) -> Retained<CKRecordZoneID> {
        objc2::rc::autoreleasepool(|_| unsafe {
            CKRecordZoneID::initWithZoneName_ownerName(
                CKRecordZoneID::alloc(),
                &NSString::from_str(&self.zone),
                &NSString::from_str(&self.owner),
            )
        })
    }
    fn record_id(&self, name: &str) -> Retained<CKRecordID> {
        objc2::rc::autoreleasepool(|_| unsafe {
            CKRecordID::initWithRecordName_zoneID(
                CKRecordID::alloc(),
                &NSString::from_str(name),
                &self.zone_id(),
            )
        })
    }
    fn fetch(&self, name: &str, keys: &[&str]) -> native::CloudKitRequest<Record> {
        native::fetch(&self.database, &self.record_id(name), keys, Ok)
    }
    fn object(&self, path: &ObjectPath, size: u64) -> Record {
        let mut record = Record::new("CovenObject", &self.record_id(record::record_name(path)));
        record.set_object_fields(path, size);
        record
    }
    async fn save_bytes(
        &self,
        path: &ObjectPath,
        bytes: &[u8],
        policy: CKRecordSavePolicy,
    ) -> Result<(), StorageError> {
        let mut record = self.object(path, bytes.len() as u64);
        let asset = record.set_asset(&self.files, bytes)?;
        native::modify(
            &self.database,
            vec![record],
            &[],
            policy,
            Some(path.as_str()),
            vec![asset],
        )
        .wait()
        .await?;
        Ok(())
    }
    async fn require_owner(&self) -> Result<(), StorageError> {
        if native::current_account(&self.container).wait().await? != self.owner {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        Ok(())
    }
    async fn read_bytes(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectRead, StorageError> {
        let files = self.files.clone();
        let request = native::fetch(
            &self.database,
            &self.record_id(path.as_str()),
            &["path", "size", "upload", "data"],
            move |record| {
                let metadata = record.metadata()?;
                let body = match record.optional_text("upload")? {
                    Some(token) => Body::Parts(token),
                    None => Body::Inline(record.asset_bytes(&files)?),
                };
                Ok((metadata, body))
            },
        );
        let (metadata, body) = request.wait().await?;
        if metadata.path != *path {
            return Err(StorageFailure::Protocol.into());
        }
        let (start, end) = match range {
            Some(range) if range.end() <= metadata.size => (range.start(), range.end()),
            Some(_) => return Err(StorageFailure::InvalidRange.into()),
            None => (0, metadata.size),
        };
        let token = match body {
            Body::Inline(bytes) => {
                if bytes.len() as u64 != metadata.size {
                    return Err(StorageFailure::Protocol.into());
                }
                let bytes = match range {
                    Some(range) => bytes[range.start() as usize..range.end() as usize].to_vec(),
                    None => bytes,
                };
                return Ok(ObjectRead { metadata, bytes });
            }
            Body::Parts(token) => token,
        };
        let root = record::upload_name(path, &token)?;
        let mut output = Vec::new();
        let mut offset = start;
        while offset < end {
            let part_start = offset / PART_SIZE as u64 * PART_SIZE as u64;
            let name = record::part_name(&root, part_start);
            let files = self.files.clone();
            let request = native::fetch(
                &self.database,
                &self.record_id(&name),
                &["data"],
                move |record| record.asset_bytes(&files),
            );
            let bytes = request.wait().await?;
            let expected = (metadata.size - part_start).min(PART_SIZE as u64);
            if bytes.len() as u64 != expected {
                return Err(
                    StorageFailure::Protocol.with_source("CloudKit returned an incomplete asset")
                );
            }
            let within = usize::try_from(offset - part_start)
                .map_err(|error| StorageFailure::InvalidRange.with_source(error))?;
            let count = usize::try_from((end - offset).min(expected - (offset - part_start)))
                .map_err(|error| StorageFailure::InvalidRange.with_source(error))?;
            output.extend_from_slice(&bytes[within..within + count]);
            offset += count as u64;
        }
        Ok(ObjectRead {
            metadata,
            bytes: output,
        })
    }
}

#[async_trait::async_trait]
impl ProviderOps for CloudKitStorage {
    fn config(&self) -> StorageConfig {
        StorageConfig::CloudKit {
            container: self.container_id.clone(),
            owner: self.owner.clone(),
            zone: self.zone.clone(),
        }
    }
    fn single_request_limit(&self) -> u64 {
        PART_SIZE as u64
    }
    async fn account(&self) -> Result<String, StorageError> {
        self.account_identity().await
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.save_bytes(path, bytes, CKRecordSavePolicy::IfServerRecordUnchanged)
            .await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.save_bytes(path, bytes, CKRecordSavePolicy::AllKeys)
            .await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        match self
            .fetch(record::record_name(path), record::OBJECT_KEYS)
            .wait()
            .await
        {
            Ok(record) => {
                let metadata = record.metadata()?;
                metadata.check(path)?;
                Ok(Some(metadata))
            }
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
    async fn read_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectRead, StorageError> {
        self.read_bytes(path, range).await
    }
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError> {
        let mut cursor = None;
        loop {
            let prefix = listing.prefix().clone();
            let request = native::query(
                &self.database,
                &self.zone_id(),
                query::Selection::Objects(prefix.clone()),
                cursor,
                move |record| record.listed(&prefix),
                #[cfg(test)]
                self.query_pages.clone(),
            );
            let (records, next) = request.wait().await?;
            for object in records {
                listing.insert(object)?;
            }
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(()),
            }
        }
    }
    async fn list_writers(&self, store: StoreId) -> Result<Vec<DeviceId>, StorageError> {
        let mut writers = std::collections::BTreeSet::new();
        let mut cursor = None;
        loop {
            let request = native::query(
                &self.database,
                &self.zone_id(),
                query::Selection::Writers(store),
                cursor,
                move |record| record.writer(store),
                #[cfg(test)]
                self.query_pages.clone(),
            );
            let (page, next) = request.wait().await?;
            for writer in page {
                if !writers.insert(writer) {
                    return Err(StorageFailure::Protocol.with_source("CloudKit repeated a writer"));
                }
            }
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(writers.into_iter().collect()),
            }
        }
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        // Parts belong to the published record via native deleteSelf references.
        let request = native::modify(
            &self.database,
            vec![],
            &[self.record_id(path.as_str())],
            CKRecordSavePolicy::IfServerRecordUnchanged,
            None,
            vec![],
        );
        match request.wait().await {
            Ok(_) => Ok(()),
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.grant(account).await
    }
    async fn revoke_access(
        &self,
        member: &MemberAccess,
    ) -> Result<ProviderRevocation, StorageError> {
        self.revoke(member).await.map(ProviderRevocation::Remaining)
    }
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        self.accept(invitation).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.prepare_upload(path, total)
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.resume(session).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
    ) -> Result<(), StorageError> {
        self.save_part(session, bytes, end).await
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.publish(session).await
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.abort(session).await
    }
}

#[cfg(test)]
#[path = "cloudkit_tests.rs"]
mod tests;
