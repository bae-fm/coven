//! Built-in CloudKit storage. All payloads reaching this adapter are encrypted.

#[path = "cloudkit_access.rs"]
mod access;
#[path = "cloudkit_native.rs"]
mod native;
#[path = "cloudkit_query.rs"]
mod query;
#[path = "cloudkit_read.rs"]
mod read;
#[path = "cloudkit_record.rs"]
mod record;
#[path = "cloudkit_upload.rs"]
mod upload;
#[path = "cloudkit_zone.rs"]
mod zone;
use crate::*;
use coven_foundation::id_source::IdSourceRef;
use objc2::{rc::Retained, AnyThread};
use objc2_cloud_kit::{CKContainer, CKDatabase, CKRecordID, CKRecordSavePolicy, CKRecordZoneID};
use objc2_foundation::NSString;
pub(super) use record::NativeError;
use record::{Record, PART_SIZE};
pub(super) use zone::prepare;

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
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<StoredObject, StorageError> {
        let mut record = self.object(path, bytes.len() as u64);
        let asset = record.set_asset(&self.files, bytes)?;
        publication.check()?;
        native::conditional_object(
            |operation| unsafe { self.database.addOperation(operation) },
            record,
            path,
            vec![asset],
        )
        .wait()
        .await?
        .first()
        .ok_or(StorageFailure::Protocol.with_source("CloudKit omitted its saved record"))?
        .metadata()
    }
    /// Fetch the native record carrying exactly the observed change tag. The
    /// same transaction replaces its bytes and removes its former asset chain.
    async fn replacement_object(
        &self,
        previous: &StoredObject,
        size: u64,
    ) -> Result<(Record, Vec<Retained<CKRecordID>>), StorageError> {
        let path = &previous.path;
        let mut record = self
            .fetch(path.as_str(), record::OBJECT_KEYS)
            .wait()
            .await
            .map_err(|error| {
                if error.failure() == StorageFailure::NotFound {
                    StorageFailure::Conflict.with_source(error)
                } else {
                    error
                }
            })?;
        let current = record.metadata()?;
        current.check(path)?;
        if current.replace_token != previous.replace_token {
            return Err(StorageFailure::Conflict.into());
        }
        let obsolete = match record.optional_text("upload")? {
            Some(token) => vec![self.record_id(&record::upload_name(path, &token)?)],
            None => vec![],
        };
        record.set_object_fields(path, size);
        record.remove_field("data");
        record.remove_field("upload");
        Ok((record, obsolete))
    }
    async fn require_owner(&self) -> Result<(), StorageError> {
        if native::current_account(&self.container).wait().await? != self.owner {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        Ok(())
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
    async fn create(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
        // CloudKit's own client sends the asset and says nothing of its bytes
        // until the save ends (§20.1).
        _sent: &SendProgress,
    ) -> Result<Option<StoredObject>, StorageError> {
        self.save_bytes(publication, path, bytes).await.map(Some)
    }
    async fn replace(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let (mut record, obsolete) = self
            .replacement_object(previous, bytes.len() as u64)
            .await?;
        let asset = record.set_asset(&self.files, bytes)?;
        publication.check()?;
        native::modify(
            &self.database,
            vec![record],
            &obsolete,
            CKRecordSavePolicy::IfServerRecordUnchanged,
            Some(previous.path.as_str()),
            vec![asset],
        )
        .wait()
        .await
        .map(drop)
        .map_err(StorageError::replacement)
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
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectStream<'_>, StorageError> {
        self.read_stream(path, range).await
    }
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError> {
        let mut cursor = None;
        loop {
            let prefix = listing.prefix().clone();
            let request = native::query(
                &self.database,
                &self.zone_id(),
                query::Selection(prefix.clone()),
                cursor,
                move |record| record.listed(&prefix),
                #[cfg(test)]
                self.query_pages.clone(),
            );
            let (records, next) = request.wait().await?;
            for object in records.into_iter().flatten() {
                listing.insert(object)?;
            }
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(()),
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
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        self.revoke(account).await
    }
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        self.accept(invitation).await
    }
    async fn begin_upload(
        &self,
        publication: &PublicationRequest,
        target: &UploadTarget,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        publication.check()?;
        self.prepare_upload(target, total)
    }
    async fn resume_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        publication.check()?;
        self.resume(session).await
    }
    async fn upload_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
        // As for `create`, CloudKit says nothing of a part's bytes.
        _sent: &SendProgress,
    ) -> Result<(), StorageError> {
        self.save_part(publication, session, bytes, end).await
    }
    async fn finish_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        self.publish(publication, session).await
    }
    async fn abort_upload(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        self.abort(publication, session).await
    }
}

#[cfg(test)]
#[path = "cloudkit_tests.rs"]
mod tests;
