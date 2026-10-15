//! Native record conversion, identity and failure classification.

use crate::{CloudProvider, ObjectPath, ObjectPrefix, StorageError, StorageFailure, StoredObject};
use coven_format::value::StorageTime;
use coven_foundation::files::{AssetFile, AssetFiles};
use objc2::{rc::Retained, runtime::ProtocolObject, AnyThread, Message};
use objc2_cloud_kit::{
    CKAsset, CKErrorCode, CKErrorDomain, CKPartialErrorsByItemIDKey, CKRecord,
    CKRecordChangedErrorClientRecordKey, CKRecordID, CKReference, CKReferenceAction, CKShare,
};
use objc2_foundation::{NSCopying, NSDictionary, NSError, NSString, NSURL};
use std::time::Duration;

pub(super) use crate::session::CLOUDKIT_PART_SIZE as PART_SIZE;
pub(super) const OBJECT_KEYS: &[&str] = &["path", "size", "upload"];
pub(super) const SESSION_KEYS: &[&str] = &["path", "total", "confirmed", "state"];

/// An exclusively owned CloudKit record, moved between a callback and its caller.
/// Raw references never cross an await; mutation requires exclusive Rust access.
pub(super) struct Record(Retained<CKRecord>);
// SAFETY: CKRecord supports background use. This wrapper transfers ownership;
// it is not Sync or Clone, and callback records are copied before transfer.
unsafe impl Send for Record {}

impl Record {
    pub(super) fn new(kind: &str, id: &CKRecordID) -> Self {
        objc2::rc::autoreleasepool(|_| {
            Self(unsafe {
                CKRecord::initWithRecordType_recordID(
                    CKRecord::alloc(),
                    &NSString::from_str(kind),
                    id,
                )
            })
        })
    }
    pub(super) fn from_callback(record: &CKRecord) -> Self {
        objc2::rc::autoreleasepool(|_| Self(record.copy()))
    }
    pub(super) fn from_share(share: Retained<CKShare>) -> Self {
        Self(share.into_super())
    }
    pub(super) fn raw(&self) -> &CKRecord {
        &self.0
    }
    pub(super) fn share(&mut self) -> Result<&CKShare, StorageError> {
        self.0.downcast_ref::<CKShare>().ok_or_else(|| {
            StorageFailure::Protocol.with_source("CloudKit returned a non-share record")
        })
    }
    pub(super) fn name(&self) -> String {
        objc2::rc::autoreleasepool(|_| unsafe { self.0.recordID().recordName().to_string() })
    }
    pub(super) fn text(&self, key: &str) -> Result<String, StorageError> {
        self.optional_text(key)?.ok_or_else(|| {
            StorageFailure::Protocol.with_source(format!("CloudKit record omitted {key}"))
        })
    }
    pub(super) fn optional_text(&self, key: &str) -> Result<Option<String>, StorageError> {
        objc2::rc::autoreleasepool(|_| {
            match unsafe { self.0.objectForKey(&NSString::from_str(key)) } {
                None => Ok(None),
                Some(value) => value
                    .downcast::<NSString>()
                    .map(|s| Some(s.to_string()))
                    .map_err(|_| {
                        StorageFailure::Protocol
                            .with_source(format!("CloudKit record has non-string {key}"))
                    }),
            }
        })
    }
    pub(super) fn set_text(&mut self, key: &str, value: &str) {
        objc2::rc::autoreleasepool(|_| unsafe {
            self.0.setObject_forKey(
                Some(ProtocolObject::from_ref(&*NSString::from_str(value))),
                &NSString::from_str(key),
            );
        })
    }
    pub(super) fn set_reference(&mut self, key: &str, id: &CKRecordID) {
        objc2::rc::autoreleasepool(|_| unsafe {
            let reference = CKReference::initWithRecordID_action(
                CKReference::alloc(),
                id,
                CKReferenceAction::DeleteSelf,
            );
            self.0.setObject_forKey(
                Some(ProtocolObject::from_ref(&*reference)),
                &NSString::from_str(key),
            );
        })
    }
    pub(super) fn number(&self, key: &str) -> Result<u64, StorageError> {
        self.text(key)?
            .parse()
            .map_err(|error| StorageFailure::Protocol.with_source(error))
    }
    pub(super) fn set_number(&mut self, key: &str, value: u64) {
        self.set_text(key, &value.to_string());
    }
    pub(super) fn set_object_fields(&mut self, path: &ObjectPath, size: u64) {
        self.set_text("path", path.as_str());
        self.set_number("size", size);
        for (field, directory) in super::query::directories(path) {
            self.set_text(field, directory);
        }
        if let Some((_, number)) = path.store_log_position() {
            self.set_number("number", number.get());
        }
    }
    pub(super) fn writer(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<coven_foundation::id_source::DeviceId, StorageError> {
        if unsafe { self.0.recordType() }.to_string() != "CovenObject" {
            return Err(
                StorageFailure::Protocol.with_source("CloudKit returned an unpublished writer")
            );
        }
        super::query::writer_id(&self.name(), &self.text("path")?, store)
    }
    pub(super) fn metadata(&self) -> Result<StoredObject, StorageError> {
        objc2::rc::autoreleasepool(|_| unsafe {
            if self.0.recordType().to_string() != "CovenObject" {
                return Err(StorageFailure::Protocol.into());
            }
            publication_metadata(
                &self.name(),
                &self.text("path")?,
                &self.text("size")?,
                self.0.creationDate().map(|v| v.timeIntervalSince1970()),
                self.0.modificationDate().map(|v| v.timeIntervalSince1970()),
                self.0.recordChangeTag().map(|v| v.to_string()).as_deref(),
            )
        })
    }
    pub(super) fn listed(&self, prefix: &ObjectPrefix) -> Result<StoredObject, StorageError> {
        let object = self.metadata()?;
        check_prefix(prefix, &object.path)?;
        Ok(object)
    }
    pub(super) fn set_asset(
        &mut self,
        files: &AssetFiles,
        bytes: &[u8],
    ) -> Result<AssetFile, StorageError> {
        objc2::rc::autoreleasepool(|_| {
            if bytes.len() > PART_SIZE {
                return Err(StorageFailure::InvalidPart.into());
            }
            let file = files
                .stage(bytes)
                .map_err(|error| StorageFailure::File.with_source(error))?;
            let url = NSURL::from_file_path(file.path()).ok_or(StorageFailure::InvalidPath)?;
            unsafe {
                let asset = CKAsset::initWithFileURL(CKAsset::alloc(), &url);
                self.0.setObject_forKey(
                    Some(ProtocolObject::from_ref(&*asset)),
                    &NSString::from_str("data"),
                );
            }
            Ok(file)
        })
    }
    /// Read the staged file while still in the fetch callback. Memory is bounded
    /// even if the server's asset disagrees with its declared size.
    pub(super) fn asset_bytes(&self, files: &AssetFiles) -> Result<Vec<u8>, StorageError> {
        objc2::rc::autoreleasepool(|_| {
            let value = unsafe { self.0.objectForKey(&NSString::from_str("data")) }
                .ok_or(StorageFailure::Protocol)?;
            let asset = value
                .downcast::<CKAsset>()
                .map_err(|_| StorageFailure::Protocol)?;
            let url = unsafe { asset.fileURL() }.ok_or(StorageFailure::Protocol)?;
            let path = url.to_file_path().ok_or(StorageFailure::InvalidPath)?;
            files
                .read(&path, PART_SIZE)
                .map_err(|error| StorageFailure::File.with_source(error))
        })
    }
}

pub(super) fn record_name(path: &ObjectPath) -> &str {
    path.as_str()
}
pub(super) fn upload_name(path: &ObjectPath, token: &str) -> Result<String, StorageError> {
    crate::session::check_cloudkit_id(token)?;
    Ok(format!("{}/.coven-uploads/{token}", path.store()))
}
pub(super) fn part_name(root: &str, offset: u64) -> String {
    format!("{root}/{offset}")
}
pub(super) fn check_prefix(prefix: &ObjectPrefix, path: &ObjectPath) -> Result<(), StorageError> {
    if !prefix.contains(path) {
        return Err(StorageFailure::Protocol
            .with_source("CloudKit query returned a record outside its prefix"));
    }
    Ok(())
}
/// CloudKit dates count whole milliseconds (§4), but `NSDate` reports them
/// as floating-point seconds, which rarely land exactly on a millisecond.
/// Round to the nearest one so the time sits on the provider's tick. A
/// non-finite date or one before the epoch is a protocol error.
fn whole_milliseconds(seconds: f64) -> Result<StorageTime, StorageError> {
    let milliseconds = (seconds * 1000.0).round();
    if !milliseconds.is_finite() || milliseconds < 0.0 || milliseconds >= u64::MAX as f64 {
        return Err(StorageFailure::Protocol.with_source("CloudKit date out of range"));
    }
    let milliseconds = milliseconds as u64;
    StorageTime::new(
        milliseconds / 1000,
        u32::try_from(milliseconds % 1000).expect("below one thousand") * 1_000_000,
    )
    .map_err(|error| StorageFailure::Protocol.with_source(error))
}

pub(super) fn publication_metadata(
    name: &str,
    path: &str,
    size: &str,
    created: Option<f64>,
    modified: Option<f64>,
    revision: Option<&str>,
) -> Result<StoredObject, StorageError> {
    let path =
        ObjectPath::parse(path).map_err(|error| StorageFailure::Protocol.with_source(error))?;
    let revision = revision
        .filter(|value| !value.is_empty())
        .ok_or(StorageFailure::Protocol)?;
    if name != record_name(&path) {
        return Err(StorageFailure::Protocol.into());
    }
    let size = size
        .parse()
        .map_err(|error| StorageFailure::Protocol.with_source(error))?;
    let seconds = if path.is_replaceable() {
        modified
    } else {
        created
    }
    .ok_or(StorageFailure::Protocol)?;
    let stored_at = whole_milliseconds(seconds)?;
    Ok(StoredObject {
        path,
        size,
        stored_at,
        time_resolution: Duration::from_millis(1),
        object_id: name.to_owned(),
        revision: revision.to_owned(),
        // CloudKit exposes no asset checksum through CKRecord/CKAsset.
        checksum: None,
    })
}

pub(super) fn failure(code: CKErrorCode) -> StorageFailure {
    match code {
        CKErrorCode::NetworkUnavailable
        | CKErrorCode::NetworkFailure
        | CKErrorCode::ServerResponseLost => StorageFailure::Network,
        CKErrorCode::NotAuthenticated => StorageFailure::Authentication,
        CKErrorCode::PermissionFailure
        | CKErrorCode::ManagedAccountRestricted
        | CKErrorCode::ParticipantMayNeedVerification => StorageFailure::PermissionDenied,
        CKErrorCode::UnknownItem => StorageFailure::NotFound,
        CKErrorCode::ZoneNotFound | CKErrorCode::UserDeletedZone => {
            StorageFailure::ContainerNotFound
        }
        CKErrorCode::ServerRecordChanged => StorageFailure::AlreadyExists,
        CKErrorCode::QuotaExceeded => StorageFailure::QuotaExceeded,
        CKErrorCode::RequestRateLimited
        | CKErrorCode::ServiceUnavailable
        | CKErrorCode::ZoneBusy
        | CKErrorCode::AccountTemporarilyUnavailable => StorageFailure::RateLimited,
        CKErrorCode::BadContainer
        | CKErrorCode::MissingEntitlement
        | CKErrorCode::BadDatabase
        | CKErrorCode::InvalidArguments => StorageFailure::InvalidConfiguration,
        CKErrorCode::AssetFileNotFound
        | CKErrorCode::AssetFileModified
        | CKErrorCode::AssetNotAvailable => StorageFailure::File,
        _ => StorageFailure::Refused,
    }
}

fn classified(error: &NSError) -> StorageFailure {
    unsafe {
        if &*error.domain() != CKErrorDomain {
            return StorageFailure::Refused;
        }
        if CKErrorCode(error.code()) != CKErrorCode::PartialFailure {
            return failure(CKErrorCode(error.code()));
        }
        let info = error.userInfo();
        let Some(value) = info.objectForKey(CKPartialErrorsByItemIDKey) else {
            return StorageFailure::Protocol;
        };
        let Some(errors) = value.downcast_ref::<NSDictionary>() else {
            return StorageFailure::Protocol;
        };
        let mut classification = None;
        for value in errors.allValues() {
            let Some(error) = value.downcast_ref::<NSError>() else {
                return StorageFailure::Protocol;
            };
            if &*error.domain() == CKErrorDomain
                && CKErrorCode(error.code()) == CKErrorCode::BatchRequestFailed
            {
                continue;
            }
            let next = classified(error);
            match classification {
                None => classification = Some(next),
                Some(prior) if prior == next => {}
                Some(_) => return StorageFailure::Refused,
            }
        }
        match classification {
            Some(value) => value,
            None => StorageFailure::Protocol,
        }
    }
}

struct NativeError(Retained<NSError>);
impl std::fmt::Debug for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NSError's userInfo can contain whole records and share URLs.
        write!(f, "{} ({})", self.0.domain(), self.0.code())
    }
}
impl std::error::Error for NativeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}
pub(super) fn native_error(error: &NSError) -> StorageError {
    StorageError::Provider {
        provider: CloudProvider::CloudKit,
        failure: classified(error),
        source: Box::new(NativeError(error.retain())),
    }
}

// Only an occupied destination is AlreadyExists. A changed upload/session/share
// must reach its initiator as a refusal, never as an occupied path to compare.
pub(super) fn save_error(error: &NSError, created: Option<&str>) -> StorageError {
    let mut result = native_error(error);
    if let StorageError::Provider { failure, .. } = &mut result {
        if *failure == StorageFailure::AlreadyExists
            && !created.is_some_and(|name| conflict_names(error, name))
        {
            *failure = StorageFailure::Refused;
        }
    }
    result
}
fn conflict_names(error: &NSError, name: &str) -> bool {
    unsafe {
        let info = error.userInfo();
        if let Some(value) = info.objectForKey(CKPartialErrorsByItemIDKey) {
            if let Some(errors) = value.downcast_ref::<NSDictionary>() {
                for key in errors.allKeys() {
                    if let Some(id) = key.downcast_ref::<CKRecordID>() {
                        if id.recordName().to_string() == name {
                            if let Some(value) = errors.objectForKey(&key) {
                                if let Some(error) = value.downcast_ref::<NSError>() {
                                    return classified(error) == StorageFailure::AlreadyExists;
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }
        match info.objectForKey(CKRecordChangedErrorClientRecordKey) {
            Some(value) => value
                .downcast_ref::<CKRecord>()
                .is_some_and(|record| record.recordID().recordName().to_string() == name),
            None => false,
        }
    }
}
