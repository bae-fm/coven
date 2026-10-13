//! Metadata fields returned by the account providers.

use super::http;
use crate::*;
use serde_json::Value;
use std::time::Duration;

pub(super) fn drive(path: &ObjectPath, item: &Value) -> Result<StoredObject, StorageError> {
    if http::string(item, "name")? != path.as_str()
        || item["trashed"].as_bool() == Some(true)
        || item["mimeType"]
            .as_str()
            .is_some_and(|kind| kind.starts_with("application/vnd.google-apps."))
    {
        return Err(StorageFailure::Protocol.with_source("Drive returned another object"));
    }
    let field = if path.is_replaceable() {
        "modifiedTime"
    } else {
        "createdTime"
    };
    Ok(StoredObject {
        path: path.clone(),
        size: http::string(item, "size")?
            .parse()
            .map_err(|e| StorageFailure::Protocol.with_source(e))?,
        stored_at: http::timestamp(item, field)?,
        time_resolution: resolution(http::string(item, field)?)?,
        object_id: http::string(item, "id")?.into(),
        // File version includes metadata-only changes such as viewing the file.
        revision: http::string(item, "headRevisionId")?.into(),
        checksum: checksum(item, "md5Checksum", ChecksumValue::Md5Hex)?,
    })
}

pub(super) fn onedrive(path: &ObjectPath, item: &Value) -> Result<StoredObject, StorageError> {
    if !item["file"].is_object() || http::string(item, "name")? != path.file_name() {
        return Err(StorageFailure::Protocol.with_source("OneDrive returned another object"));
    }
    let field = if path.is_replaceable() {
        "lastModifiedDateTime"
    } else {
        "createdDateTime"
    };
    let hashes = &item["file"]["hashes"];
    let checksum = if hashes.get("sha1Hash").is_some() {
        checksum(hashes, "sha1Hash", ChecksumValue::Sha1Hex)?
    } else {
        checksum(hashes, "quickXorHash", ChecksumValue::QuickXorBase64)?
    };
    Ok(StoredObject {
        path: path.clone(),
        size: item["size"]
            .as_u64()
            .ok_or(StorageFailure::Protocol.with_source("OneDrive omitted object size"))?,
        stored_at: http::timestamp(item, field)?,
        time_resolution: resolution(http::string(item, field)?)?,
        object_id: http::string(item, "id")?.into(),
        revision: http::string(item, "eTag")?.into(),
        checksum,
    })
}

pub(super) fn dropbox(path: &ObjectPath, item: &Value) -> Result<StoredObject, StorageError> {
    if http::string(item, "path_lower")? != path.absolute() {
        return Err(StorageFailure::Protocol.with_source("Dropbox returned another object"));
    }
    Ok(StoredObject {
        path: path.clone(),
        size: item["size"]
            .as_u64()
            .ok_or(StorageFailure::Protocol.with_source("Dropbox omitted object size"))?,
        stored_at: http::timestamp(item, "server_modified")?,
        time_resolution: Duration::from_secs(1),
        object_id: http::string(item, "id")?.into(),
        revision: http::string(item, "rev")?.into(),
        checksum: checksum(item, "content_hash", ChecksumValue::DropboxContentHash)?,
    })
}

fn checksum(
    item: &Value,
    field: &str,
    value: fn(String) -> ChecksumValue,
) -> Result<Option<ObjectChecksum>, StorageError> {
    match item.get(field) {
        None => Ok(None),
        Some(_) => Ok(Some(ObjectChecksum::FullObject(value(
            http::string(item, field)?.into(),
        )))),
    }
}

fn resolution(timestamp: &str) -> Result<Duration, StorageError> {
    let digits = match timestamp.split_once('.') {
        None => 0,
        Some((_, fraction)) => fraction.chars().take_while(char::is_ascii_digit).count(),
    };
    if digits > 9 {
        return Err(StorageFailure::Protocol.with_source("timestamp precision exceeds nanoseconds"));
    }
    Ok(Duration::from_nanos(10u64.pow(9 - digits as u32)))
}

#[cfg(test)]
#[path = "object_metadata_tests.rs"]
mod tests;
