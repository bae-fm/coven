//! S3's ETag, replacement revision and checksum are separate facts.
use super::S3Storage;
use crate::*;
use aws_sdk_s3::{
    operation::{get_object::GetObjectOutput, head_object::HeadObjectOutput},
    types::ChecksumType,
};

pub(super) fn required<'a>(
    value: Option<&'a str>,
    reason: &'static str,
) -> Result<&'a str, StorageError> {
    value.filter(|value| !value.is_empty()).ok_or(StorageFailure::Protocol.with_source(reason))
}

fn revision(
    storage: &S3Storage,
    path: &ObjectPath,
    etag: Option<&str>,
    version: Option<&str>,
    metadata: Option<&std::collections::HashMap<String, String>>,
) -> Result<String, StorageError> {
    if !path.is_replaceable() {
        return Ok(required(etag, "S3 omitted ETag")?.into());
    }
    match storage.settings.revision {
        S3Revision::Generation => Ok(required(version, "GCS omitted generation")?.into()),
        S3Revision::ETag => match version.filter(|version| *version != "null") {
            Some(version) => {
                Ok(format!("version:{}", required(Some(version), "S3 omitted version")?))
            }
            None => Ok(format!(
                "coven:{}",
                required(
                    metadata.and_then(|values| values.get("coven-revision")).map(String::as_str),
                    "S3 omitted replacement revision"
                )?
            )),
        },
    }
}

fn checksum(
    policy: S3Checksums,
    kind: Option<&ChecksumType>,
    value: Option<ChecksumValue>,
) -> Result<Option<ObjectChecksum>, StorageError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if policy == S3Checksums::Absent {
        return Err(
            StorageFailure::Protocol.with_source("S3 returned a checksum its profile lacks")
        );
    }
    match kind {
        Some(ChecksumType::FullObject) => Ok(Some(ObjectChecksum::FullObject(value))),
        Some(ChecksumType::Composite) => Ok(Some(ObjectChecksum::Composite(value))),
        _ => Err(StorageFailure::Protocol.with_source("S3 omitted checksum scope")),
    }
}

// The SDK has separate HEAD/GET response types with the same native metadata.
macro_rules! observation {
    ($storage:expr, $path:expr, $response:expr, $size:expr) => {{
        let response = $response;
        let checksum_value = if let Some(value) = response.checksum_sha256() {
            Some(ChecksumValue::Sha256Base64(required(Some(value), "empty SHA256")?.into()))
        } else if let Some(value) = response.checksum_crc64_nvme() {
            Some(ChecksumValue::Crc64NvmeBase64(required(Some(value), "empty CRC64NVME")?.into()))
        } else if let Some(value) = response.checksum_crc32_c() {
            Some(ChecksumValue::Crc32cBase64(required(Some(value), "empty CRC32C")?.into()))
        } else if let Some(value) = response.checksum_crc32() {
            Some(ChecksumValue::Crc32Base64(required(Some(value), "empty CRC32")?.into()))
        } else {
            response
                .checksum_sha1()
                .map(|value| {
                    required(Some(value), "empty SHA1")
                        .map(|value| ChecksumValue::Sha1Base64(value.into()))
                })
                .transpose()?
        };
        Ok(StoredObject {
            path: $path.clone(),
            size: $size,
            stored_at: crate::providers::http::storage_time(
                *response
                    .last_modified()
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted storage time"))?,
            )?,
            time_resolution: std::time::Duration::from_secs(1),
            object_id: $storage.prefix.key(&$path),
            revision: revision(
                $storage,
                $path,
                response.e_tag(),
                response.version_id(),
                response.metadata(),
            )?,
            replace_token: match $storage.settings.revision {
                S3Revision::ETag => required(response.e_tag(), "S3 omitted ETag")?.into(),
                S3Revision::Generation => {
                    required(response.version_id(), "GCS omitted generation")?.into()
                }
            },
            checksum: checksum(
                $storage.settings.checksums,
                response.checksum_type(),
                checksum_value,
            )?,
        })
    }};
}

pub(super) fn head(
    storage: &S3Storage,
    path: &ObjectPath,
    response: &HeadObjectOutput,
) -> Result<StoredObject, StorageError> {
    let size = u64::try_from(
        response
            .content_length()
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted content length"))?,
    )
    .map_err(|e| StorageFailure::Protocol.with_source(e))?;
    observation!(storage, path, response, size)
}
pub(super) fn get(
    storage: &S3Storage,
    path: &ObjectPath,
    response: &GetObjectOutput,
    size: u64,
) -> Result<StoredObject, StorageError> {
    observation!(storage, path, response, size)
}
