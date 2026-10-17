//! The S3 behaviors a location uses (§20.7). Catalogued profiles fill them; a
//! custom profile carries its own, so every device behaves identically.
use serde::{Deserialize, Serialize};

/// An S3 location's behaviors: a catalogued service's, or its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Profile {
    /// The catalogued service's settings.
    Named(S3ProfileName),
    /// Settings detected at setup or chosen by the app; restore codes carry them.
    Custom(S3Settings),
}

/// An S3 service whose catalogued profile a location names (§20.7).
/// Self-hosted services have arbitrary endpoints, so no hostname identifies them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3ProfileName {
    /// Amazon S3.
    Aws,
    /// Google Cloud Storage through its XML API.
    Gcs,
    /// Cloudflare R2.
    R2,
    /// RustFS, self-hosted.
    RustFs,
    /// SeaweedFS's S3 gateway, self-hosted.
    SeaweedFs,
    /// Ceph RADOS Gateway, self-hosted.
    CephRgw,
    /// Versity Gateway, self-hosted.
    VersityGw,
}

/// The bucket's versioning state, read through the service's XML API (§20.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum S3BucketVersioning {
    /// The bucket has never enabled versioning.
    Disabled,
    /// New writes retain earlier versions.
    Enabled,
    /// Versioning was suspended; earlier versions may remain.
    Suspended,
    /// The hosted service has no object-versioning feature (R2).
    Unavailable,
}

/// Every S3 behavior the adapter varies, one value each (§20.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Settings {
    /// How requests are authenticated.
    pub signing: S3Signing,
    /// The condition a create-once single PUT sends.
    pub single_put: S3SinglePut,
    /// Which creates publish by a conditional copy of a staged temporary.
    pub copy_publication: S3CopyPublication,
    /// What the native multipart `Last-Modified` dates.
    pub multipart_timestamp: S3MultipartTimestamp,
    /// How returned checksums are read.
    pub checksums: S3Checksums,
    /// The listing protocol.
    pub listing: S3Listing,
    /// Where a replaceable object's revision comes from.
    pub revision: S3Revision,
    /// Which response headers carry metadata, generation and hashes.
    pub response_headers: S3ResponseHeaders,
    /// Whether HEAD and GET request checksum mode.
    pub request_checksums: bool,
}

/// Request authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Signing {
    /// AWS Signature Version 4.
    AwsSigV4,
    /// Google V4 HMAC with `x-goog-*` headers.
    GoogleHmac,
}

/// The create-once condition on a single PUT.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3SinglePut {
    /// `If-None-Match: *`.
    IfNoneMatch,
    /// `x-goog-if-generation-match: 0`.
    GenerationMatch,
}

/// Which creates publish by a conditional copy of a reserved temporary (§20.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3CopyPublication {
    /// None: a single PUT and a multipart completion (`If-None-Match: *` on
    /// CompleteMultipartUpload) each carry the create-once condition.
    Never,
    /// Multipart uploads assemble a temporary and copy it with this
    /// destination condition; a single PUT carries the create-once condition.
    Multipart(S3CopyCondition),
    /// Every create, single-request or multipart, stages a temporary and
    /// copies it with this destination condition.
    Every(S3CopyCondition),
}
impl S3CopyPublication {
    /// The condition of the copy that publishes a multipart upload, if any.
    pub(crate) fn multipart(self) -> Option<S3CopyCondition> {
        match self {
            Self::Never => None,
            Self::Multipart(condition) | Self::Every(condition) => Some(condition),
        }
    }
    /// The condition of the copy that publishes a single-request create, if any.
    pub(crate) fn single_request(self) -> Option<S3CopyCondition> {
        match self {
            Self::Never | Self::Multipart(_) => None,
            Self::Every(condition) => Some(condition),
        }
    }
}

/// The destination condition on a publishing copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3CopyCondition {
    /// `If-None-Match: *` (AWS).
    IfNoneMatch,
    /// `x-goog-if-generation-match: 0` (GCS).
    GenerationMatch,
    /// `cf-copy-destination-if-none-match: *` (R2).
    CloudflareIfNoneMatch,
}

/// What a native multipart object's `Last-Modified` dates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3MultipartTimestamp {
    /// Completion of the upload.
    Completion,
    /// Initiation of the upload.
    Start,
}

/// How returned checksums are read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Checksums {
    /// Returned checksums state their scope; a missing scope is a protocol error.
    ScopeStated,
    /// Checksums without a stated scope are discarded.
    UnstatedIgnored,
    /// None are returned; any returned checksum is a protocol error.
    Absent,
}

/// The listing protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Listing {
    /// ListObjectsV2 with continuation tokens.
    V2,
    /// ListObjects with key markers.
    V1,
}

/// Where a replaceable object's revision comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Revision {
    /// Native versions, or fresh `coven-revision` metadata; immutable objects use the ETag.
    ETag,
    /// GCS's `x-goog-generation`.
    Generation,
}

/// Which response headers carry metadata, generation and hashes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3ResponseHeaders {
    /// `x-amz-*` headers.
    S3,
    /// `x-goog-*` headers, translated at the protocol boundary.
    Google,
}
