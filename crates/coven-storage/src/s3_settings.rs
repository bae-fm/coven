//! The S3 behaviors a location uses (§4). Catalogued profiles fill them; a
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

/// An S3 service whose catalogued profile a location names (§4).
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
}

/// Every S3 behavior the adapter varies, one value each (§4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Settings {
    /// How requests are authenticated.
    pub signing: S3Signing,
    /// The condition a create-once single PUT sends.
    pub single_put: S3SinglePut,
    /// How a multipart upload is published.
    pub multipart: S3Multipart,
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

/// How a multipart upload is published.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Multipart {
    /// `If-None-Match: *` on CompleteMultipartUpload.
    ConditionalCompletion,
    /// Upload to a reserved temporary, then copy it with this destination condition.
    Copy(S3CopyCondition),
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
