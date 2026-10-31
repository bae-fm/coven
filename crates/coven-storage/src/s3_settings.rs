//! The S3 behaviors a location uses (§20.7). Catalogued profiles fill them; a
//! custom profile carries its own, so every device behaves identically.
use coven_format::code_fields::{FieldReader, FieldWriter};
use coven_format::Error as FieldError;
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
    /// Which publications proceed by a conditional copy of a staged temporary.
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

/// Which publications proceed by a conditional copy of a reserved temporary (§20.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3CopyPublication {
    /// None: a single PUT and a multipart completion each carry the
    /// destination condition (`If-None-Match` or `If-Match`).
    Never,
    /// Multipart uploads assemble a temporary and copy it with this
    /// destination condition; a single PUT carries the destination condition.
    Multipart(S3CopyCondition),
    /// Every create or replacement, single-request or multipart, stages a temporary and
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
    /// The condition of the copy that publishes a single-request body, if any.
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

// A profile in a code's storage part (format.md D13): `0` and the catalogued
// service's tag, or `1` and the custom settings, one bit field each.
impl S3Profile {
    pub(crate) fn pack(&self, out: &mut FieldWriter) -> Result<(), FieldError> {
        match self {
            Self::Named(name) => {
                out.u8(0)?;
                out.u8(name.tag())
            }
            Self::Custom(settings) => {
                out.u8(1)?;
                out.u16(settings.bits())
            }
        }
    }

    pub(crate) fn unpack(input: &mut FieldReader<'_>) -> Result<Self, FieldError> {
        match input.u8()? {
            0 => Ok(Self::Named(S3ProfileName::from_tag(input.u8()?)?)),
            1 => Ok(Self::Custom(S3Settings::from_bits(input.u16()?)?)),
            tag => Err(FieldError::UnknownTag { field: "S3 profile", tag }),
        }
    }
}

impl S3ProfileName {
    const ALL: [Self; 7] = [
        Self::Aws,
        Self::Gcs,
        Self::R2,
        Self::RustFs,
        Self::SeaweedFs,
        Self::CephRgw,
        Self::VersityGw,
    ];

    fn tag(self) -> u8 {
        match self {
            Self::Aws => 0,
            Self::Gcs => 1,
            Self::R2 => 2,
            Self::RustFs => 3,
            Self::SeaweedFs => 4,
            Self::CephRgw => 5,
            Self::VersityGw => 6,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, FieldError> {
        Self::ALL
            .into_iter()
            .find(|name| name.tag() == tag)
            .ok_or(FieldError::UnknownTag { field: "S3 profile name", tag })
    }
}

// Custom settings' bits, from the lowest: signing (1 bit), single PUT (1),
// copy publication (3, below),
// multipart timestamp (1), checksums (2: 0-2), listing (1), revision (1),
// response headers (1) and request checksums (1). The top four bits are zero.
const SIGNING: u32 = 0;
const SINGLE_PUT: u32 = 1;
const COPY_PUBLICATION: u32 = 2;
const MULTIPART_TIMESTAMP: u32 = 5;
const CHECKSUMS: u32 = 6;
const LISTING: u32 = 8;
const REVISION: u32 = 9;
const RESPONSE_HEADERS: u32 = 10;
const REQUEST_CHECKSUMS: u32 = 11;
const USED_BITS: u32 = 12;
/// Copy publication's three bits are an index into this list; 7 is refused.
const COPY_PUBLICATIONS: [S3CopyPublication; 7] = [
    S3CopyPublication::Never,
    S3CopyPublication::Multipart(S3CopyCondition::IfNoneMatch),
    S3CopyPublication::Multipart(S3CopyCondition::GenerationMatch),
    S3CopyPublication::Multipart(S3CopyCondition::CloudflareIfNoneMatch),
    S3CopyPublication::Every(S3CopyCondition::IfNoneMatch),
    S3CopyPublication::Every(S3CopyCondition::GenerationMatch),
    S3CopyPublication::Every(S3CopyCondition::CloudflareIfNoneMatch),
];

impl S3Settings {
    fn bits(&self) -> u16 {
        // The index of this publication in `COPY_PUBLICATIONS`.
        let copy_publication = match self.copy_publication {
            S3CopyPublication::Never => 0,
            S3CopyPublication::Multipart(S3CopyCondition::IfNoneMatch) => 1,
            S3CopyPublication::Multipart(S3CopyCondition::GenerationMatch) => 2,
            S3CopyPublication::Multipart(S3CopyCondition::CloudflareIfNoneMatch) => 3,
            S3CopyPublication::Every(S3CopyCondition::IfNoneMatch) => 4,
            S3CopyPublication::Every(S3CopyCondition::GenerationMatch) => 5,
            S3CopyPublication::Every(S3CopyCondition::CloudflareIfNoneMatch) => 6,
        };
        let checksums = match self.checksums {
            S3Checksums::ScopeStated => 0,
            S3Checksums::UnstatedIgnored => 1,
            S3Checksums::Absent => 2,
        };
        u16::from(self.signing == S3Signing::GoogleHmac) << SIGNING
            | u16::from(self.single_put == S3SinglePut::GenerationMatch) << SINGLE_PUT
            | copy_publication << COPY_PUBLICATION
            | u16::from(self.multipart_timestamp == S3MultipartTimestamp::Start)
                << MULTIPART_TIMESTAMP
            | checksums << CHECKSUMS
            | u16::from(self.listing == S3Listing::V1) << LISTING
            | u16::from(self.revision == S3Revision::Generation) << REVISION
            | u16::from(self.response_headers == S3ResponseHeaders::Google) << RESPONSE_HEADERS
            | u16::from(self.request_checksums) << REQUEST_CHECKSUMS
    }

    fn from_bits(bits: u16) -> Result<Self, FieldError> {
        let field = |shift: u32, width: u32| (bits >> shift) & ((1 << width) - 1);
        let set = |shift: u32| field(shift, 1) == 1;
        let unknown =
            |field: &'static str, tag: u16| FieldError::UnknownTag { field, tag: tag as u8 };
        if bits >> USED_BITS != 0 {
            return Err(unknown("S3 settings", bits >> USED_BITS));
        }
        Ok(Self {
            signing: if set(SIGNING) { S3Signing::GoogleHmac } else { S3Signing::AwsSigV4 },
            single_put: if set(SINGLE_PUT) {
                S3SinglePut::GenerationMatch
            } else {
                S3SinglePut::IfNoneMatch
            },
            copy_publication: *COPY_PUBLICATIONS
                .get(usize::from(field(COPY_PUBLICATION, 3)))
                .ok_or(unknown("S3 copy publication", field(COPY_PUBLICATION, 3)))?,
            multipart_timestamp: if set(MULTIPART_TIMESTAMP) {
                S3MultipartTimestamp::Start
            } else {
                S3MultipartTimestamp::Completion
            },
            checksums: match field(CHECKSUMS, 2) {
                0 => S3Checksums::ScopeStated,
                1 => S3Checksums::UnstatedIgnored,
                2 => S3Checksums::Absent,
                value => return Err(unknown("S3 checksums", value)),
            },
            listing: if set(LISTING) { S3Listing::V1 } else { S3Listing::V2 },
            revision: if set(REVISION) { S3Revision::Generation } else { S3Revision::ETag },
            response_headers: if set(RESPONSE_HEADERS) {
                S3ResponseHeaders::Google
            } else {
                S3ResponseHeaders::S3
            },
            request_checksums: set(REQUEST_CHECKSUMS),
        })
    }
}
