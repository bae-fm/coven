//! Add a service by putting its bucket, region, endpoint and HMAC keys in an env
//! file (COVEN_LIVE_S3_{BUCKET,REGION,ENDPOINT,ACCESS_KEY_ID,SECRET_ACCESS_KEY}), then
//! run `COVEN_LIVE_S3_ENV=<file> cargo test -p coven-storage s3_certification --
//! --ignored --nocapture`. The suite probes native behavior, runs the adapter's
//! live contracts, deletes its objects/uploads, and prints a Rust profile entry.
//! An entry is certified only after every §4 check passes; a failing service's
//! entry stays uncertified. Commit either with the evidence.
use crate::{S3ProfileName, StorageError, StorageFailure};
use url::Url;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EndpointMatch {
    Host(&'static str),
    HostSuffix(&'static str),
}
impl EndpointMatch {
    pub(super) fn matches(self, host: &str) -> bool {
        match self {
            Self::Host(expected) => host == expected,
            Self::HostSuffix(suffix) => host.ends_with(suffix) && host.len() > suffix.len(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Signing {
    AwsSigV4,
    GoogleHmac,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SinglePut {
    IfNoneMatch,
    GenerationMatch,
}
impl SinglePut {
    pub(super) fn header(self) -> (&'static str, &'static str) {
        match self {
            Self::IfNoneMatch => ("if-none-match", "*"),
            Self::GenerationMatch => ("x-goog-if-generation-match", "0"),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Multipart {
    ConditionalCompletion,
    Copy {
        header: &'static str,
        value: &'static str,
    },
}
impl Multipart {
    pub(super) fn uses_copy(self) -> bool {
        matches!(self, Self::Copy { .. })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MultipartTimestamp {
    Completion,
    Start,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Checksums {
    ScopeStated,
    UnstatedIgnored,
    /// The service returns no checksum, so §4's complete-byte comparison
    /// applies. A returned checksum contradicts the certified observation and
    /// fails as a protocol error, whatever its scope.
    Absent,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Listing {
    V2,
    V1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Revision {
    // Immutable objects use ETag. Replacements use a version or coven-revision
    // metadata, since an ETag alone need not change when bytes are replaced.
    ETag,
    Generation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BucketCheck {
    Head,
    List,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResponseHeaders {
    S3,
    Google,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Certification<'date> {
    Uncertified,
    Passed { date: &'date str, suite: u32 },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct S3Profile {
    pub(super) name: S3ProfileName,
    /// The hosted service's hostnames; None for a self-hosted service, whose
    /// arbitrary endpoint only an explicit profile name can select.
    pub(super) endpoint: Option<EndpointMatch>,
    pub(super) signing: Signing,
    pub(super) single_put: SinglePut,
    pub(super) multipart: Multipart,
    pub(super) multipart_timestamp: MultipartTimestamp,
    pub(super) checksums: Checksums,
    pub(super) listing: Listing,
    pub(super) revision: Revision,
    pub(super) bucket_check: BucketCheck,
    pub(super) response_headers: ResponseHeaders,
    pub(super) request_checksums: bool,
    pub(super) certification: Certification<'static>,
}

/// Every catalogued service; hosted ones match hostnames in this order.
pub(super) const PROFILES: &[S3Profile] = &[AWS, GCS, R2, GARAGE, RUSTFS, SEAWEEDFS];
const AWS: S3Profile = S3Profile {
    name: S3ProfileName::Aws,
    endpoint: Some(EndpointMatch::HostSuffix(".amazonaws.com")),
    signing: Signing::AwsSigV4,
    single_put: SinglePut::IfNoneMatch,
    multipart: Multipart::ConditionalCompletion,
    multipart_timestamp: MultipartTimestamp::Start,
    checksums: Checksums::ScopeStated,
    listing: Listing::V2,
    revision: Revision::ETag,
    bucket_check: BucketCheck::Head,
    response_headers: ResponseHeaders::S3,
    request_checksums: true,
    certification: Certification::Uncertified,
};
const GCS: S3Profile = S3Profile {
    name: S3ProfileName::Gcs,
    endpoint: Some(EndpointMatch::Host("storage.googleapis.com")),
    signing: Signing::GoogleHmac,
    single_put: SinglePut::GenerationMatch,
    multipart: Multipart::Copy {
        header: "x-goog-if-generation-match",
        value: "0",
    },
    multipart_timestamp: MultipartTimestamp::Completion,
    checksums: Checksums::ScopeStated,
    listing: Listing::V2,
    revision: Revision::Generation,
    bucket_check: BucketCheck::List,
    response_headers: ResponseHeaders::Google,
    request_checksums: false,
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 1,
    },
};
// Detected by certification suite 1; the raw observations are in spec/s3-certification.md.
// Cloudflare documents the API differences at https://developers.cloudflare.com/r2/api/s3/api/.
// Native completion passed create-once and completion-time checks, so no copy is needed.
const R2: S3Profile = S3Profile {
    name: S3ProfileName::R2,
    endpoint: Some(EndpointMatch::HostSuffix(".r2.cloudflarestorage.com")),
    signing: Signing::AwsSigV4,
    single_put: SinglePut::IfNoneMatch,
    multipart: Multipart::ConditionalCompletion,
    multipart_timestamp: MultipartTimestamp::Completion,
    checksums: Checksums::UnstatedIgnored,
    listing: Listing::V2,
    revision: Revision::ETag,
    bucket_check: BucketCheck::Head,
    response_headers: ResponseHeaders::S3,
    request_checksums: true,
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 1,
    },
};
// Self-hosted services are reached at arbitrary endpoints and selected only by
// name. The raw observations are in spec/s3-certification.md.
// Garage v2.4.1, under suite 1, ignored every create-once condition the adapter
// can send: both single-PUT conditions, conditional completion and both copy
// conditions. Its multipart Last-Modified records initiation, and it returned
// no checksum. The fields it lacks keep AWS's values, which it accepts unenforced.
const GARAGE: S3Profile = S3Profile {
    name: S3ProfileName::Garage,
    endpoint: None,
    signing: Signing::AwsSigV4,
    single_put: SinglePut::IfNoneMatch,
    multipart: Multipart::ConditionalCompletion,
    multipart_timestamp: MultipartTimestamp::Start,
    checksums: Checksums::ScopeStated,
    listing: Listing::V2,
    revision: Revision::ETag,
    bucket_check: BucketCheck::Head,
    response_headers: ResponseHeaders::S3,
    request_checksums: true,
    certification: Certification::Uncertified,
};
// Detected by certification suite 2. RustFS and SeaweedFS refused occupied
// names on PUT and multipart completion with HTTP 412 and timestamp multipart
// completion, so no copy is needed. Neither returned a checksum, even when
// asked; §4's complete-byte comparison applies instead.
const RUSTFS: S3Profile = S3Profile {
    name: S3ProfileName::RustFs,
    endpoint: None,
    signing: Signing::AwsSigV4,
    single_put: SinglePut::IfNoneMatch,
    multipart: Multipart::ConditionalCompletion,
    multipart_timestamp: MultipartTimestamp::Completion,
    checksums: Checksums::Absent,
    listing: Listing::V2,
    revision: Revision::ETag,
    bucket_check: BucketCheck::Head,
    response_headers: ResponseHeaders::S3,
    request_checksums: false,
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 2,
    },
};
const SEAWEEDFS: S3Profile = S3Profile {
    name: S3ProfileName::SeaweedFs,
    endpoint: None,
    signing: Signing::AwsSigV4,
    single_put: SinglePut::IfNoneMatch,
    multipart: Multipart::ConditionalCompletion,
    multipart_timestamp: MultipartTimestamp::Completion,
    checksums: Checksums::Absent,
    listing: Listing::V2,
    revision: Revision::ETag,
    bucket_check: BucketCheck::Head,
    response_headers: ResponseHeaders::S3,
    request_checksums: false,
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 2,
    },
};

impl S3Profile {
    fn named(name: S3ProfileName) -> Self {
        match name {
            S3ProfileName::Aws => AWS,
            S3ProfileName::Gcs => GCS,
            S3ProfileName::R2 => R2,
            S3ProfileName::Garage => GARAGE,
            S3ProfileName::RustFs => RUSTFS,
            S3ProfileName::SeaweedFs => SEAWEEDFS,
        }
    }
    /// An explicit name selects its profile; otherwise the endpoint host does,
    /// with AWS for no endpoint and for hosts no profile claims. A catalogued
    /// host requires https and admits only its own name. Without an endpoint,
    /// requests reach AWS, so only AWS may be named.
    pub(super) fn select(
        endpoint: Option<&Url>,
        name: Option<S3ProfileName>,
    ) -> Result<Self, StorageError> {
        let invalid =
            |reason: &'static str| Err(StorageFailure::InvalidConfiguration.with_source(reason));
        let Some(endpoint) = endpoint else {
            return match name {
                None | Some(S3ProfileName::Aws) => Ok(AWS),
                Some(_) => invalid("S3 profile requires its endpoint"),
            };
        };
        let catalogued = endpoint.host_str().and_then(|host| {
            PROFILES
                .iter()
                .find(|profile| profile.endpoint.is_some_and(|known| known.matches(host)))
        });
        match (catalogued, name) {
            (Some(_), _) if endpoint.scheme() != "https" => {
                invalid("hosted S3 endpoint requires https")
            }
            (Some(profile), Some(name)) if profile.name != name => {
                invalid("S3 profile contradicts its endpoint host")
            }
            (Some(profile), _) => Ok(*profile),
            (None, Some(name)) => Ok(Self::named(name)),
            (None, None) => Ok(AWS),
        }
    }
}
