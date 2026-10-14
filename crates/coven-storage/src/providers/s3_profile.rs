//! Add a service by putting its bucket, region, endpoint and HMAC keys in an env
//! file (COVEN_LIVE_S3_{BUCKET,REGION,ENDPOINT,ACCESS_KEY_ID,SECRET_ACCESS_KEY}), then
//! run `COVEN_LIVE_S3_ENV=<file> cargo test -p coven-storage s3_certification --
//! --ignored --nocapture`. The suite runs production detection, the adapter's
//! live contracts, deletes its objects/uploads, and prints a Rust profile entry.
//! An entry is certified only after every §4 check passes; a failing service's
//! entry stays uncertified. Commit either with the evidence. A location that
//! names no profile at an uncatalogued endpoint gets detected settings at setup.
use crate::{
    S3Checksums, S3CopyCondition, S3Listing, S3Multipart, S3MultipartTimestamp, S3Profile,
    S3ProfileName, S3ResponseHeaders, S3Revision, S3Settings, S3Signing, S3SinglePut, StorageError,
    StorageFailure,
};
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
impl S3SinglePut {
    pub(super) fn header(self) -> (&'static str, &'static str) {
        match self {
            Self::IfNoneMatch => ("if-none-match", "*"),
            Self::GenerationMatch => ("x-goog-if-generation-match", "0"),
        }
    }
}
impl S3CopyCondition {
    pub(super) fn header(self) -> (&'static str, &'static str) {
        match self {
            Self::IfNoneMatch => ("if-none-match", "*"),
            Self::GenerationMatch => ("x-goog-if-generation-match", "0"),
            Self::CloudflareIfNoneMatch => ("cf-copy-destination-if-none-match", "*"),
        }
    }
}
impl S3Multipart {
    pub(super) fn uses_copy(self) -> bool {
        matches!(self, Self::Copy(_))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Certification<'date> {
    Uncertified,
    Passed { date: &'date str, suite: u32 },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CataloguedProfile {
    pub(super) name: S3ProfileName,
    /// The hosted service's hostnames; None for a self-hosted service, whose
    /// arbitrary endpoint only an explicit profile name can select.
    pub(super) endpoint: Option<EndpointMatch>,
    pub(super) settings: S3Settings,
    pub(super) certification: Certification<'static>,
}

/// Every catalogued service; hosted ones match hostnames in this order.
pub(super) const PROFILES: &[CataloguedProfile] = &[AWS, GCS, R2, RUSTFS, SEAWEEDFS];
/// The plain S3 behaviors detection starts from and keeps where nothing held.
/// AWS's entry differs in its copy publication and start timestamp.
pub(super) const PLAIN: S3Settings = S3Settings {
    signing: S3Signing::AwsSigV4,
    single_put: S3SinglePut::IfNoneMatch,
    multipart: S3Multipart::ConditionalCompletion,
    multipart_timestamp: S3MultipartTimestamp::Completion,
    checksums: S3Checksums::ScopeStated,
    listing: S3Listing::V2,
    revision: S3Revision::ETag,
    response_headers: S3ResponseHeaders::S3,
    request_checksums: true,
};
// AWS enforces If-None-Match on completion, but its multipart Last-Modified
// records initiation. AWS documents If-None-Match on CopyObject since 2025-10
// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html);
// without a live account the setup safety check verifies it per bucket.
const AWS: CataloguedProfile = CataloguedProfile {
    name: S3ProfileName::Aws,
    endpoint: Some(EndpointMatch::HostSuffix(".amazonaws.com")),
    settings: S3Settings {
        multipart: S3Multipart::Copy(S3CopyCondition::IfNoneMatch),
        multipart_timestamp: S3MultipartTimestamp::Start,
        ..PLAIN
    },
    certification: Certification::Uncertified,
};
const GCS: CataloguedProfile = CataloguedProfile {
    name: S3ProfileName::Gcs,
    endpoint: Some(EndpointMatch::Host("storage.googleapis.com")),
    settings: S3Settings {
        signing: S3Signing::GoogleHmac,
        single_put: S3SinglePut::GenerationMatch,
        multipart: S3Multipart::Copy(S3CopyCondition::GenerationMatch),
        multipart_timestamp: S3MultipartTimestamp::Completion,
        checksums: S3Checksums::ScopeStated,
        listing: S3Listing::V2,
        revision: S3Revision::Generation,
        response_headers: S3ResponseHeaders::Google,
        request_checksums: false,
    },
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 1,
    },
};
// Detected by certification suite 1; the raw observations are in spec/s3-certification.md.
// Cloudflare documents the API differences at https://developers.cloudflare.com/r2/api/s3/api/.
// Native completion passed create-once and completion-time checks, so no copy is needed.
const R2: CataloguedProfile = CataloguedProfile {
    name: S3ProfileName::R2,
    endpoint: Some(EndpointMatch::HostSuffix(".r2.cloudflarestorage.com")),
    settings: S3Settings {
        checksums: S3Checksums::UnstatedIgnored,
        ..PLAIN
    },
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 1,
    },
};
// Self-hosted services are reached at arbitrary endpoints and selected only by
// name. The raw observations are in spec/s3-certification.md.
// Detected by certification suite 2. RustFS and SeaweedFS refused occupied
// names on PUT and multipart completion with HTTP 412 and timestamp multipart
// completion, so no copy is needed. Neither returned a checksum, even when
// asked; §4's complete-byte comparison applies instead.
const RUSTFS: CataloguedProfile = CataloguedProfile {
    name: S3ProfileName::RustFs,
    endpoint: None,
    settings: S3Settings {
        checksums: S3Checksums::Absent,
        request_checksums: false,
        ..PLAIN
    },
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 2,
    },
};
const SEAWEEDFS: CataloguedProfile = CataloguedProfile {
    name: S3ProfileName::SeaweedFs,
    endpoint: None,
    settings: S3Settings {
        checksums: S3Checksums::Absent,
        request_checksums: false,
        ..PLAIN
    },
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 2,
    },
};

impl CataloguedProfile {
    pub(super) fn named(name: S3ProfileName) -> Self {
        match name {
            S3ProfileName::Aws => AWS,
            S3ProfileName::Gcs => GCS,
            S3ProfileName::R2 => R2,
            S3ProfileName::RustFs => RUSTFS,
            S3ProfileName::SeaweedFs => SEAWEEDFS,
        }
    }
    /// The catalogued profile a hosted endpoint's host selects.
    pub(super) fn hosting(endpoint: &Url) -> Option<Self> {
        let host = endpoint.host_str()?;
        PROFILES
            .iter()
            .find(|profile| profile.endpoint.is_some_and(|known| known.matches(host)))
            .copied()
    }
}

/// The settings a location uses. A named or custom profile is used as given;
/// otherwise the endpoint host selects, with AWS for no endpoint. A catalogued
/// host requires https and admits only its own name. Without an endpoint,
/// requests reach AWS, so only AWS may be named. Ok(None) is an uncatalogued
/// endpoint naming no profile: only setup's detection can supply its settings.
pub(crate) fn settings(
    endpoint: Option<&Url>,
    profile: Option<&S3Profile>,
) -> Result<Option<S3Settings>, StorageError> {
    let invalid =
        |reason: &'static str| Err(StorageFailure::InvalidConfiguration.with_source(reason));
    let Some(endpoint) = endpoint else {
        return match profile {
            None | Some(S3Profile::Named(S3ProfileName::Aws)) => Ok(Some(AWS.settings)),
            Some(_) => invalid("S3 profile requires its endpoint"),
        };
    };
    match (CataloguedProfile::hosting(endpoint), profile) {
        (Some(_), _) if endpoint.scheme() != "https" => {
            invalid("hosted S3 endpoint requires https")
        }
        (Some(hosted), Some(S3Profile::Named(name))) if hosted.name != *name => {
            invalid("S3 profile contradicts its endpoint host")
        }
        (Some(_), Some(S3Profile::Custom(_))) => {
            invalid("a catalogued S3 host admits only its own profile")
        }
        (Some(hosted), _) => Ok(Some(hosted.settings)),
        (None, Some(S3Profile::Named(name))) => Ok(Some(CataloguedProfile::named(*name).settings)),
        (None, Some(S3Profile::Custom(settings))) => Ok(Some(*settings)),
        (None, None) => Ok(None),
    }
}
