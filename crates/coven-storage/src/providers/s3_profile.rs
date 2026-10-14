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
/// S3's `CopyObject` copies at most 5 GiB in one atomic request.
const SINGLE_COPY_LIMIT: u64 = 5 * 1024 * 1024 * 1024;
impl S3CopyCondition {
    /// The largest object one conditional copy publishes. S3's `CopyObject`
    /// (AWS, SeaweedFS, R2's extension) is limited; GCS's XML copy is a
    /// different operation with no such limit here.
    pub(super) fn single_copy_limit(self) -> Option<u64> {
        match self {
            Self::IfNoneMatch | Self::CloudflareIfNoneMatch => Some(SINGLE_COPY_LIMIT),
            Self::GenerationMatch => None,
        }
    }
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
/// The finest scope a member key can have on a service (§4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum S3KeyScope {
    /// A key reaches a whole bucket, so every store has its own bucket.
    Bucket,
    /// A key can be limited to the store's prefix, so stores can share a bucket.
    Prefix,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Certification<'date> {
    Uncertified,
    Passed { date: &'date str, suite: u32 },
}
/// Whether setup admits a catalogued profile. A profile that failed a
/// certification race is refused before any request, whatever its bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Setup {
    Admitted,
    Refused,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CataloguedProfile {
    pub(super) name: S3ProfileName,
    /// The hosted service's hostnames; None for a self-hosted service, whose
    /// arbitrary endpoint only an explicit profile name can select.
    pub(super) endpoint: Option<EndpointMatch>,
    pub(super) settings: S3Settings,
    pub(super) key_scope: S3KeyScope,
    /// Separate evidence: a sequential or unversioned run cannot certify this.
    pub(super) versioned_buckets: Certification<'static>,
    /// How coven makes and deletes member keys with admin access; None where
    /// the console makes them (§4).
    pub(super) minter: Option<&'static super::minting::KeyMinter>,
    pub(super) certification: Certification<'static>,
    pub(super) setup: Setup,
}

/// Every catalogued service; hosted ones match hostnames in this order.
pub(super) const PROFILES: &[CataloguedProfile] = &[AWS, GCS, R2, RUSTFS, SEAWEEDFS, CEPH_RGW];
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
    // IAM policies limit a key to the store's prefix with `s3:prefix`
    // conditions on listing and resource ARNs ending `<prefix>/<store>/*`.
    key_scope: S3KeyScope::Prefix,
    versioned_buckets: Certification::Uncertified,
    minter: Some(&super::iam::AWS_IAM),
    certification: Certification::Uncertified,
    setup: Setup::Admitted,
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
    // A managed folder `<prefix>/<store>/` grants the HMAC key's service
    // account `roles/storage.objectUser` on that folder alone.
    key_scope: S3KeyScope::Prefix,
    versioned_buckets: Certification::Uncertified,
    minter: Some(&super::gcs_keys::GCS_KEYS),
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 1,
    },
    setup: Setup::Admitted,
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
    // An R2 API token is limited to buckets, never to a prefix inside one
    // (https://developers.cloudflare.com/r2/api/tokens/); only temporary
    // credentials, which need a session token, take a prefix.
    key_scope: S3KeyScope::Bucket,
    versioned_buckets: Certification::Uncertified,
    minter: Some(&super::r2_keys::R2_TOKENS),
    certification: Certification::Passed {
        date: "2026-10-13",
        suite: 1,
    },
    setup: Setup::Admitted,
};
// Self-hosted services are reached at arbitrary endpoints and selected only by
// name. The raw observations are in spec/s3-certification.md.
// Detected by certification suite 2. RustFS and SeaweedFS refused occupied
// names on PUT and multipart completion with HTTP 412 and timestamp multipart
// completion, so no copy is needed. Neither returned a checksum, even when
// asked; §4's complete-byte comparison applies instead. RustFS 1.0.1 passed
// suite 4 with versioning disabled and enabled: one conditional writer won
// every race in both buckets.
const RUSTFS: CataloguedProfile = CataloguedProfile {
    name: S3ProfileName::RustFs,
    endpoint: None,
    settings: S3Settings {
        checksums: S3Checksums::Absent,
        request_checksums: false,
        ..PLAIN
    },
    // An access key's IAM policy takes `s3:prefix` conditions and object
    // resources under the store's prefix.
    key_scope: S3KeyScope::Prefix,
    versioned_buckets: Certification::Passed {
        date: "2026-10-14",
        suite: 4,
    },
    minter: Some(&super::rustfs_keys::RUSTFS_ACCOUNTS),
    certification: Certification::Passed {
        date: "2026-10-14",
        suite: 4,
    },
    setup: Setup::Admitted,
};
// SeaweedFS 4.48 failed suite 4 with versioning disabled and enabled:
// concurrent conditional multipart completions both succeeded, so its suite 2
// certificate, which predates the races, is withdrawn and setup refuses it.
// Multipart uploads therefore assemble a temporary and publish it with one
// `CopyObject` conditioned on `If-None-Match: *`, as AWS does.
const SEAWEEDFS: CataloguedProfile = CataloguedProfile {
    name: S3ProfileName::SeaweedFs,
    endpoint: None,
    settings: S3Settings {
        multipart: S3Multipart::Copy(S3CopyCondition::IfNoneMatch),
        checksums: S3Checksums::Absent,
        request_checksums: false,
        ..PLAIN
    },
    // An identity's S3 policy takes `s3:prefix` conditions and object
    // resources under the store's prefix.
    key_scope: S3KeyScope::Prefix,
    versioned_buckets: Certification::Uncertified,
    minter: Some(&super::iam::SEAWEEDFS_IAM),
    certification: Certification::Uncertified,
    setup: Setup::Refused,
};

// Ceph RGW 20.2.4 has the same detected settings as RustFS. Versioned
// conditional writes can both succeed in a race; see spec/s3-certification.md.
const CEPH_RGW: CataloguedProfile = CataloguedProfile {
    name: S3ProfileName::CephRgw,
    endpoint: None,
    settings: RUSTFS.settings,
    // RGW bucket policies honor `s3:prefix` conditions and object resources
    // under the store's prefix.
    key_scope: S3KeyScope::Prefix,
    versioned_buckets: Certification::Uncertified,
    minter: Some(&super::iam::CEPH_RGW_IAM),
    certification: Certification::Passed {
        date: "2026-10-14",
        suite: 4,
    },
    setup: Setup::Admitted,
};

impl CataloguedProfile {
    pub(super) fn named(name: S3ProfileName) -> Self {
        match name {
            S3ProfileName::Aws => AWS,
            S3ProfileName::Gcs => GCS,
            S3ProfileName::R2 => R2,
            S3ProfileName::RustFs => RUSTFS,
            S3ProfileName::SeaweedFs => SEAWEEDFS,
            S3ProfileName::CephRgw => CEPH_RGW,
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

/// What a location's endpoint and profile select.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Selected {
    Catalogued(CataloguedProfile),
    Custom(S3Settings),
    /// An uncatalogued endpoint naming no profile: only setup's detection
    /// can supply its settings, recorded as a custom profile.
    Undetected,
}
impl Selected {
    pub(super) fn settings(self) -> Option<S3Settings> {
        match self {
            Self::Catalogued(profile) => Some(profile.settings),
            Self::Custom(settings) => Some(settings),
            Self::Undetected => None,
        }
    }
    /// Custom settings say nothing about keys, so they take the
    /// conservative bucket scope, as does a location detection will make custom.
    pub(super) fn key_scope(self) -> S3KeyScope {
        match self {
            Self::Catalogued(profile) => profile.key_scope,
            Self::Custom(_) | Self::Undetected => S3KeyScope::Bucket,
        }
    }
}

/// What a location selects. A named or custom profile is used as given;
/// otherwise the endpoint host selects, with AWS for no endpoint. A catalogued
/// host requires https and admits only its own name. Without an endpoint,
/// requests reach AWS, so only AWS may be named.
pub(super) fn select(
    endpoint: Option<&Url>,
    profile: Option<&S3Profile>,
) -> Result<Selected, StorageError> {
    let invalid =
        |reason: &'static str| Err(StorageFailure::InvalidConfiguration.with_source(reason));
    let Some(endpoint) = endpoint else {
        return match profile {
            None | Some(S3Profile::Named(S3ProfileName::Aws)) => Ok(Selected::Catalogued(AWS)),
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
        (Some(hosted), _) => Ok(Selected::Catalogued(hosted)),
        (None, Some(S3Profile::Named(name))) => {
            Ok(Selected::Catalogued(CataloguedProfile::named(*name)))
        }
        (None, Some(S3Profile::Custom(settings))) => Ok(Selected::Custom(*settings)),
        (None, None) => Ok(Selected::Undetected),
    }
}

/// Refuse a location whose catalogued profile setup does not admit, before
/// any request (§4).
pub(crate) fn admit_setup(
    endpoint: Option<&Url>,
    profile: Option<&S3Profile>,
) -> Result<(), crate::StorageSetupError> {
    match select(endpoint, profile)? {
        Selected::Catalogued(CataloguedProfile {
            name,
            setup: Setup::Refused,
            ..
        }) => Err(crate::StorageSetupError::S3ProfileRefused { profile: name }),
        _ => Ok(()),
    }
}

/// The settings a location uses; Ok(None) is an uncatalogued endpoint
/// naming no profile, whose settings only setup's detection can supply.
pub(crate) fn settings(
    endpoint: Option<&Url>,
    profile: Option<&S3Profile>,
) -> Result<Option<S3Settings>, StorageError> {
    select(endpoint, profile).map(Selected::settings)
}
