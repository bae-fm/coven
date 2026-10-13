//! Add a service by putting its bucket, region, endpoint and HMAC keys in an env
//! file (COVEN_LIVE_S3_{BUCKET,REGION,ENDPOINT,ACCESS_KEY_ID,SECRET_ACCESS_KEY}), then
//! run `COVEN_LIVE_S3_ENV=<file> cargo test -p coven-storage s3_certification --
//! --ignored --nocapture`. The suite probes native behavior, runs the adapter's
//! live contracts, deletes its objects/uploads, and prints a Rust profile entry.
//! Paste the entry only after every §4 check passes; commit it with the evidence.
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
    pub(super) name: &'static str,
    pub(super) endpoint: EndpointMatch,
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

pub(super) const PROFILES: &[S3Profile] = &[
    S3Profile {
        name: "AWS",
        endpoint: EndpointMatch::HostSuffix(".amazonaws.com"),
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
    },
    S3Profile {
        name: "GCS",
        endpoint: EndpointMatch::Host("storage.googleapis.com"),
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
    },
];
impl S3Profile {
    pub(super) fn for_endpoint(endpoint: Option<&Url>) -> Self {
        if let Some(host) = endpoint.and_then(Url::host_str) {
            for profile in PROFILES {
                if profile.endpoint.matches(host) {
                    return *profile;
                }
            }
        }
        PROFILES[0]
    }
}
