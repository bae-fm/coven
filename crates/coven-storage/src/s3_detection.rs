//! What S3 detection observed, the settings derived from it, and the setup
//! safety check of effective settings against it (§4). The probes themselves
//! run in the adapter; this is the pure decision over their observations.
use crate::{
    S3BucketCheck, S3Checksums, S3CopyCondition, S3Listing, S3Multipart, S3MultipartTimestamp,
    S3Profile, S3ResponseHeaders, S3Revision, S3Settings, S3Signing, S3SinglePut, StorageCheck,
    StorageConfig, StorageFailure, StorageSetupError,
};

/// What detection observed at one location (§4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Detection {
    /// Settings derived from the observations; prefill advanced settings with them.
    pub settings: S3Settings,
    /// One observation per probe, in the order run.
    pub observations: Vec<S3Observation>,
}

/// One probe's outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Observation {
    /// The behavior probed.
    pub probe: S3Probe,
    /// Whether the behavior held, as described on each probe.
    pub held: bool,
    /// Native outcome; names no key, endpoint or object.
    pub detail: String,
}

/// A behavior detection probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum S3Probe {
    /// A read-only listing is accepted with this signer.
    Signing(S3Signing),
    /// The condition refuses an occupied name, leaving its bytes and time.
    SinglePut(S3SinglePut),
    /// An unfinished multipart upload is absent from HEAD, GET and listing.
    PartialUploadHidden,
    /// Conditional completion refuses an occupied name, leaving bytes, ETag and time.
    ConditionalCompletion,
    /// Native multipart `Last-Modified` dates completion; otherwise it dates start.
    CompletionTimestamp,
    /// A copy with this condition refuses an occupied name, leaving bytes and time.
    CopyPublication(S3CopyCondition),
    /// A copy is dated later than its source.
    CopyTimestamp,
    /// This protocol lists every probe object; V2 must also agree with V1.
    Listing(S3Listing),
    /// HEAD with checksum mode is accepted.
    ChecksumMode,
    /// A checksum arrives without checksum mode.
    PassiveChecksum,
    /// Some probe returned a checksum.
    ChecksumReturned,
    /// Every returned checksum stated its scope.
    ChecksumScopeStated,
    /// GCS's `x-goog-generation` arrived.
    GoogleGeneration,
    /// GCS's `x-goog-hash` or generation arrived.
    GoogleHeaders,
    /// HEAD Bucket is accepted.
    BucketHead,
    /// A zero-entry listing is accepted.
    BucketList,
}

/// Detection's preference orders (§4): the first enforced condition is chosen.
pub(crate) const SIGNERS: [S3Signing; 2] = [S3Signing::GoogleHmac, S3Signing::AwsSigV4];
pub(crate) const SINGLE_PUTS: [S3SinglePut; 2] =
    [S3SinglePut::IfNoneMatch, S3SinglePut::GenerationMatch];
pub(crate) const COPY_CONDITIONS: [S3CopyCondition; 3] = [
    S3CopyCondition::GenerationMatch,
    S3CopyCondition::IfNoneMatch,
    S3CopyCondition::CloudflareIfNoneMatch,
];

impl S3Detection {
    /// Derive settings from observations. Where no behavior holds, `plain`'s
    /// value stays and [`Self::check`] names the failure. `plain`'s bucket
    /// check is kept when both bucket checks are permitted.
    pub(crate) fn derive(observations: Vec<S3Observation>, plain: S3Settings) -> Self {
        let detection = Self {
            settings: plain,
            observations,
        };
        let held = |probe| detection.held(probe);
        let first = |candidates: &[S3Probe]| candidates.iter().copied().find(|probe| held(*probe));
        let mut settings = plain;
        if let Some(S3Probe::Signing(signing)) = first(&SIGNERS.map(S3Probe::Signing)) {
            settings.signing = signing;
        }
        if let Some(S3Probe::SinglePut(condition)) = first(&SINGLE_PUTS.map(S3Probe::SinglePut)) {
            settings.single_put = condition;
        }
        settings.multipart_timestamp = if held(S3Probe::CompletionTimestamp) {
            S3MultipartTimestamp::Completion
        } else {
            S3MultipartTimestamp::Start
        };
        settings.multipart = if held(S3Probe::ConditionalCompletion)
            && held(S3Probe::PartialUploadHidden)
            && held(S3Probe::CompletionTimestamp)
        {
            S3Multipart::ConditionalCompletion
        } else if let (Some(S3Probe::CopyPublication(condition)), true) = (
            first(&COPY_CONDITIONS.map(S3Probe::CopyPublication)),
            held(S3Probe::CopyTimestamp),
        ) {
            S3Multipart::Copy(condition)
        } else {
            S3Multipart::ConditionalCompletion
        };
        settings.listing = if held(S3Probe::Listing(S3Listing::V2)) {
            S3Listing::V2
        } else {
            S3Listing::V1
        };
        settings.checksums = if !held(S3Probe::ChecksumReturned) {
            S3Checksums::Absent
        } else if !held(S3Probe::ChecksumScopeStated) {
            S3Checksums::UnstatedIgnored
        } else {
            S3Checksums::ScopeStated
        };
        // Request the checksum extension only when the response needs it.
        settings.request_checksums = held(S3Probe::ChecksumMode)
            && !held(S3Probe::PassiveChecksum)
            && settings.checksums != S3Checksums::Absent;
        settings.revision = if held(S3Probe::GoogleGeneration) {
            S3Revision::Generation
        } else {
            S3Revision::ETag
        };
        settings.response_headers = if held(S3Probe::GoogleHeaders) {
            S3ResponseHeaders::Google
        } else {
            S3ResponseHeaders::S3
        };
        // Keep a least-permission listing check when both work; extra bucket
        // privileges do not prove they are required.
        settings.bucket_check = match (held(S3Probe::BucketHead), held(S3Probe::BucketList)) {
            (true, true) => plain.bucket_check,
            (false, true) => S3BucketCheck::List,
            _ => S3BucketCheck::Head,
        };
        Self {
            settings,
            ..detection
        }
    }

    /// Whether this probe's behavior held; an unrun probe did not.
    pub fn held(&self, probe: S3Probe) -> bool {
        self.observations
            .iter()
            .any(|observation| observation.probe == probe && observation.held)
    }

    /// Refuses settings that contradict the observations, naming the first
    /// failed check as setup would: CreateOnce, MultipartCreateOnce,
    /// PublicationTime, then Behavior.
    pub fn check(&self, settings: &S3Settings) -> Result<(), StorageSetupError> {
        let fail = |check, reason: &'static str| {
            Err(StorageSetupError::ProviderCheck {
                check,
                source: StorageFailure::Protocol.with_source(reason),
            })
        };
        let held = |probe| self.held(probe);
        if !held(S3Probe::SinglePut(settings.single_put)) {
            return fail(
                StorageCheck::CreateOnce,
                "the single-PUT condition did not refuse an occupied name with its bytes and time unchanged",
            );
        }
        let (create_once, dated) = match settings.multipart {
            S3Multipart::ConditionalCompletion => (
                held(S3Probe::ConditionalCompletion) && held(S3Probe::PartialUploadHidden),
                held(S3Probe::CompletionTimestamp),
            ),
            S3Multipart::Copy(condition) => (
                held(S3Probe::CopyPublication(condition)),
                held(S3Probe::CopyTimestamp),
            ),
        };
        if !create_once {
            return fail(
                StorageCheck::MultipartCreateOnce,
                "multipart publication did not refuse an occupied name with its bytes and time unchanged, or showed an unfinished upload",
            );
        }
        if !dated {
            return fail(
                StorageCheck::PublicationTime,
                "multipart publication is not dated by complete publication",
            );
        }
        let observed = if held(S3Probe::CompletionTimestamp) {
            S3MultipartTimestamp::Completion
        } else {
            S3MultipartTimestamp::Start
        };
        if settings.multipart_timestamp != observed {
            return fail(
                StorageCheck::PublicationTime,
                "the multipart timestamp setting contradicts the observed timestamp",
            );
        }
        let behavior = [
            (
                held(S3Probe::Signing(settings.signing)),
                "the service refused this signer",
            ),
            (
                held(S3Probe::Listing(settings.listing)),
                "this listing protocol failed or disagreed with V1",
            ),
            (
                match settings.checksums {
                    S3Checksums::Absent => !held(S3Probe::ChecksumReturned),
                    S3Checksums::ScopeStated => held(S3Probe::ChecksumScopeStated),
                    S3Checksums::UnstatedIgnored => true,
                },
                "the checksum policy contradicts the returned checksums",
            ),
            (
                !settings.request_checksums || held(S3Probe::ChecksumMode),
                "the service refused checksum mode",
            ),
            (
                settings.revision == S3Revision::ETag || held(S3Probe::GoogleGeneration),
                "generation revisions need GCS's generation header",
            ),
            (
                (settings.response_headers == S3ResponseHeaders::Google)
                    == held(S3Probe::GoogleHeaders),
                "the response-header setting contradicts the headers returned",
            ),
            (
                held(match settings.bucket_check {
                    S3BucketCheck::Head => S3Probe::BucketHead,
                    S3BucketCheck::List => S3Probe::BucketList,
                }),
                "the service refused this bucket check",
            ),
        ];
        match behavior.into_iter().find(|(agrees, _)| !agrees) {
            Some((_, reason)) => fail(StorageCheck::Behavior, reason),
            None => Ok(()),
        }
    }

    /// The location setup records, after checking its effective settings:
    /// an uncatalogued endpoint naming no profile gets these detected
    /// settings as its custom profile; any other location is kept as given.
    pub fn resolve(&self, location: StorageConfig) -> Result<StorageConfig, StorageSetupError> {
        let StorageConfig::S3 {
            bucket,
            region,
            endpoint,
            profile,
            prefix,
        } = location
        else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("expected S3 configuration")
                .into());
        };
        let (profile, settings) =
            match crate::providers::s3_settings(endpoint.as_ref(), profile.as_ref())? {
                Some(settings) => (profile, settings),
                None => (Some(S3Profile::Custom(self.settings)), self.settings),
            };
        self.check(&settings)?;
        Ok(StorageConfig::S3 {
            bucket,
            region,
            endpoint,
            profile,
            prefix,
        })
    }
}

#[cfg(test)]
#[path = "s3_detection_tests.rs"]
mod tests;
