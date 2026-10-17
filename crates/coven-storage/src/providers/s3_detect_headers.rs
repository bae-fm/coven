//! Detection's raw response-header observations: checksums and their scope,
//! and GCS's generation and hash headers, before any translation (Appendix E.7).
use super::*;
use crate::S3Probe;
use aws_sdk_s3::config::{
    interceptors::BeforeDeserializationInterceptorContextRef, ConfigBag, Intercept,
    RuntimeComponents,
};
use aws_smithy_runtime_api::box_error::BoxError;
use std::sync::{Arc, Mutex};

const CHECKSUMS: [&str; 5] = [
    "x-amz-checksum-sha256",
    "x-amz-checksum-sha1",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
];

#[derive(Clone, Copy, Debug, Default)]
struct Facts {
    checksum: bool,
    unstated: bool,
    google: bool,
    generation: bool,
}

/// Reads each response after transmission, before the adapter's own header
/// translation or unscoped-checksum removal can change it.
#[derive(Debug, Clone, Default)]
struct Capture(Arc<Mutex<Facts>>);
impl Capture {
    fn facts(&self) -> Facts {
        *self.0.lock().expect("header facts lock")
    }
}
impl Intercept for Capture {
    fn name(&self) -> &'static str {
        "DetectionHeaderFacts"
    }
    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.response().headers();
        let mut facts = self.0.lock().expect("header facts lock");
        facts.google |= headers.get("x-goog-hash").is_some();
        facts.generation |= headers.get("x-goog-generation").is_some();
        let checksum = CHECKSUMS.iter().any(|name| headers.get(*name).is_some());
        facts.checksum |= checksum || facts.google;
        facts.unstated |= checksum && headers.get("x-amz-checksum-type").is_none();
        Ok(())
    }
}

/// HEAD the first key without and with checksum mode, then HEAD and GET
/// every key, requesting checksums only where the service accepts that mode.
pub(super) async fn observe(probes: &mut Probes<'_>, keys: &[String]) -> Result<(), StorageError> {
    let s = probes.s;
    let capture = Capture::default();
    let head = |key: &str, mode: bool| {
        s.client
            .head_object()
            .bucket(&s.bucket)
            .key(key)
            .set_checksum_mode(mode.then_some(aws_sdk_s3::types::ChecksumMode::Enabled))
            .customize()
            .interceptor(capture.clone())
            .send()
    };
    let first = keys
        .first()
        .ok_or(StorageFailure::Protocol.with_source("no checksum probe"))?;
    head(first, false).await.map_err(s3_error)?;
    let passive = capture.facts().checksum;
    let requested = head(first, true).await.map_err(s3_error);
    let mode = requested.is_ok();
    let detail = outcome(&requested);
    if let Err(error) = requested {
        if !unsupported(&error) {
            return Err(error);
        }
    }
    for key in keys {
        head(key, mode).await.map_err(s3_error)?;
        let read = s
            .client
            .get_object()
            .bucket(&s.bucket)
            .key(key)
            .set_checksum_mode(mode.then_some(aws_sdk_s3::types::ChecksumMode::Enabled))
            .customize()
            .interceptor(capture.clone())
            .send()
            .await
            .map_err(s3_error)?;
        read.body
            .collect()
            .await
            .map_err(|source| StorageError::Provider {
                provider: CloudProvider::S3,
                failure: StorageFailure::Network,
                source: Box::new(source),
            })?;
    }
    let facts = capture.facts();
    let summary = format!(
        "checksum={} unstated={} google_hash={} generation={}",
        facts.checksum, facts.unstated, facts.google, facts.generation
    );
    probes.observe(S3Probe::ChecksumMode, mode, detail);
    probes.observe(S3Probe::PassiveChecksum, passive, summary.clone());
    probes.observe(S3Probe::ChecksumReturned, facts.checksum, summary.clone());
    probes.observe(
        S3Probe::ChecksumScopeStated,
        !facts.unstated,
        summary.clone(),
    );
    probes.observe(S3Probe::GoogleGeneration, facts.generation, summary.clone());
    probes.observe(
        S3Probe::GoogleHeaders,
        facts.google || facts.generation,
        summary,
    );
    Ok(())
}
