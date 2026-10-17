//! The device clock S3 signs with, corrected by the service's own evidence
//! (§20.7, E5). `RequestTimeTooSkewed` means the device clock is off, not that
//! access was denied: the request is retried once, signed with the offset the
//! response's `Date` states, and a second refusal is `ClockSkew`. Every
//! attempt, the retry included, waits for the store's provider cooldown and
//! rechecks publication authority.

use crate::{Cooldown, CoolingDown, StorageFailure};
use aws_sdk_s3::config::{
    interceptors::{BeforeTransmitInterceptorContextRef, FinalizerInterceptorContextMut},
    ConfigBag, Intercept, RuntimeComponents,
};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        interceptors::context::InterceptorContext,
        orchestrator::HttpResponse,
        retries::classifiers::{ClassifyRetry, RetryAction, RetryClassifierPriority},
    },
};
use coven_foundation::clock::ClockRef;
use std::{
    fmt,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

/// S3 refuses a signature dated more than 15 minutes from its own clock.
const SIGNATURE_TOLERANCE: Duration = Duration::from_secs(15 * 60);

/// The injected clock, shifted by the offset the service last stated. Both
/// the SigV4 and the Google HMAC signer read the time here.
#[derive(Clone)]
pub(super) struct SigningClock {
    clock: ClockRef,
    /// Milliseconds the service's clock is ahead of the device's.
    offset: Arc<AtomicI64>,
}

impl SigningClock {
    pub(super) fn new(clock: ClockRef) -> Self {
        Self {
            clock,
            offset: Arc::new(AtomicI64::new(0)),
        }
    }

    fn shift(time: SystemTime, offset: i64) -> SystemTime {
        let by = Duration::from_millis(offset.unsigned_abs());
        if offset >= 0 {
            time.checked_add(by)
        } else {
            time.checked_sub(by)
        }
        .unwrap_or(time)
    }

    /// Whether `response` refused this device's clock: S3's code, or HEAD's
    /// bodyless 403 dated beyond the signature tolerance from the time signed.
    fn refused(&self, response: &HttpResponse) -> bool {
        if response.status().as_u16() != 403 {
            return false;
        }
        match response.body().bytes() {
            Some(body) if !body.is_empty() => error_code(body) == Some("RequestTimeTooSkewed"),
            _ => server_time(response).is_some_and(|server| {
                let signed = aws_smithy_async::time::TimeSource::now(self);
                let apart = server
                    .duration_since(signed)
                    .or_else(|_| signed.duration_since(server))
                    .unwrap_or_default();
                apart > SIGNATURE_TOLERANCE
            }),
        }
    }

    /// Adopt the offset `response` states, if it states one.
    fn adopt(&self, response: &HttpResponse) -> bool {
        let Some(server) = server_time(response) else {
            return false;
        };
        let device = self.clock.now();
        let offset = match server.duration_since(device) {
            Ok(ahead) => i64::try_from(ahead.as_millis()).unwrap_or(i64::MAX),
            Err(behind) => i64::try_from(behind.duration().as_millis())
                .map(|behind| -behind)
                .unwrap_or(i64::MIN),
        };
        self.offset.store(offset, Ordering::Release);
        true
    }
}

impl fmt::Debug for SigningClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SigningClock")
    }
}

impl aws_smithy_async::time::TimeSource for SigningClock {
    fn now(&self) -> SystemTime {
        Self::shift(self.clock.now(), self.offset.load(Ordering::Acquire))
    }
}

fn server_time(response: &HttpResponse) -> Option<SystemTime> {
    use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
    let date = response.headers().get("date")?;
    SystemTime::try_from(DateTime::from_str(date.trim(), DateTimeFormat::HttpDate).ok()?).ok()
}

/// The `Code` of an S3 XML error body.
fn error_code(body: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(body).ok()?;
    let start = text.find("<Code>")? + "<Code>".len();
    let end = start + text[start..].find("</Code>")?;
    Some(text[start..end].trim())
}

/// Marks a response that refused this device's clock, for the retry and for
/// classifying the final failure as `ClockSkew`.
#[derive(Clone, Copy, Debug)]
pub(super) struct ClockRefused {
    /// Whether the response stated the offset the retry signs with.
    corrected: bool,
}

/// Whether the final response behind an S3 failure refused this device's clock.
pub(super) fn clock_refused(response: Option<&HttpResponse>) -> bool {
    response.is_some_and(|response| response.extension::<ClockRefused>().is_some())
}

/// Recognizes clock refusals and gates every attempt on the provider cooldown.
#[derive(Clone)]
pub(super) struct ClockSkew {
    signing: SigningClock,
    cooldown: Arc<dyn Cooldown>,
}

impl ClockSkew {
    pub(super) fn new(signing: SigningClock, cooldown: Arc<dyn Cooldown>) -> Self {
        Self { signing, cooldown }
    }
}

impl fmt::Debug for ClockSkew {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClockSkew")
    }
}

impl Intercept for ClockSkew {
    fn name(&self) -> &'static str {
        "ClockSkew"
    }

    fn read_before_transmit(
        &self,
        _: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        match self.cooldown.remaining() {
            Some(remaining) => Err(Box::new(
                StorageFailure::RateLimited.with_source(CoolingDown { remaining }),
            )),
            None => Ok(()),
        }
    }

    fn modify_before_attempt_completion(
        &self,
        context: &mut FinalizerInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let Some(response) = context.response_mut() else {
            return Ok(());
        };
        if self.signing.refused(response) {
            let corrected = self.signing.adopt(response);
            response.add_extension(ClockRefused { corrected });
        }
        Ok(())
    }
}

/// Coven retries only a clock refusal whose response stated an offset; every
/// other outcome reaches coven's own retry policy unchanged.
#[derive(Debug)]
pub(super) struct ClockRetry;

impl ClassifyRetry for ClockRetry {
    fn classify_retry(&self, context: &InterceptorContext) -> RetryAction {
        match context
            .response()
            .and_then(|response| response.extension::<ClockRefused>())
        {
            Some(ClockRefused { corrected: true }) => RetryAction::server_error(),
            _ => RetryAction::RetryForbidden,
        }
    }

    fn name(&self) -> &'static str {
        "ClockRetry"
    }

    fn priority(&self) -> RetryClassifierPriority {
        RetryClassifierPriority::run_after(RetryClassifierPriority::transient_error_classifier())
    }
}

/// One retry, sent at once: the attempt only lacked a correct signing time.
pub(super) fn retry_config() -> aws_sdk_s3::config::retry::RetryConfig {
    aws_sdk_s3::config::retry::RetryConfig::standard()
        .with_max_attempts(2)
        .with_initial_backoff(Duration::ZERO)
}

#[cfg(test)]
#[path = "s3_clock_tests.rs"]
mod tests;
