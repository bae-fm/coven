//! Waiting between automatic attempts (§6.7, §21.2).
//!
//! Ordinary delays double from one second to five minutes. A provider's
//! `Retry-After` is a cooldown for every consumer of the store's connection:
//! the connection refuses requests while it is live and records it, durably,
//! before returning the response that asked for it.

use crate::{StorageError, StorageFailure};
use std::time::Duration;

/// The longest ordinary delay between unsuccessful attempts.
pub const LONGEST_RETRY_DELAY: Duration = Duration::from_secs(300);

/// The delay after the unsuccessful attempt numbered `attempt`, counting from
/// zero: 1, 2, 4, …, 256, then 300 seconds.
pub fn retry_delay(attempt: u32) -> Duration {
    let seconds = 1u64
        .checked_shl(attempt)
        .unwrap_or(u64::MAX)
        .min(LONGEST_RETRY_DELAY.as_secs());
    Duration::from_secs(seconds)
}

/// The wait a provider requested with a response (§6.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryAfter {
    /// Delay-seconds, or an HTTP-date measured against the response's own
    /// `Date`, so a skewed device clock cannot shorten it.
    Wait(Duration),
    /// An HTTP-date the response gives no usable `Date` to measure against.
    NoTimeBasis,
}

impl RetryAfter {
    /// The cooldown to arm. Without a time basis coven cannot convert the
    /// requested date, and waits the longest ordinary delay instead of
    /// treating the missing basis as permission to retry at once.
    pub fn wait(self) -> Duration {
        match self {
            Self::Wait(wait) => wait,
            Self::NoTimeBasis => LONGEST_RETRY_DELAY,
        }
    }
}

/// How long to wait before resending a request refused as `RateLimited`,
/// after `attempt` earlier resends: what the provider asked for, or what is
/// left of the cooldown that refused it before sending, and never less than
/// the ordinary delay, so an absent or zero request still backs off.
pub(crate) fn throttled_wait(error: &StorageError, attempt: u32) -> Duration {
    let requested = error
        .retry_after()
        .map(RetryAfter::wait)
        .or_else(|| match error {
            StorageError::Caused { source, .. } => source
                .downcast_ref::<CoolingDown>()
                .map(|cooling| cooling.remaining),
            _ => None,
        });
    requested.unwrap_or_default().max(retry_delay(attempt))
}

/// The store's provider cooldown, shared by every consumer of its connection:
/// sync passes, operations, file transfers and joining.
#[async_trait::async_trait]
pub trait Cooldown: Send + Sync {
    /// The live cooldown's remaining wait, measured monotonically.
    fn remaining(&self) -> Option<Duration>;
    /// Arm and persist a wait the provider requested, before any consumer can
    /// retry. A failure is returned; it never permits an immediate retry.
    async fn record(&self, wait: Duration) -> Result<(), StorageError>;
}

/// Refuse a request while the store's shared provider cooldown is live.
pub(crate) fn cooling(cooldown: &dyn Cooldown) -> Result<(), StorageError> {
    match cooldown.remaining() {
        Some(remaining) => Err(StorageFailure::RateLimited.with_source(CoolingDown { remaining })),
        None => Ok(()),
    }
}

/// Persist provider-requested waits before returning them to any consumer.
pub(crate) async fn observed<T>(
    cooldown: &dyn Cooldown,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    let error = match result {
        Ok(value) => return Ok(value),
        Err(error) => error,
    };
    let Some(after) = error.retry_after() else {
        return Err(error);
    };
    if after == RetryAfter::NoTimeBasis {
        tracing::warn!(
            ?error,
            "Retry-After names a date without a response Date; waiting the longest ordinary delay"
        );
    }
    let wait = after.wait();
    match cooldown.record(wait).await {
        Ok(()) => Err(error),
        Err(cause) => Err(StorageFailure::File.with_source(CooldownNotRecorded {
            wait,
            response: error,
            cause,
        })),
    }
}

/// No cooldown is kept: raw fixtures and probes outside a store's lifetime.
pub struct NoCooldown;

#[async_trait::async_trait]
impl Cooldown for NoCooldown {
    fn remaining(&self) -> Option<Duration> {
        None
    }
    async fn record(&self, _: Duration) -> Result<(), StorageError> {
        Ok(())
    }
}

/// A request refused before it was sent, while a provider cooldown is live.
#[derive(Debug, thiserror::Error)]
#[error("provider cooldown has {remaining:?} left")]
pub struct CoolingDown {
    /// The cooldown's remaining wait when the request was refused.
    pub remaining: Duration,
}

/// The provider asked for a cooldown that could not be persisted. The work
/// that observed it fails; automatic retry waits for the app.
#[derive(Debug, thiserror::Error)]
#[error("provider cooldown of {wait:?} was not persisted: {cause}")]
pub struct CooldownNotRecorded {
    /// The requested wait, still armed for this session.
    pub wait: Duration,
    /// The provider's response asking for it.
    #[source]
    pub response: StorageError,
    /// Why persisting it failed.
    pub cause: StorageError,
}

#[cfg(test)]
#[path = "cooldown_tests.rs"]
pub(crate) mod tests;
