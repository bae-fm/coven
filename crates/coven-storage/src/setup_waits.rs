//! The waits one storage setup call spends on requests a provider refused
//! for rate limiting (§20.8).
//!
//! Setup runs while the person waits for it, so a refused request is sent
//! again in place instead of failing setup. Every step of one call draws on
//! one minute; past it, the refusal fails setup.

use crate::{cooldown::throttled_wait, StorageError, StorageFailure};
use coven_foundation::clock::Clock;
use std::{future::Future, sync::Mutex, time::Duration};

/// The longest one setup call waits, in total, to send rate-limited
/// requests again.
pub const SETUP_RATE_LIMIT_WAIT: Duration = Duration::from_secs(60);

/// What one setup call has spent waiting to send refused requests again.
/// Its steps and the connection it makes share one; once the call returns,
/// [`Self::end`] stops the connection, now the store's, from resending.
#[derive(Default)]
pub struct SetupWaits(Mutex<Spent>);

#[derive(Default)]
struct Spent {
    resends: u32,
    waited: Duration,
    ended: bool,
}

impl SetupWaits {
    /// The setup call has returned. Its connection's later requests fail on a
    /// rate limit as any connection's do, and sync retries them (§6.7).
    pub fn end(&self) {
        self.0.lock().expect("setup waits poisoned").ended = true;
    }

    /// Whether to send the request that failed with `error` again, after
    /// waiting by `clock`: a rate-limited refusal whose cleanup did not also
    /// fail, while the call runs and the wait fits in what is left of
    /// [`SETUP_RATE_LIMIT_WAIT`]. The wait is the provider's request or what
    /// is left of the cooldown that refused it, and never less than the
    /// ordinary delay (§6.7).
    pub(crate) async fn wait_to_resend(&self, clock: &dyn Clock, error: &StorageError) -> bool {
        if error.failure() != StorageFailure::RateLimited
            || matches!(error, StorageError::Cleanup { .. })
        {
            return false;
        }
        let (wait, resends) = {
            let mut spent = self.0.lock().expect("setup waits poisoned");
            let wait = throttled_wait(error, spent.resends);
            let waited = spent.waited.saturating_add(wait);
            if spent.ended || waited > SETUP_RATE_LIMIT_WAIT {
                return false;
            }
            spent.waited = waited;
            spent.resends += 1;
            (wait, spent.resends)
        };
        tracing::info!(?wait, resends, "storage setup rate limited; sending again");
        clock.sleep(wait).await;
        true
    }

    /// Send `request`, and again after each rate-limited refusal this call
    /// can still wait out. Only for a request whose refusal stored nothing.
    pub async fn resend<T, F: Future<Output = Result<T, StorageError>>>(
        &self,
        clock: &dyn Clock,
        mut request: impl FnMut() -> F,
    ) -> Result<T, StorageError> {
        loop {
            let result = request().await;
            if let Err(error) = &result {
                if self.wait_to_resend(clock, error).await {
                    continue;
                }
            }
            return result;
        }
    }
}

#[cfg(test)]
#[path = "setup_waits_tests.rs"]
mod tests;
