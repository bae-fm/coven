//! Dropbox's asynchronous sharing jobs, checked until they end.

use super::http::{self, OAuthSession, ProviderResponse};
use crate::{CloudProvider, StorageError, StorageFailure};
use serde_json::Value;

/// How a Dropbox job ended.
pub(super) enum JobEnd {
    /// Its `complete` status.
    Complete(Value),
    /// The provider's failure its `failed` status names.
    Failed(StorageError),
}

/// How the Dropbox job whose status `check` reads ended, named `what` in
/// failures. The job is followed until Dropbox reports its end, with checks
/// spaced by the backoff of §6.7: 1, 2, 4, …, 256, then every 300 seconds. A
/// failed `check` ends the following with its own failure, leaving the job's
/// end unknown.
pub(super) async fn ended<F, Fut>(
    session: &OAuthSession,
    what: &'static str,
    mut check: F,
) -> Result<JobEnd, StorageError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(Value, ProviderResponse), StorageError>>,
{
    let mut waits = 0u32;
    loop {
        let (value, original) = check().await?;
        match http::string(&value, ".tag")? {
            "complete" => return Ok(JobEnd::Complete(value)),
            "in_progress" => {
                session.wait_out(crate::retry_delay(waits)).await;
                waits = waits.saturating_add(1);
            }
            "failed" => return Ok(JobEnd::Failed(original.into_error(CloudProvider::Dropbox))),
            _ => {
                return Err(
                    StorageFailure::Protocol.with_source(format!("invalid Dropbox {what} status"))
                )
            }
        }
    }
}
