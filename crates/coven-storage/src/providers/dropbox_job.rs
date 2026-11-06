//! Dropbox's asynchronous sharing jobs, checked until they end.

use super::http::{self, OAuthSession, ProviderResponse};
use crate::{CloudProvider, StorageError, StorageFailure};
use serde_json::Value;

/// Job status checks before reporting a timeout.
const JOB_CHECKS: u32 = 7;

/// The `complete` status of the Dropbox job whose status `check` reads,
/// named `what` in failures. Checks back off like every automatic wait:
/// about a minute across seven checks, 1, 2, 4, …, 32 seconds apart. A
/// `failed` status is the provider's failure, and a job still in progress
/// after the last check fails as a network timeout.
pub(super) async fn completed<F, Fut>(
    session: &OAuthSession,
    what: &'static str,
    mut check: F,
) -> Result<Value, StorageError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(Value, ProviderResponse), StorageError>>,
{
    for attempt in 0..JOB_CHECKS {
        let (value, original) = check().await?;
        match http::string(&value, ".tag")? {
            "complete" => return Ok(value),
            "in_progress" if attempt + 1 < JOB_CHECKS => {
                session.wait_out(crate::retry_delay(attempt)).await
            }
            "in_progress" => break,
            "failed" => return Err(original.into_error(CloudProvider::Dropbox)),
            _ => {
                return Err(
                    StorageFailure::Protocol.with_source(format!("invalid Dropbox {what} status"))
                )
            }
        }
    }
    Err(StorageError::Provider {
        provider: CloudProvider::Dropbox,
        failure: StorageFailure::Network,
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("Dropbox {what} timed out"),
        )),
    })
}
