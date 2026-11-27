//! Dropbox's asynchronous sharing jobs, checked until they end.

use super::http::{self, OAuthSession, ProviderResponse};
use crate::{CloudProvider, StorageError, StorageFailure};
use serde_json::Value;

/// The `complete` status of the Dropbox job whose status `check` reads,
/// named `what` in failures. The job is followed until Dropbox reports its
/// end, with checks spaced by the backoff of §6.7: 1, 2, 4, …, 256, then
/// every 300 seconds. A `failed` status is the provider's failure, and a
/// failed `check` ends the following with its own.
pub(super) async fn completed<F, Fut>(
    session: &OAuthSession,
    what: &'static str,
    mut check: F,
) -> Result<Value, StorageError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(Value, ProviderResponse), StorageError>>,
{
    let mut waits = 0u32;
    loop {
        let (value, original) = check().await?;
        match http::string(&value, ".tag")? {
            "complete" => return Ok(value),
            "in_progress" => {
                session.wait_out(crate::retry_delay(waits)).await;
                waits = waits.saturating_add(1);
            }
            "failed" => return Err(original.into_error(CloudProvider::Dropbox)),
            _ => {
                return Err(
                    StorageFailure::Protocol.with_source(format!("invalid Dropbox {what} status"))
                )
            }
        }
    }
}
