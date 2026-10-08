//! Refresh the builder's held sign-in while bootstrap waits and loads.

use super::*;

pub(super) async fn account_credentials(
    builder: &mut CovenBuilder,
    provider: CloudProvider,
    cancel: &watch::Receiver<bool>,
) -> Result<StorageCredentials, BootstrapError> {
    if provider == CloudProvider::CloudKit {
        return Ok(StorageCredentials::CloudKit);
    }
    let flow = builder
        .oauth_flow()
        .ok_or(OAuthError::Unavailable(provider))?;
    let authentication = builder
        .authentication
        .as_mut()
        .ok_or(OAuthError::Reauthorize(provider))?;
    let mut cancellation = cancel.clone();
    tokio::select! {
        biased;
        _ = cancelled(&mut cancellation) => Err(BootstrapError::Cancelled),
        result = authentication.credentials(provider, &flow, builder.clock.now()) => Ok(result?),
    }
}

pub(super) async fn refresh_sign_in(
    builder: &mut CovenBuilder,
    data: &mut ConnectionCredentials,
    storage: &dyn coven_storage::Storage,
    cancel: &watch::Receiver<bool>,
) -> Result<(), BootstrapError> {
    if let StorageCredentials::OAuth(tokens) = &data.credentials {
        if tokens
            .expires_at
            .is_some_and(|expiry| builder.clock.now() >= expiry)
        {
            data.credentials =
                account_credentials(builder, data.location.provider(), cancel).await?;
            let StorageCredentials::OAuth(tokens) = &data.credentials else {
                unreachable!("OAuth sign-in")
            };
            storage
                .set_oauth_tokens(tokens.clone())
                .await
                .map_err(SyncError::from)?;
        }
    }
    Ok(())
}
