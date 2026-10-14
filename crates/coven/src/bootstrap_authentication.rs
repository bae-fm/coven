//! The builder's held sign-in, as the custody a bootstrap connection
//! refreshes in until publication commits it to device custody (§4, E10).

use super::*;
use coven_storage::ProviderAuth;
use coven_sync::SessionSignIn;

/// The session sign-in for a bootstrap connection to `provider`.
pub(super) fn account_sign_in(
    builder: &CovenBuilder,
    provider: CloudProvider,
) -> Result<Arc<SessionSignIn>, BootstrapError> {
    builder
        .oauth_flow()
        .ok_or(OAuthError::Unavailable(provider))?;
    let held = builder
        .authentication
        .as_ref()
        .ok_or(OAuthError::Reauthorize(provider))?;
    crate::authentication::check_provider(held, provider)?;
    Ok(held.clone())
}

/// How bootstrap reaches `data`: an S3 key, the Apple account, or the
/// session sign-in it publishes with the store.
pub(super) fn provider_auth(
    data: &ConnectionCredentials,
    sign_in: Option<&Arc<SessionSignIn>>,
) -> Result<ProviderAuth, BootstrapError> {
    Ok(match (&data.credentials, sign_in) {
        (StorageCredentials::OAuth(_), Some(sign_in)) => ProviderAuth::OAuth {
            tokens: sign_in.tokens()?,
            custody: sign_in.clone(),
        },
        (StorageCredentials::S3(keys), None) => ProviderAuth::S3(keys.clone()),
        (StorageCredentials::CloudKit, None) => ProviderAuth::CloudKit,
        _ => {
            return Err(SyncError::from(
                StorageFailure::InvalidConfiguration
                    .with_source("only an OAuth location connects through a sign-in"),
            )
            .into())
        }
    })
}
