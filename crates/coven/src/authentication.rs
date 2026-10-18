//! Session custody for a sign-in awaiting storage setup or bootstrap
//! publication (§20.2, E10): a completed sign-in is held for this session only.

use crate::{CloudProvider, OAuthError};
use coven_sync::SessionSignIn;

/// A held sign-in serves only its own provider; another needs a new sign-in.
pub(crate) fn check_provider(
    held: &SessionSignIn,
    provider: CloudProvider,
) -> Result<(), OAuthError> {
    if held.provider() != provider {
        return Err(OAuthError::Reauthorize(provider));
    }
    Ok(())
}

/// Record only the account reached by this sign-in, never a label supplied by a code.
pub(crate) async fn label_location(
    connector: &dyn coven_storage::providers::StorageConnector,
    mut location: crate::StorageConfig,
    auth: coven_storage::ProviderAuth,
    cooldown: std::sync::Arc<dyn coven_storage::Cooldown>,
) -> Result<crate::StorageConfig, crate::StorageError> {
    coven_storage::ConnectionCredentials {
        location: location.clone(),
        credentials: auth.credentials(),
    }
    .validate()?;
    match &mut location {
        crate::StorageConfig::Dropbox { account_label, .. }
        | crate::StorageConfig::OneDrive { account_label, .. } => {
            *account_label = Some(connector.account_label(auth, cooldown).await?);
        }
        crate::StorageConfig::S3 { .. } | crate::StorageConfig::CloudKit { .. } => {}
    }
    Ok(location)
}

#[cfg(test)]
#[path = "authentication_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) use tests::SignIn;
