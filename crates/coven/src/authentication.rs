//! Session custody for a sign-in awaiting storage setup or bootstrap
//! publication (§20.2, E10): a completed sign-in is held for this session only.

use crate::error::setup_error;
use crate::StorageSetupError;
use crate::{CloudProvider, OAuthError};
use coven_storage::ProviderAuth;
use coven_sync::SessionSignIn;
use std::sync::Arc;

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

/// How a connection starts with the held sign-in: its tokens now, refreshing
/// in its custody. A held sign-in serves only its own provider.
pub(crate) fn held_auth(
    held: &Arc<SessionSignIn>,
    provider: CloudProvider,
) -> Result<ProviderAuth, StorageSetupError> {
    check_provider(held, provider)?;
    Ok(ProviderAuth::OAuth { tokens: held.tokens().map_err(setup_error)?, custody: held.clone() })
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

/// Record the folder's name and web address as this sign-in sees it, never a
/// label the app passed. Setup alone reads them (§20.2); the Dropbox App
/// folder has none.
pub(crate) async fn label_folder(
    connector: &dyn coven_storage::providers::StorageConnector,
    mut location: crate::StorageConfig,
    auth: coven_storage::ProviderAuth,
    cooldown: std::sync::Arc<dyn coven_storage::Cooldown>,
) -> Result<crate::StorageConfig, crate::StorageError> {
    let label = match &location {
        crate::StorageConfig::Dropbox {
            location: crate::DropboxLocation::SharedFolder { .. },
            ..
        }
        | crate::StorageConfig::OneDrive { .. } => {
            Some(connector.folder_label(&location, auth, cooldown).await?)
        }
        crate::StorageConfig::Dropbox { location: crate::DropboxLocation::AppFolder, .. }
        | crate::StorageConfig::S3 { .. }
        | crate::StorageConfig::CloudKit { .. } => None,
    };
    match &mut location {
        crate::StorageConfig::Dropbox { folder_label, .. }
        | crate::StorageConfig::OneDrive { folder_label, .. } => *folder_label = label,
        crate::StorageConfig::S3 { .. } | crate::StorageConfig::CloudKit { .. } => {}
    }
    Ok(location)
}

#[cfg(test)]
#[path = "authentication_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) use tests::SignIn;
