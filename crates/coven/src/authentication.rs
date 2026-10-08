//! Session custody for a sign-in awaiting storage setup or bootstrap publication.

use crate::{CloudProvider, OAuthError};
use coven_storage::{providers::OAuthFlow, OAuthTokens, StorageCredentials};

pub(crate) struct Authentication {
    pub(crate) provider: CloudProvider,
    pub(crate) tokens: OAuthTokens,
}

impl Authentication {
    pub(crate) async fn credentials(
        &mut self,
        provider: CloudProvider,
        flow: &OAuthFlow,
        now: std::time::SystemTime,
    ) -> Result<StorageCredentials, OAuthError> {
        if self.provider != provider {
            return Err(OAuthError::Reauthorize(provider));
        }
        if self.tokens.expires_at.is_some_and(|expiry| now >= expiry) {
            self.tokens = flow.refresh(provider, &self.tokens).await?;
        }
        Ok(StorageCredentials::OAuth(self.tokens.clone()))
    }
}

#[cfg(test)]
#[path = "authentication_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) use tests::SignIn;
