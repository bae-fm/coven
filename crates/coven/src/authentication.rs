//! Session custody for a sign-in awaiting storage setup or bootstrap publication.

use crate::{CloudProvider, OAuthError};
use coven_storage::{providers::OAuthFlow, OAuthTokens, StorageCredentials};

pub(crate) struct Authentication {
    pub(crate) tokens: OAuthTokens,
}

impl Authentication {
    pub(crate) async fn credentials(
        &mut self,
        provider: CloudProvider,
        flow: &OAuthFlow,
        now: std::time::SystemTime,
    ) -> Result<StorageCredentials, OAuthError> {
        if self.tokens.access.provider() != provider {
            return Err(OAuthError::Reauthorize(provider));
        }
        if self.tokens.expires_at.is_some_and(|expiry| now >= expiry) {
            let refresh_token = self
                .tokens
                .refresh_token
                .as_ref()
                .ok_or(OAuthError::Reauthorize(provider))?;
            self.tokens = flow
                .refresh(&self.tokens.client_id, self.tokens.access, refresh_token)
                .await?;
        }
        Ok(StorageCredentials::OAuth(self.tokens.clone()))
    }
}

#[cfg(test)]
#[path = "authentication_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) use tests::SignIn;
