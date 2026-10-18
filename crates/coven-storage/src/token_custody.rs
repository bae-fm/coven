//! Where a connection's OAuth sign-in is held, and its one refresh after the
//! provider rejects a token (§20.2, E10). No device clock decides expiry.

use crate::{OAuthTokens, S3Credentials, StorageCredentials, StorageError};
use std::sync::Arc;

/// The custody of a connection's sign-in: coven's session custody before
/// setup or bootstrap publishes the store, device custody afterwards (§20.2).
#[async_trait::async_trait]
pub trait TokenCustody: Send + Sync {
    /// Replace tokens the provider rejected with HTTP 401. When the held
    /// sign-in no longer is `rejected` (another connection refreshed it, or a
    /// new sign-in was committed), return the held one; otherwise refresh once
    /// and commit the replacement here before returning it. A failed refresh
    /// or commit returns its error and installs nothing.
    async fn replace(&self, rejected: &OAuthTokens) -> Result<OAuthTokens, StorageError>;
}

/// What a connection authenticates with (§20.2).
#[derive(Clone)]
pub enum ProviderAuth {
    /// This member's S3 key.
    S3(S3Credentials),
    /// A provider sign-in, refreshed in its custody when the provider rejects it.
    OAuth {
        /// The tokens the connection starts with.
        tokens: OAuthTokens,
        /// Where a rejected token's replacement is committed.
        custody: Arc<dyn TokenCustody>,
    },
    /// The device's Apple account.
    CloudKit,
}

impl ProviderAuth {
    /// Committed credentials, with the custody that refreshes an OAuth sign-in.
    pub fn new(credentials: StorageCredentials, custody: Arc<dyn TokenCustody>) -> Self {
        match credentials {
            StorageCredentials::S3(keys) => Self::S3(keys),
            StorageCredentials::OAuth(tokens) => Self::OAuth { tokens, custody },
            StorageCredentials::CloudKit => Self::CloudKit,
        }
    }

    /// The credentials this connection starts with.
    pub fn credentials(&self) -> StorageCredentials {
        match self {
            Self::S3(keys) => StorageCredentials::S3(keys.clone()),
            Self::OAuth { tokens, .. } => StorageCredentials::OAuth(tokens.clone()),
            Self::CloudKit => StorageCredentials::CloudKit,
        }
    }
}

/// The tokens one connection sends, and its single path to a replacement.
/// Requests rejected concurrently with the same token share one replacement.
pub(crate) struct HeldTokens {
    current: tokio::sync::RwLock<OAuthTokens>,
    custody: Arc<dyn TokenCustody>,
    replacing: tokio::sync::Mutex<()>,
}

impl HeldTokens {
    pub(crate) fn new(tokens: OAuthTokens, custody: Arc<dyn TokenCustody>) -> Self {
        Self {
            current: tokio::sync::RwLock::new(tokens),
            custody,
            replacing: tokio::sync::Mutex::new(()),
        }
    }

    /// Hold the tokens so requests wait for them.
    #[cfg(test)]
    pub(crate) async fn hold(&self) -> tokio::sync::RwLockWriteGuard<'_, OAuthTokens> {
        self.current.write().await
    }

    /// The tokens the next request sends.
    pub(crate) async fn current(&self) -> OAuthTokens {
        self.current.read().await.clone()
    }

    /// Install tokens its owner has already committed to custody. A
    /// replacement in flight finishes first: it was committed earlier, so
    /// these later tokens must not be overwritten by it.
    pub(crate) async fn install(&self, tokens: OAuthTokens) {
        let _one = self.replacing.lock().await;
        *self.current.write().await = tokens;
    }

    /// The tokens to retry with after the provider rejected `used`.
    pub(crate) async fn rejected(&self, used: &OAuthTokens) -> Result<OAuthTokens, StorageError> {
        let _one = self.replacing.lock().await;
        let current = self.current().await;
        if current.access_token.as_str() != used.access_token.as_str() {
            return Ok(current);
        }
        let replacement = self.custody.replace(used).await?;
        *self.current.write().await = replacement.clone();
        Ok(replacement)
    }
}

#[cfg(test)]
#[path = "token_custody_tests.rs"]
pub(crate) mod tests;
