//! Shared OAuth token lifecycle for the consumer-cloud backends.
//!
//! Google Drive, Dropbox, and OneDrive all cache an access token, refresh it on
//! expiry (persisting the new tokens to the keyring), and retry a request once
//! on a 401. This holds that logic in one place; each backend owns an
//! `OAuthSession` and routes its requests through `api_call`.

use tokio::sync::RwLock;
use tracing::{info, warn};

use super::CloudHomeError;
use crate::clock::ClockRef;
use crate::keys::{CloudHomeCredentials, KeyService};
use crate::oauth::{self, OAuthConfig, OAuthTokens};

/// Owns a provider's OAuth tokens and refreshes them as needed.
pub struct OAuthSession {
    tokens: RwLock<OAuthTokens>,
    key_service: KeyService,
    clock: ClockRef,
    config: OAuthConfig,
    /// Human-readable provider name, used only in log lines.
    provider_label: &'static str,
}

impl OAuthSession {
    pub fn new(
        tokens: OAuthTokens,
        key_service: KeyService,
        clock: ClockRef,
        config: OAuthConfig,
        provider_label: &'static str,
    ) -> Self {
        Self {
            tokens: RwLock::new(tokens),
            key_service,
            clock,
            config,
            provider_label,
        }
    }

    /// The current access token, refreshing if it's expired or about to expire.
    async fn access_token(&self) -> Result<String, CloudHomeError> {
        let tokens = self.tokens.read().await;
        match tokens.expires_at {
            Some(expires_at) if self.clock.now().timestamp() < expires_at - 60 => {
                return Ok(tokens.access_token.clone());
            }
            // No expiry info: assume valid.
            None => return Ok(tokens.access_token.clone()),
            _ => {}
        }
        drop(tokens);
        self.refresh().await
    }

    /// Refresh the tokens and persist them to the keyring.
    async fn refresh(&self) -> Result<String, CloudHomeError> {
        let mut tokens = self.tokens.write().await;

        // Another task may have refreshed while we waited for the write lock.
        if let Some(expires_at) = tokens.expires_at {
            if self.clock.now().timestamp() < expires_at - 60 {
                return Ok(tokens.access_token.clone());
            }
        }

        let refresh_token = tokens.refresh_token.as_deref().ok_or_else(|| {
            CloudHomeError::Storage(
                "no refresh token available, re-authorization needed".to_string(),
            )
        })?;

        let new_tokens = oauth::refresh(&self.config, refresh_token, self.clock.as_ref())
            .await
            .map_err(|e| CloudHomeError::Storage(format!("OAuth refresh failed: {e}")))?;

        let json = serde_json::to_string(&new_tokens)
            .map_err(|e| CloudHomeError::Storage(format!("serialize tokens: {e}")))?;
        if let Err(e) = self
            .key_service
            .set_cloud_home_credentials(&CloudHomeCredentials::OAuth { token_json: json })
        {
            warn!("Failed to persist refreshed OAuth tokens: {e}");
        }

        let access_token = new_tokens.access_token.clone();
        *tokens = new_tokens;

        info!("Refreshed {} OAuth tokens", self.provider_label);
        Ok(access_token)
    }

    /// Build and send a request with the current token, refreshing and retrying
    /// once on a 401.
    pub async fn api_call(
        &self,
        build_request: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let token = self.access_token().await?;
        let resp = build_request(&token)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("request failed: {e}")))?;

        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            let new_token = self.refresh().await?;
            build_request(&new_token)
                .send()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("retry request failed: {e}")))
        } else {
            Ok(resp)
        }
    }
}
