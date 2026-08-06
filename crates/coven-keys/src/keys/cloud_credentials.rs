//! Cloud-home OAuth credentials: the bearer tokens the key service holds in
//! custody for a provider session.

use serde::{Deserialize, Serialize};

/// Tokens returned from an OAuth authorization or refresh.
///
/// `Debug` is hand-written: `access_token` and `refresh_token` are bearer
/// credentials and print as `<redacted>` so `{:?}` in an error path cannot
/// leak them.
#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    /// Unix timestamp when the access token expires. None if unknown.
    pub expires_at: Option<i64>,
}

impl std::fmt::Debug for OAuthTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthTokens")
            .field("access_token", &"<redacted>")
            // Presence (whether the session can refresh) is observable; the
            // token itself is redacted.
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "<redacted>"),
            )
            .field("expires_at", &self.expires_at)
            .finish()
    }
}
