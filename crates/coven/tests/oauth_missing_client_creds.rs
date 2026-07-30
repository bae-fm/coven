#![cfg(feature = "oauth-providers")]

use coven::{CloudProvider, OAuthClients};

#[test]
fn oauth_flow_without_provider_credentials_names_the_missing_provider() {
    let err = OAuthClients::empty()
        .build_authorize_request(
            CloudProvider::GoogleDrive,
            "http://127.0.0.1:19284/callback",
        )
        .expect_err("an OAuth flow without Google Drive credentials must fail");

    let message = err.to_string();
    assert!(
        message.contains("GoogleDrive"),
        "error names the missing provider: {message}"
    );
}
