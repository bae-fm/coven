//! An OAuth flow started before the host registered client credentials must
//! name the missing `set_oauth_client_creds` startup call, not fail later with
//! an opaque provider error (or silently proceed on empty credentials). Its own
//! test binary so the process-global registry is genuinely unset.
#![cfg(feature = "oauth-providers")]

use coven::config::CloudProvider;

#[test]
fn oauth_flow_without_registered_creds_names_the_setup_step() {
    let err = coven::build_authorize_request_for_provider(
        CloudProvider::GoogleDrive,
        "http://127.0.0.1:19284/callback",
    )
    .expect_err("an OAuth flow with no registered client credentials must fail");

    let message = err.to_string();
    assert!(
        message.contains("set_oauth_client_creds"),
        "error names the missing startup call: {message}"
    );
}
