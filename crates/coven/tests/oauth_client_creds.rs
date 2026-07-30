#![cfg(feature = "oauth-providers")]

use std::collections::HashMap;

use coven::{CloudProvider, OAuthClientCreds, OAuthClients};

fn clients(client_id: &str) -> OAuthClients {
    OAuthClients::new(HashMap::from([(
        CloudProvider::GoogleDrive,
        OAuthClientCreds {
            client_id: client_id.to_string(),
            client_secret: None,
        },
    )]))
    .expect("Google Drive is an OAuth provider")
}

#[test]
fn separate_apps_retain_separate_oauth_client_credentials() {
    let first = clients("first-client");
    let second = clients("second-client");

    let first_request = first
        .build_authorize_request(
            CloudProvider::GoogleDrive,
            "http://127.0.0.1:19284/callback",
        )
        .expect("build the first app's authorization request");
    let second_request = second
        .build_authorize_request(
            CloudProvider::GoogleDrive,
            "http://127.0.0.1:19284/callback",
        )
        .expect("build the second app's authorization request");

    let client_id = |request: &coven::AuthorizeRequest| {
        url::Url::parse(&request.auth_url)
            .expect("authorization URL")
            .query_pairs()
            .find(|(name, _)| name == "client_id")
            .map(|(_, value)| value.into_owned())
            .expect("client_id query parameter")
    };
    assert_eq!(client_id(&first_request), "first-client");
    assert_eq!(client_id(&second_request), "second-client");
}

#[test]
fn oauth_clients_reject_non_oauth_providers() {
    let error = OAuthClients::new(HashMap::from([(
        CloudProvider::S3,
        OAuthClientCreds {
            client_id: "not-an-oauth-client".to_string(),
            client_secret: None,
        },
    )]))
    .expect_err("S3 must not enter the OAuth client set");

    assert!(error.to_string().contains("does not use OAuth"), "{error}");
}
