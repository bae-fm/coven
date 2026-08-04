use super::*;
#[cfg(feature = "oauth-providers")]
use test_support::{oauth_config, serve_token_response};

#[cfg(feature = "oauth-providers")]
#[test]
fn pkce_verifier_is_url_safe() {
    let request = OAuthClients::for_tests()
        .build_authorize_request(
            crate::config::CloudProvider::GoogleDrive,
            "http://localhost/callback",
        )
        .expect("build authorize request");
    let verifier = request.verifier;
    assert!(verifier.len() >= 43);
    assert!(verifier
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
}

#[test]
fn pkce_challenge_is_deterministic() {
    let verifier = "test-verifier-string";
    let c1 = code_challenge(verifier);
    let c2 = code_challenge(verifier);
    assert_eq!(c1, c2);
}

#[test]
fn pkce_challenge_is_base64url() {
    let verifier = "test-verifier-string";
    let challenge = code_challenge(verifier);
    assert!(challenge
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
}

#[test]
fn oauth_tokens_serialization_roundtrip() {
    let tokens = OAuthTokens {
        access_token: "at_123".to_string(),
        refresh_token: Some("rt_456".to_string()),
        expires_at: Some(1700000000),
    };
    let json = serde_json::to_string(&tokens).unwrap();
    let parsed: OAuthTokens = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.access_token, "at_123");
    assert_eq!(parsed.refresh_token, Some("rt_456".to_string()));
    assert_eq!(parsed.expires_at, Some(1700000000));
}

#[test]
fn debug_redacts_access_and_refresh_tokens() {
    let tokens = OAuthTokens {
        access_token: "access-token-do-not-print".to_string(),
        refresh_token: Some("refresh-token-do-not-print".to_string()),
        expires_at: Some(1700000000),
    };
    let debug = format!("{tokens:?}");

    assert!(debug.contains("<redacted>"), "{debug}");
    // Non-secret expiry stays visible.
    assert!(debug.contains("1700000000"), "{debug}");
    assert!(
        !debug.contains("access-token-do-not-print"),
        "access token leaked: {debug}"
    );
    assert!(
        !debug.contains("refresh-token-do-not-print"),
        "refresh token leaked: {debug}"
    );
    // refresh_token presence is still observable.
    assert!(debug.contains("refresh_token: Some"), "{debug}");
}

fn parse_token_response(body: &str) -> TokenResponse {
    serde_json::from_str(body).expect("parse TokenResponse")
}

#[cfg(feature = "oauth-providers")]
fn authorize_url_params(auth_url: &str) -> HashMap<String, String> {
    let (_, query) = auth_url.split_once('?').expect("authorize URL has query");
    serde_urlencoded::from_str(query).expect("parse authorize query")
}

#[cfg(feature = "oauth-providers")]
#[test]
fn oauth_callback_html_renders_result_text() {
    let html = oauth_callback_html("Authorization denied", "Denied message");

    assert!(html.contains("<h1>Authorization denied</h1>"));
    assert!(html.contains("<p>Denied message</p>"));
    assert!(!html.contains("{{title}}"));
    assert!(!html.contains("{{message}}"));
}

#[cfg(feature = "oauth-providers")]
#[test]
fn build_authorize_request_includes_matching_random_state() {
    let clients = OAuthClients::for_tests();

    let first = clients
        .build_authorize_request(
            crate::config::CloudProvider::GoogleDrive,
            "http://localhost/callback",
        )
        .expect("build first authorize URL");
    let second = clients
        .build_authorize_request(
            crate::config::CloudProvider::GoogleDrive,
            "http://localhost/callback",
        )
        .expect("build second authorize URL");

    let first_params = authorize_url_params(&first.auth_url);
    let first_state = first_params
        .get("state")
        .expect("authorize URL includes state");
    assert!(
        first
            .verify_callback_state(Some(first_state.as_str()))
            .is_ok(),
        "AuthorizeRequest verifies the state it put in the URL",
    );
    assert!(
        !first_state.is_empty(),
        "state must be present for callback validation",
    );
    let second_params = authorize_url_params(&second.auth_url);
    let second_state = second_params
        .get("state")
        .expect("second authorize URL includes state");
    assert_ne!(
        first_state, second_state,
        "separate OAuth flows must carry separate state values",
    );
}

#[cfg(feature = "oauth-providers")]
#[test]
fn callback_rejects_missing_or_wrong_state() {
    let expected_state = "expected-state";
    let valid = HashMap::from([
        ("code".to_string(), "auth-code".to_string()),
        ("state".to_string(), expected_state.to_string()),
    ]);
    let wrong = HashMap::from([
        ("code".to_string(), "auth-code".to_string()),
        ("state".to_string(), "wrong-state".to_string()),
    ]);
    let missing = HashMap::from([("code".to_string(), "auth-code".to_string())]);

    assert_eq!(
        verify_callback_state(valid.get("state").map(String::as_str), expected_state)
            .and_then(|()| callback_code(&valid))
            .as_deref(),
        Ok("auth-code"),
    );
    assert!(
        verify_callback_state(wrong.get("state").map(String::as_str), expected_state).is_err(),
        "wrong state must reject the callback",
    );
    assert!(
        verify_callback_state(missing.get("state").map(String::as_str), expected_state).is_err(),
        "missing state must reject the callback",
    );
}

#[cfg(feature = "oauth-providers")]
#[tokio::test]
async fn exchange_code_rejects_wrong_state_before_token_request() {
    let clients = OAuthClients::for_tests();
    let request = clients
        .build_authorize_request(
            crate::config::CloudProvider::GoogleDrive,
            "http://localhost/callback",
        )
        .expect("build authorize request");

    let result = clients
        .exchange_code(
            crate::config::CloudProvider::GoogleDrive,
            "auth-code",
            Some("wrong-state"),
            &request,
            "http://localhost/callback",
            &crate::clock::SystemClock,
        )
        .await;

    assert!(
        matches!(result, Err(OAuthError::Denied(ref msg)) if msg.contains("state mismatch")),
        "expected state rejection before token request, got {result:?}",
    );
}

#[cfg(feature = "oauth-providers")]
#[tokio::test]
async fn exchange_code_posts_authorization_code_params() {
    let (token_url, request_body, server) = serve_token_response(
        r#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}"#,
    )
    .await;
    let config = oauth_config(token_url);
    let client = reqwest::Client::new();

    let tokens = exchange_code(
        &client,
        &config,
        "auth-code",
        "pkce-verifier",
        "http://localhost/callback",
        &crate::clock::SystemClock,
    )
    .await
    .expect("exchange code");

    let body = request_body.await.expect("request body");
    server.await.expect("token server");
    assert_eq!(
        body.get("grant_type").map(String::as_str),
        Some("authorization_code")
    );
    assert_eq!(body.get("code").map(String::as_str), Some("auth-code"));
    assert_eq!(
        body.get("redirect_uri").map(String::as_str),
        Some("http://localhost/callback")
    );
    assert_eq!(body.get("client_id").map(String::as_str), Some("client-id"));
    assert_eq!(
        body.get("code_verifier").map(String::as_str),
        Some("pkce-verifier")
    );
    assert_eq!(
        body.get("client_secret").map(String::as_str),
        Some("client-secret")
    );
    assert_eq!(tokens.access_token, "new-access");
    assert_eq!(tokens.refresh_token.as_deref(), Some("new-refresh"));
}

#[cfg(feature = "oauth-providers")]
#[tokio::test]
async fn refresh_posts_refresh_token_params_and_reuses_existing_refresh_token() {
    let (token_url, request_body, server) =
        serve_token_response(r#"{"access_token":"refreshed-access","expires_in":3600}"#).await;
    let config = oauth_config(token_url);
    let client = reqwest::Client::new();

    let tokens = refresh(
        &client,
        &config,
        "existing-refresh",
        &crate::clock::SystemClock,
    )
    .await
    .expect("refresh");

    let body = request_body.await.expect("request body");
    server.await.expect("token server");
    assert_eq!(
        body.get("grant_type").map(String::as_str),
        Some("refresh_token")
    );
    assert_eq!(
        body.get("refresh_token").map(String::as_str),
        Some("existing-refresh")
    );
    assert_eq!(body.get("client_id").map(String::as_str), Some("client-id"));
    assert_eq!(
        body.get("client_secret").map(String::as_str),
        Some("client-secret")
    );
    assert_eq!(tokens.access_token, "refreshed-access");
    assert_eq!(tokens.refresh_token.as_deref(), Some("existing-refresh"));
}

#[test]
fn into_tokens_classifies_invalid_grant_as_reauthorize() {
    let body =
        r#"{"error":"invalid_grant","error_description":"Token has been expired or revoked"}"#;
    let result = parse_token_response(body)
        .into_tokens(reqwest::StatusCode::BAD_REQUEST, &crate::clock::SystemClock);
    match result {
        Err(OAuthError::Reauthorize(detail)) => {
            assert!(
                detail.contains("expired or revoked"),
                "expected error_description in detail, got: {detail}",
            );
        }
        other => panic!("expected Reauthorize, got {other:?}"),
    }
}

#[test]
fn into_tokens_classifies_unauthorized_client_as_reauthorize() {
    let body = r#"{"error":"unauthorized_client","error_description":"Client has been revoked"}"#;
    let result = parse_token_response(body)
        .into_tokens(reqwest::StatusCode::BAD_REQUEST, &crate::clock::SystemClock);
    assert!(
        matches!(result, Err(OAuthError::Reauthorize(_))),
        "expected Reauthorize, got {result:?}",
    );
}

#[test]
fn into_tokens_leaves_other_provider_errors_as_token_exchange() {
    let body = r#"{"error":"invalid_request","error_description":"missing parameter"}"#;
    let result = parse_token_response(body)
        .into_tokens(reqwest::StatusCode::BAD_REQUEST, &crate::clock::SystemClock);
    match result {
        Err(OAuthError::TokenExchange(msg)) => {
            assert!(
                msg.contains("missing parameter"),
                "expected description in message, got: {msg}",
            );
        }
        other => panic!("expected TokenExchange, got {other:?}"),
    }
}

#[test]
fn into_tokens_returns_tokens_on_success() {
    let body = r#"{"access_token":"new_at","refresh_token":"new_rt","expires_in":3600}"#;
    let tokens = parse_token_response(body)
        .into_tokens(reqwest::StatusCode::OK, &crate::clock::SystemClock)
        .expect("into_tokens");
    assert_eq!(tokens.access_token, "new_at");
    assert_eq!(tokens.refresh_token.as_deref(), Some("new_rt"));
    assert!(tokens.expires_at.is_some());
}

#[test]
fn into_tokens_missing_access_token_error_does_not_include_tokens() {
    let body = r#"{"refresh_token":"refresh-token-that-must-not-be-logged"}"#;
    let result =
        parse_token_response(body).into_tokens(reqwest::StatusCode::OK, &crate::clock::SystemClock);
    match result {
        Err(OAuthError::TokenExchange(msg)) => {
            assert!(
                msg.contains("missing access_token"),
                "expected access_token error, got: {msg}",
            );
            assert!(
                msg.contains("HTTP 200"),
                "expected status in error, got: {msg}",
            );
            assert!(
                !msg.contains("refresh-token-that-must-not-be-logged"),
                "error included refresh token: {msg}",
            );
        }
        other => panic!("expected TokenExchange, got {other:?}"),
    }
}

#[cfg(feature = "oauth-providers")]
#[tokio::test]
async fn parse_failure_error_does_not_include_tokens() {
    let (token_url, _request_body, server) =
        serve_token_response(r#"{"access_token":"access-token-that-must-not-be-logged""#).await;
    let config = oauth_config(token_url);
    let client = reqwest::Client::new();

    let result = exchange_code(
        &client,
        &config,
        "auth-code",
        "pkce-verifier",
        "http://localhost/callback",
        &crate::clock::SystemClock,
    )
    .await;

    server.await.expect("token server");
    match result {
        Err(OAuthError::TokenExchange(msg)) => {
            assert!(
                msg.contains("parse token response"),
                "expected parse error, got: {msg}",
            );
            assert!(
                msg.contains("HTTP 200"),
                "expected status in error, got: {msg}",
            );
            assert!(
                !msg.contains("access-token-that-must-not-be-logged"),
                "error included access token: {msg}",
            );
        }
        other => panic!("expected TokenExchange, got {other:?}"),
    }
}
