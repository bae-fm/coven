use super::*;
use crate::providers::tests::{json, response, TestServer};
use axum::{body::Bytes, extract::State, Router};
use coven_crypto::SecretText;
use coven_foundation::clock::FixedClock;
use std::{sync::Mutex, time::SystemTime};
#[derive(Default)]
struct Provider {
    requests: Vec<std::collections::BTreeMap<String, String>>,
    deny: bool,
    unavailable: bool,
}
async fn token(State(state): State<Arc<Mutex<Provider>>>, body: Bytes) -> axum::response::Response {
    let mut state = state.lock().unwrap();
    state
        .requests
        .push(url::form_urlencoded::parse(&body).into_owned().collect());
    if state.unavailable {
        return response(503, "service unavailable");
    }
    if state.deny {
        return response(
            400,
            serde_json::json!({"error":"invalid_grant"}).to_string(),
        );
    }
    json(serde_json::json!({"access_token":"new-token","token_type":"Bearer","expires_in":3600}))
}

#[tokio::test]
async fn token_endpoint_unavailable_retains_network_classification() {
    let state = Arc::new(Mutex::new(Provider {
        unavailable: true,
        ..Provider::default()
    }));
    let server = TestServer::new(Router::new().fallback(token).with_state(state)).await;
    let mut clients = OAuthClients::new(
        None,
        Some("dropbox".into()),
        None,
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
    );
    clients.token_override = Some(server.url.clone());
    let previous = OAuthTokens {
        access_token: SecretText::new("old".into()),
        refresh_token: Some(SecretText::new("refresh".into())),
        expires_at: None,
    };
    let error = clients
        .refresh(CloudProvider::Dropbox, &previous)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error, OAuthError::Storage(ref error) if error.failure() == crate::StorageFailure::Network)
    );
}
#[tokio::test]
async fn exchanges_bind_state_provider_redirect_and_injected_clock() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    ));
    let mut clients = OAuthClients::new(
        Some("google".into()),
        Some("dropbox".into()),
        Some("onedrive".into()),
        clock.clone(),
    );
    clients.token_override = Some(server.url.clone());
    for provider in [
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        let request = clients
            .build_authorize_request(provider, "coven://callback")
            .unwrap();
        let url = url::Url::parse(&request.auth_url).unwrap();
        let params: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(params["code_challenge_method"], "S256");
        assert!(!request.auth_url.contains(request.verifier.as_str()));
        assert!(matches!(
            clients
                .exchange_code(provider, "code", None, &request, "coven://callback")
                .await,
            Err(OAuthError::StateMismatch)
        ));
        assert!(matches!(
            clients
                .exchange_code(
                    provider,
                    "code",
                    Some(request.state.as_str()),
                    &request,
                    "other://callback"
                )
                .await,
            Err(OAuthError::RequestMismatch)
        ));
        let tokens = clients
            .exchange_code(
                provider,
                "code",
                Some(request.state.as_str()),
                &request,
                "coven://callback",
            )
            .await
            .unwrap();
        assert_eq!(
            tokens.expires_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(3700))
        );
        assert_eq!(
            state.lock().unwrap().requests.last().unwrap()["code_verifier"],
            request.verifier.as_str()
        );
    }
    assert_eq!(state.lock().unwrap().requests.len(), 3);
    let request = clients
        .build_authorize_request(CloudProvider::GoogleDrive, "coven://callback")
        .unwrap();
    assert!(matches!(
        clients
            .exchange_code(
                CloudProvider::Dropbox,
                "code",
                Some(request.state.as_str()),
                &request,
                "coven://callback"
            )
            .await,
        Err(OAuthError::RequestMismatch)
    ));
}
#[tokio::test]
async fn refresh_retains_unrotated_refresh_token_and_reports_revocation() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let mut clients = OAuthClients::new(
        None,
        Some("dropbox".into()),
        None,
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
    );
    clients.token_override = Some(server.url.clone());
    let previous = OAuthTokens {
        access_token: SecretText::new("old".into()),
        refresh_token: Some(SecretText::new("refresh".into())),
        expires_at: None,
    };
    let refreshed = clients
        .refresh(CloudProvider::Dropbox, &previous)
        .await
        .unwrap();
    assert_eq!(refreshed.refresh_token.unwrap().as_str(), "refresh");
    state.lock().unwrap().deny = true;
    assert!(matches!(
        clients.refresh(CloudProvider::Dropbox, &previous).await,
        Err(OAuthError::Storage(error)) if error.failure() == crate::StorageFailure::Authentication
    ));
    let (_cancel, rx) = watch::channel(true);
    assert!(matches!(
        clients.authorize(CloudProvider::Dropbox, rx).await,
        Err(OAuthError::Cancelled)
    ));
}

#[tokio::test]
async fn local_redirect_exchanges_code_and_cancellation_closes_listener() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let mut clients = OAuthClients::new(
        None,
        Some("dropbox".into()),
        None,
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
    );
    clients.token_override = Some(server.url.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let redirect = format!("http://{address}/callback");
    let request = clients
        .build_authorize_request(CloudProvider::Dropbox, &redirect)
        .unwrap();
    let callback = format!(
        "{redirect}?code=callback-code&state={}",
        request.state.as_str()
    );
    let (_cancel, rx) = watch::channel(false);
    let (tokens, response) = tokio::join!(
        clients.receive_redirect(listener, CloudProvider::Dropbox, rx, request, &redirect),
        http::client().unwrap().get(callback).send(),
    );
    assert_eq!(tokens.unwrap().access_token.as_str(), "new-token");
    assert!(response.unwrap().status().is_success());
    assert_eq!(state.lock().unwrap().requests[0]["code"], "callback-code");
    assert!(tokio::net::TcpStream::connect(address).await.is_err());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let request = clients
        .build_authorize_request(CloudProvider::Dropbox, &redirect)
        .unwrap();
    let (cancel, rx) = watch::channel(false);
    let (result, ()) = tokio::join!(
        clients.receive_redirect(listener, CloudProvider::Dropbox, rx, request, &redirect),
        async {
            cancel.send(true).unwrap();
        },
    );
    assert!(matches!(result, Err(OAuthError::Cancelled)));
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[test]
fn onedrive_sign_in_accepts_personal_and_organization_accounts() {
    let clients = OAuthClients::new(
        None,
        None,
        Some("app".into()),
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
    );
    let request = clients
        .build_authorize_request(CloudProvider::OneDrive, "coven://callback")
        .unwrap();
    let url = url::Url::parse(&request.auth_url).unwrap();
    assert_eq!(url.path(), "/common/oauth2/v2.0/authorize");
    assert_eq!(
        clients.config(CloudProvider::OneDrive).unwrap().2,
        "https://login.microsoftonline.com/common/oauth2/v2.0/token"
    );
}
