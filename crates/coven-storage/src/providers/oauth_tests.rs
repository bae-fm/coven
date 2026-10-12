use super::*;
use crate::providers::tests::{json, response, TestServer};
use axum::{body::Bytes, extract::State, Router};
use coven_foundation::clock::FixedClock;
use std::{collections::BTreeMap, sync::Mutex, time::SystemTime};

#[derive(Default)]
struct Provider {
    requests: Vec<BTreeMap<String, String>>,
    error_response: Option<(u16, String)>,
}
async fn token(State(state): State<Arc<Mutex<Provider>>>, body: Bytes) -> axum::response::Response {
    let mut state = state.lock().unwrap();
    state
        .requests
        .push(url::form_urlencoded::parse(&body).into_owned().collect());
    if let Some((status, body)) = &state.error_response {
        let mut reply = response(*status, body.clone());
        reply
            .headers_mut()
            .insert("retry-after", "37".parse().unwrap());
        reply
            .headers_mut()
            .insert("x-request-id", "secret-request-id".parse().unwrap());
        return reply;
    }
    json(serde_json::json!({"access_token":"new-token","token_type":"Bearer","expires_in":3600}))
}
struct Presenter {
    reply: String,
    requests: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl OAuthPresenter for Presenter {
    fn redirect_uri(&self, _: CloudProvider) -> &str {
        "https://CALLBACK.example"
    }
    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError> {
        self.requests.lock().unwrap().push(authorization_url.into());
        match self.reply.as_str() {
            "cancel" => return Err(OAuthError::Cancelled),
            "wait" => return std::future::pending().await,
            _ => {}
        }
        let url = url::Url::parse(authorization_url).unwrap();
        let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        Ok(SecretText::new(
            self.reply.replace("STATE", &params["state"]),
        ))
    }
}
fn clients(clock: Arc<FixedClock>) -> OAuthClients {
    OAuthClients::new(
        Some("google".into()),
        Some("dropbox".into()),
        Some("onedrive".into()),
        clock,
    )
}
fn presenter(reply: impl Into<String>) -> Arc<Presenter> {
    Arc::new(Presenter {
        reply: reply.into(),
        requests: Mutex::new(Vec::new()),
    })
}

#[tokio::test]
async fn redirect_provider_errors_keep_their_classification_and_original_callback() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    for provider in [
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        for (code, expected) in [
            ("invalid_client", StorageFailure::InvalidConfiguration),
            ("unauthorized_client", StorageFailure::InvalidConfiguration),
            ("invalid_scope", StorageFailure::InvalidConfiguration),
            ("invalid_resource", StorageFailure::InvalidConfiguration),
            ("invalid_request", StorageFailure::InvalidConfiguration),
            (
                "unsupported_response_type",
                StorageFailure::InvalidConfiguration,
            ),
            ("interaction_required", StorageFailure::Authentication),
            ("consent_required", StorageFailure::Authentication),
            ("server_error", StorageFailure::Network),
            ("temporarily_unavailable", StorageFailure::Network),
            ("unknown_provider_error", StorageFailure::Refused),
            ("access_denied", StorageFailure::PermissionDenied),
        ] {
            let presenter = presenter(format!("https://CALLBACK.example?error={code}&state=STATE&error_description=secret%20description&error_uri=https%3A%2F%2Fprovider.example%2Fsecret-help&extra=secret-extension"));
            let flow = OAuthFlow::new(
                clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
                    .with_token_endpoint(server.url.clone()),
                presenter.clone(),
            );
            let error = flow.authenticate(provider).await.err().unwrap();
            assert!(!format!("{error} {error:?}").contains("secret"));
            let original = if code == "access_denied" {
                let OAuthError::Denied(original) = error else {
                    panic!("denial lost: {error:?}")
                };
                original
            } else {
                let OAuthError::Storage(error) = error else {
                    panic!("{provider:?}: {code}: {error:?}")
                };
                assert_eq!(error.failure(), expected, "{provider:?}: {code}");
                assert_eq!(error.retryable(), expected == StorageFailure::Network);
                let StorageError::Provider {
                    provider: actual_provider,
                    source,
                    ..
                } = error
                else {
                    panic!("provider callback discarded")
                };
                assert_eq!(actual_provider, provider);
                *source
                    .downcast::<crate::providers::OAuthRedirectError>()
                    .unwrap()
            };
            let requests = presenter.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            let url = url::Url::parse(&requests[0]).unwrap();
            let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(
                original.callback(),
                presenter.reply.replace("STATE", &params["state"])
            );
            assert!(!format!("{original} {original:?}").contains(&params["state"]));
        }
    }
    assert!(state.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn presenter_flow_binds_state_redirect_and_pkce_for_each_provider() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    ));
    let presenter = presenter("https://callback.example?code=code&state=STATE");
    let clients = clients(clock).with_token_endpoint(server.url.clone());
    let flow = OAuthFlow::new(clients, presenter.clone());
    for provider in [
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        let tokens = flow.authenticate(provider).await.unwrap();
        assert_eq!(
            tokens.expires_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(3700))
        );
        let presented = presenter.requests.lock().unwrap().last().unwrap().clone();
        let url = url::Url::parse(&presented).unwrap();
        let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(params["code_challenge_method"], "S256");
        let requests = state.lock().unwrap();
        let exchange = requests.requests.last().unwrap();
        assert_eq!(exchange["code"], "code");
        assert_eq!(params["redirect_uri"], "https://CALLBACK.example");
        assert_eq!(exchange["redirect_uri"], params["redirect_uri"]);
        assert_eq!(exchange["client_id"], params["client_id"]);
        assert!(!presented.contains(&exchange["code_verifier"]));
        let challenge = PkceCodeChallenge::from_code_verifier_sha256(
            &oauth2::PkceCodeVerifier::new(exchange["code_verifier"].clone()),
        );
        assert_eq!(challenge.as_str(), params["code_challenge"]);
        if provider == CloudProvider::OneDrive {
            assert_eq!(url.path(), "/common/oauth2/v2.0/authorize");
            assert_eq!(
                flow.clients.config(provider).unwrap().2,
                "https://login.microsoftonline.com/common/oauth2/v2.0/token"
            );
        }
    }
    assert_eq!(state.lock().unwrap().requests.len(), 3);
}

#[tokio::test]
async fn invalid_or_denied_redirects_never_reach_the_token_endpoint() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    for (reply, expected) in [
        ("https://callback.example?code=code", "state"),
        ("https://callback.example?code=code&state=wrong", "state"),
        ("other://callback.example?code=code&state=STATE", "redirect"),
        (
            "https://elsewhere.example?code=code&state=STATE",
            "redirect",
        ),
        (
            "https://callback.example/other?code=code&state=STATE",
            "redirect",
        ),
        (
            "https://callback.example?code=code&state=STATE#fragment",
            "redirect",
        ),
        (
            "https://callback.example?code=code&state=STATE&state=STATE",
            "redirect",
        ),
        (
            "https://callback.example?code=one&code=two&state=STATE",
            "redirect",
        ),
        (
            "https://callback.example?error=access_denied&state=STATE",
            "denied",
        ),
        (
            "https://callback.example?error=invalid_client&state=wrong",
            "state",
        ),
        ("https://callback.example?error=server_error", "state"),
        (
            "https://elsewhere.example?error=server_error&state=STATE",
            "redirect",
        ),
        (
            "https://callback.example?error=server_error&error=access_denied&state=STATE",
            "redirect",
        ),
        ("https://callback.example?error=&state=STATE", "redirect"),
        (
            "https://callback.example?error=server_error&code=code&state=STATE",
            "redirect",
        ),
        ("https://callback.example?state=STATE", "code"),
        ("https://callback.example?code=&state=STATE", "code"),
        ("cancel", "cancel"),
    ] {
        let clients = clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
            .with_token_endpoint(server.url.clone());
        let flow = OAuthFlow::new(clients, presenter(reply));
        let error = flow
            .authenticate(CloudProvider::Dropbox)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(
                (expected, &error),
                ("state", OAuthError::StateMismatch)
                    | ("redirect", OAuthError::InvalidRedirect)
                    | ("denied", OAuthError::Denied(_))
                    | ("code", OAuthError::MissingCode)
                    | ("cancel", OAuthError::Cancelled)
            ),
            "{reply}: {error:?}"
        );
    }
    assert!(state.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn refresh_retains_unrotated_token() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let flow = OAuthFlow::new(
        clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
            .with_token_endpoint(server.url.clone()),
        presenter("cancel"),
    );
    let previous = OAuthTokens {
        access_token: SecretText::new("old".into()),
        refresh_token: Some(SecretText::new("refresh".into())),
        expires_at: None,
    };
    let refreshed = flow
        .refresh(CloudProvider::Dropbox, &previous)
        .await
        .unwrap();
    assert_eq!(refreshed.refresh_token.unwrap().as_str(), "refresh");
}

#[tokio::test]
async fn google_client_secret_reaches_only_google_token_exchanges() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let presenter = presenter("https://callback.example?code=code&state=STATE");
    let flow = OAuthFlow::new(
        clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
            .with_google_client_secret(SecretText::new("desktop-secret".into()))
            .unwrap()
            .with_token_endpoint(server.url.clone()),
        presenter.clone(),
    );
    let refresh_token = SecretText::new("refresh".into());
    for provider in [
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        flow.authenticate(provider).await.unwrap();
        let saved = OAuthTokens {
            access_token: SecretText::new(String::new()),
            refresh_token: Some(refresh_token.clone()),
            expires_at: None,
        };
        let refreshed = flow.refresh(provider, &saved).await.unwrap();
        assert_eq!(refreshed.refresh_token.unwrap().as_str(), "refresh");
        let requests = std::mem::take(&mut state.lock().unwrap().requests);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["grant_type"], "authorization_code");
        assert_eq!(requests[1]["grant_type"], "refresh_token");
        for request in requests {
            assert_eq!(
                request.get("client_secret").map(String::as_str),
                (provider == CloudProvider::GoogleDrive).then_some("desktop-secret")
            );
        }
    }
    assert!(presenter
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|url| !url.contains("desktop-secret")));
}

#[test]
fn google_client_secret_requires_a_configured_client_and_nonempty_secret() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    assert!(matches!(
        OAuthClients::new(None, None, None, clock.clone())
            .with_google_client_secret(SecretText::new("desktop-secret".into())),
        Err(OAuthError::Unavailable(CloudProvider::GoogleDrive))
    ));
    assert!(matches!(
        clients(clock).with_google_client_secret(SecretText::new(String::new())),
        Err(OAuthError::Storage(error)) if error.failure() == StorageFailure::InvalidConfiguration
    ));
}

#[tokio::test]
async fn token_errors_retain_the_original_response_during_sign_in_and_refresh() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let flow = OAuthFlow::new(
        clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
            .with_token_endpoint(server.url.clone()),
        presenter("https://callback.example?code=code&state=STATE"),
    );
    let previous = OAuthTokens {
        access_token: SecretText::new("old".into()),
        refresh_token: Some(SecretText::new("refresh".into())),
        expires_at: None,
    };
    for provider in [
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        for (status, code, expected) in [
            (400, "invalid_grant", StorageFailure::Authentication),
            (400, "invalid_client", StorageFailure::InvalidConfiguration),
            (401, "invalid_client", StorageFailure::InvalidConfiguration),
            (
                400,
                "unauthorized_client",
                StorageFailure::InvalidConfiguration,
            ),
            (400, "deleted_client", StorageFailure::InvalidConfiguration),
            (400, "invalid_scope", StorageFailure::InvalidConfiguration),
            (
                400,
                "invalid_resource",
                StorageFailure::InvalidConfiguration,
            ),
            (400, "interaction_required", StorageFailure::Authentication),
            (400, "consent_required", StorageFailure::Authentication),
            (400, "temporarily_unavailable", StorageFailure::Network),
            (400, "server_error", StorageFailure::Network),
            (429, "unrecognized_error", StorageFailure::RateLimited),
            (503, "unrecognized_error", StorageFailure::Network),
            (400, "unrecognized_error", StorageFailure::Refused),
        ] {
            let body = format!(
                r#"{{ "error": "{code}", "error_description": "secret-description", "extra": "secret-extension" }}"#
            );
            state.lock().unwrap().error_response = Some((status, body.clone()));
            for result in [
                flow.authenticate(provider).await,
                flow.refresh(provider, &previous).await,
            ] {
                let error = result.err().unwrap();
                assert!(!format!("{error} {error:?}").contains("secret"));
                let OAuthError::Storage(error) = error else {
                    panic!("{provider:?}: {code}: {error:?}")
                };
                assert_eq!(error.failure(), expected, "{provider:?}: {code}");
                assert_eq!(
                    error.retryable(),
                    matches!(
                        expected,
                        StorageFailure::Network | StorageFailure::RateLimited
                    )
                );
                let StorageError::Provider {
                    provider: actual_provider,
                    source,
                    ..
                } = error
                else {
                    panic!("provider response discarded")
                };
                assert_eq!(actual_provider, provider);
                let original = source.downcast_ref::<http::ProviderResponse>().unwrap();
                assert_eq!(original.status(), status);
                assert_eq!(original.headers()["retry-after"], "37");
                assert_eq!(original.headers()["x-request-id"], "secret-request-id");
                assert_eq!(original.body(), body.as_bytes());
            }
            let requests = std::mem::take(&mut state.lock().unwrap().requests);
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0]["grant_type"], "authorization_code");
            assert_eq!(requests[1]["grant_type"], "refresh_token");
        }
    }
}

#[tokio::test]
async fn presentation_timeout_uses_the_injected_clock() {
    use std::{future::Future, task::Poll};
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let flow = OAuthFlow::new(clients(clock.clone()), presenter("wait"));
    let mut pending = Box::pin(flow.authenticate(CloudProvider::Dropbox));
    for seconds in [0, 299] {
        clock.set(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    clock.set(SystemTime::UNIX_EPOCH + Duration::from_secs(300));
    std::future::poll_fn(|cx| {
        assert!(matches!(
            pending.as_mut().poll(cx),
            Poll::Ready(Err(OAuthError::Timeout))
        ));
        Poll::Ready(())
    })
    .await;
}
