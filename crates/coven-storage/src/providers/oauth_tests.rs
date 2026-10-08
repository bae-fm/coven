use super::*;
use crate::providers::tests::{json, response, TestServer};
use axum::{body::Bytes, extract::State, Router};
use coven_foundation::clock::FixedClock;
use std::{collections::BTreeMap, sync::Mutex, time::SystemTime};

#[derive(Default)]
struct Provider {
    requests: Vec<BTreeMap<String, String>>,
    status: Option<u16>,
}
async fn token(State(state): State<Arc<Mutex<Provider>>>, body: Bytes) -> axum::response::Response {
    let mut state = state.lock().unwrap();
    state
        .requests
        .push(url::form_urlencoded::parse(&body).into_owned().collect());
    if let Some(status) = state.status {
        return response(
            status,
            match status {
                400 => r#"{"error":"invalid_grant"}"#,
                503 => "service unavailable",
                _ => panic!("unsupported fake status"),
            },
        );
    }
    json(serde_json::json!({"access_token":"new-token","token_type":"Bearer","expires_in":3600}))
}
struct Presenter {
    reply: &'static str,
    requests: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl OAuthPresenter for Presenter {
    fn redirect_uri(&self, _: CloudProvider) -> &str {
        "https://CALLBACK.example"
    }
    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError> {
        self.requests.lock().unwrap().push(authorization_url.into());
        match self.reply {
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
fn presenter(reply: &'static str) -> Arc<Presenter> {
    Arc::new(Presenter {
        reply,
        requests: Mutex::new(Vec::new()),
    })
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
                    | ("denied", OAuthError::Denied)
                    | ("code", OAuthError::MissingCode)
                    | ("cancel", OAuthError::Cancelled)
            ),
            "{reply}: {error:?}"
        );
    }
    assert!(state.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn refresh_retains_unrotated_token_and_classifies_provider_failures() {
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
    for (status, failure) in [
        (400, crate::StorageFailure::Authentication),
        (503, crate::StorageFailure::Network),
    ] {
        state.lock().unwrap().status = Some(status);
        assert!(
            matches!(flow.refresh(CloudProvider::Dropbox, &previous).await,
            Err(OAuthError::Storage(error)) if error.failure() == failure)
        );
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
