use super::*;
use crate::providers::tests::{json, TestServer};
use axum::{body::Bytes, extract::State, Router};
use coven_foundation::clock::FixedClock;
use std::{collections::BTreeMap, sync::Mutex, time::SystemTime};

struct Presenter {
    requests: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl OAuthPresenter for Presenter {
    fn redirect_uri(&self, _: CloudProvider) -> &str {
        "http://localhost:19284/callback"
    }
    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError> {
        self.requests.lock().unwrap().push(authorization_url.into());
        let url = url::Url::parse(authorization_url).unwrap();
        let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        Ok(SecretText::new(format!(
            "http://localhost:19284/callback?code=admin-code&state={}",
            params["state"]
        )))
    }
}

/// Each token request's form, and the scope the fake grants.
type Granted = Arc<Mutex<(Vec<BTreeMap<String, String>>, Option<String>)>>;

async fn token(State(state): State<Granted>, body: Bytes) -> axum::response::Response {
    let mut state = state.lock().unwrap();
    state
        .0
        .push(url::form_urlencoded::parse(&body).into_owned().collect());
    let mut reply = serde_json::json!({"access_token": "admin-token", "token_type": "Bearer"});
    if let Some(scope) = &state.1 {
        reply["scope"] = serde_json::json!(scope);
    }
    json(reply)
}

#[tokio::test]
async fn admin_sign_in_requests_cloud_platform_with_pkce_and_keeps_no_refresh_token() {
    for (granted, accepted) in [
        (None, true),
        (Some(CLOUD_PLATFORM.to_owned()), true),
        (
            Some("https://www.googleapis.com/auth/devstorage.read_only".to_owned()),
            false,
        ),
    ] {
        let state: Granted = Arc::new(Mutex::new((Vec::new(), granted)));
        let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
        let clients = OAuthClients::new(
            Some("google-client".into()),
            None,
            None,
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
        )
        .with_google_client_secret(SecretText::new("client-secret".into()))
        .unwrap()
        .with_token_endpoint(server.url.clone());
        let presenter = Arc::new(Presenter {
            requests: Mutex::new(Vec::new()),
        });
        let flow = OAuthFlow::new(clients, presenter.clone());
        let result = flow.google_cloud_admin().await;
        assert_eq!(result.is_ok(), accepted);
        if accepted {
            assert_eq!(result.unwrap().as_str(), "admin-token");
        } else {
            assert!(matches!(
                result,
                Err(OAuthError::Storage(error)) if error.failure() == StorageFailure::PermissionDenied
            ));
        }
        let shown = url::Url::parse(&presenter.requests.lock().unwrap()[0]).unwrap();
        let shown: BTreeMap<_, _> = shown.query_pairs().into_owned().collect();
        assert_eq!(shown["scope"], CLOUD_PLATFORM);
        assert_eq!(shown["access_type"], "online");
        assert_eq!(shown["code_challenge_method"], "S256");
        let sent = &state.lock().unwrap().0[0];
        assert_eq!(sent["code"], "admin-code");
        assert_eq!(sent["client_secret"], "client-secret");
        assert!(!sent["code_verifier"].is_empty());
    }
}
