use super::*;
use crate::providers::tests::full_access;
use crate::providers::tests::{json, response, TestServer};
use axum::{body::Bytes, extract::State, Router};
use coven_foundation::clock::FixedClock;
use std::{collections::BTreeMap, sync::Mutex, time::SystemTime};

#[derive(Default)]
struct Provider {
    requests: Vec<BTreeMap<String, String>>,
    error_response: Option<(u16, String)>,
    scope: Option<String>,
}
async fn token(State(state): State<Arc<Mutex<Provider>>>, body: Bytes) -> axum::response::Response {
    let mut state = state.lock().unwrap();
    let request: BTreeMap<String, String> =
        url::form_urlencoded::parse(&body).into_owned().collect();
    // Both providers issue a refresh token for the offline access every
    // sign-in requests; this fake never rotates it on refresh.
    let issues_refresh = request["grant_type"] == "authorization_code";
    state.requests.push(request);
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
    let mut reply =
        serde_json::json!({"access_token":"new-token","token_type":"Bearer","expires_in":3600});
    if let Some(scope) = &state.scope {
        reply["scope"] = serde_json::json!(scope);
    }
    if issues_refresh {
        reply["refresh_token"] = serde_json::json!("issued");
    }
    json(reply)
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
        Some(DropboxClients::FullDropbox {
            app_key: "dropbox".into(),
        }),
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
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
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
            let error = flow
                .authenticate(full_access(provider))
                .await
                .err()
                .unwrap();
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
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
        flow.authenticate(full_access(provider)).await.unwrap();
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
        assert_eq!(
            params.get("token_access_type").map(String::as_str),
            (provider == CloudProvider::Dropbox).then_some("offline")
        );
        if provider == CloudProvider::OneDrive {
            assert_eq!(url.path(), "/common/oauth2/v2.0/authorize");
            assert_eq!(
                flow.clients.config(full_access(provider)).unwrap().2,
                "https://login.microsoftonline.com/common/oauth2/v2.0/token"
            );
        }
    }
    assert_eq!(state.lock().unwrap().requests.len(), 2);
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
            .authenticate(crate::OAuthAccess::Dropbox(
                crate::DropboxAccess::FullDropbox,
            ))
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
        client_id: "dropbox".into(),
        access: full_access(CloudProvider::Dropbox),
        access_token: SecretText::new("old".into()),
        refresh_token: Some(SecretText::new("refresh".into())),
    };
    let refreshed = flow.clients.refresh(&previous).await.unwrap();
    assert_eq!(refreshed.refresh_token.unwrap().as_str(), "refresh");
}

#[tokio::test]
async fn google_client_secret_never_reaches_storage_sign_in() {
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
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
        flow.authenticate(full_access(provider)).await.unwrap();
        let refreshed = flow
            .clients
            .refresh(&OAuthTokens {
                client_id: flow.clients.config(full_access(provider)).unwrap().0.into(),
                access: full_access(provider),
                access_token: SecretText::new("old".into()),
                refresh_token: Some(refresh_token.clone()),
            })
            .await
            .unwrap();
        assert_eq!(refreshed.refresh_token.unwrap().as_str(), "refresh");
        let requests = std::mem::take(&mut state.lock().unwrap().requests);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["grant_type"], "authorization_code");
        assert_eq!(requests[1]["grant_type"], "refresh_token");
        for request in requests {
            assert_eq!(request.get("client_secret"), None);
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
        Err(OAuthError::Unavailable(CloudProvider::S3))
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

    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
        let previous = OAuthTokens {
            client_id: flow.clients.config(full_access(provider)).unwrap().0.into(),
            access: full_access(provider),
            access_token: SecretText::new("old".into()),
            refresh_token: Some(SecretText::new("refresh".into())),
        };
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
            let signed_in = match flow.authenticate(full_access(provider)).await {
                Err(OAuthError::Storage(error)) => error,
                result => panic!("{provider:?}: {code}: {:?}", result.err()),
            };
            let refreshed = flow.clients.refresh(&previous).await.err().unwrap();
            for error in [signed_in, refreshed] {
                assert!(!format!("{error} {error:?}").contains("secret"));
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
    let mut pending = Box::pin(flow.authenticate(crate::OAuthAccess::Dropbox(
        crate::DropboxAccess::FullDropbox,
    )));
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

mod access_tests {
    use super::*;

    #[tokio::test]
    async fn dropbox_choice_selects_its_app_key_scopes_and_refresh_client() {
        let state = Arc::new(Mutex::new(Provider::default()));
        let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
        let presenter = presenter("https://callback.example?code=code&state=STATE");
        let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
        let flow = OAuthFlow::new(
            OAuthClients::new(
                None,
                Some(DropboxClients::Both {
                    app_folder_key: "folder-key".into(),
                    full_dropbox_key: "full-key".into(),
                }),
                None,
                clock.clone(),
            )
            .with_token_endpoint(server.url.clone()),
            presenter.clone(),
        );
        for (access, key) in [
            (DropboxAccess::AppFolder, "folder-key"),
            (DropboxAccess::FullDropbox, "full-key"),
        ] {
            let access = OAuthAccess::Dropbox(access);
            state.lock().unwrap().scope = Some(access.scope().into());
            let mut tokens = flow.authenticate(access).await.unwrap();
            assert_eq!(tokens.access, access);
            assert_eq!(tokens.client_id, key);
            tokens.refresh_token = Some(SecretText::new("refresh".into()));
            assert_eq!(flow.clients.refresh(&tokens).await.unwrap().access, access);
            let requests = std::mem::take(&mut state.lock().unwrap().requests);
            assert_eq!(requests.len(), 2);
            assert!(requests.iter().all(|request| request["client_id"] == key));
            let url = url::Url::parse(presenter.requests.lock().unwrap().last().unwrap()).unwrap();
            let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(params["scope"], access.scope());
            assert_eq!(params["client_id"], key);
        }
        for (registered, absent) in [
            (
                DropboxClients::AppFolder {
                    app_key: "folder-key".into(),
                },
                DropboxAccess::FullDropbox,
            ),
            (
                DropboxClients::FullDropbox {
                    app_key: "full-key".into(),
                },
                DropboxAccess::AppFolder,
            ),
        ] {
            let flow = OAuthFlow::new(
                OAuthClients::new(None, Some(registered), None, clock.clone()),
                presenter.clone(),
            );
            let required = OAuthAccess::Dropbox(absent);
            assert!(
                matches!(flow.authenticate(required).await, Err(OAuthError::Storage(error)) if error.failure() == StorageFailure::AccessNotRegistered { required })
            );
        }
        assert_eq!(presenter.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn refresh_preserves_omitted_scopes_and_refuses_reduced_grants() {
        let state = Arc::new(Mutex::new(Provider::default()));
        let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
        let flow = OAuthFlow::new(
            clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
                .with_token_endpoint(server.url.clone()),
            presenter("cancel"),
        );
        let refresh = SecretText::new("refresh".into());
        for (client, access, reduced) in [(
            "dropbox",
            OAuthAccess::Dropbox(DropboxAccess::FullDropbox),
            OAuthAccess::Dropbox(DropboxAccess::AppFolder).scope(),
        )] {
            state.lock().unwrap().scope = None;
            let held = OAuthTokens {
                client_id: client.into(),
                access,
                access_token: SecretText::new("old".into()),
                refresh_token: Some(refresh.clone()),
            };
            let tokens = flow.clients.refresh(&held).await.unwrap();
            assert_eq!(tokens.access, access);
            assert_eq!(tokens.client_id, client);
            state.lock().unwrap().scope = Some(reduced.into());
            assert!(matches!(
                flow.clients.refresh(&held).await,
                Err(error) if error.failure() == StorageFailure::AccessNotGranted { required: access }
            ));
        }
    }

    #[tokio::test]
    async fn changed_client_cannot_refresh_the_held_authorization() {
        let state = Arc::new(Mutex::new(Provider::default()));
        let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
        let flow = OAuthFlow::new(
            clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
                .with_token_endpoint(server.url.clone()),
            presenter("cancel"),
        );
        let tokens = OAuthTokens {
            client_id: "another-client".into(),
            access: OAuthAccess::Dropbox(DropboxAccess::FullDropbox),
            access_token: SecretText::new("token".into()),
            refresh_token: Some(SecretText::new("refresh".into())),
        };
        assert!(
            matches!(flow.clients.refresh(&tokens).await, Err(error) if error.failure() == StorageFailure::InvalidConfiguration)
        );
        assert!(state.lock().unwrap().requests.is_empty());
    }
}

#[tokio::test]
async fn refresh_without_a_refresh_token_or_client_names_the_needed_action() {
    let clients = clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)));
    let mut held = OAuthTokens {
        client_id: "onedrive".into(),
        access: OAuthAccess::OneDrive,
        access_token: SecretText::new("token".into()),
        refresh_token: None,
    };
    let error = clients.refresh(&held).await.err().unwrap();
    assert_eq!(error.failure(), StorageFailure::Authentication);
    let StorageError::Provider { source, .. } = error else {
        panic!("sign-in cause discarded")
    };
    assert!(matches!(
        source.downcast_ref::<OAuthError>(),
        Some(OAuthError::Reauthorize(CloudProvider::OneDrive))
    ));
    held.refresh_token = Some(SecretText::new("refresh".into()));
    let unconfigured = OAuthClients::new(None, None, None, clients.clock.clone());
    assert_eq!(
        unconfigured.refresh(&held).await.err().unwrap().failure(),
        StorageFailure::InvalidConfiguration
    );
}

#[tokio::test]
async fn token_parsing_preserves_provider_and_admin_refresh_policies() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let flow = OAuthFlow::new(
        clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
            .with_token_endpoint(server.url.clone()),
        presenter("https://callback.example?code=code&state=STATE"),
    );
    for (body, failure, invalid_refresh) in [
        (
            r#"{"access_token":"new-token","token_type":"bEaReR"}"#,
            None,
            false,
        ),
        (
            r#"{"access_token":"new-token","token_type":"Bearer","refresh_token":"refresh"}"#,
            None,
            false,
        ),
        (
            r#"{"access_token":"new-token","token_type":"Bearer","refresh_token":{"unexpected":"shape"}}"#,
            None,
            true,
        ),
        (
            r#"{"access_token":"new-token","token_type":"Bearer","refresh_token":"one","refresh_token":"two"}"#,
            None,
            true,
        ),
        (
            r#"{"access_token":"","token_type":"Bearer"}"#,
            Some(StorageFailure::Protocol),
            false,
        ),
        (
            r#"{"access_token":"new-token","token_type":"Basic"}"#,
            Some(StorageFailure::Protocol),
            false,
        ),
        (
            r#"{"token_type":"Bearer"}"#,
            Some(StorageFailure::Encoding),
            false,
        ),
    ] {
        state.lock().unwrap().error_response = Some((200, body.into()));
        let admin = flow.google_cloud_admin().await;
        match failure {
            Some(expected) => assert!(
                matches!(admin, Err(OAuthError::Storage(error)) if error.failure() == expected)
            ),
            None => assert_eq!(admin.unwrap().as_str(), "new-token"),
        }
        for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
            let result = flow.authenticate(full_access(provider)).await;
            match if invalid_refresh {
                Some(StorageFailure::Encoding)
            } else {
                failure
            } {
                Some(expected) => assert!(
                    matches!(result, Err(OAuthError::Storage(error)) if error.failure() == expected)
                ),
                // Microsoft grants offline access only as a refresh token.
                None if provider == CloudProvider::OneDrive && !body.contains("refresh_token") => {
                    assert!(matches!(
                        result,
                        Err(OAuthError::Storage(error))
                            if error.failure() == StorageFailure::AccessNotGranted { required: OAuthAccess::OneDrive }
                    ));
                }
                None => {
                    let tokens = result.unwrap();
                    assert_eq!(tokens.access_token.as_str(), "new-token");
                    assert_eq!(
                        tokens.refresh_token.as_ref().map(SecretText::as_str),
                        body.contains("refresh_token").then_some("refresh")
                    );
                }
            }
        }
    }
    let body = r#"{"error":"invalid_grant","error_description":"secret-description"}"#;
    state.lock().unwrap().error_response = Some((400, body.into()));
    let Err(OAuthError::Storage(StorageError::Provider {
        provider,
        failure,
        source,
    })) = flow.google_cloud_admin().await
    else {
        panic!("admin token response lost its provider cause");
    };
    assert_eq!(provider, CloudProvider::S3);
    assert_eq!(failure, StorageFailure::Authentication);
    assert_eq!(
        source
            .downcast_ref::<http::ProviderResponse>()
            .unwrap()
            .body(),
        body.as_bytes()
    );
    assert!(state
        .lock()
        .unwrap()
        .requests
        .iter()
        .all(|request| !request.contains_key("client_secret")));
}

/// Microsoft's token replies list the Graph permissions without
/// `offline_access`, whose grant is the refresh token itself.
#[tokio::test]
async fn onedrive_accepts_microsofts_reply_and_refuses_insufficient_grants() {
    let state = Arc::new(Mutex::new(Provider::default()));
    let server = TestServer::new(Router::new().fallback(token).with_state(state.clone())).await;
    let flow = OAuthFlow::new(
        clients(Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
            .with_token_endpoint(server.url.clone()),
        presenter("https://callback.example?code=code&state=STATE"),
    );
    let reply = |scope: &str, refresh: bool| {
        let mut reply = serde_json::json!({
            "token_type": "Bearer",
            "scope": scope,
            "expires_in": 3600,
            "ext_expires_in": 3600,
            "access_token": "graph-token",
        });
        if refresh {
            reply["refresh_token"] = serde_json::json!("graph-refresh");
        }
        reply.to_string()
    };
    let held = |refresh: &str| OAuthTokens {
        client_id: "onedrive".into(),
        access: OAuthAccess::OneDrive,
        access_token: SecretText::new("old".into()),
        refresh_token: Some(SecretText::new(refresh.into())),
    };
    for scope in [
        "Files.ReadWrite.All User.Read",
        "User.Read Files.ReadWrite.All openid profile email",
        "https://graph.microsoft.com/Files.ReadWrite.All https://graph.microsoft.com/User.Read",
    ] {
        state.lock().unwrap().error_response = Some((200, reply(scope, true)));
        let tokens = flow.authenticate(OAuthAccess::OneDrive).await.unwrap();
        assert_eq!(tokens.access, OAuthAccess::OneDrive, "{scope}");
        assert_eq!(tokens.refresh_token.unwrap().as_str(), "graph-refresh");
        let refreshed = flow.clients.refresh(&held("held")).await.unwrap();
        assert_eq!(refreshed.refresh_token.unwrap().as_str(), "graph-refresh");
        // A refresh reply without a new refresh token keeps the held one.
        state.lock().unwrap().error_response = Some((200, reply(scope, false)));
        let refreshed = flow.clients.refresh(&held("held")).await.unwrap();
        assert_eq!(refreshed.refresh_token.unwrap().as_str(), "held");
        assert!(matches!(
            flow.authenticate(OAuthAccess::OneDrive).await,
            Err(OAuthError::Storage(error))
                if error.failure() == StorageFailure::AccessNotGranted { required: OAuthAccess::OneDrive }
        ));
    }
    for scope in [
        "User.Read",
        "Files.Read.All User.Read",
        "Files.ReadWrite.All",
        "",
    ] {
        state.lock().unwrap().error_response = Some((200, reply(scope, true)));
        assert!(
            matches!(
                flow.authenticate(OAuthAccess::OneDrive).await,
                Err(OAuthError::Storage(error))
                    if error.failure() == StorageFailure::AccessNotGranted { required: OAuthAccess::OneDrive }
            ),
            "{scope}"
        );
        assert!(
            matches!(
                flow.clients.refresh(&held("held")).await,
                Err(error)
                    if error.failure() == StorageFailure::AccessNotGranted { required: OAuthAccess::OneDrive }
            ),
            "{scope}"
        );
    }
    let requests = &state.lock().unwrap().requests;
    assert!(requests
        .iter()
        .filter(|request| request["grant_type"] == "authorization_code")
        .all(|request| request["client_id"] == "onedrive"));
}
