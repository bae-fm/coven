use super::OAuthSession;
use crate::{CloudProvider, OAuthTokens};
use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, Response, StatusCode},
    Router,
};
use coven_crypto::SecretText;
use coven_foundation::clock::FixedClock;
use std::{sync::Arc, time::SystemTime};

pub(crate) struct TestServer {
    pub(crate) url: String,
    task: tokio::task::JoinHandle<()>,
}
impl TestServer {
    pub(crate) async fn new(app: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                app.layer(axum::extract::DefaultBodyLimit::disable()),
            )
            .await
            .unwrap();
        });
        Self { url, task }
    }
}
impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(crate) fn session(provider: CloudProvider) -> OAuthSession {
    session_with_clock(provider, Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)))
}
pub(crate) fn session_with_clock(
    provider: CloudProvider,
    clock: coven_foundation::clock::ClockRef,
) -> OAuthSession {
    OAuthSession::new(
        provider,
        OAuthTokens {
            access_token: SecretText::new("token".into()),
            refresh_token: None,
            expires_at: None,
        },
        clock,
    )
    .unwrap()
}
pub(crate) fn json(value: serde_json::Value) -> Response<Body> {
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}
pub(crate) fn response(status: u16, bytes: impl Into<Bytes>) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(bytes.into()))
        .unwrap()
}
pub(crate) fn read(bytes: &[u8], headers: &HeaderMap) -> Response<Body> {
    let Some(range) = headers.get("Range") else {
        return response(200, bytes.to_vec());
    };
    let (start, end) = range
        .to_str()
        .unwrap()
        .strip_prefix("bytes=")
        .unwrap()
        .split_once('-')
        .unwrap();
    let start: usize = start.parse().unwrap();
    let end: usize = end.parse().unwrap();
    if start >= bytes.len() {
        return response(416, Vec::new());
    }
    let end = end.min(bytes.len() - 1);
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(
            "content-range",
            format!("bytes {start}-{end}/{}", bytes.len()),
        )
        .body(Body::from(bytes[start..=end].to_vec()))
        .unwrap()
}
pub(crate) fn query(uri: &axum::http::Uri) -> std::collections::BTreeMap<String, String> {
    url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .into_owned()
        .collect()
}

pub(crate) async fn assert_token_refresh(
    make: impl FnOnce(&str) -> Arc<dyn crate::Storage>,
    reply: serde_json::Value,
) {
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let received = requests.clone();
    let server = TestServer::new(Router::new().fallback(move |headers: HeaderMap| {
        let received = received.clone();
        let reply = reply.clone();
        async move {
            received
                .lock()
                .unwrap()
                .push(headers["authorization"].to_str().unwrap().to_owned());
            json(reply)
        }
    }))
    .await;
    let storage = make(&server.url);
    storage.list(&crate::ObjectPrefix::all()).await.unwrap();
    let initial = std::mem::take(&mut *requests.lock().unwrap());
    assert!(!initial.is_empty());
    assert!(initial.iter().all(|token| token == "Bearer token"));
    storage
        .set_oauth_tokens(OAuthTokens {
            access_token: SecretText::new("refreshed".into()),
            refresh_token: None,
            expires_at: None,
        })
        .await
        .unwrap();
    storage.list(&crate::ObjectPrefix::all()).await.unwrap();
    let refreshed = requests.lock().unwrap();
    assert_eq!(refreshed.len(), initial.len());
    assert!(refreshed.iter().all(|token| token == "Bearer refreshed"));
}
