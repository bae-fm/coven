use super::OAuthConfig;
use axum::{extract::Form, routing::post, Router};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{oneshot, Mutex};

pub(crate) async fn serve_token_response(
    response_body: &'static str,
) -> (
    String,
    oneshot::Receiver<HashMap<String, String>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind token server");
    let url = format!(
        "http://{}/token",
        listener.local_addr().expect("local addr")
    );

    let (request_tx, request_rx) = oneshot::channel();
    let request_tx = Arc::new(Mutex::new(Some(request_tx)));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));

    let app = Router::new().route(
        "/token",
        post(move |Form(params): Form<HashMap<String, String>>| {
            let request_tx = request_tx.clone();
            let shutdown_tx = shutdown_tx.clone();
            async move {
                let request_tx = request_tx
                    .lock()
                    .await
                    .take()
                    .expect("token request sender available");
                request_tx.send(params).expect("send token request to test");
                let shutdown_tx = shutdown_tx
                    .lock()
                    .await
                    .take()
                    .expect("token server shutdown sender available");
                shutdown_tx.send(()).expect("send token server shutdown");
                ([("content-type", "application/json")], response_body)
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                shutdown_rx.await.expect("receive token server shutdown");
            })
            .await
            .expect("serve token response");
    });
    (url, request_rx, server)
}

pub(crate) fn oauth_config(token_url: String) -> OAuthConfig {
    OAuthConfig {
        client_id: "client-id".to_string(),
        client_secret: Some("client-secret".to_string()),
        auth_url: "http://auth.example/authorize".to_string(),
        token_url,
        scopes: vec![],
        redirect_port: 19284,
        extra_auth_params: vec![],
    }
}
