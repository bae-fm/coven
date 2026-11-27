//! A provider API that answers each expected request in turn.

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use serde_json::Value;
use std::{collections::VecDeque, sync::Arc};

pub(super) struct Reply {
    method: &'static str,
    path: String,
    body: Option<Value>,
    pub(super) value: Value,
    status: u16,
    token: usize,
    retry: Option<u64>,
}
impl Reply {
    pub(super) fn new(method: &'static str, path: &str, body: Option<Value>, value: Value) -> Self {
        Self { method, path: path.into(), body, value, status: 200, token: 0, retry: None }
    }
    pub(super) fn status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }
    pub(super) fn token(mut self, token: usize) -> Self {
        self.token = token;
        self
    }
    pub(super) fn retry(mut self, seconds: u64) -> Self {
        self.retry = Some(seconds);
        self
    }
}
pub(super) struct Fake {
    pub(super) url: String,
    replies: Arc<std::sync::Mutex<VecDeque<Reply>>>,
    /// How many replies have been sent.
    sent: Arc<tokio::sync::watch::Sender<usize>>,
    task: tokio::task::JoinHandle<()>,
}
impl Fake {
    pub(super) async fn new() -> Self {
        let replies = Arc::new(std::sync::Mutex::new(VecDeque::<Reply>::new()));
        let sent = Arc::new(tokio::sync::watch::channel(0).0);
        let counted = sent.clone();
        let router = Router::new()
            .fallback(
                move |State(replies): State<Arc<std::sync::Mutex<VecDeque<Reply>>>>,
                      method: Method,
                      uri: Uri,
                      headers: HeaderMap,
                      bytes: Bytes| {
                    let counted = counted.clone();
                    async move {
                        let reply = replies
                            .lock()
                            .unwrap()
                            .pop_front()
                            .expect("unexpected provider request");
                        assert_eq!(method.as_str(), reply.method);
                        assert_eq!(uri.path_and_query().unwrap().as_str(), reply.path);
                        assert_eq!(
                            headers["authorization"],
                            format!("Bearer access-{}", reply.token)
                        );
                        assert!(!headers.contains_key("Dropbox-API-Path-Root"));
                        match reply.body {
                            Some(body) => {
                                assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), body)
                            }
                            None => assert!(bytes.is_empty()),
                        }
                        let mut response = Response::builder()
                            .status(reply.status)
                            .header("content-type", "application/json");
                        if let Some(seconds) = reply.retry {
                            response = response.header("Retry-After", seconds.to_string());
                        }
                        counted.send_modify(|sent| *sent += 1);
                        response.body(Body::from(reply.value.to_string())).unwrap()
                    }
                },
            )
            .with_state(replies.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, replies, sent, task }
    }
    /// Wait until every expected reply has been asked for and sent.
    pub(super) async fn drained(&self) {
        let mut sent = self.sent.subscribe();
        let drained = sent.wait_for(|_| self.replies.lock().unwrap().is_empty());
        coven_foundation::clock::within(drained)
            .await
            .expect("the provider requests are sent")
            .expect("the fake keeps its count");
    }
    pub(super) fn expect(&self, reply: Reply) {
        self.replies.lock().unwrap().push_back(reply);
    }
    pub(super) fn consumed(&self) {
        assert!(self.replies.lock().unwrap().is_empty(), "provider requests were not sent");
    }
}
impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}
