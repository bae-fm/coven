//! Absolute permission-state helpers shared by Google Drive and OneDrive.
//!
//! Both revoke a member the same way: list the folder's permissions, find the
//! entry whose email matches the member, and DELETE it (tolerating a 404 if it is
//! already gone). They differ only in the list/delete URLs, the JSON array field
//! holding the permissions, where each entry keeps the email, and how the next
//! permissions page is named — so the algorithm lives here and each backend
//! supplies those differences.

use super::http::{ensure_ok, ok_json, NotFound};
use super::oauth_rest::PageTokenTracker;
use super::oauth_session::OAuthSession;
use super::CloudHomeError;

pub(super) async fn permission_by_email(
    session: &OAuthSession,
    member_id: &str,
    list_url: &str,
    permissions_field: &str,
    email_of: &impl Fn(&serde_json::Value) -> Option<String>,
    next_page: &impl Fn(&serde_json::Value) -> Result<Option<String>, CloudHomeError>,
) -> Result<Option<serde_json::Value>, CloudHomeError> {
    let mut page_url = list_url.to_string();
    let mut page_tokens = PageTokenTracker::new("permission listing");
    loop {
        let resp = session
            .api_call(|token| session.client().get(&page_url).bearer_auth(token))
            .await?;
        let resp = ensure_ok(resp, "list permissions", NotFound::Status).await?;
        let json: serde_json::Value = ok_json(resp, "parse permissions").await?;

        if let Some(permission) = json[permissions_field].as_array().and_then(|perms| {
            perms.iter().find_map(|p| {
                if email_of(p).map(|e| e.eq_ignore_ascii_case(member_id)) == Some(true) {
                    Some(p.clone())
                } else {
                    None
                }
            })
        }) {
            return Ok(Some(permission));
        }

        let Some(next_url) = next_page(&json)? else {
            return Ok(None);
        };
        page_url = page_tokens.record(&next_url)?;
    }
}

/// Drive the provider permission for `member_id` to absent, accepting an
/// already-absent state and verifying absence after deletion.
pub(super) async fn ensure_absent_by_email(
    session: &OAuthSession,
    member_id: &str,
    list_url: &str,
    permissions_field: &str,
    email_of: impl Fn(&serde_json::Value) -> Option<String>,
    delete_url: impl Fn(&str) -> String,
    next_page: impl Fn(&serde_json::Value) -> Result<Option<String>, CloudHomeError>,
) -> Result<(), CloudHomeError> {
    let Some(permission) = permission_by_email(
        session,
        member_id,
        list_url,
        permissions_field,
        &email_of,
        &next_page,
    )
    .await?
    else {
        return Ok(());
    };
    let permission_id = permission["id"].as_str().ok_or_else(|| {
        CloudHomeError::Transport(format!("permission for {member_id} has no string id"))
    })?;

    let url = delete_url(permission_id);
    let resp = session
        .api_call(|token| session.client().delete(&url).bearer_auth(token))
        .await?;
    match ensure_ok(
        resp,
        &format!("revoke access for {member_id}"),
        NotFound::Status,
    )
    .await
    {
        Ok(_) => {}
        Err(CloudHomeError::NotFound(_)) => {
            tracing::debug!("revoke for {member_id}: permission already absent (404 on delete)");
        }
        Err(e) => return Err(e),
    }
    if permission_by_email(
        session,
        member_id,
        list_url,
        permissions_field,
        &email_of,
        &next_page,
    )
    .await?
    .is_some()
    {
        return Err(CloudHomeError::Transport(format!(
            "permission for {member_id} remains after deletion"
        )));
    }
    Ok(())
}

#[cfg(all(test, feature = "oauth-providers"))]
mod tests {
    use super::*;
    use crate::clock::FixedClock;
    use crate::keys::StoreKeys;
    use crate::oauth::test_support::oauth_config;
    use crate::oauth::OAuthTokens;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{Method, Response, StatusCode, Uri};
    use axum::Router;
    use chrono::{TimeZone, Utc};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    /// How the fake provider ends its second listing page.
    #[derive(Clone, Copy)]
    enum SecondPageEnd {
        /// No cursor — the listing is complete.
        Complete,
        /// The page-two cursor again, which is how a provider sends a scan round
        /// the same page forever.
        RepeatsItsCursor,
    }

    #[derive(Clone)]
    struct FakePermissionsState {
        requests: Arc<Mutex<Vec<String>>>,
        target_on_second_page: bool,
        second_page_end: SecondPageEnd,
        delete_status: StatusCode,
        deleted: Arc<AtomicBool>,
    }

    async fn fake_permissions_endpoint(
        State(state): State<FakePermissionsState>,
        method: Method,
        uri: Uri,
    ) -> Response<Body> {
        state
            .requests
            .lock()
            .unwrap()
            .push(format!("{method} {}", uri));

        match (method, uri.path(), uri.query()) {
            (Method::GET, "/permissions", None) => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"permissions":[{"id":"first","email":"other@example.com"}],"next":"/permissions?page=2"}"#,
                ))
                .expect("build page one response"),
            (Method::GET, "/permissions", Some("page=2")) => {
                let permissions = if state.target_on_second_page
                    && !state.deleted.load(Ordering::SeqCst)
                {
                    r#"[{"id":"target","email":"target@example.com"}]"#
                } else {
                    r#"[{"id":"second","email":"second@example.com"}]"#
                };
                let next = match state.second_page_end {
                    SecondPageEnd::Complete => String::new(),
                    SecondPageEnd::RepeatsItsCursor => {
                        r#","next":"/permissions?page=2""#.to_string()
                    }
                };
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"permissions":{permissions}{next}}}"#
                    )))
                    .expect("build page two response")
            }
            (Method::DELETE, "/permissions/target", None) => {
                state.deleted.store(true, Ordering::SeqCst);
                Response::builder()
                    .status(state.delete_status)
                    .body(Body::empty())
                    .expect("build delete response")
            }
            _ => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("unexpected request: {uri}")))
                .expect("build bad request response"),
        }
    }

    async fn spawn_permissions_endpoint(
        target_on_second_page: bool,
        second_page_end: SecondPageEnd,
        delete_status: StatusCode,
    ) -> (
        String,
        Arc<Mutex<Vec<String>>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake permissions endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = FakePermissionsState {
            requests: requests.clone(),
            target_on_second_page,
            second_page_end,
            delete_status,
            deleted: Arc::new(AtomicBool::new(false)),
        };
        let app = Router::new()
            .fallback(fake_permissions_endpoint)
            .with_state(state);

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("fake permissions endpoint failed");
        });

        (endpoint, requests, shutdown_tx)
    }

    fn session(label: &str) -> OAuthSession {
        crate::keys::test_keyring::install();
        OAuthSession::new(
            OAuthTokens {
                access_token: "access-token".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            StoreKeys::new(format!("sharing-pagination-{label}")),
            Arc::new(FixedClock(Utc.timestamp_opt(1_700_000_000, 0).unwrap())),
            oauth_config("http://127.0.0.1/token".to_string()),
            "Provider",
        )
    }

    async fn revoke_against_fake(
        label: &str,
        target_on_second_page: bool,
        second_page_end: SecondPageEnd,
        delete_status: StatusCode,
    ) -> Result<(Arc<Mutex<Vec<String>>>, tokio::sync::oneshot::Sender<()>), CloudHomeError> {
        let (endpoint, requests, shutdown) =
            spawn_permissions_endpoint(target_on_second_page, second_page_end, delete_status).await;
        let session = session(label);
        let list_url = format!("{endpoint}/permissions");
        let next_base = endpoint.clone();
        ensure_absent_by_email(
            &session,
            "target@example.com",
            &list_url,
            "permissions",
            |permission| permission["email"].as_str().map(str::to_string),
            |permission_id| format!("{endpoint}/permissions/{permission_id}"),
            move |page| {
                Ok(page["next"]
                    .as_str()
                    .map(|next_path| format!("{next_base}{next_path}")))
            },
        )
        .await
        .map(|()| (requests, shutdown))
    }

    #[tokio::test]
    async fn ensure_absent_finds_permission_on_second_page_and_verifies_removal() {
        let (requests, shutdown) = revoke_against_fake(
            "found",
            true,
            SecondPageEnd::Complete,
            StatusCode::NO_CONTENT,
        )
        .await
        .expect("revoke target on second page");

        assert_eq!(
            requests.lock().unwrap().as_slice(),
            [
                "GET /permissions",
                "GET /permissions?page=2",
                "DELETE /permissions/target",
                "GET /permissions",
                "GET /permissions?page=2",
            ]
        );
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn ensure_absent_accepts_absent_member_after_scanning_all_pages() {
        let (requests, shutdown) = revoke_against_fake(
            "absent",
            false,
            SecondPageEnd::Complete,
            StatusCode::NO_CONTENT,
        )
        .await
        .expect("already absent is the desired state");
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            ["GET /permissions", "GET /permissions?page=2"]
        );
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn ensure_absent_accepts_delete_404_then_verifies_absence() {
        let (requests, shutdown) = revoke_against_fake(
            "delete-404",
            true,
            SecondPageEnd::Complete,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect("404 delete means already absent");

        assert_eq!(
            requests.lock().unwrap().as_slice(),
            [
                "GET /permissions",
                "GET /permissions?page=2",
                "DELETE /permissions/target",
                "GET /permissions",
                "GET /permissions?page=2",
            ]
        );
        let _ = shutdown.send(());
    }

    /// A provider that hands back a cursor it already gave sends the scan round
    /// the same page forever. The scan refuses the repeat instead of paging on.
    #[tokio::test]
    async fn repeated_page_cursor_is_refused_rather_than_paged_forever() {
        let revoke = revoke_against_fake(
            "cursor-loop",
            false,
            SecondPageEnd::RepeatsItsCursor,
            StatusCode::NO_CONTENT,
        );
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), revoke)
            .await
            .expect("a repeated cursor must end the scan, not page forever");

        let error = outcome.expect_err("a repeated page cursor is refused");
        assert!(
            matches!(&error, CloudHomeError::Transport(message)
                if message.contains("repeated page token")),
            "got {error:?}",
        );
    }
}
