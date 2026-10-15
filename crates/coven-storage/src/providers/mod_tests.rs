use super::OAuthSession;
#[cfg(not(any(target_os = "ios", target_os = "android")))]
use super::{
    http, DesktopOAuthPresenter, DropboxClients, GoogleDriveClient, OAuthClients, OAuthFlow,
    ProviderConnector, StorageConnector,
};
use crate::test_utils::STORE_ID;
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
pub(crate) fn full_access(provider: CloudProvider) -> crate::OAuthAccess {
    match provider {
        CloudProvider::GoogleDrive => {
            crate::OAuthAccess::GoogleDrive(crate::GoogleDriveAccess::Full)
        }
        CloudProvider::Dropbox => crate::OAuthAccess::Dropbox(crate::DropboxAccess::FullDropbox),
        CloudProvider::OneDrive => crate::OAuthAccess::OneDrive,
        _ => panic!("provider does not use OAuth"),
    }
}

/// A held sign-in whose access token is unknown, as live credentials files
/// supply it: refreshing it is the first request.
pub(crate) fn refresh_only(
    client_id: &str,
    access: crate::OAuthAccess,
    refresh_token: SecretText,
) -> OAuthTokens {
    OAuthTokens {
        client_id: client_id.into(),
        access,
        access_token: SecretText::new(String::new()),
        refresh_token: Some(refresh_token),
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
            client_id: "test-client".into(),
            access: full_access(provider),
            access_token: SecretText::new("token".into()),
            refresh_token: None,
        },
        Arc::new(crate::test_utils::RefusedRefresh),
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
    storage
        .list(&crate::ObjectPrefix::all(STORE_ID))
        .await
        .unwrap();
    let initial = std::mem::take(&mut *requests.lock().unwrap());
    assert!(!initial.is_empty());
    assert!(initial.iter().all(|token| token == "Bearer token"));
    storage
        .set_oauth_tokens(OAuthTokens {
            client_id: "test-client".into(),
            access: full_access(storage.config().provider()),
            access_token: SecretText::new("refreshed".into()),
            refresh_token: None,
        })
        .await
        .unwrap();
    storage
        .list(&crate::ObjectPrefix::all(STORE_ID))
        .await
        .unwrap();
    let refreshed = requests.lock().unwrap();
    assert_eq!(refreshed.len(), initial.len());
    assert!(refreshed.iter().all(|token| token == "Bearer refreshed"));
}

/// HTTP fixture shared by Drive object and account-access tests.
pub(crate) mod google_drive {
    use super::{json as reply, query, read, response};
    use axum::{
        body::{Body, Bytes},
        extract::State,
        http::{HeaderMap, Method, Response, Uri},
    };
    use serde_json::{json, Value};
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    #[derive(Default)]
    pub(crate) struct Remote {
        pub(crate) next: u64,
        pub(crate) hide_listings: bool,
        pub(crate) replace_during_read: bool,
        pub(crate) non_owner: bool,
        pub(crate) shared: bool,
        pub(crate) single_uploads: usize,
        pub(crate) files: BTreeMap<String, (Value, Vec<u8>)>,
        pub(crate) uploads: BTreeMap<String, (Value, Vec<u8>, usize)>,
        pub(crate) fail_reply: bool,
        pub(crate) partial: Option<usize>,
        pub(crate) permissions: BTreeMap<String, String>,
        pub(crate) extra_permissions: BTreeMap<String, Value>,
        pub(crate) permission_mutations: Vec<(String, String)>,
        pub(crate) refuse_share: bool,
        pub(crate) lose_share_reply: bool,
        pub(crate) lose_delete_reply: bool,
    }
    impl Remote {
        pub(crate) fn directory(&mut self, path: &str) -> String {
            let mut parent = "folder".to_owned();
            for name in path.split('/') {
                let found = self
                    .files
                    .values()
                    .find(|(item, _)| item["name"] == name && item["parents"] == json!([parent]));
                parent = match found {
                    Some((item, _)) => item["id"].as_str().unwrap().to_owned(),
                    None => {
                        self.next += 1;
                        let id = format!("dir{}", self.next);
                        let item = metadata(
                            json!({"id":id,"name":name,"parents":[parent],"mimeType":"application/vnd.google-apps.folder"}),
                            0,
                        );
                        self.files.insert(id.clone(), (item, Vec::new()));
                        id
                    }
                };
            }
            parent
        }
        pub(crate) fn object_ids(&self) -> Vec<String> {
            self.files
                .iter()
                .filter(|(_, (item, _))| item["mimeType"] != "application/vnd.google-apps.folder")
                .map(|(id, _)| id.clone())
                .collect()
        }
    }
    pub(crate) fn metadata(mut value: Value, size: usize) -> Value {
        value["size"] = json!(size.to_string());
        value["capabilities"] = json!({"canDelete":true,"canRemoveMyDriveParent":true});
        value["ownedByMe"] = json!(true);
        value["createdTime"] = json!("2026-10-06T00:00:00Z");
        value["modifiedTime"] = json!("2026-10-06T00:00:00Z");
        value["version"] = json!("1");
        value["headRevisionId"] = json!("content-1");
        value
    }
    pub(crate) async fn endpoint(
        State(state): State<Arc<Mutex<Remote>>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response<Body> {
        respond(&state, &method, &uri, &headers, &body)
    }

    pub(crate) fn respond(
        state: &Mutex<Remote>,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &Bytes,
    ) -> Response<Body> {
        assert_eq!(headers["authorization"], "Bearer token");
        let mut state = state.lock().unwrap();
        let q = query(uri);
        let parts: Vec<_> = uri.path().trim_start_matches('/').split('/').collect();
        let value: Value = if body.is_empty() {
            Value::Null
        } else if headers
            .get("content-type")
            .is_some_and(|v| v.to_str().unwrap().starts_with("application/json"))
        {
            serde_json::from_slice(body).unwrap()
        } else {
            Value::Null
        };
        if parts == ["drive", "files", "folder"] && method == Method::GET {
            return reply(
                json!({"id":"folder", "ownedByMe":!state.non_owner,"shared":state.shared,"mimeType":"application/vnd.google-apps.folder"}),
            );
        }
        if parts == ["drive", "files", "generateIds"] {
            state.next += 1;
            return reply(json!({"ids":[format!("id{}",state.next)]}));
        }
        if parts == ["upload", "files"] {
            assert_eq!(method, Method::POST);
            if q["uploadType"] == "multipart" {
                let content_type = headers["content-type"].to_str().unwrap();
                let boundary = content_type
                    .strip_prefix("multipart/related; boundary=")
                    .unwrap();
                let marker = format!("--{boundary}");
                let metadata_start = body.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                let separator =
                    format!("\r\n{marker}\r\nContent-Type: application/octet-stream\r\n\r\n");
                let metadata_end = body
                    .windows(separator.len())
                    .position(|w| w == separator.as_bytes())
                    .unwrap();
                let mut value: Value =
                    serde_json::from_slice(&body[metadata_start..metadata_end]).unwrap();
                let end = format!("\r\n{marker}--\r\n");
                assert!(body.ends_with(end.as_bytes()));
                let bytes = body[metadata_end + separator.len()..body.len() - end.len()].to_vec();
                state.next += 1;
                state.single_uploads += 1;
                let id = format!("id{}", state.next);
                value["id"] = json!(id);
                let value = metadata(value, bytes.len());
                state.files.insert(id, (value.clone(), bytes));
                return reply(value);
            }
            assert_eq!(q["uploadType"], "resumable");
            let id = value["id"].as_str().unwrap().to_owned();
            let total = headers["x-upload-content-length"]
                .to_str()
                .unwrap()
                .parse()
                .unwrap();
            state.uploads.insert(id.clone(), (value, Vec::new(), total));
            return Response::builder()
                .header(
                    "location",
                    format!("http://{}/session/{id}", headers["host"].to_str().unwrap()),
                )
                .body(Body::empty())
                .unwrap();
        }
        if parts.first() == Some(&"session") {
            let id = parts[1].to_owned();
            if method == Method::DELETE {
                let status = if state.uploads.remove(&id).is_some() {
                    499
                } else {
                    404
                };
                return response(status, Vec::new());
            }
            if let Some((metadata, _)) = state.files.get(&id) {
                return reply(metadata.clone());
            }
            let range = headers["content-range"].to_str().unwrap();
            let partial = state.partial.take();
            let Some((metadata, bytes, total)) = state.uploads.get_mut(&id) else {
                return response(404, "{}");
            };
            if !range.starts_with("bytes */") {
                let start: usize = range
                    .strip_prefix("bytes ")
                    .unwrap()
                    .split('-')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap();
                assert_eq!(start, bytes.len());
                bytes.extend_from_slice(&body[..partial.unwrap_or(body.len())]);
            }
            let size = bytes.len();
            let complete = size == *total;
            if complete {
                let (value, bytes, _) = state.uploads.remove(&id).unwrap();
                let value = self::metadata(value, bytes.len());
                state.files.insert(id, (value.clone(), bytes));
                return reply(value);
            }
            let _ = metadata;
            if !range.starts_with("bytes */") && std::mem::replace(&mut state.fail_reply, false) {
                return response(503, "{}");
            }
            let mut response = Response::builder().status(308);
            if size > 0 {
                response = response.header("range", format!("bytes=0-{}", size - 1));
            }
            return response.body(Body::empty()).unwrap();
        }
        if parts.len() >= 4 && parts[2] == "folder" && parts[3] == "permissions" {
            return match *method {
                Method::GET => {
                    let entries: Vec<_> = state.permissions.iter().map(|(email,role)|json!({"id":email,"type":"user","emailAddress":email,"role":role,"permissionDetails":[{"inherited":false}]})).chain(state.extra_permissions.values().cloned()).collect();
                    let start = q
                        .get("pageToken")
                        .map(|value| value.parse::<usize>().unwrap())
                        .unwrap_or(0);
                    let mut page = json!({"permissions":entries.iter().skip(start).take(1).collect::<Vec<_>>()});
                    if entries.len() > start + 1 {
                        page["nextPageToken"] = json!((start + 1).to_string());
                    }
                    reply(page)
                }
                Method::POST => {
                    let email = value["emailAddress"].as_str().unwrap();
                    state
                        .permission_mutations
                        .push(("create".into(), email.into()));
                    if state.refuse_share {
                        return response(
                            403,
                            r#"{"error":{"errors":[{"reason":"insufficientFilePermissions"}]}}"#,
                        );
                    }
                    if let Some(permission) = state
                        .extra_permissions
                        .values_mut()
                        .find(|permission| permission["emailAddress"] == email)
                    {
                        permission["role"] = value["role"].clone();
                        permission["permissionDetails"]
                            .as_array_mut()
                            .unwrap()
                            .push(json!({"inherited":false}));
                    } else {
                        state
                            .permissions
                            .insert(email.into(), value["role"].as_str().unwrap().into());
                    }
                    if std::mem::replace(&mut state.lose_share_reply, false) {
                        return response(503, "{}");
                    }
                    reply(json!({"id":"permission"}))
                }
                Method::PATCH => {
                    state
                        .permission_mutations
                        .push(("update".into(), parts[4].into()));
                    if state.refuse_share {
                        return response(
                            403,
                            r#"{"error":{"errors":[{"reason":"insufficientFilePermissions"}]}}"#,
                        );
                    }
                    if let Some(permission) = state.extra_permissions.get_mut(parts[4]) {
                        if permission["permissionDetails"]
                            .as_array()
                            .is_some_and(|details| {
                                details.iter().all(|detail| detail["inherited"] == true)
                            })
                        {
                            return response(
                                403,
                                r#"{"error":{"errors":[{"reason":"cannotModifyInheritedTeamDrivePermission"}]}}"#,
                            );
                        }
                        permission["role"] = value["role"].clone();
                    } else {
                        state
                            .permissions
                            .insert(parts[4].into(), value["role"].as_str().unwrap().into());
                    }
                    if std::mem::replace(&mut state.lose_share_reply, false) {
                        return response(503, "{}");
                    }
                    reply(json!({}))
                }
                Method::DELETE => {
                    state
                        .permission_mutations
                        .push(("delete".into(), parts[4].into()));
                    if let Some(permission) = state.extra_permissions.get_mut(parts[4]) {
                        if permission["role"] == "owner" {
                            return response(
                                403,
                                r#"{"error":{"errors":[{"reason":"cannotDeletePermission"}]}}"#,
                            );
                        }
                        if let Some(details) = permission["permissionDetails"].as_array_mut() {
                            details.retain(|detail| detail["inherited"] == true);
                            if !details.is_empty() {
                                permission["role"] = json!("reader");
                                return response(204, Vec::new());
                            }
                        }
                        state.extra_permissions.remove(parts[4]);
                    }
                    state.permissions.remove(parts[4]);
                    response(204, Vec::new())
                }
                _ => panic!(),
            };
        }
        if parts == ["drive", "files"] {
            if method == Method::POST {
                state.next += 1;
                let id = format!("id{}", state.next);
                let mut value = value;
                value["id"] = json!(id);
                let value = metadata(value, 0);
                state.files.insert(id, (value.clone(), Vec::new()));
                return reply(value);
            }
            if state.hide_listings && !q["q"].contains("name = '") {
                return reply(json!({"files":[]}));
            }
            let filter = q["q"]
                .split("name = '")
                .nth(1)
                .map(|s| s.split('\'').next().unwrap());
            let parents: Vec<_> = q["q"]
                .split(" and ")
                .next()
                .unwrap()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .split(" or ")
                .map(|term| {
                    term.strip_prefix('\'')
                        .unwrap()
                        .strip_suffix("' in parents")
                        .unwrap()
                })
                .collect();
            let files: Vec<_> = state
                .files
                .values()
                .filter(|(v, _)| {
                    parents
                        .iter()
                        .any(|parent| v["parents"].as_array().unwrap().contains(&json!(parent)))
                        && filter.is_none_or(|name| v["name"] == name)
                        && v["trashed"] != true
                })
                .map(|(v, _)| v.clone())
                .collect();
            let start = q
                .get("pageToken")
                .map(|s| s.parse::<usize>().unwrap())
                .unwrap_or(0);
            let mut page = json!({"files":files.iter().skip(start).take(1).collect::<Vec<_>>()});
            if files.len() > start + 1 {
                page["nextPageToken"] = json!((start + 1).to_string());
            }
            return reply(page);
        }
        if parts.len() == 3 && parts[1] == "files" {
            let id = parts[2];
            if method == Method::DELETE {
                state.files.remove(id);
                if std::mem::replace(&mut state.lose_delete_reply, false) {
                    return response(503, "{}");
                }
                return response(204, Vec::new());
            }
            let change_version = q.get("alt").map(String::as_str) == Some("media")
                && std::mem::take(&mut state.replace_during_read);
            let Some((value, bytes)) = state.files.get_mut(id) else {
                return response(404, "{}");
            };
            if method == Method::PATCH {
                if q.contains_key("removeParents") {
                    value["parents"] = json!([]);
                } else {
                    *bytes = body.to_vec();
                    value["size"] = json!(bytes.len().to_string());
                    let version: u64 = value["version"].as_str().unwrap().parse().unwrap();
                    value["version"] = json!((version + 1).to_string());
                    value["headRevisionId"] = json!(format!("content-{}", version + 1));
                }
                return reply(value.clone());
            }
            if q.get("alt").map(String::as_str) == Some("media") {
                if change_version {
                    let version: u64 = value["version"].as_str().unwrap().parse().unwrap();
                    value["version"] = json!((version + 1).to_string());
                    value["headRevisionId"] = json!(format!("content-{}", version + 1));
                }
                return read(bytes, headers);
            }
            return reply(value.clone());
        }
        panic!("unexpected Drive request {method} {uri}")
    }
}

/// Load the live Google Desktop credentials through the production OAuth flow.
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub(crate) async fn live_drive_tokens(clock: coven_foundation::clock::ClockRef) -> OAuthTokens {
    use super::OAuthClients;
    let (client_id, client_secret, refresh_token) =
        read_live_settings("COVEN_LIVE_DRIVE_ENV", |settings| {
            (
                settings.take("GOOGLE_CLIENT_ID").as_str().to_owned(),
                settings.take("GOOGLE_CLIENT_SECRET"),
                settings.take("GOOGLE_REFRESH_TOKEN"),
            )
        });
    let clients = OAuthClients::new(
        Some(super::GoogleDriveClient::new(
            client_id.clone(),
            crate::GoogleDriveAccess::Full,
        )),
        None,
        None,
        clock,
    )
    .with_google_client_secret(client_secret)
    .unwrap();
    clients
        .refresh(&refresh_only(
            &client_id,
            full_access(CloudProvider::GoogleDrive),
            refresh_token,
        ))
        .await
        .unwrap()
}

/// A live env file's settings, held without exposing their contents in
/// diagnostics. A loader takes every key it reads inside
/// [`read_live_settings`]; a key it leaves fails the run, so a setting placed
/// where nothing reads it is refused rather than silently ignored.
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub(crate) struct LiveSettings {
    variable: String,
    settings: std::collections::BTreeMap<String, SecretText>,
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
impl LiveSettings {
    /// Parse `KEY=value` lines, optionally `export`ed and quoted; blank lines
    /// and `#` comments are skipped.
    pub(crate) fn parse(variable: &str, text: &SecretText) -> Self {
        let mut settings = std::collections::BTreeMap::new();
        for line in text.as_str().lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let line = line.strip_prefix("export ").unwrap_or(line);
            let (key, value) = line
                .split_once('=')
                .expect("credentials must use KEY=value");
            let value = value.trim();
            let value = match value.as_bytes().first() {
                Some(b'\'' | b'"') => {
                    assert!(
                        value.len() >= 2 && value.as_bytes().first() == value.as_bytes().last(),
                        "unterminated credential quote"
                    );
                    &value[1..value.len() - 1]
                }
                _ => value,
            };
            assert!(!value.is_empty(), "empty credential");
            assert!(
                settings
                    .insert(key.trim().to_owned(), SecretText::new(value.to_owned()))
                    .is_none(),
                "duplicate credential"
            );
        }
        Self {
            variable: variable.to_owned(),
            settings,
        }
    }
    /// A setting the loader requires.
    pub(crate) fn take(&mut self, key: &str) -> SecretText {
        self.settings
            .remove(key)
            .unwrap_or_else(|| panic!("{} lacks {key}", self.variable))
    }
    /// A setting the loader reads when present.
    pub(crate) fn optional(&mut self, key: &str) -> Option<SecretText> {
        self.settings.remove(key)
    }
    /// Run `read` over the settings, then refuse any it left.
    pub(crate) fn read<T>(mut self, read: impl FnOnce(&mut Self) -> T) -> T {
        let value = read(&mut self);
        let unread: Vec<&String> = self.settings.keys().collect();
        assert!(
            unread.is_empty(),
            "{} holds settings nothing reads: {unread:?}",
            self.variable
        );
        value
    }
}

/// Read the env file `variable` names through `read`, refusing any setting
/// `read` does not take.
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub(crate) fn read_live_settings<T>(
    variable: &str,
    read: impl FnOnce(&mut LiveSettings) -> T,
) -> T {
    let path = std::env::var(variable)
        .unwrap_or_else(|_| panic!("{variable} must name the credentials file"));
    let text = SecretText::new(std::fs::read_to_string(path).expect("credentials file"));
    LiveSettings::parse(variable, &text).read(read)
}

/// Every live loader reads its env file through one reader, which accepts
/// shell-style `export` lines, quotes and comments, and refuses a setting its
/// loader leaves unread.
#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[test]
fn live_settings_refuse_unread_settings() {
    let text = SecretText::new(
        "# credentials\nexport DROPBOX_APP_KEY='key'\n\nDROPBOX_REFRESH_TOKEN=\"token\"\nEXTRA=1\n"
            .to_owned(),
    );
    let read = |settings: &mut LiveSettings| {
        (
            settings.take("DROPBOX_APP_KEY").as_str().to_owned(),
            settings.take("DROPBOX_REFRESH_TOKEN").as_str().to_owned(),
        )
    };
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        LiveSettings::parse("LIVE_ENV", &text).read(read)
    }))
    .unwrap_err();
    assert_eq!(
        refused.downcast_ref::<String>().unwrap(),
        "LIVE_ENV holds settings nothing reads: [\"EXTRA\"]"
    );
    let read_all = LiveSettings::parse("LIVE_ENV", &text).read(|settings| {
        assert!(settings.optional("ABSENT").is_none());
        settings.take("EXTRA");
        read(settings)
    });
    assert_eq!(read_all, ("key".to_owned(), "token".to_owned()));
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub(crate) mod listing_lag {
    //! Live measurements, with monotonic request bounds and no provider response dumps.
    use crate::StorageError;
    use coven_format::value::StorageTime;
    use std::{future::Future, time::Duration};
    use tokio::time::{Instant, MissedTickBehavior};

    pub(crate) const TRIALS: u64 = 20;
    pub(crate) const POLL: Duration = Duration::from_millis(250);
    pub(crate) const LIMIT: Duration = Duration::from_secs(300);

    pub(crate) struct Sample {
        appeared: Option<Duration>,
        omissions: usize,
    }

    /// Start the timer at the create reply, before fetching any extra metadata.
    /// Request start/end bracket each listing observation, not visibility's onset.
    pub(crate) async fn observe<Fut: Future<Output = Result<bool, StorageError>>>(
        label: &str,
        trial: u64,
        replied: Instant,
        stored_at: StorageTime,
        mut listed: impl FnMut() -> Fut,
    ) -> Sample {
        let mut omissions = 0;
        let mut interval = tokio::time::interval(POLL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let deadline = replied + LIMIT;
        loop {
            let observation = tokio::time::timeout_at(deadline, async {
                interval.tick().await;
                let started = replied.elapsed();
                let present = listed().await.unwrap();
                (started, replied.elapsed(), present)
            })
            .await;
            match observation {
                Ok((start, end, present)) => {
                    println!(
                        "listing_lag provider_case={label} trial={trial} stored_at={stored_at:?} list_start_s={:.9} list_end_s={:.9} present={present}",
                        start.as_secs_f64(),
                        end.as_secs_f64()
                    );
                    if present {
                        return Sample {
                            appeared: Some(end),
                            omissions,
                        };
                    }
                    omissions += 1;
                }
                Err(_) => {
                    println!(
                        "listing_lag provider_case={label} trial={trial} stored_at={stored_at:?} timeout_s=300 omissions={omissions}"
                    );
                    return Sample {
                        appeared: None,
                        omissions,
                    };
                }
            }
        }
    }

    pub(crate) fn report(label: &str, samples: &[Sample]) {
        assert!(!samples.is_empty());
        let mut times: Vec<_> = samples
            .iter()
            .filter_map(|sample| sample.appeared)
            .collect();
        times.sort();
        let omitted = samples.iter().filter(|sample| sample.omissions > 0).count();
        let timeouts = samples.len() - times.len();
        if !times.is_empty() {
            let middle = times.len() / 2;
            let median = if times.len().is_multiple_of(2) {
                (times[middle - 1] + times[middle]) / 2
            } else {
                times[middle]
            };
            println!(
                "listing_lag SUMMARY provider_case={label} trials={} min_s={:.9} median_s={:.9} max_s={:.9} omitted_trials={omitted} timeouts={timeouts}",
                samples.len(),
                times[0].as_secs_f64(),
                median.as_secs_f64(),
                times[times.len() - 1].as_secs_f64()
            );
        } else {
            println!(
                "listing_lag SUMMARY provider_case={label} trials={} min_s=unobserved median_s=unobserved max_s=unobserved omitted_trials={omitted} timeouts={timeouts}",
                samples.len()
            );
        }
        assert_eq!(timeouts, 0, "objects did not appear within five minutes");
    }

    #[tokio::test(start_paused = true)]
    async fn poll_records_omissions_without_delaying_the_first_request() {
        let attempts = std::cell::Cell::new(0);
        let replied = Instant::now();
        let sample = observe(
            "fixture",
            1,
            replied,
            StorageTime::new(0, 0).unwrap(),
            || {
                let attempt = attempts.get();
                assert_eq!(replied.elapsed(), POLL * attempt);
                attempts.set(attempt + 1);
                std::future::ready(Ok(attempt == 2))
            },
        )
        .await;
        assert_eq!(attempts.get(), 3);
        assert_eq!(sample.omissions, 2);
        assert_eq!(sample.appeared, Some(POLL * 2));
    }

    #[tokio::test(start_paused = true)]
    async fn poll_deadline_cancels_an_unfinished_listing() {
        let replied = Instant::now();
        let sample = observe(
            "fixture",
            1,
            replied,
            StorageTime::new(0, 0).unwrap(),
            || async {
                tokio::time::sleep(LIMIT + POLL).await;
                Ok(true)
            },
        )
        .await;
        assert!(sample.appeared.is_none());
        assert_eq!(sample.omissions, 0);
        assert!(replied.elapsed() >= LIMIT && replied.elapsed() < LIMIT + POLL);
    }
}

/// Compare an adapter's occupied-path decisions and complete bytes with Lean.
/// Run the Lean `createRunner` on one JSON input.
pub(crate) fn create_runner(input: &serde_json::Value) -> serde_json::Value {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let runner = std::env::var("COVEN_CREATE_LEAN").expect("COVEN_CREATE_LEAN names createRunner");
    let mut process = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    process
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(input).unwrap())
        .unwrap();
    let output = process.wait_with_output().unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

/// Compare creates with the Lean model. A copy-publication adapter
/// (`copy`) is compared with `CopyPublication.publish`: each create stages
/// its own temporary and publishes it with one conditional copy.
pub(crate) async fn compare_create_model(storage: &dyn crate::Storage, copy: bool) {
    use crate::{ObjectPath, StorageFailure};
    let attempts: Vec<_> = (0..32u8).map(|i| {
        serde_json::json!({"path": u64::from(i % 4) + 1, "bytes": vec![i; usize::from((i * 3) % 7)], "copy": copy})
    }).collect();
    let expected: Vec<serde_json::Value> =
        serde_json::from_value(create_runner(&serde_json::Value::from(attempts.clone()))).unwrap();
    assert_eq!(expected.len(), attempts.len());
    for (attempt, expected) in attempts.iter().zip(&expected) {
        let path = ObjectPath::device_log(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::new(attempt["path"].as_u64().unwrap()).unwrap(),
        );
        let bytes: Vec<u8> = serde_json::from_value(attempt["bytes"].clone()).unwrap();
        match storage.create(&path, &bytes).await {
            Ok(()) => assert_eq!(expected["occupied"], false),
            Err(error) => {
                assert_eq!(error.failure(), StorageFailure::AlreadyExists);
                assert_eq!(expected["occupied"], true);
            }
        }
        let bytes: Vec<u8> = serde_json::from_value(expected["bytes"].clone()).unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), bytes);
    }
}

#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[path = "access_live.rs"]
mod access_live;
