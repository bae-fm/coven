//! Opt-in personal OneDrive evidence. Credentials and transfer URLs never enter output.
//! Run with COVEN_LIVE_ONEDRIVE_ENV naming the client-id/refresh-token file:
//! cargo test -p coven-storage --all-features --lib onedrive_live -- --ignored --nocapture

use super::*;
use crate::providers::{OAuthClients, OAuthError, OAuthFlow, OAuthPresenter};
use coven_foundation::{
    clock::SystemClock,
    id_source::{IdSource, UuidIds},
};
use std::{
    collections::BTreeMap,
    ops::AsyncFnOnce,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    sync::Mutex,
    time::{Duration, Instant},
};

// Serialize ownership of the shared coven-live parent, including its deletion.
static LIVE_LOCK: Mutex<()> = Mutex::new(());

struct RefreshOnly;
#[async_trait]
impl OAuthPresenter for RefreshOnly {
    fn redirect_uri(&self, _: CloudProvider) -> &str {
        panic!("live evidence must use refresh, not authorization presentation")
    }
    async fn present(&self, _: &str) -> Result<SecretText, OAuthError> {
        panic!("live evidence must use refresh, not authorization presentation")
    }
}

async fn tokens(clock: ClockRef) -> OAuthTokens {
    let path = std::env::var("COVEN_LIVE_ONEDRIVE_ENV")
        .expect("COVEN_LIVE_ONEDRIVE_ENV must name the credentials file");
    let contents = SecretText::new(std::fs::read_to_string(path).expect("read credentials file"));
    let mut settings = BTreeMap::new();
    for line in contents.as_str().lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (key, value) = line
            .split_once('=')
            .expect("credentials must use KEY=VALUE");
        let value = value.trim();
        let value = match value.as_bytes().first() {
            Some(b'\'' | b'"') => {
                assert!(value.len() >= 2 && value.ends_with(value.as_bytes()[0] as char));
                &value[1..value.len() - 1]
            }
            _ => value,
        };
        assert!(
            settings
                .insert(key.trim(), SecretText::new(value.to_owned()))
                .is_none(),
            "duplicate credentials key"
        );
    }
    let mut take = |key| {
        settings
            .remove(key)
            .expect("required OneDrive credential missing")
    };
    let client_id = take("ONEDRIVE_CLIENT_ID");
    let refresh_token = take("ONEDRIVE_REFRESH_TOKEN");
    assert!(!client_id.as_str().is_empty() && !refresh_token.as_str().is_empty());
    OAuthFlow::new(
        OAuthClients::new(None, None, Some(client_id.as_str().to_owned()), clock),
        Arc::new(RefreshOnly),
    )
    .refresh(
        PROVIDER,
        &OAuthTokens {
            // Refresh reads only refresh_token; no access token has been obtained yet.
            access_token: SecretText::new(String::new()),
            refresh_token: Some(refresh_token),
            expires_at: None,
        },
    )
    .await
    .expect("refresh OneDrive credentials through coven OAuth")
}

fn run(test: impl AsyncFnOnce(&mut Live<'_>)) {
    let serial = LIVE_LOCK.lock().expect("live test lock");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let clock: ClockRef = Arc::new(SystemClock);
    let tokens = runtime.block_on(tokens(clock.clone()));
    let session = OAuthSession::new(PROVIDER, tokens.clone(), clock.clone()).unwrap();
    let drive = runtime.block_on(async {
        http::json(
            PROVIDER,
            session
                .send(
                    Method::GET,
                    &format!("{GRAPH}/me/drive?$select=id"),
                    &[],
                    Body::Empty,
                    true,
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap()
    });
    let mut folders = Folders {
        session,
        drive: http::string(&drive, "id").unwrap().into(),
        owned: Vec::new(),
    };
    let mut uploads = Vec::new();
    let store = StoreId(UuidIds.new_id());
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            let folder_id = folders.prepare(store).await;
            let provider = OneDriveStorage::new(
                StorageConfig::OneDrive {
                    drive_id: folders.drive.clone(),
                    folder_id,
                },
                OAuthSession::new(PROVIDER, tokens, clock.clone()).unwrap(),
            )
            .unwrap();
            let mut live = Live {
                storage: Arc::new(StorageConnection::from_provider(Arc::new(provider))),
                store,
                clock,
                uploads: &mut uploads,
            };
            test(&mut live).await;
        })
    }));
    // This final step runs after success, returned errors unwrapped by the test,
    // and assertion panics. No detached cleanup task can outlive the runtime.
    let failures = runtime.block_on(async {
        let mut failures = Vec::new();
        for (storage, upload) in &uploads {
            if let Err(error) = storage.abort_upload(upload).await {
                failures.push(error);
            }
        }
        failures.extend(folders.cleanup().await);
        failures
    });
    drop(serial);
    assert!(failures.is_empty(), "live cleanup failures: {failures:?}");
    println!("onedrive_live cleanup: upload sessions cancelled; owned folders deleted and confirmed absent");
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_conformance() {
    run(async |live| {
        crate::test_utils::Conformance::new(live.storage.clone(), live.store)
            .run()
            .await
            .unwrap();
        println!("onedrive_live conformance: passed");
    });
}

fn publication_time(pause: Duration) {
    // Observed with 3-, 10- and 20-second pauses: createdDateTime falls inside
    // the session-start markers, before the completion markers. The adapter
    // exposes that initiation time, contrary to §4's complete-publication time.
    run(async |live| {
        let path = live.path(1);
        let start_before = live.marker(2).await;
        let session_started = live.clock.now();
        let mut upload = live.begin(&path).await;
        let session_returned = live.clock.now();
        let start_after = live.marker(3).await;
        let mut bytes = vec![42; upload.part_size()];
        live.storage.upload_part(&mut upload, &bytes).await.unwrap();
        assert!(!upload.is_complete());
        let first_part = Instant::now();
        live.clock.sleep(pause).await;
        let complete_before = live.marker(4).await;
        let completion_started = live.clock.now();
        let elapsed = first_part.elapsed();
        live.storage.upload_part(&mut upload, b"z").await.unwrap();
        live.storage.finish_upload(&mut upload).await.unwrap();
        let completed = live.clock.now();
        let complete_after = live.marker(5).await;
        let native = live.storage.provider.metadata(&path).await.unwrap();
        let raw = http::string(&native, "createdDateTime").unwrap();
        let listed = live.listed(&path).await;
        println!("onedrive_live publication pause={}s elapsed={elapsed:?}: start_before={} start_after={} complete_before={} createdDateTime={raw} complete_after={} listed={:?}; local_session_start={session_started:?} local_session_return={session_returned:?} local_completion_start={completion_started:?} local_completed={completed:?}", pause.as_secs(), start_before.0, start_after.0, complete_before.0, complete_after.0, listed.stored_at);
        assert!(elapsed >= pause);
        assert!(start_before.1 <= start_after.1);
        assert!(
            start_after.1 < complete_before.1,
            "markers must distinguish session start from completion"
        );
        assert_eq!(
            listed.stored_at,
            http::timestamp(&native, "createdDateTime").unwrap()
        );
        assert!(
            complete_before.1 <= listed.stored_at && listed.stored_at <= complete_after.1,
            "§4 requires complete-publication time, inside completion markers"
        );
        assert!(upload.is_complete());
        bytes.push(b'z');
        assert_eq!(listed.size, bytes.len() as u64);
        assert!(
            live.storage.read(&path).await.unwrap() == bytes,
            "complete upload bytes changed"
        );
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; failing: createdDateTime records session start"]
fn onedrive_live_publication_time_3_seconds() {
    publication_time(Duration::from_secs(3));
}
#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; failing: createdDateTime records session start"]
fn onedrive_live_publication_time_10_seconds() {
    publication_time(Duration::from_secs(10));
}
#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; failing: createdDateTime records session start"]
fn onedrive_live_publication_time_20_seconds() {
    publication_time(Duration::from_secs(20));
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; failing: unfinished upload is listed and readable"]
fn onedrive_live_unfinished_upload_is_absent() {
    run(async |live| {
        // Observed on personal OneDrive: with 7,864,320 of 7,864,321 bytes
        // confirmed, list includes the destination and read succeeds. Neither
        // operation establishes complete publication while a session is pending.
        let path = live.path(1);
        let mut upload = live.begin(&path).await;
        let bytes = vec![42; upload.part_size()];
        live.storage.upload_part(&mut upload, &bytes).await.unwrap();
        let listed = live
            .storage
            .list(&ObjectPrefix::all(live.store))
            .await
            .unwrap();
        let read = live.storage.read(&path).await;
        println!(
            "onedrive_live unfinished: confirmed={} total={} listed={} read_failure={:?}",
            upload.confirmed_bytes(),
            upload.total_bytes(),
            listed.iter().any(|object| object.path == path),
            read.as_ref().err().map(StorageError::failure)
        );
        assert!(!upload.is_complete());
        assert!(!listed.iter().any(|object| object.path == path));
        assert_eq!(read.unwrap_err().failure(), StorageFailure::NotFound);
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; failing: the adapter exposes no replacement revision"]
fn onedrive_live_replacement_revision() {
    run(async |live| {
        let path = ObjectPath::positions(live.store, DeviceId(31));
        let mut etags = Vec::new();
        for (label, bytes) in [
            ("initial", b"first"),
            ("equal", b"first"),
            ("different_same_size", b"other"),
        ] {
            live.storage.replace(&path, bytes).await.unwrap();
            let native = live.storage.provider.metadata(&path).await.unwrap();
            let etag = http::string(&native, "eTag").unwrap().to_owned();
            let ctag = http::string(&native, "cTag").unwrap().to_owned();
            let modified = http::string(&native, "lastModifiedDateTime").unwrap();
            let listed = live.listed(&path).await;
            // Destructure exhaustively: adding an adapter revision must require
            // updating this evidence to compare that field on both replacements.
            let StoredObject {
                path: _,
                size,
                stored_at,
            } = &listed;
            println!("onedrive_live revision {label}: eTag={etag:?} cTag={ctag:?} lastModifiedDateTime={modified} adapter_revision=absent adapter_size={size} adapter_stored_at={stored_at:?}");
            assert_eq!(*size, bytes.len() as u64);
            assert_eq!(live.storage.read(&path).await.unwrap(), bytes);
            etags.push(etag);
        }
        assert_ne!(
            etags[0], etags[1],
            "native eTag must change on equal-byte replacement"
        );
        assert_ne!(
            etags[1], etags[2],
            "native eTag must change on same-size replacement"
        );
        // Failing evidence: Storage has no status/revision operation and
        // StoredObject contains only path, size and createdDateTime.
        // Native eTag and cTag both advanced through suffixes 1, 2, 3 for the
        // initial, equal-byte and different-byte posts. They cannot serve
        // callers when the adapter discards them.
        panic!(
            "§4 replacement revision is absent from the adapter; native eTag/cTag printed above"
        );
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_immutable_time_is_stable() {
    run(async |live| {
        let path = live.path(1);
        live.storage.create(&path, b"immutable").await.unwrap();
        let first = live.listed(&path).await;
        let before = live.storage.provider.metadata(&path).await.unwrap();
        live.clock.sleep(Duration::from_secs(3)).await;
        live.marker(2).await;
        assert_eq!(live.storage.read(&path).await.unwrap(), b"immutable");
        let second = live.listed(&path).await;
        let after = live.storage.provider.metadata(&path).await.unwrap();
        println!(
            "onedrive_live immutable time: before={} after={} listed_before={:?} listed_after={:?}",
            http::string(&before, "createdDateTime").unwrap(),
            http::string(&after, "createdDateTime").unwrap(),
            first.stored_at,
            second.stored_at
        );
        assert_eq!(first, second);
        assert_eq!(
            http::timestamp(&before, "createdDateTime").unwrap(),
            first.stored_at
        );
        assert_eq!(
            http::timestamp(&after, "createdDateTime").unwrap(),
            second.stored_at
        );
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_create_once() {
    run(async |live| {
        let path = live.path(1);
        live.storage.create(&path, b"first").await.unwrap();
        let before = live.listed(&path).await;
        let result = live.storage.create(&path, b"other").await;
        let bytes = live.storage.read(&path).await.unwrap();
        let after = live.listed(&path).await;
        println!("onedrive_live create-once: failure={:?} retained_bytes={bytes:?} before={:?} after={:?}", result.as_ref().err().map(StorageError::failure), before.stored_at, after.stored_at);
        assert_eq!(result.unwrap_err().failure(), StorageFailure::AlreadyExists);
        assert_eq!(bytes, b"first");
        assert_eq!(before, after);
    });
}
