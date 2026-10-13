//! Opt-in personal OneDrive evidence. Credentials and transfer URLs never enter output.
//! Run with COVEN_LIVE_ONEDRIVE_ENV naming the client-id/refresh-token file:
//! cargo test -p coven-storage --all-features --lib onedrive_live -- --ignored --nocapture

use super::*;
use crate::providers::{
    pagination::Pagination,
    tests::listing_lag::{observe, report, TRIALS},
    OAuthClients, OAuthError, OAuthFlow, OAuthPresenter,
};
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
use tokio::time::Instant as Monotonic;

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
        let observed = live.observed(&path).await;
        println!("onedrive_live publication pause={}s elapsed={elapsed:?}: start_before={} start_after={} complete_before={} createdDateTime={raw} complete_after={} observed={:?}; local_session_start={session_started:?} local_session_return={session_returned:?} local_completion_start={completion_started:?} local_completed={completed:?}", pause.as_secs(), start_before.0, start_after.0, complete_before.0, complete_after.0, observed.stored_at);
        assert!(elapsed >= pause);
        assert!(start_before.1 <= start_after.1);
        assert!(
            start_after.1 < complete_before.1,
            "markers must distinguish session start from completion"
        );
        assert_eq!(
            observed.stored_at,
            http::timestamp(&native, "createdDateTime").unwrap()
        );
        assert!(
            complete_before.1 <= observed.stored_at && observed.stored_at <= complete_after.1,
            "OneDrive createdDateTime reports session start; §4 requires complete-publication time inside completion markers"
        );
        assert!(upload.is_complete());
        bytes.push(b'z');
        assert_eq!(observed.size, bytes.len() as u64);
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
        let status = live.storage.status(&path).await.unwrap();
        println!(
            "onedrive_live unfinished: confirmed={} total={} listed={} read_failure={:?}",
            upload.confirmed_bytes(),
            upload.total_bytes(),
            listed.iter().any(|object| object.path == path),
            read.as_ref().err().map(StorageError::failure)
        );
        assert!(!upload.is_complete());
        assert!(
            status.is_none(),
            "OneDrive native upload exposes unfinished exact status: {status:?}"
        );
        assert!(!listed.iter().any(|object| object.path == path));
        assert_eq!(read.unwrap_err().failure(), StorageFailure::NotFound);
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
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
            let observed = live.observed(&path).await;
            println!("onedrive_live revision {label}: eTag={etag:?} cTag={ctag:?} lastModifiedDateTime={modified} adapter_revision={:?}", observed.revision);
            assert_eq!(observed.size, bytes.len() as u64);
            assert_eq!(observed.revision, etag);
            let read = live.storage.read_object(&path, None).await.unwrap();
            assert_eq!(read.bytes, bytes);
            assert_eq!(read.metadata, observed);
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
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_immutable_time_is_stable() {
    run(async |live| {
        let path = live.path(1);
        live.storage.create(&path, b"immutable").await.unwrap();
        let first = live.observed(&path).await;
        let before = live.storage.provider.metadata(&path).await.unwrap();
        live.clock.sleep(Duration::from_secs(3)).await;
        live.marker(2).await;
        assert_eq!(live.storage.read(&path).await.unwrap(), b"immutable");
        let second = live.observed(&path).await;
        let after = live.storage.provider.metadata(&path).await.unwrap();
        println!(
            "onedrive_live immutable time: before={} after={} observed_before={:?} observed_after={:?}",
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
        let before = live.observed(&path).await;
        let result = live.storage.create(&path, b"other").await;
        let bytes = live.storage.read(&path).await.unwrap();
        let after = live.observed(&path).await;
        println!("onedrive_live create-once: failure={:?} retained_bytes={bytes:?} before={:?} after={:?}", result.as_ref().err().map(StorageError::failure), before.stored_at, after.stored_at);
        assert_eq!(result.unwrap_err().failure(), StorageFailure::AlreadyExists);
        assert_eq!(bytes, b"first");
        assert_eq!(before, after);
    });
}

async fn created(live: &Live<'_>, path: &ObjectPath) -> (Monotonic, Value) {
    live.storage
        .create(path, b"listing visibility")
        .await
        .unwrap();
    let replied = Monotonic::now();
    let wall = live.clock.now();
    let native = live.storage.provider.metadata(path).await.unwrap();
    println!(
        "listing_lag OneDrive create_reply={wall:?} stored_at={}",
        http::string(&native, "createdDateTime").unwrap()
    );
    (replied, native)
}

async fn child_listed(
    provider: &OneDriveStorage,
    parent: &str,
    child: &str,
) -> Result<bool, StorageError> {
    let mut url = provider.item(parent, &["children"])?;
    let mut pages = Pagination::new();
    loop {
        pages.check(&url)?;
        http::same_origin(&provider.api, &url)?;
        let value = http::json(
            PROVIDER,
            provider.send(Method::GET, &url, Body::Empty).await?,
        )
        .await?;
        for item in http::array(&value, "value")? {
            if http::string(item, "id")? == child {
                assert!(item["folder"].is_object());
                return Ok(true);
            }
        }
        match value.get("@odata.nextLink") {
            Some(_) => url = http::string(&value, "@odata.nextLink")?.to_owned(),
            None => return Ok(false),
        }
    }
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; creates and deletes live objects"]
fn onedrive_live_listing_lag() {
    run(async |live| {
        let path = |number| {
            ObjectPath::store_log(
                live.store,
                DeviceId(31),
                std::num::NonZeroU64::new(number).unwrap(),
            )
        };
        let seed = path(1);
        let (replied, native) = created(live, &seed).await;
        let prefix = ObjectPrefix::store_log(live.store, DeviceId(31));
        let warm = observe(
            "OneDrive_setup",
            0,
            replied,
            http::timestamp(&native, "createdDateTime").unwrap(),
            || async {
                Ok(live
                    .storage
                    .list(&prefix)
                    .await?
                    .iter()
                    .any(|object| object.path == seed))
            },
        )
        .await;
        report("OneDrive_setup", &[warm]);
        let mut samples = Vec::new();
        for trial in 1..=TRIALS {
            let path = path(trial + 1);
            let (replied, native) = created(live, &path).await;
            let stored_at = http::timestamp(&native, "createdDateTime").unwrap();
            samples.push(
                observe("OneDrive_object", trial, replied, stored_at, || async {
                    Ok(live
                        .storage
                        .list(&prefix)
                        .await?
                        .iter()
                        .any(|object| object.path == path))
                })
                .await,
            );
            live.storage.delete(&path).await.unwrap();
        }
        report("OneDrive_object", &samples);
        let provider = &live.storage.provider;
        let root = provider.item(&provider.folder, &[]).unwrap();
        let parent_url = format!("{root}:/{}/store-log:", live.store);
        let parent = http::json(
            PROVIDER,
            provider
                .send(Method::GET, &parent_url, Body::Empty)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let parent_id = http::string(&parent, "id").unwrap();
        let native_folder = http::json(
            PROVIDER,
            provider
                .send(
                    Method::POST,
                    &provider.item(parent_id, &["children"]).unwrap(),
                    Body::Json(
                        json!({"name":"32","folder":{},"@microsoft.graph.conflictBehavior":"fail"}),
                    ),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let folder_reply = Monotonic::now();
        let folder_time = http::timestamp(&native_folder, "createdDateTime").unwrap();
        let folder = http::string(&native_folder, "id").unwrap();
        let path = ObjectPath::store_log(live.store, DeviceId(32), std::num::NonZeroU64::MIN);
        let all = ObjectPrefix::store_logs(live.store);
        let (folder_sample, (object, discovery)) = tokio::join!(
            observe(
                "OneDrive_new_device_folder",
                1,
                folder_reply,
                folder_time,
                || async { child_listed(provider, parent_id, folder).await }
            ),
            async {
                let (replied, native) = created(live, &path).await;
                let stored_at = http::timestamp(&native, "createdDateTime").unwrap();
                assert_eq!(
                    http::string(&native["parentReference"], "id").unwrap(),
                    folder
                );
                // Both observations start after entry 1's create reply. Folder
                // discovery is independent of the recursive object listing.
                tokio::join!(
                    observe(
                        "OneDrive_new_device_object",
                        1,
                        replied,
                        stored_at,
                        || async {
                            Ok(live
                                .storage
                                .list(&all)
                                .await?
                                .iter()
                                .any(|object| object.path == path))
                        }
                    ),
                    observe(
                        "OneDrive_device_discovery_after_entry",
                        1,
                        replied,
                        stored_at,
                        || async { child_listed(provider, parent_id, folder).await }
                    ),
                )
            },
        );
        report("OneDrive_new_device_folder", &[folder_sample]);
        report("OneDrive_new_device_object", &[object]);
        report("OneDrive_device_discovery_after_entry", &[discovery]);
    });
}
