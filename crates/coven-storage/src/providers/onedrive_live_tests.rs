//! Opt-in personal OneDrive evidence. Credentials and transfer URLs never enter output.
//! Run with COVEN_LIVE_ONEDRIVE_ENV naming the client-id/refresh-token file:
//! cargo test -p coven-storage --all-features --lib onedrive_live -- --ignored --nocapture

use super::*;
use crate::providers::{
    tests::listing_lag::{observe, report, TRIALS},
    OAuthClients, OAuthTokenRefresh,
};
use coven_foundation::{
    clock::SystemClock,
    id_source::{IdSource, UuidIds},
};
use std::{
    ops::AsyncFnOnce,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::time::Instant as Monotonic;

// Serialize ownership of the shared coven-live parent, including its deletion.
static LIVE_LOCK: Mutex<()> = Mutex::new(());

async fn tokens(variable: &str, clock: ClockRef) -> OAuthTokens {
    let (client_id, refresh_token) =
        crate::providers::tests::read_live_settings(variable, |settings| {
            (
                settings.take("ONEDRIVE_CLIENT_ID"),
                settings.take("ONEDRIVE_REFRESH_TOKEN"),
            )
        });
    OAuthClients::new(None, None, Some(client_id.as_str().to_owned()), clock)
        .refresh(&crate::providers::tests::refresh_only(
            client_id.as_str(),
            crate::OAuthAccess::OneDrive,
            refresh_token,
        ))
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
    let tokens = runtime.block_on(tokens("COVEN_LIVE_ONEDRIVE_ENV", clock.clone()));
    let session = OAuthSession::new(
        PROVIDER,
        tokens.clone(),
        Arc::new(crate::test_utils::RefusedRefresh),
        clock.clone(),
    )
    .unwrap();
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
    let mut native_uploads = Vec::new();
    let store = StoreId(UuidIds.new_id());
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            let folder_id = folders.prepare(store).await;
            let provider = OneDriveStorage::new(
                StorageConfig::OneDrive {
                    drive_id: folders.drive.clone(),
                    folder_id,
                },
                OAuthSession::new(
                    PROVIDER,
                    tokens,
                    Arc::new(crate::test_utils::RefusedRefresh),
                    clock.clone(),
                )
                .unwrap(),
                Arc::new(UuidIds),
            )
            .unwrap();
            let mut live = Live {
                storage: Arc::new(StorageConnection::from_provider(Arc::new(provider))),
                store,
                clock,
                uploads: &mut uploads,
                native_uploads: &mut native_uploads,
            };
            test(&mut live).await;
        })
    }));
    // This final step runs after success, returned errors unwrapped by the test,
    // and assertion panics. No detached cleanup task can outlive the runtime.
    let failures = runtime.block_on(async {
        let mut failures = Vec::new();
        for url in &native_uploads {
            let result = async {
                let response = folders
                    .session
                    .send(Method::DELETE, url.as_str(), &[], Body::Empty, false)
                    .await?;
                if response.status().as_u16() != 404 {
                    http::checked(PROVIDER, response).await?;
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                failures.push(error);
            }
        }
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
        let suite = crate::test_utils::Conformance::new(live.storage.clone(), live.store);
        suite.run().await.unwrap();
        let storage: Arc<dyn Storage> = live.storage.clone();
        suite
            .creation_race([storage.clone(), storage.clone(), storage])
            .await
            .unwrap();
        println!("onedrive_live conformance: passed");
    });
}

#[test]
#[ignore = "COVEN_LIVE_ONEDRIVE_ENV and COVEN_LIVE_ONEDRIVE_B_ENV; races in this run's shared folder"]
fn onedrive_live_sequence() {
    run(async |live| {
        let owner = tokens("COVEN_LIVE_ONEDRIVE_ENV", live.clock.clone()).await;
        let recipient = tokens("COVEN_LIVE_ONEDRIVE_B_ENV", live.clock.clone()).await;
        let mut writers: Vec<Arc<dyn Storage>> = Vec::new();
        for _ in 0..4 {
            for tokens in [&owner, &recipient] {
                let session = OAuthSession::new(
                    PROVIDER,
                    tokens.clone(),
                    Arc::new(crate::test_utils::RefusedRefresh),
                    live.clock.clone(),
                )
                .unwrap();
                writers.push(Arc::new(StorageConnection::from_provider(Arc::new(
                    OneDriveStorage::new(live.storage.config(), session, Arc::new(UuidIds))
                        .unwrap(),
                ))));
            }
        }
        let account = writers[1].account().await.unwrap();
        assert!(
            account != writers[0].account().await.unwrap(),
            "race requires two accounts"
        );
        let AccessGrant::Granted { invitation } =
            live.storage.grant_access(&account).await.unwrap()
        else {
            panic!("OneDrive did not grant race access");
        };
        writers[1].join(live.store, &invitation).await.unwrap();
        use crate::providers::tests::sequence_live as suite;
        suite::lost_replies(
            live.clock.as_ref(),
            writers[0].as_ref(),
            writers[1].as_ref(),
            live.store,
        )
        .await
        .unwrap();
        suite::slots(live.clock.as_ref(), &writers, live.store)
            .await
            .unwrap();
        // OneDrive can refuse every session of one race, leaving the floor
        // unchanged; the stage counts those rounds and retries them.
        suite::floor(
            live.clock.as_ref(),
            &writers,
            live.store,
            suite::AllRefused::Allowed,
        )
        .await
        .unwrap();
        assert!(live
            .storage
            .revoke_access(&account)
            .await
            .unwrap()
            .is_empty());
        // The enclosing run deletes only its own folder, including its grants,
        // even if setup, a race or the explicit revocation failed.
    });
}

fn publication_time(pause: Duration) {
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
        let raw = http::string(&native, "lastModifiedDateTime").unwrap();
        let observed = live.observed(&path).await;
        println!("onedrive_live publication pause={}s elapsed={elapsed:?}: start_before={} start_after={} complete_before={} lastModifiedDateTime={raw} complete_after={} observed={:?}; local_session_start={session_started:?} local_session_return={session_returned:?} local_completion_start={completion_started:?} local_completed={completed:?}", pause.as_secs(), start_before.0, start_after.0, complete_before.0, complete_after.0, observed.stored_at);
        assert!(elapsed >= pause);
        assert!(start_before.1 <= start_after.1);
        assert!(
            start_after.1 < complete_before.1,
            "markers must distinguish session start from completion"
        );
        assert_eq!(
            observed.stored_at,
            http::timestamp(&native, "lastModifiedDateTime").unwrap()
        );
        assert!(
            complete_before.1 <= observed.stored_at && observed.stored_at <= complete_after.1,
            "§20.6 requires the copy publication time inside completion markers"
        );
        assert!(upload.is_complete());
        bytes.push(b'z');
        assert_eq!(observed.size, bytes.len() as u64);
        assert!(
            live.storage.read(&path).await.unwrap() == bytes,
            "complete upload bytes changed"
        );
        live.assert_temporaries_deleted().await;
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_publication_time_3_seconds() {
    publication_time(Duration::from_secs(3));
}
#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_publication_time_10_seconds() {
    publication_time(Duration::from_secs(10));
}
#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_publication_time_20_seconds() {
    publication_time(Duration::from_secs(20));
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_unfinished_upload_is_absent() {
    run(async |live| {
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
            "unfinished upload exposes final exact status: {status:?}"
        );
        assert!(!listed.iter().any(|object| object.path == path));
        assert_eq!(read.unwrap_err().failure(), StorageFailure::NotFound);
        live.storage.abort_upload(&upload).await.unwrap();
        live.assert_temporaries_deleted().await;
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV and a live personal OneDrive"]
fn onedrive_live_replacement_revision() {
    run(async |live| {
        let path = ObjectPath::floor(live.store);
        let mut etags = Vec::new();
        let mut observations = Vec::new();
        // The empty body goes through a deferred session's zero-body commit.
        for (label, bytes) in [
            ("initial", b"first".as_slice()),
            ("equal", b"first"),
            ("different_same_size", b"other"),
            ("empty", b""),
            ("after_empty", b"refilled"),
        ] {
            if label == "initial" {
                live.storage.create(&path, bytes).await.unwrap();
            } else {
                live.storage
                    .replace(&live.storage.status(&path).await.unwrap().unwrap(), bytes)
                    .await
                    .unwrap();
            }
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
            observations.push(observed);
        }
        assert_ne!(
            etags[0], etags[1],
            "native eTag must change on equal-byte replacement"
        );
        assert_ne!(
            etags[1], etags[2],
            "native eTag must change on same-size replacement"
        );
        assert_ne!(etags[2], etags[3], "native eTag must change on emptying");
        assert_ne!(etags[3], etags[4], "native eTag must change after empty");
        let current = live.storage.read_object(&path, None).await.unwrap();
        for (label, stale) in [("empty", &observations[2]), ("nonempty", &observations[3])] {
            let bytes: &[u8] = if label == "empty" { b"" } else { b"stale" };
            let refused = live.storage.replace(stale, bytes).await.unwrap_err();
            println!(
                "onedrive_live revision stale_{label}: {:?}",
                refused.failure()
            );
            assert_eq!(refused.failure(), StorageFailure::Conflict);
            assert_eq!(
                live.storage.read_object(&path, None).await.unwrap(),
                current
            );
        }
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
            http::string(&before, "lastModifiedDateTime").unwrap(),
            http::string(&after, "lastModifiedDateTime").unwrap(),
            first.stored_at,
            second.stored_at
        );
        assert_eq!(first, second);
        assert_eq!(
            http::timestamp(&before, "lastModifiedDateTime").unwrap(),
            first.stored_at
        );
        assert_eq!(
            http::timestamp(&after, "lastModifiedDateTime").unwrap(),
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
        http::string(&native, "lastModifiedDateTime").unwrap()
    );
    (replied, native)
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; creates and deletes live objects"]
fn onedrive_live_listing_lag() {
    run(async |live| {
        let path = |number: u64| {
            ObjectPath::file(
                live.store,
                DeviceId(31),
                coven_foundation::id_source::FileId(uuid::Uuid::from_u128(number.into())),
            )
        };
        let seed = path(1);
        let (replied, native) = created(live, &seed).await;
        let prefix = ObjectPrefix::device_files(live.store, DeviceId(31));
        let warm = observe(
            "OneDrive_setup",
            0,
            replied,
            http::timestamp(&native, "lastModifiedDateTime").unwrap(),
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
            let stored_at = http::timestamp(&native, "lastModifiedDateTime").unwrap();
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
    });
}

async fn monitor(provider: &OneDriveStorage, response: reqwest::Response) {
    assert_eq!(response.status().as_u16(), 202, "copy must be asynchronous");
    let url = response
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap();
    validate_download_url(&provider.api, url).unwrap();
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        assert!(
            Instant::now() < deadline,
            "copy monitor exceeded five minutes"
        );
        let response = provider
            .session
            .send(Method::GET, url, &[], Body::Empty, false)
            .await
            .unwrap();
        let status = response.status().as_u16();
        // Some Graph monitors redirect to the completed resource instead of
        // returning the asyncJobStatus body. Do not send credentials to it.
        if status == 303 {
            println!("onedrive_copy monitor HTTP=303 completed");
            return;
        }
        let value = http::json(PROVIDER, response).await.unwrap();
        let state = http::string(&value, "status").unwrap();
        println!("onedrive_copy monitor HTTP={status} status={state}");
        match state {
            "completed" => return,
            "notStarted" | "inProgress" | "waiting" => {
                provider.session.sleep(Duration::from_secs(1)).await
            }
            "failed" | "deleteFailed" => panic!(
                "copy failed: code={}",
                http::string(&value["error"], "code").unwrap()
            ),
            _ => panic!("unexpected copy monitor status"),
        }
    }
}

// Native prerequisites for publishing by conditional copy.
#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; transfers over 250 MB and observes for ten minutes"]
fn onedrive_live_conditional_copy() {
    run(async |live| {
        let provider = &live.storage.provider;
        let path = live.path(1);
        let session_before = live.marker(2).await;
        provider
            .parents(
                &PublicationRequest::new(
                    Arc::new(crate::test_utils::UnrestrictedPublications),
                    path.clone(),
                ),
                &path,
            )
            .await
            .unwrap();
        let marker = provider.metadata(&live.path(2)).await.unwrap();
        let parent = http::string(&marker["parentReference"], "id").unwrap();
        let root = provider.item(&provider.folder, &[]).unwrap();
        let store = native(
            provider,
            Method::GET,
            &format!("{root}:/{}:", live.store),
            Body::Empty,
        )
        .await;
        let temporary_folder = native(
            provider,
            Method::POST,
            &provider.item(http::string(&store, "id").unwrap(), &["children"]).unwrap(),
            Body::Json(json!({"name":".coven-uploads","folder":{},"@microsoft.graph.conflictBehavior":"fail"})),
        ).await;
        let temporary_parent = http::string(&temporary_folder, "id").unwrap();
        let temporary_name = UuidIds.new_id().to_string();
        let temporary_url = format!(
            "{}:/{temporary_name}:",
            provider.item(temporary_parent, &[]).unwrap()
        );
        let session = native(
            provider,
            Method::POST,
            &format!("{temporary_url}/createUploadSession"),
            Body::Json(
                json!({"item":{"name":temporary_name,"@microsoft.graph.conflictBehavior":"fail"}}),
            ),
        )
        .await;
        let upload_url = http::string(&session, "uploadUrl").unwrap();
        validate_download_url(&provider.api, upload_url).unwrap();
        live.native_uploads.push(SecretText::new(upload_url.into()));
        let session_after = live.marker(3).await;
        let total = 32 * crate::session::ONEDRIVE_PART_SIZE + 1;
        assert!(total as u64 > provider.single_request_limit());
        let mut temporary = None;
        for start in (0..total).step_by(crate::session::ONEDRIVE_PART_SIZE) {
            let end = (start + crate::session::ONEDRIVE_PART_SIZE).min(total);
            let response = provider
                .session
                .send(
                    Method::PUT,
                    upload_url,
                    &[
                        (
                            "Content-Range",
                            format!("bytes {start}-{}/{total}", end - 1),
                        ),
                        ("Content-Type", "application/octet-stream".into()),
                    ],
                    Body::Bytes(pattern(start, end - start)),
                    false,
                )
                .await
                .unwrap();
            let status = response.status().as_u16();
            let value = http::json(PROVIDER, response).await.unwrap();
            println!("onedrive_copy upload: HTTP={status} confirmed={end} total={total}");
            if end == total {
                assert!(matches!(status, 200 | 201));
                temporary = Some(value);
            } else {
                assert_eq!(status, 202);
            }
            if start == 0 {
                let partial = native(provider, Method::GET, &temporary_url, Body::Empty).await;
                report_item("unfinished temporary", &partial);
                let status = live.storage.status(&path).await.unwrap();
                let read = live.storage.read(&path).await;
                let published = children(provider, parent).await;
                println!(
                    "onedrive_copy unfinished final: status_absent={} read_failure={:?} listed={}",
                    status.is_none(),
                    read.as_ref().err().map(StorageError::failure),
                    published
                        .iter()
                        .any(|item| item["name"] == path.file_name())
                );
                assert!(status.is_none());
                assert_eq!(read.unwrap_err().failure(), StorageFailure::NotFound);
                let listed = live
                    .storage
                    .list(&ObjectPrefix::all(live.store))
                    .await
                    .unwrap();
                assert!(!listed.iter().any(|object| object.path == path));
                assert!(!published
                    .iter()
                    .any(|item| item["name"] == path.file_name()));
                let native_temporaries = children(provider, temporary_parent).await;
                assert!(native_temporaries
                    .iter()
                    .any(|item| item["name"] == temporary_name));
                assert!(!published.iter().any(|item| item["name"] == temporary_name));
            }
        }
        let temporary = temporary.unwrap();
        report_item("completed temporary", &temporary);
        live.clock.sleep(Duration::from_secs(3)).await;
        let before = live.marker(4).await;
        let copy_started = live.clock.now();
        let response = copy_response(
            provider,
            http::string(&temporary, "id").unwrap(),
            parent,
            path.file_name(),
        )
        .await;
        println!(
            "onedrive_copy accepted: HTTP={} local_request={copy_started:?}",
            response.status().as_u16()
        );
        monitor(provider, response).await;
        let after = live.marker(5).await;
        let copied = provider.metadata(&path).await.unwrap();
        report_item("copied", &copied);
        println!("onedrive_copy markers: session_before={} session_after={} copy_before={} copy_after={}", session_before.0, session_after.0, before.0, after.0);
        let modified = http::timestamp(&copied, "lastModifiedDateTime").unwrap();
        assert!(
            session_after.1 < before.1,
            "markers must distinguish session start from publication"
        );
        assert!(
            before.1 <= modified && modified <= after.1,
            "copy lastModifiedDateTime must lie inside publication markers"
        );
        assert!(
            http::timestamp(&temporary, "createdDateTime").unwrap() < modified,
            "copy time must differ from session start"
        );
        assert_eq!(copied["size"].as_u64(), Some(total as u64));
        exact_bytes(provider, &copied, total).await;
        println!("onedrive_copy exact_bytes={total}");

        // Refuse a different source, so replacing with equal bytes cannot hide
        // an overwrite. Check native revision, timestamps and complete bytes.
        let conflict = copy_response(
            provider,
            http::string(&marker, "id").unwrap(),
            parent,
            path.file_name(),
        )
        .await;
        println!("onedrive_copy conflict HTTP={}", conflict.status().as_u16());
        assert_eq!(conflict.status().as_u16(), 409);
        assert_eq!(
            http::response_error(PROVIDER, conflict).await.failure(),
            StorageFailure::AlreadyExists
        );
        let unchanged = provider.metadata(&path).await.unwrap();
        for field in [
            "id",
            "size",
            "eTag",
            "createdDateTime",
            "lastModifiedDateTime",
        ] {
            assert_eq!(
                copied[field], unchanged[field],
                "conflicting copy changed {field}"
            );
        }
        exact_bytes(provider, &unchanged, total).await;

        let observed_since = Instant::now();
        loop {
            let elapsed = observed_since.elapsed();
            let native = provider.metadata(&path).await.unwrap();
            let listed = children(provider, parent).await;
            let listed = listed
                .iter()
                .find(|item| item["id"] == copied["id"])
                .expect("copied object in listing");
            // A ranged read exercises the real metadata/download read path on
            // every observation without transferring the whole object again.
            let read = live
                .storage
                .read_object(&path, Some(ByteRange::new(0, 1024).unwrap()))
                .await
                .unwrap();
            assert_eq!(read.bytes, pattern(0, 1024));
            assert_eq!(read.metadata.stored_at, modified);
            let published = live
                .storage
                .list(&ObjectPrefix::all(live.store))
                .await
                .unwrap();
            assert_eq!(
                published
                    .iter()
                    .find(|object| object.path == path)
                    .unwrap()
                    .stored_at,
                modified
            );
            println!("onedrive_copy stability elapsed={elapsed:?} exact={} listed={} read_stored_at={:?}", native["lastModifiedDateTime"], listed["lastModifiedDateTime"], read.metadata.stored_at);
            assert_eq!(
                native["lastModifiedDateTime"],
                copied["lastModifiedDateTime"]
            );
            assert_eq!(
                listed["lastModifiedDateTime"],
                copied["lastModifiedDateTime"]
            );
            assert_eq!(native["eTag"], copied["eTag"]);
            if elapsed >= Duration::from_secs(600) {
                break;
            }
            live.clock.sleep(Duration::from_secs(30)).await;
        }
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_ONEDRIVE_ENV; creates and deletes live objects"]
fn onedrive_live_cached_folders_and_scoped_listing() {
    run(async |live| {
        let first = ObjectPath::file(
            live.store,
            DeviceId(31),
            coven_foundation::id_source::FileId(uuid::Uuid::from_u128(1)),
        );
        let second = ObjectPath::file(
            live.store,
            DeviceId(31),
            coven_foundation::id_source::FileId(uuid::Uuid::from_u128(2)),
        );
        live.storage.create(&first, b"first").await.unwrap();
        live.storage.create(&second, b"second").await.unwrap();
        let floor = ObjectPath::floor(live.store);
        live.storage.create(&floor, b"unrelated").await.unwrap();
        let prefix = ObjectPrefix::device_files(live.store, DeviceId(31));
        let listed = live.storage.list(&prefix).await.unwrap();
        assert!(listed.iter().all(|object| prefix.contains(&object.path)));
        assert_eq!(live.storage.read(&second).await.unwrap(), b"second");
        let native = live.storage.provider.metadata(&first).await.unwrap();
        let parent = http::string(&native["parentReference"], "id").unwrap();
        let provider = &live.storage.provider;
        http::checked(
            PROVIDER,
            provider
                .send(
                    Method::DELETE,
                    &provider.item(parent, &[]).unwrap(),
                    Body::Empty,
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            live.storage.list(&prefix).await.unwrap_err().failure(),
            StorageFailure::NotFound
        );
        assert!(live.storage.list(&prefix).await.unwrap().is_empty());
        live.storage.create(&first, b"recreated").await.unwrap();
        assert_eq!(live.storage.read(&first).await.unwrap(), b"recreated");
        assert_eq!(live.storage.read(&floor).await.unwrap(), b"unrelated");
        println!("onedrive_live folders: scoped listing; warm create/read; disappeared folder refused; retry resolved and recreated parent; unrelated floor retained");
    });
}
