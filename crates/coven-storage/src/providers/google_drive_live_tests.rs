use super::*;

async fn run<F, Fut>(test: F)
where
    F: FnOnce(Arc<Drive>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let clock: ClockRef = Arc::new(SystemClock);
    let session = OAuthSession::new(
        PROVIDER,
        live_drive_tokens(clock.clone()).await,
        clock.clone(),
    )
    .unwrap();
    let url = http::endpoint(
        "https://www.googleapis.com/drive/v3",
        &["files", "generateIds"],
        &[("count", "2"), ("space", "drive"), ("type", "files")],
    )
    .unwrap();
    let generated = http::json(
        PROVIDER,
        session
            .send(Method::GET, &url, &[], RequestBody::Empty, true)
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let ids = http::array(&generated, "ids").unwrap();
    assert_eq!(ids.len(), 2);
    let parent = ids[0].as_str().unwrap().to_owned();
    let folder = ids[1].as_str().unwrap().to_owned();
    let ids = UuidIds;
    let provider = GoogleDriveStorage::new(
        StorageConfig::GoogleDrive { folder_id: folder },
        ids.new_device_id(),
        session,
    )
    .unwrap();
    let fixture = Arc::new(Drive {
        storage: Arc::new(StorageConnection::from_provider(Arc::new(provider))),
        store: StoreId(ids.new_id()),
        parent,
        clock,
        uploads: Mutex::new(Vec::new()),
        reserved: Mutex::new(Vec::new()),
    });
    let work = fixture.clone();
    // Await a task to catch assertion panics as well as failed provider calls.
    // Cleanup also covers a lost folder-create reply because IDs were reserved first.
    let result = tokio::spawn(async move {
        for (id, name, parent) in [
            (&work.parent, "coven-live".to_owned(), "root"),
            (&work.storage.provider.folder, work.store.to_string(), work.parent.as_str()),
        ] {
            work.json(Method::POST, &["files"], "id", RequestBody::Json(json!({
                "id": id, "name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]
            }))).await;
        }
        test(work).await;
    }).await;
    fixture.cleanup().await;
    if let Err(error) = result {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("Drive test cancelled: {error}");
    }
}

use crate::providers::tests::listing_lag::{observe, report, TRIALS};
use tokio::time::Instant;

async fn reserve(drive: &Drive) -> String {
    let provider = &drive.storage.provider;
    let url = provider
        .url(
            &["files", "generateIds"],
            &[("count", "1"), ("space", "drive"), ("type", "files")],
        )
        .unwrap();
    let value = http::json(
        PROVIDER,
        provider
            .send(Method::GET, &url, RequestBody::Empty)
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let ids = http::array(&value, "ids").unwrap();
    assert_eq!(ids.len(), 1);
    let id = ids[0].as_str().expect("reserved file id").to_owned();
    drive.reserved.lock().unwrap().push(id.clone());
    id
}

async fn folder(
    drive: &Drive,
    parent: &str,
    name: &str,
) -> (String, Instant, std::time::SystemTime) {
    let id = reserve(drive).await;
    let native = drive.json(Method::POST, &["files"], "id,createdTime", RequestBody::Json(json!({
        "id": id, "name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]
    }))).await;
    let replied = Instant::now();
    assert_eq!(http::string(&native, "id").unwrap(), id);
    println!(
        "listing_lag Drive folder_stored_at={}",
        http::string(&native, "createdTime").unwrap()
    );
    (
        id,
        replied,
        http::timestamp(&native, "createdTime").unwrap(),
    )
}

async fn created(drive: &Drive, parent: &str, name: &str) -> (Instant, Value) {
    let provider = &drive.storage.provider;
    let id = reserve(drive).await;
    let (content_type, bytes) = multipart_content(
        &json!({"id": id, "name": name, "parents": [parent]}),
        b"listing visibility",
    )
    .unwrap();
    let url = http::endpoint(
        &provider.upload_api,
        &["files"],
        &[
            ("uploadType", "multipart"),
            ("supportsAllDrives", "true"),
            ("fields", "id,size,createdTime"),
        ],
    )
    .unwrap();
    // Measure from the native upload reply, before adapter duplicate checks.
    let value = http::json(
        PROVIDER,
        provider
            .session
            .send(
                Method::POST,
                &url,
                &[("Content-Type", content_type)],
                RequestBody::Bytes(bytes),
                true,
            )
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let replied = Instant::now();
    println!(
        "listing_lag Drive create_reply={:?} stored_at={}",
        drive.clock.now(),
        http::string(&value, "createdTime").unwrap()
    );
    assert_eq!(http::string(&value, "id").unwrap(), id);
    (replied, value)
}

async fn child_listed(
    drive: &Drive,
    parent: &str,
    child: &str,
    folders: bool,
) -> Result<bool, StorageError> {
    let mut query = format!("'{}' in parents and trashed = false", escape(parent));
    if folders {
        query.push_str(" and mimeType = 'application/vnd.google-apps.folder'");
    }
    for item in drive.storage.provider.pages(&query).await? {
        if http::string(&item, "id")? == child {
            return Ok(true);
        }
    }
    Ok(false)
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DRIVE_ENV and a real Google Drive account"]
async fn drive_live_conformance() {
    run(|drive| async move {
        crate::test_utils::Conformance::new(drive.storage.clone(), drive.store)
            .run()
            .await
            .unwrap();
        println!("Drive conformance store={}: passed", drive.store);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DRIVE_ENV and a real Google Drive account"]
async fn drive_live_complete_publication_time() {
    run(|drive| async move {
        for (trial, pause) in [3, 10, 20].into_iter().enumerate() {
            let first_marker = trial as u64 * 4 + 1;
            let before = drive.marker(first_marker).await;
            let path = drive.path(first_marker + 1);
            let started = drive.clock.now();
            let mut upload = drive.begin(&path).await;
            let session_ready = drive.clock.now();
            let part = vec![0x5a; upload.part_size()];
            drive.storage.upload_part(&mut upload, &part).await.unwrap();
            assert!(!upload.is_complete());
            assert_eq!(upload.confirmed_bytes(), part.len() as u64);
            let wait = tokio::time::Instant::now();
            drive.clock.sleep(Duration::from_secs(pause)).await;
            // Server timestamps bracket publication independently of the device clock.
            let before_final = drive.marker(first_marker + 2).await;
            let between_parts = wait.elapsed();
            let completing = drive.clock.now();
            drive.storage.upload_part(&mut upload, b"z").await.unwrap();
            drive.storage.finish_upload(&mut upload).await.unwrap();
            let completed = drive.clock.now();
            let after = drive.marker(first_marker + 3).await;
            let observed = drive.observed(&path).await;
            let native = drive.metadata(&path).await;
            println!("Drive publication pause={pause}s, start={started:?}, session_ready={session_ready:?}, completing={completing:?}, completed={completed:?}, between_parts={:?}, observed={:?}; before={before}; before_final={before_final}; after={after}; object={native}", between_parts, observed.stored_at);
            assert!(between_parts >= Duration::from_secs(pause));
            assert_eq!(observed.size, part.len() as u64 + 1);
            let mut expected = part;
            expected.push(b'z');
            assert_eq!(drive.storage.read(&path).await.unwrap(), expected);
            assert_eq!(observed.stored_at, http::timestamp(&native, "createdTime").unwrap());
            assert!(http::timestamp(&before_final, "createdTime").unwrap() > http::timestamp(&before, "createdTime").unwrap());
            assert!(observed.stored_at >= http::timestamp(&before_final, "createdTime").unwrap(),
                "§4 requires first complete publication, after the delayed final part starts");
            assert!(observed.stored_at <= http::timestamp(&after, "createdTime").unwrap());
        }
    }).await;
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DRIVE_ENV and a real Google Drive account"]
async fn drive_live_unfinished_upload_is_invisible() {
    run(|drive| async move {
        let path = drive.path(1);
        let mut upload = drive.begin(&path).await;
        let part = vec![0x5a; upload.part_size()];
        drive
            .storage
            .upload_part(&mut upload, &part)
            .await
            .unwrap();
        assert!(!upload.is_complete());
        let listed = drive
            .storage
            .list(&ObjectPrefix::all(drive.store))
            .await
            .unwrap();
        let read = drive.storage.read(&path).await;
        let crate::session::SessionState::GoogleDrive { file_id, .. } = &upload.state else {
            panic!("expected an unfinished Drive upload");
        };
        let provider = &drive.storage.provider;
        let url = provider
            .url(&["files", file_id.as_str()], &[("alt", "media")])
            .unwrap();
        let native_status = provider
            .send(Method::GET, &url, RequestBody::Empty)
            .await
            .unwrap()
            .status();
        println!(
            "Drive unfinished confirmed={}, total={}, listed={listed:?}, native_read_status={native_status}, read_length_or_failure={:?}",
            upload.confirmed_bytes(),
            upload.total_bytes(),
            read.as_ref().map(Vec::len).map_err(StorageError::failure)
        );
        assert_eq!(
            native_status,
            reqwest::StatusCode::NOT_FOUND,
            "unfinished bytes must not be readable by native file ID"
        );
        assert!(listed.is_empty(), "unfinished bytes must not be listed");
        assert_eq!(drive.storage.status(&path).await.unwrap(), None);
        assert!(matches!(read, Err(error) if error.failure() == StorageFailure::NotFound));
    })
    .await;
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DRIVE_ENV and a real Google Drive account"]
async fn drive_live_replacement_revision() {
    run(|drive| async move {
        let path = ObjectPath::positions(drive.store, drive.storage.provider.device);
        let mut observed = Vec::new();
        let mut native = Vec::new();
        for bytes in [b"first".as_slice(), b"first", b"other"] {
            drive.storage.replace(&path, bytes).await.unwrap();
            observed.push(drive.observed(&path).await);
            native.push(drive.metadata(&path).await);
            assert_eq!(drive.storage.read(&path).await.unwrap(), bytes);
        }
        println!("Drive replacement status={observed:?}; native={native:?}");
        for pair in native.windows(2) {
            assert_ne!(
                http::string(&pair[0], "headRevisionId").unwrap(),
                http::string(&pair[1], "headRevisionId").unwrap(),
                "native content revision must change even for identical bytes"
            );
        }
        for pair in observed.windows(2) {
            assert_ne!(
                pair[0].revision, pair[1].revision,
                "replacement revision must reach callers"
            );
        }
    })
    .await;
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DRIVE_ENV and a real Google Drive account"]
async fn drive_live_immutable_time_is_stable() {
    run(|drive| async move {
        let path = drive.path(1);
        drive.storage.create(&path, b"immutable").await.unwrap();
        let first = drive.observed(&path).await;
        let native_first = drive.metadata(&path).await;
        assert_eq!(drive.storage.read(&path).await.unwrap(), b"immutable");
        drive.clock.sleep(Duration::from_secs(5)).await;
        let later = drive.marker(2).await;
        assert_eq!(
        drive.storage.create(&path, b"immutable").await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
        assert_eq!(drive.storage.read(&path).await.unwrap(), b"immutable");
        let last = drive.observed(&path).await;
        let native_last = drive.metadata(&path).await;
        println!("Drive immutable first={first:?}, last={last:?}; native_first={native_first}; native_last={native_last}; later={later}");
        assert_eq!(first, last);
        assert_eq!(native_first["createdTime"], native_last["createdTime"]);
        assert!(http::timestamp(&later, "createdTime").unwrap() > first.stored_at);
    }).await;
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DRIVE_ENV and a real Google Drive account"]
async fn drive_live_create_once_preserves_bytes() {
    run(|drive| async move {
        let path = drive.path(1);
        drive.storage.create(&path, b"original").await.unwrap();
        let first = drive.observed(&path).await;
        let second = drive.storage.create(&path, b"replaced").await;
        let bytes = drive.storage.read(&path).await.unwrap();
        let last = drive.observed(&path).await;
        println!(
            "Drive create-once result={second:?}, bytes={bytes:?}, first={first:?}, last={last:?}"
        );
        assert_eq!(second.unwrap_err().failure(), StorageFailure::AlreadyExists);
        assert_eq!(bytes, b"original");
        assert_eq!(first, last);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DRIVE_ENV; creates and deletes live objects"]
async fn drive_live_duplicate_folders() {
    run(|drive| async move {
        let provider = &drive.storage.provider;
        let (store, _, _) = folder(&drive, &provider.folder, &drive.store.to_string()).await;
        let (first, _, first_time) = folder(&drive, &store, "positions").await;
        let (second, _, second_time) = folder(&drive, &store, "positions").await;
        let (_, a) = created(&drive, &first, "31").await;
        let (_, b) = created(&drive, &second, "32").await;
        let prefix = ObjectPrefix::positions(drive.store);
        let listed = drive.storage.list(&prefix).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].object_id, http::string(&a, "id").unwrap());
        assert_eq!(listed[1].object_id, http::string(&b, "id").unwrap());
        for device in [31, 32] {
            let path = ObjectPath::positions(drive.store, DeviceId(device));
            assert_eq!(
                drive.storage.read(&path).await.unwrap(),
                b"listing visibility"
            );
        }
        let expected = if (first_time, &first) < (second_time, &second) {
            &first
        } else {
            &second
        };
        let path = ObjectPath::positions(drive.store, DeviceId(33));
        drive
            .storage
            .replace(&path, b"new publication")
            .await
            .unwrap();
        let published = drive.observed(&path).await;
        let native = drive
            .json(
                Method::GET,
                &["files", &published.object_id],
                "id,parents",
                RequestBody::Empty,
            )
            .await;
        assert_eq!(native["parents"], json!([expected]));
        assert_eq!(drive.storage.list(&prefix).await.unwrap().len(), 3);
        for id in [&first, &second] {
            let native = drive
                .json(
                    Method::GET,
                    &["files", id],
                    "id,parents",
                    RequestBody::Empty,
                )
                .await;
            assert_eq!(native["parents"], json!([store]));
        }
    })
    .await;
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DRIVE_ENV; creates and deletes live objects"]
async fn drive_live_listing_lag() {
    run(|drive| async move {
        let provider = &drive.storage.provider;
        let path = |number| {
            ObjectPath::store_log(
                drive.store,
                provider.device,
                std::num::NonZeroU64::new(number).unwrap(),
            )
        };
        let prefix = ObjectPrefix::store_log(drive.store, provider.device);
        let seed = path(1);
        let writer = provider
            .create_parent(
                &PublicationRequest::new(
                    Arc::new(crate::test_utils::UnrestrictedPublications),
                    seed.clone(),
                ),
                &seed,
            )
            .await
            .unwrap();
        let (replied, native) = created(&drive, &writer, seed.file_name()).await;
        let warm = observe(
            "Drive_setup",
            0,
            replied,
            http::timestamp(&native, "createdTime").unwrap(),
            || async {
                Ok(drive
                    .storage
                    .list(&prefix)
                    .await?
                    .iter()
                    .any(|object| object.path == seed))
            },
        )
        .await;
        report("Drive_setup", &[warm]);
        let mut samples = Vec::new();
        for trial in 1..=TRIALS {
            let path = path(trial + 1);
            let (replied, native) = created(&drive, &writer, path.file_name()).await;
            samples.push(
                observe(
                    "Drive_object",
                    trial,
                    replied,
                    http::timestamp(&native, "createdTime").unwrap(),
                    || async {
                        Ok(drive
                            .storage
                            .list(&prefix)
                            .await?
                            .iter()
                            .any(|object| object.path == path))
                    },
                )
                .await,
            );
        }
        report("Drive_object", &samples);
        let parent = provider.folder_ids.lock().await[&format!("{}/store-log", drive.store)]
            .first()
            .unwrap()
            .1
            .clone();
        let (device, folder_reply, folder_time) = folder(&drive, &parent, "32").await;
        let (folder_sample, (object, discovery)) = tokio::join!(
            observe(
                "Drive_new_device_folder",
                1,
                folder_reply,
                folder_time,
                || async { child_listed(&drive, &parent, &device, true).await }
            ),
            async {
                let (replied, native) = created(&drive, &device, "1").await;
                let stored_at = http::timestamp(&native, "createdTime").unwrap();
                let id = http::string(&native, "id").unwrap();
                tokio::join!(
                    observe("Drive_new_device_object", 1, replied, stored_at, || async {
                        child_listed(&drive, &device, id, false).await
                    }),
                    observe(
                        "Drive_device_discovery_after_entry",
                        1,
                        replied,
                        stored_at,
                        || async { child_listed(&drive, &parent, &device, true).await }
                    ),
                )
            },
        );
        report("Drive_new_device_folder", &[folder_sample]);
        report("Drive_new_device_object", &[object]);
        report("Drive_device_discovery_after_entry", &[discovery]);
    })
    .await;
}
