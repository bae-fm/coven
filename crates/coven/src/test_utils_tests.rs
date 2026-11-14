use crate::*;
use coven_crypto::custody::{InMemoryCustody, KeyringCustody, StoreKeychain};
use coven_database::test_utils::WriteCheckpoint;
use coven_format::sealed_slot::{SlotKind, SlotObjectPrefix};
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

const SCHEMA: &str = "
CREATE TABLE notes(id TEXT NOT NULL,body TEXT NOT NULL,audience TEXT NOT NULL,PRIMARY KEY(audience,id));
CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT);";

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::SharedKey)
            .key_columns(["audience", "id"])
            .audience_column("audience"),
        SyncedTable::new("files", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
    ]
}

fn namespaces() -> Vec<Namespace> {
    vec![Namespace::new("files", Provenance::AppProvided, 1 << 30)]
}

fn member(seed: u8) -> MemberIdentity {
    MemberIdentity::new(MemberId(uuid::Uuid::from_bytes([seed; 16])), MemberKeys::from_seed(seed))
}

struct Fixture {
    app: Coven,
    directory: StoreDir,
    ids: IdSourceRef,
    clock: Arc<FixedClock>,
    storage: Arc<MemoryStorage>,
    /// The connector over `storage` every handle opens with.
    connector: Arc<crate::tests::Connector>,
    keys: Arc<KeyringCustody<StoreKeyring>>,
    member: MemberIdentity,
    _root: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> (Self, CovenHandle) {
        let root = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(root.path().into());
        let ids: IdSourceRef = Arc::new(SequentialIds::new());
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .location(StorageConfig::S3 {
                    bucket: "guarantees".into(),
                    region: "test".into(),
                    endpoint: None,
                    profile: None,
                })
                .clock(clock.clone())
                // Parts the size providers' are (8 MiB): each part commits
                // its upload session, an fsync of the store's WAL, so smaller
                // parts make a large snapshot cost hundreds of fsyncs no
                // device would make.
                .transfer_limits(8 * 1024 * 1024, 8 * 1024 * 1024)
                .build()
                .unwrap(),
        );
        let directory = app.create_closed_store("Guarantees", ids.clone()).await.unwrap();
        let keys = Arc::new(KeyringCustody::new(Arc::new(StoreKeychain::new(
            app.keychain.clone(),
            directory.id(),
        ))));
        // The identity creation made is the store's creator: the store's
        // history binds it.
        let created = Custody::<MemberIdentity>::unlock(&KeyringCustody::new(Arc::new(
            StoreKeychain::new(app.keychain.clone(), directory.id()),
        )))
        .unwrap()
        .unwrap();
        let f = Self {
            app,
            directory,
            ids,
            clock,
            connector: crate::tests::Connector::new(storage.clone()),
            storage,
            keys,
            member: created,
            _root: root,
        };
        let handle = f.open().await;
        handle
            .setup_s3_storage(
                f.storage.config(),
                "owner-key".into(),
                SecretText::new("secret".into()),
                |_| {},
            )
            .await
            .unwrap();
        person(&handle).await;
        // Keep a connection for explicit member/file operations, with the loop stopped.
        stop_connected(&f, &handle).await;
        (f, handle)
    }

    async fn open(&self) -> CovenHandle {
        self.app
            .builder()
            .synced_tables(tables())
            .namespaces(namespaces())
            .migrations(vec![Migration::addition(1, "records", SCHEMA)])
            .clock(self.clock.clone())
            .id_source(self.ids.clone())
            .storage_connector(self.connector.clone())
            .key_custody(CustodyChoice::Custom(self.keys.clone()))
            .identity_custody(CustodyChoice::Custom(Arc::new(InMemoryCustody::new(
                self.member.clone(),
            ))))
            .open(self.directory.id(), |_| ())
            .await
            .unwrap()
    }

    async fn sync(&self, handle: &CovenHandle) {
        self.clock.set(self.clock.now() + Duration::from_secs(1));
        handle.start_sync().await.unwrap();
        person(handle).await;
        stop_connected(self, handle).await;
    }
}

/// Wait for the first pass that begins after the request, and check it
/// ended synced.
async fn person(handle: &CovenHandle) {
    match handle.retry_now().await.unwrap().await {
        RetryOutcome::Synced { .. } => {}
        outcome => panic!("{outcome:?}"),
    }
}

/// Stop the loop and wait for its release, which records a failure as the
/// last pass's: the pass ended synced, and nothing has failed since. Then
/// connect without syncing.
async fn stop_connected(f: &Fixture, handle: &CovenHandle) {
    f.connector.stop(handle).await;
    assert_eq!(handle.test_last_pass().latest().await, Some(LastPass::Synced));
    handle.unlock_store_key().await.unwrap();
}

async fn note(handle: &CovenHandle, body: String) {
    handle.write(move |sql| {
        sql.execute("INSERT INTO notes VALUES('note',?1,'store') ON CONFLICT(audience,id) DO UPDATE SET body=excluded.body", [body])?;
        Ok::<_, CovenError>(())
    }).await.unwrap();
}

async fn read_note(handle: &CovenHandle) -> String {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query_row(
                "SELECT body FROM notes WHERE id='note'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn assert_uploaded(
    f: &Fixture,
    handle: &CovenHandle,
    queued: &[coven_format::write::WriteRecord],
) {
    assert!(!queued.is_empty());
    f.sync(handle).await;
    assert!(handle.test_queued_writes().await.unwrap().is_empty());
    // The writes landed in the last slot this device applied.
    let path = ObjectPath::slot(f.directory.id(), handle.applied_slot().await.try_into().unwrap());
    let bytes = f.storage.read(&path).await.unwrap();
    let prefix =
        SlotObjectPrefix::decode(&bytes[..SlotObjectPrefix::length(&bytes).unwrap()]).unwrap();
    assert!(matches!(prefix.kind, SlotKind::Writes));
}

#[test]
fn a_committed_app_write_survives_dropping_the_handle_and_uploads_after_reopen() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (f, queued) = runtime.block_on(async {
        let (f, handle) = Fixture::new().await;
        note(&handle, "committed".into()).await;
        let queued = handle.test_queued_writes().await.unwrap();
        drop(handle);
        (f, queued)
    });
    // End the crashed installation's runtime without calling close or draining sync.
    drop(runtime);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let handle = f.open().await;
        assert_eq!(read_note(&handle).await, "committed");
        assert_eq!(handle.test_queued_writes().await.unwrap(), queued);
        assert_uploaded(&f, &handle, &queued).await;
        handle.close().await.unwrap();
    });
}

const CRASH_WRITE: &str = "INSERT INTO notes VALUES('note','committed','store'); DELETE FROM files";
// Deleting the row releases no bytes: its file's queued upload keeps its copy
// (§17.9). Crashes while removing released bytes are covered where upload
// completion releases them, in coven-database's file removal tests.
// The write releases no bytes, so no cleanup transaction follows it: file
// removal runs after the writer is free and only for committed records.
/// Every checkpoint the write passes, in order. Live queries judge it before
/// it commits; the test crashes at each one after.
const WRITE_CHECKPOINTS: [WriteCheckpoint; 4] = [
    WriteCheckpoint::Judged,
    WriteCheckpoint::Committed,
    WriteCheckpoint::Observed,
    WriteCheckpoint::StagingReleased,
];

#[test]
fn every_post_commit_crash_preserves_the_write_for_upload() {
    let post_commit = WRITE_CHECKPOINTS.into_iter().enumerate().skip(1);
    for (index, checkpoint) in post_commit {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let f = runtime.block_on(async {
            let (f, handle) = Fixture::new().await;
            handle.set_uploads_paused(true).await.unwrap();
            handle
                .test_staged_write(
                    |batch| {
                        Ok::<_, CovenError>(batch.put_file(
                            "files",
                            b"obsolete".to_vec(),
                            crate::Place::Cloud,
                        ))
                    },
                    |sql, file| {
                        sql.execute("INSERT INTO files(id,file) VALUES('obsolete',?1)", [file])?;
                        Ok(())
                    },
                )
                .await
                .unwrap();
            drop(handle);
            f
        });
        drop(runtime);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "coven::test_utils::tests::crashing_local_write", "--nocapture"])
            .env("COVEN_GUARANTEE_ROOT", f._root.path())
            .env("COVEN_GUARANTEE_STORE", f.directory.id().to_string())
            .env("COVEN_GUARANTEE_POINT", index.to_string())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{index} ({checkpoint:?}): {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let handle = f.open().await;
            assert_eq!(read_note(&handle).await, "committed", "{checkpoint:?}");
            assert_eq!(
                handle
                    .read(|sql| Ok::<_, CovenError>(sql.query_row(
                        "SELECT count(*) FROM files",
                        [],
                        |r| r.get::<_, i64>(0)
                    )?))
                    .await
                    .unwrap(),
                0
            );
            let queued = handle.test_queued_writes().await.unwrap();
            assert_eq!(queued.len(), 2, "{checkpoint:?}");
            assert_eq!(queued[1].header.position.number, 2);
            assert_uploaded(&f, &handle, &queued).await;
            handle.close().await.unwrap();
        });
    }
}

#[test]
fn crashing_local_write() {
    let Ok(root) = std::env::var("COVEN_GUARANTEE_ROOT") else {
        return;
    };
    let store =
        StoreId(uuid::Uuid::parse_str(&std::env::var("COVEN_GUARANTEE_STORE").unwrap()).unwrap());
    let point: usize = std::env::var("COVEN_GUARANTEE_POINT").unwrap().parse().unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = coven_database::DatabaseBuilder::new(
            coven_foundation::files::StoreDirectories::new(
                root.into(),
                crate::task::HeardBugs::shared(),
            )
            .store_dir(&store),
        )
        .synced_tables(tables())
        .namespaces(namespaces())
        .migrations(vec![Migration::addition(1, "records", SCHEMA)])
        .clock(Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1001))))
        .id_source(Arc::new(SequentialIds::new()))
        .open()
        .await
        .unwrap();
        let next = std::cell::Cell::new(0);
        db.test_on_write_checkpoint(move |checkpoint| {
            let index = next.get();
            assert_eq!(checkpoint, WRITE_CHECKPOINTS[index]);
            next.set(index + 1);
            if index == point {
                std::process::exit(86);
            }
        })
        .await
        .unwrap();
        db.write(|sql| {
            sql.execute_batch(CRASH_WRITE)?;
            Ok(())
        })
        .await
        .unwrap();
        panic!("write did not reach the requested post-commit checkpoint");
    });
}

/// Each fact sync writes reaches an app's live query on the sync state, in
/// the order it was written (E5): a pass that claims a slot shows syncing
/// while it runs, then synced with the time it ended.
#[tokio::test]
async fn a_live_query_follows_each_sync_status_in_order() {
    let (f, handle) = Fixture::new().await;
    let mut status = handle.test_sync_status();
    let before = status.next().await;
    assert_eq!(before.state, SyncState::Synced);
    note(&handle, "claimed".into()).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    f.storage.hold_next_creation(ObjectPrefix::all(f.directory.id()), entered, released).await;
    f.clock.set(f.clock.now() + Duration::from_secs(1));
    handle.start_sync().await.unwrap();
    entering.await.unwrap();
    assert_eq!(
        status.next().await,
        SyncStatus {
            state: SyncState::Syncing(SyncActivity::Sending),
            last_synced: before.last_synced
        }
    );
    release.send(()).unwrap();
    assert_eq!(
        status.next().await,
        SyncStatus { state: SyncState::Synced, last_synced: Some(f.clock.now()) }
    );
    handle.stop_sync().await.unwrap();
    handle.close().await.unwrap();
}

#[tokio::test]
async fn app_reads_and_writes_finish_while_a_sync_pass_is_held() {
    let (f, handle) = Fixture::new().await;
    note(&handle, "before sync".into()).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    // Held as it claims the slot for the write before it: the pass has
    // moved, so the state shows it syncing.
    f.storage.hold_next_creation(ObjectPrefix::all(f.directory.id()), entered, released).await;
    handle.start_sync().await.unwrap();
    entering.await.unwrap();
    let mut status = handle.test_sync_status();
    status.wait_for(|status| matches!(status.state, SyncState::Syncing(_))).await;
    assert_eq!(read_note(&handle).await, "before sync");
    note(&handle, "during sync".into()).await;
    assert_eq!(read_note(&handle).await, "during sync");
    assert_eq!(
        handle.test_sync_status().latest().await.state,
        SyncState::Syncing(SyncActivity::Sending)
    );
    let queued = handle.test_queued_writes().await.unwrap();
    release.send(()).unwrap();
    person(&handle).await;
    handle.stop_sync().await.unwrap();
    assert_uploaded(&f, &handle, &queued).await;
    handle.close().await.unwrap();
}

#[tokio::test]
async fn identical_app_files_have_distinct_stored_ciphertext() {
    let (f, handle) = Fixture::new().await;
    handle.set_uploads_paused(true).await.unwrap();
    let content = vec![42; 2 * 65536 + 17];
    let input = content.clone();
    let staged = handle
        .test_staged_write(
            move |batch| {
                Ok::<_, CovenError>([
                    batch.put_file("files", input.clone(), crate::Place::Cloud),
                    batch.put_file("files", input.clone(), crate::Place::Cloud),
                ])
            },
            |sql, files| {
                sql.execute("INSERT INTO files(id,file) VALUES('first',?1),('second',?2)", files)?;
                Ok(files)
            },
        )
        .await
        .unwrap();
    handle.set_uploads_paused(false).await.unwrap();
    // Uploads run only while sync is started (§17.4).
    handle.start_sync().await.unwrap();
    let mut uploads: crate::LiveQuery<_, crate::CovenError> = handle.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| r.get::<_, i64>(0))?)
    });
    while uploads.next().await.expect("the live query answers").unwrap() != 0 {}
    assert!(handle.pending().await.unwrap().iter().all(|record| !matches!(
        record.subject,
        PendingSubject::Upload { .. } | PendingSubject::Download { .. }
    )));
    let objects = f.storage.list(&ObjectPrefix::files(f.directory.id())).await.unwrap();
    assert_eq!(objects.len(), 2);
    let a = f.storage.read(&objects[0].path).await.unwrap();
    let b = f.storage.read(&objects[1].path).await.unwrap();
    assert_ne!(objects[0].path, objects[1].path);
    assert_eq!(a.len(), b.len());
    assert_ne!(a, b);
    // Exclude tags: different paths can change tags even with a reused file key.
    let header =
        coven_format::file::FileHeader::decode(&a[..coven_format::file::FILE_HEADER_LEN]).unwrap();
    assert_eq!(a.len() as u64, header.encrypted_size().unwrap());
    for index in 0..header.chunk_count() {
        let chunk = header.chunk(index).unwrap();
        let start = chunk.offset as usize;
        let range = start..start + chunk.plaintext_length;
        assert_ne!(&a[range.clone()], &b[range]);
    }
    for staged in staged {
        let file = handle.file_ref(staged).await.unwrap();
        let FilePlace::Cloud { location, .. } = file.place() else {
            panic!("the upload stored the file");
        };
        // Each file is stored at the path its staging write fixed.
        assert!(objects.iter().any(|object| object.path == location.path(f.directory.id())));
        handle.evict_file(file.id()).await.unwrap();
        assert_eq!(
            handle.read_file(&file, &crate::FileLease::for_test().await).await.unwrap(),
            content
        );
    }
    handle.close().await.unwrap();
}

#[tokio::test]
async fn growth_snapshots_let_retention_delete_covered_slots_and_superseded_snapshots() {
    async fn grow_and_retain(f: &Fixture, handle: &CovenHandle, body: String) {
        note(handle, body).await;
        f.sync(handle).await;
        // Retention waits for the announcements' grace (§16.6).
        f.clock.set(f.clock.now() + Duration::from_secs(8 * 86400));
        for _ in 0..3 {
            f.sync(handle).await;
        }
    }
    let (f, handle) = Fixture::new().await;
    let store = f.directory.id();
    note(&handle, "before snapshot".into()).await;
    f.sync(&handle).await;
    note(&handle, "x".repeat(1024 * 1024 + 1)).await;
    f.sync(&handle).await;
    // Encoded growth past 1 MiB requests the first snapshot.
    let old = crate::tests::snapshots(&f.storage, store).await;
    assert_eq!(old.len(), 1);
    f.clock.set(f.clock.now() + Duration::from_secs(8 * 86400));
    for _ in 0..3 {
        f.sync(&handle).await;
    }
    let first = ObjectPath::slot(store, 1.try_into().unwrap());
    assert_eq!(f.storage.read(&first).await.unwrap_err().failure(), StorageFailure::NotFound);
    // Growth past the first snapshot's size requests a newer one, which
    // supersedes it once past grace and outside the floor.
    grow_and_retain(&f, &handle, "y".repeat(4 * 1024 * 1024 + 1)).await;
    let new = crate::tests::snapshots(&f.storage, store).await;
    assert!(!new.is_empty());
    assert!(new
        .iter()
        .all(|snapshot| snapshot.path != old[0].path
            && snapshot.path.device() == old[0].path.device()));
    assert_eq!(f.storage.read(&old[0].path).await.unwrap_err().failure(), StorageFailure::NotFound);
    assert_eq!(read_note(&handle).await, "y".repeat(4 * 1024 * 1024 + 1));
    handle.close().await.unwrap();
}

/// Admit a member with `keys` through an invite; returns the id it fixed.
async fn admit_member(f: &Fixture, handle: &CovenHandle, keys: &MemberKeys) -> MemberId {
    let (id, _, _) = handle
        .create_invite(
            MemberRole::Member,
            InviteAccess::S3AccessKey {
                access_key_id: "member-key".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            crate::InviteLifetime::SevenDays,
            "Ana".into(),
        )
        .await
        .unwrap();
    // A pass grants the invite's access and fixes its code.
    f.sync(handle).await;
    let invite = handle
        .read(Invite::list)
        .await
        .unwrap()
        .into_iter()
        .find(|invite| invite.id == id)
        .unwrap();
    let code = coven_format::codes::InviteCode::from_text(invite.shown_code()).unwrap();
    let path = code.request_path();
    let bytes = coven_sync::sealed_join_request(
        &code.secret,
        &path,
        &coven_format::objects::JoinRequest {
            invite: invite.id,
            member: code.member,
            keys: keys.public_keys(),
            device: coven_foundation::id_source::DeviceId(70),
            device_name: "Departing member".into(),
            joiner: Joiner::NewPerson { name: "Departing".into() }.requesting(
                coven_sync::test_joining_circle(
                    code.store,
                    code.member,
                    &keys.public_keys().sealing,
                    &UuidIds,
                )
                .unwrap(),
            ),
        },
        keys,
    )
    .unwrap();
    // The peer supplies a signed request; admission itself uses the app API.
    f.storage.create(&path, &bytes).await.unwrap();
    f.sync(handle).await;
    let request = handle
        .read(Invite::list)
        .await
        .unwrap()
        .into_iter()
        .find_map(|invite| invite.shown_request().cloned())
        .unwrap();
    handle.approve_join_request(&request).await.unwrap();
    // A pass binds the approved pair.
    f.sync(handle).await;
    assert!(handle.members().await.unwrap().iter().any(|m| m.id == code.member));
    code.member
}

#[tokio::test]
async fn removal_preserves_key_material_and_publishes_only_the_member_target() {
    let (f, handle) = Fixture::new().await;
    let removed = admit_member(&f, &handle, member(29).keys()).await;
    let (circle, created) = handle.circles().create("Shared").await.unwrap();
    f.sync(&handle).await;
    crate::tests::landed(&handle, Some(created)).await;
    let added = handle.circles().add_member(circle, &removed).await.unwrap();
    f.sync(&handle).await;
    crate::tests::landed(&handle, added).await;
    let before = f.keys.unlock().unwrap().unwrap();
    let removal = handle.remove_member(&removed).await.unwrap();
    f.sync(&handle).await;
    crate::tests::landed(&handle, removal).await;
    assert!(!handle.members().await.unwrap().iter().any(|m| m.id == removed));
    assert!(!handle.circles().members(circle).await.unwrap().iter().any(|m| m.member == removed));
    // Every key stays; the pass that applied the removal then added the
    // replacements of the keys the removed member held (§12.2).
    let ring = f.keys.unlock().unwrap().unwrap();
    for key in before.store_key_ids() {
        assert_eq!(ring.store_key(key).unwrap().hash(), before.store_key(key).unwrap().hash());
    }
    for key in before.circle_key_ids(circle) {
        let held = ring.circle_key(circle, key).unwrap().hash();
        assert_eq!(held, before.circle_key(circle, key).unwrap().hash());
    }
    assert!(ring.store_key_ids().count() > before.store_key_ids().count());
    handle.close().await.unwrap();
}

/// A store opens in a new installation, as in another process, over a
/// keychain loaded from the device-only entries its creator saved: the same
/// device, with its store keys. An installation without them can't open it.
#[tokio::test]
async fn a_store_opens_over_the_keychain_its_installation_saved() {
    let app_dir = tempfile::tempdir().unwrap();
    let saved = tempfile::tempdir().unwrap();
    let saved = saved.path().join("keychain");
    let builder = |app: &Coven| {
        app.builder()
            .synced_tables(tables())
            .namespaces(namespaces())
            .migrations(vec![Migration::addition(1, "records", SCHEMA)])
    };
    let app = Coven::in_memory(app_dir.path().into());
    let handle =
        builder(&app).create_store("Kept", "Test device", &[], |_, _| Ok(())).await.unwrap();
    let store = handle.store_id();
    note(&handle, "kept".into()).await;
    handle.close().await.unwrap();
    let keys = StoreKeyring::new(StoreKey::generate(KeyId(SequentialIds::new().new_id())).unwrap());
    app.keep_store_keys(&app.store_dir(&store), &keys).unwrap();
    app.save_device_keychain(&saved).unwrap();
    drop(app);

    let without = Coven::in_memory(app_dir.path().into());
    let refused = builder(&without).open(store, |_| ()).await;
    assert!(
        matches!(refused, Err(CovenError::DeviceKeyMissing { store: missing }) if missing == store)
    );

    let app = Coven::in_memory_with_saved_keychain(app_dir.path().into(), &saved).unwrap();
    let handle = builder(&app).open(store, |_| ()).await.unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    assert_eq!(read_note(&handle).await, "kept");
    handle.close().await.unwrap();
}

/// The budget an app gives its installation reaches the file owner of every
/// store the installation opens: a device-only call, which takes a place of
/// that budget before it does anything else, waits while the app holds them
/// all, and goes on once one comes back (api.md E1).
#[tokio::test]
async fn a_store_opened_through_an_installation_uses_the_budget_the_app_gave_it() {
    let root = tempfile::tempdir().unwrap();
    let budget = FileBudget::new(std::num::NonZeroUsize::MIN);
    let watch = budget.watch();
    let app = Coven::in_memory_sharing(
        root.path().to_owned(),
        crate::task::HeardBugs::shared(),
        budget.clone(),
    );
    let directory = app.create_closed_store("Household", Arc::new(UuidIds)).await.unwrap();
    let handle = app
        .builder()
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    let held = budget.lease(1).await.unwrap();
    let waited = watch.lease_waits_from_now(1);
    let folder = tempfile::tempdir().unwrap();
    let (cancel, cancelled) = tokio::sync::watch::channel(false);
    let call = tokio::spawn({
        let (handle, folder) = (handle.clone(), folder.path().to_owned());
        async move { handle.prepare_device_only(&folder, Vec::new(), &cancelled, |_| {}).await }
    });
    coven_foundation::clock::within(waited).await.unwrap();
    assert!(!call.is_finished());
    drop(held);
    coven_foundation::clock::within(call).await.unwrap().unwrap().unwrap();
    drop(cancel);
    handle.close().await.unwrap();
}
