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
CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT);";

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::SharedKey)
            .key_columns(["audience", "id"])
            .audience_column("audience"),
        SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
            "files",
            1 << 30,
            Provenance::AppProvided,
        )),
    ]
}

fn member(seed: u8) -> MemberIdentity {
    MemberIdentity::new(
        MemberId(uuid::Uuid::from_bytes([seed; 16])),
        MemberKeys::from_secret_bytes(&[b"CVMK\x01".as_slice(), &[seed; 64]].concat()).unwrap(),
    )
}

struct Fixture {
    app: TestCoven,
    directory: StoreDir,
    layout: StoreLayout,
    ids: IdSourceRef,
    clock: Arc<FixedClock>,
    storage: Arc<MemoryStorage>,
    keys: Arc<KeyringCustody<StoreKeyring>>,
    member: MemberIdentity,
    _root: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> (Self, CovenHandle) {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        let app = TestCoven::new();
        let ids: IdSourceRef = Arc::new(SequentialIds::new());
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .location(StorageConfig::S3 {
                    bucket: "guarantees".into(),
                    region: "test".into(),
                    prefix: crate::S3Prefix::parse("store").unwrap(),
                    endpoint: None,
                    profile: None,
                })
                .clock(clock.clone())
                .transfer_limits(8 * 1024 * 1024, 65536)
                .build()
                .unwrap(),
        );
        let directory = app
            .create_closed_store(&layout, "Guarantees", ids.clone())
            .await
            .unwrap();
        let keys = Arc::new(KeyringCustody::new(Arc::new(StoreKeychain::new(
            app.keychain.clone(),
            directory.id(),
        ))));
        let f = Self {
            app,
            directory,
            layout,
            ids,
            clock,
            storage,
            keys,
            member: member(17),
            _root: root,
        };
        let handle = f.open().await;
        handle
            .setup_s3_storage(
                f.storage.config(),
                "Owner",
                "owner-key".into(),
                SecretText::new("secret".into()),
            )
            .await
            .unwrap();
        synced(&handle).await;
        stop(&handle).await;
        // Keep a connection for explicit member/file operations, with the loop stopped.
        handle.unlock_store_key().await.unwrap();
        (f, handle)
    }

    async fn open(&self) -> CovenHandle {
        self.app
            .builder(self.layout.clone())
            .synced_tables(tables())
            .migrations(vec![Migration::addition(1, "records", SCHEMA)])
            .clock(self.clock.clone())
            .id_source(self.ids.clone())
            .storage_connector(self.storage.clone())
            .key_custody(KeyCustody::Custom(self.keys.clone()))
            .identity_custody(IdentityCustody::Custom(Arc::new(InMemoryCustody::new(
                self.member.clone(),
            ))))
            .open(self.directory.id())
            .await
            .unwrap()
    }

    async fn sync(&self, handle: &CovenHandle) {
        self.clock.set(self.clock.now() + Duration::from_secs(1));
        handle.start_sync().await.unwrap();
        synced(handle).await;
        stop(handle).await;
        handle.unlock_store_key().await.unwrap();
    }
}

async fn synced(handle: &CovenHandle) {
    let mut status = handle.subscribe_sync_status();
    let state = status
        .wait_for(|s| {
            matches!(
                s,
                SyncStatus::Synced { .. } | SyncStatus::Failed { .. } | SyncStatus::Offline { .. }
            )
        })
        .await
        .unwrap();
    assert!(matches!(*state, SyncStatus::Synced { .. }), "{state:?}");
}

async fn stop(handle: &CovenHandle) {
    handle.stop_sync();
    let mut status = handle.subscribe_sync_status();
    let state = status
        .wait_for(|s| matches!(s, SyncStatus::Stopped | SyncStatus::Failed { .. }))
        .await
        .unwrap();
    assert!(matches!(*state, SyncStatus::Stopped), "{state:?}");
}

async fn note(handle: &CovenHandle, body: String) {
    handle.write(move |sql| {
        sql.execute("INSERT INTO notes VALUES('note',?1,'store') ON CONFLICT(audience,id) DO UPDATE SET body=excluded.body", [body])?;
        Ok(())
    }).await.unwrap();
}

async fn read_note(handle: &CovenHandle) -> String {
    handle
        .read(|sql| Ok(sql.query_row("SELECT body FROM notes WHERE id='note'", [], |r| r.get(0))?))
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
    let path = ObjectPath::slot(
        f.directory.id(),
        handle.applied_slot().await.try_into().unwrap(),
    );
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
// (§17.7). Crashes while removing released bytes are covered where upload
// completion releases them, in coven-database's file removal tests.
// The write releases no bytes, so no cleanup transaction follows it: file
// removal runs after the writer is free and only for committed records.
const CRASH_POINTS: [WriteCheckpoint; 3] = [
    WriteCheckpoint::Committed,
    WriteCheckpoint::Observed,
    WriteCheckpoint::StagingReleased,
];

#[test]
fn every_post_commit_crash_preserves_the_write_for_upload() {
    for (index, checkpoint) in CRASH_POINTS.into_iter().enumerate() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let f = runtime.block_on(async {
            let (f, handle) = Fixture::new().await;
            handle.set_uploads_paused(true).await.unwrap();
            handle
                .write_with_files(
                    |batch| {
                        batch.put_file("files", "obsolete", b"obsolete".to_vec());
                        Ok(())
                    },
                    |sql| {
                        sql.execute("INSERT INTO files(id) VALUES('obsolete')", [])?;
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
            .args([
                "--exact",
                "test_utils::tests::crashing_local_write",
                "--nocapture",
            ])
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
                    .read(|sql| Ok(
                        sql.query_row("SELECT count(*) FROM files", [], |r| r.get::<_, i64>(0))?
                    ))
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
    let point: usize = std::env::var("COVEN_GUARANTEE_POINT")
        .unwrap()
        .parse()
        .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db =
            coven_database::DatabaseBuilder::new(StoreLayout::new(root.into()).store_dir(&store))
                .synced_tables(tables())
                .migrations(vec![Migration::addition(1, "records", SCHEMA)])
                .clock(Arc::new(FixedClock::new(
                    UNIX_EPOCH + Duration::from_secs(1001),
                )))
                .id_source(Arc::new(SequentialIds::new()))
                .open()
                .await
                .unwrap();
        let next = std::cell::Cell::new(0);
        db.test_on_write_checkpoint(move |checkpoint| {
            let index = next.get();
            assert_eq!(checkpoint, CRASH_POINTS[index]);
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

#[tokio::test]
async fn app_reads_and_writes_finish_while_a_sync_pass_is_held() {
    let (f, handle) = Fixture::new().await;
    note(&handle, "before sync".into()).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_read(ObjectPath::floor(f.directory.id()), entered, released)
        .await;
    handle.start_sync().await.unwrap();
    entering.await.unwrap();
    assert!(matches!(
        *handle.subscribe_sync_status().borrow(),
        SyncStatus::Syncing
    ));
    assert_eq!(read_note(&handle).await, "before sync");
    note(&handle, "during sync".into()).await;
    assert_eq!(read_note(&handle).await, "during sync");
    assert!(matches!(
        *handle.subscribe_sync_status().borrow(),
        SyncStatus::Syncing
    ));
    let queued = handle.test_queued_writes().await.unwrap();
    release.send(()).unwrap();
    synced(&handle).await;
    stop(&handle).await;
    assert_uploaded(&f, &handle, &queued).await;
    handle.close().await.unwrap();
}

#[tokio::test]
async fn identical_app_files_have_distinct_stored_ciphertext() {
    let (f, handle) = Fixture::new().await;
    handle.set_uploads_paused(true).await.unwrap();
    let content = vec![42; 2 * 65536 + 17];
    let input = content.clone();
    handle
        .write_with_files(
            move |batch| {
                for id in ["first", "second"] {
                    batch.put_file("files", id, input.clone());
                }
                Ok(())
            },
            |sql| {
                sql.execute("INSERT INTO files(id) VALUES('first'),('second')", [])?;
                Ok(())
            },
        )
        .await
        .unwrap();
    handle.set_uploads_paused(false).await.unwrap();
    let mut uploads = handle.subscribe(|sql| {
        Ok(
            sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    });
    while uploads
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        != 0
    {}
    assert!(handle
        .pending()
        .await
        .unwrap()
        .iter()
        .all(|record| !matches!(record.subject, PendingSubject::File { .. })));
    let objects = f
        .storage
        .list(&ObjectPrefix::files(f.directory.id()))
        .await
        .unwrap();
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
    for id in ["first", "second"] {
        let file = handle.file_ref("files", id).await.unwrap();
        // Each file is stored at the path its attaching write fixed.
        assert!(objects
            .iter()
            .any(|object| object.path
                == ObjectPath::file(f.directory.id(), file.uploader(), file.id())));
        handle.evict_file(&file).await.unwrap();
        assert_eq!(handle.read_file(&file).await.unwrap(), content);
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
    assert_eq!(
        f.storage.read(&first).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    // Growth past the first snapshot's size requests a newer one, which
    // supersedes it once past grace and outside the floor.
    grow_and_retain(&f, &handle, "y".repeat(4 * 1024 * 1024 + 1)).await;
    let new = crate::tests::snapshots(&f.storage, store).await;
    assert!(!new.is_empty());
    assert!(new
        .iter()
        .all(|snapshot| snapshot.path != old[0].path
            && snapshot.path.device() == old[0].path.device()));
    assert_eq!(
        f.storage.read(&old[0].path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    assert_eq!(read_note(&handle).await, "y".repeat(4 * 1024 * 1024 + 1));
    handle.close().await.unwrap();
}

/// Admit a member with `keys` through an invite; returns the id it fixed.
async fn admit_member(f: &Fixture, handle: &CovenHandle, keys: &MemberKeys) -> MemberId {
    use coven_format::{
        sealed_single::{SingleChunkObject, SingleChunkPrefix},
        Object,
    };
    let invite = handle
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::S3AccessKey {
                access_key_id: "member-key".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    let code = coven_format::codes::InviteCode::from_text(&invite.code).unwrap();
    let path = ObjectPath::join_request(f.directory.id(), invite.id);
    let request = Object::JoinRequest(coven_format::objects::JoinRequest {
        invite: invite.id,
        member: code.member,
        keys: keys.public_keys(),
        device_name: "Departing member".into(),
    })
    .encode()
    .unwrap();
    let prefix = SingleChunkPrefix::JoinRequest;
    let chunk = code
        .secret
        .join_request_key()
        .seal_object_chunk(path.as_str(), &prefix.encode().unwrap(), 0, 0, &request)
        .unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&prefix.encode_chunk(&chunk).unwrap());
    let bytes = SingleChunkObject::JoinRequest {
        chunk: &chunk,
        signature: keys.sign_object(path.as_str(), &hash.finish()),
    }
    .encode()
    .unwrap();
    // The peer supplies a signed request; admission itself uses the app API.
    f.storage.create(&path, &bytes).await.unwrap();
    f.sync(handle).await;
    let request = handle
        .subscribe_invites()
        .borrow()
        .iter()
        .find_map(|invite| invite.request.clone())
        .unwrap();
    handle.approve_join_request(&request).await.unwrap();
    assert!(handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|m| m.id == code.member));
    code.member
}

#[tokio::test]
async fn removal_preserves_key_material_and_publishes_only_the_member_target() {
    let (f, handle) = Fixture::new().await;
    let removed = admit_member(&f, &handle, member(29).keys()).await;
    let circle = handle.circles().create("Shared").await.unwrap();
    handle.circles().add_member(circle, &removed).await.unwrap();
    let before = f.keys.unlock().unwrap().unwrap().to_secret_bytes();
    handle.remove_member(&removed).await.unwrap();
    assert!(!handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|m| m.id == removed));
    assert!(!handle
        .circles()
        .members(circle)
        .await
        .unwrap()
        .iter()
        .any(|m| m.member == removed));
    let ring = f.keys.unlock().unwrap().unwrap();
    assert_eq!(ring.to_secret_bytes().as_bytes(), before.as_bytes());
    handle.close().await.unwrap();
}
