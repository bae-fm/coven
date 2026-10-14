use crate::*;
use coven_crypto::custody::{InMemoryCustody, KeyringCustody, StoreKeychain};
use coven_database::test_utils::WriteCheckpoint;
use coven_format::{
    sealed_snapshot::SnapshotObjectPrefix, sealed_write::WriteObjectPrefix,
    write_stream::WriteHeaderFrame,
};
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
            Provenance::AppProvided,
            CacheFill::CacheLazy,
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
                    prefix: "store".into(),
                    endpoint: None,
                    profile: None,
                })
                .clock(clock.clone())
                .transfer_limits(8 * 1024 * 1024, 65536)
                .build()
                .unwrap(),
        );
        let directory = app
            .create_store(&layout, "Guarantees", ids.clone())
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
            .migrations(vec![Migration::sql(1, "records", SCHEMA)])
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

fn open_write(
    bytes: &[u8],
    path: &ObjectPath,
    ring: &StoreKeyring,
    signer: &coven_crypto::SigningKey,
) -> coven_format::write::WriteRecord {
    let prefix_len = WriteObjectPrefix::length(bytes).unwrap();
    let aad = &bytes[..prefix_len];
    let prefix = WriteObjectPrefix::decode(aad).unwrap();
    let header_len = WriteObjectPrefix::header_chunk_length(&bytes[prefix_len..]).unwrap();
    let sealed =
        WriteObjectPrefix::header_chunk(&bytes[prefix_len..prefix_len + header_len]).unwrap();
    let mut plain = ring
        .store_key(prefix.store_key)
        .unwrap()
        .derive()
        .open_object_chunk(path.as_str(), aad, 0, 0, sealed)
        .unwrap();
    let header = WriteHeaderFrame::decode(&plain).unwrap();
    let mut layout = prefix
        .opened_header(
            &plain,
            header.parts.iter().map(|p| p.plaintext_length).collect(),
        )
        .unwrap();
    let mut offset = prefix_len + header_len;
    while let Some(chunk) = layout.next_chunk() {
        let length = layout.chunk_length(&bytes[offset..]).unwrap();
        let (_, sealed) = layout
            .decode_chunk(&bytes[offset..offset + length])
            .unwrap();
        let key = match header.parts[chunk.section as usize - 1].audience {
            Audience::Store => ring.store_key(chunk.key).unwrap().derive(),
            Audience::Circle(circle) => ring.circle_key(circle, chunk.key).unwrap().derive(),
        };
        plain.extend(
            key.open_object_chunk(path.as_str(), aad, chunk.section, chunk.index, sealed)
                .unwrap(),
        );
        offset += length;
    }
    let signature = layout.read_signature(&bytes[offset..]).unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&bytes[..offset]);
    signer
        .verify_object(path.as_str(), &hash.finish(), &signature)
        .unwrap();
    layout.finish(&[]).unwrap();
    coven_format::write_stream::decode_plaintext(&plain).unwrap()
}

async fn assert_uploaded(
    f: &Fixture,
    handle: &CovenHandle,
    queued: &[coven_format::write::WriteRecord],
) {
    assert!(!queued.is_empty());
    f.sync(handle).await;
    assert!(handle.test_queued_writes().await.unwrap().is_empty());
    let ring = f.keys.unlock().unwrap().unwrap();
    for record in queued {
        let id = record.header.position;
        let path =
            ObjectPath::device_log(f.directory.id(), id.device, id.number.try_into().unwrap());
        let bytes = f.storage.read(&path).await.unwrap();
        assert_eq!(
            open_write(&bytes, &path, &ring, &f.member.keys().public_keys().signing),
            *record
        );
    }
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
// (§16.5). Crashes while removing released bytes are covered where upload
// completion releases them, in coven-database's file removal tests.
const CRASH_POINTS: [WriteCheckpoint; 6] = [
    WriteCheckpoint::Committed,
    WriteCheckpoint::Observed,
    WriteCheckpoint::StagingReleased,
    // Cleanup commits separately from the app write; exercise both occurrences.
    WriteCheckpoint::Committed,
    WriteCheckpoint::Observed,
    WriteCheckpoint::Finished,
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
                .migrations(vec![Migration::sql(1, "records", SCHEMA)])
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
        .hold_next_listing(
            ObjectPrefix::device_logs(f.directory.id()),
            entered,
            released,
        )
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
    let mut uploads = handle.subscribe_uploads();
    loop {
        if uploads.next().await.unwrap().files.is_empty() {
            break;
        }
    }
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
    let header = coven_format::file::FileObject::decode(&a).unwrap().header();
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
async fn newer_snapshots_delete_covered_logs_and_older_own_snapshots() {
    async fn grow_and_retain(f: &Fixture, handle: &CovenHandle, body: String) {
        let logs = f
            .storage
            .list(&ObjectPrefix::device_logs(f.directory.id()))
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);
        note(handle, body).await;
        f.sync(handle).await;
        // This pass sees the position posted after snapshot publication.
        f.sync(handle).await;
        assert!(f
            .storage
            .list(&ObjectPrefix::device_logs(f.directory.id()))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            f.storage.read(&logs[0].path).await.unwrap_err().failure(),
            StorageFailure::NotFound
        );
    }
    let (f, handle) = Fixture::new().await;
    note(&handle, "before snapshot".into()).await;
    f.sync(&handle).await;
    grow_and_retain(&f, &handle, "x".repeat(1024 * 1024 + 1)).await;
    let old = f
        .storage
        .list(&ObjectPrefix::snapshots(f.directory.id()))
        .await
        .unwrap();
    assert_eq!(old.len(), 1);
    let prefix = |bytes: &[u8]| {
        SnapshotObjectPrefix::decode(&bytes[..SnapshotObjectPrefix::length(bytes).unwrap()])
            .unwrap()
    };
    let first = prefix(&f.storage.read(&old[0].path).await.unwrap());
    assert_eq!(first.writes.0[0].number, 2);
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('marker','another write','store')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    f.sync(&handle).await;
    // The first snapshot holds the note three times: its synced row, merge
    // row and original change kept as source evidence (D7 section 5).
    grow_and_retain(&f, &handle, "y".repeat(4 * 1024 * 1024 + 1)).await;
    let new = f
        .storage
        .list(&ObjectPrefix::snapshots(f.directory.id()))
        .await
        .unwrap();
    assert_eq!(new.len(), 1);
    assert_ne!(new[0].path, old[0].path);
    assert_eq!(new[0].path.device(), old[0].path.device());
    let second = prefix(&f.storage.read(&new[0].path).await.unwrap());
    assert!(first.writes.0.iter().all(|id| second.writes.covers(*id)));
    assert_eq!(second.writes.0[0].number, 4);
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
            MemberRole::Member,
            InviteAccess::S3AccessKey {
                access_key_id: "member-key".into(),
                secret_access_key: SecretText::new("secret".into()),
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
        .subscribe_join_requests()
        .borrow()
        .first()
        .unwrap()
        .clone();
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
    let mut removals = 0;
    for object in f
        .storage
        .list(&ObjectPrefix::store_logs(f.directory.id()))
        .await
        .unwrap()
    {
        let bytes = f.storage.read(&object.path).await.unwrap();
        let envelope = coven_format::sealed_single::SingleChunkObject::decode(&bytes).unwrap();
        let coven_format::sealed_single::SingleChunkObject::StoreLog { key, .. } = &envelope else {
            panic!("store log")
        };
        let plain = ring
            .store_key(*key)
            .unwrap()
            .derive()
            .open_object_chunk(
                object.path.as_str(),
                &envelope.prefix().encode().unwrap(),
                0,
                0,
                envelope.chunk(),
            )
            .unwrap();
        let coven_format::Object::StoreLog(entry) = coven_format::Object::decode(&plain).unwrap()
        else {
            panic!("entry")
        };
        use coven_format::store_log::StoreChange;
        match entry.change {
            StoreChange::CreateStore { key, key_hash, .. } => {
                assert_eq!(key_hash, ring.store_key(key).unwrap().hash());
            }
            StoreChange::CreateCircle {
                circle,
                key,
                key_hash,
                ..
            } => {
                assert_eq!(key_hash, ring.circle_key(circle, key).unwrap().hash());
            }
            StoreChange::RemoveMember { member } => {
                assert_eq!(member, removed);
                removals += 1;
            }
            _ => (),
        }
    }
    assert_eq!(removals, 1);
    handle.close().await.unwrap();
}
