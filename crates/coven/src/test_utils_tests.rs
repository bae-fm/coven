use crate::*;
use coven_crypto::custody::InMemoryCustody;
use coven_database::test_utils::WriteCheckpoint;
use coven_format::{
    sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix},
    sealed_write::WriteObjectPrefix,
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

fn member(seed: u8) -> MemberKeys {
    MemberKeys::from_secret_bytes(&[b"CVMK\x01".as_slice(), &[seed; 64]].concat()).unwrap()
}

struct Fixture {
    app: TestCoven,
    directory: StoreDir,
    layout: StoreLayout,
    ids: IdSourceRef,
    clock: Arc<FixedClock>,
    storage: Arc<MemoryStorage>,
    keys: Arc<InMemoryCustody<StoreKeyring>>,
    member: MemberKeys,
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
        let f = Self {
            app,
            directory,
            layout,
            ids,
            clock,
            storage,
            keys: Arc::new(InMemoryCustody::empty()),
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
    author: &MemberId,
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
    author
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
        let path = ObjectPath::device_log(id.device, id.number.try_into().unwrap());
        let bytes = f.storage.read(&path).await.unwrap();
        assert_eq!(
            open_write(&bytes, &path, &ring, &f.member.member_id()),
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
const CRASH_POINTS: [WriteCheckpoint; 8] = [
    WriteCheckpoint::Committed,
    WriteCheckpoint::Observed,
    WriteCheckpoint::StagingReleased,
    WriteCheckpoint::FileRemoved,
    WriteCheckpoint::FileRemovalRecorded,
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
            handle.set_uploads_paused(true);
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
        .hold_next_listing(ObjectPrefix::device_logs(), entered, released)
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
    handle.set_uploads_paused(true);
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
    handle.set_uploads_paused(false);
    let mut uploads = handle.subscribe_uploads();
    loop {
        let queue = uploads.next().await.unwrap();
        assert!(queue.files.iter().all(|file| file.last_failure.is_none()));
        if queue.files.is_empty() {
            break;
        }
    }
    let objects = f.storage.list(&ObjectPrefix::files()).await.unwrap();
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
        assert_eq!(file.location(), FileLocation::Uploaded);
        handle.evict_file(&file).await.unwrap();
        assert_eq!(handle.read_file(&file).await.unwrap(), content);
    }
    handle.close().await.unwrap();
}

#[tokio::test]
async fn newer_snapshots_delete_covered_logs_and_older_own_snapshots() {
    async fn grow_and_retain(f: &Fixture, handle: &CovenHandle, body: String) {
        let logs = f.storage.list(&ObjectPrefix::device_logs()).await.unwrap();
        assert_eq!(logs.len(), 1);
        note(handle, body).await;
        f.sync(handle).await;
        // This pass sees the position posted after snapshot publication.
        f.sync(handle).await;
        assert!(f
            .storage
            .list(&ObjectPrefix::device_logs())
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
    let old = f.storage.list(&ObjectPrefix::snapshots()).await.unwrap();
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
    grow_and_retain(&f, &handle, "y".repeat(2 * 1024 * 1024 + 1)).await;
    let new = f.storage.list(&ObjectPrefix::snapshots()).await.unwrap();
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
    assert_eq!(read_note(&handle).await, "y".repeat(2 * 1024 * 1024 + 1));
    handle.close().await.unwrap();
}

async fn admit_member(f: &Fixture, handle: &CovenHandle, keys: &MemberKeys) {
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
    let path = ObjectPath::join_request(invite.id);
    let request = Object::JoinRequest(coven_format::objects::JoinRequest {
        invite: invite.id,
        keys: coven_format::store_log::MemberPublicKeys {
            signing: keys.member_id(),
            sealing: keys.sealing_public_key(),
        },
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
        .get_members()
        .await
        .unwrap()
        .iter()
        .any(|m| m.id == keys.member_id()));
}

#[tokio::test]
async fn removal_seals_the_next_write_and_snapshots_with_rotated_keys() {
    let (f, handle) = Fixture::new().await;
    let removed = member(29);
    admit_member(&f, &handle, &removed).await;
    let circle = handle.circles().create("Shared").await.unwrap();
    handle
        .circles()
        .add_member(circle, &removed.member_id())
        .await
        .unwrap();
    let ring = f.keys.unlock().unwrap().unwrap();
    let old_store_id = ring.store_key_ids().next().unwrap();
    let old_circle_id = ring.circle_key_ids(circle).next().unwrap();
    let store_path = ObjectPath::store_key(old_store_id, &removed.member_id());
    let old_store = removed
        .open_store_key(
            store_path.as_str(),
            &f.storage.read(&store_path).await.unwrap(),
        )
        .unwrap();
    let circle_path = ObjectPath::circle_key(circle, old_circle_id, &removed.member_id());
    let old_circle = removed
        .open_circle_key(
            circle_path.as_str(),
            &f.storage.read(&circle_path).await.unwrap(),
        )
        .unwrap();
    handle.remove_member(&removed.member_id()).await.unwrap();
    assert!(!handle
        .get_members()
        .await
        .unwrap()
        .iter()
        .any(|m| m.id == removed.member_id()));
    let circle_text = circle.to_string();
    handle.write(move |sql| {
        sql.execute("INSERT INTO notes VALUES('store','after removal','store'),('circle','after removal',?1)", [circle_text])?;
        Ok(())
    }).await.unwrap();
    let queued = handle.test_queued_writes().await.unwrap();
    assert_eq!(queued.len(), 1);
    f.sync(&handle).await;
    let logs = f.storage.list(&ObjectPrefix::device_logs()).await.unwrap();
    assert_eq!(logs.len(), 1);
    let path = &logs[0].path;
    let bytes = f.storage.read(path).await.unwrap();
    let prefix_len = WriteObjectPrefix::length(&bytes).unwrap();
    let aad = &bytes[..prefix_len];
    let prefix = WriteObjectPrefix::decode(aad).unwrap();
    assert_ne!(prefix.store_key, old_store_id);
    assert_eq!(prefix.part_keys.len(), 2);
    let new_ring = f.keys.unlock().unwrap().unwrap();
    assert_eq!(
        open_write(&bytes, path, &new_ring, &f.member.member_id()),
        queued[0]
    );
    let header_len = WriteObjectPrefix::header_chunk_length(&bytes[prefix_len..]).unwrap();
    let chunk =
        WriteObjectPrefix::header_chunk(&bytes[prefix_len..prefix_len + header_len]).unwrap();
    assert!(matches!(
        old_store
            .derive()
            .open_object_chunk(path.as_str(), aad, 0, 0, chunk),
        Err(CryptoError::Authentication)
    ));
    let header_bytes = new_ring
        .store_key(prefix.store_key)
        .unwrap()
        .derive()
        .open_object_chunk(path.as_str(), aad, 0, 0, chunk)
        .unwrap();
    let header = WriteHeaderFrame::decode(&header_bytes).unwrap();
    let mut layout = prefix
        .clone()
        .opened_header(
            &header_bytes,
            header.parts.iter().map(|p| p.plaintext_length).collect(),
        )
        .unwrap();
    let mut offset = prefix_len + header_len;
    while let Some(coordinate) = layout.next_chunk() {
        let length = layout.chunk_length(&bytes[offset..]).unwrap();
        let (_, chunk) = layout
            .decode_chunk(&bytes[offset..offset + length])
            .unwrap();
        let old = match header.parts[coordinate.section as usize - 1].audience {
            Audience::Store => {
                assert_eq!(coordinate.key, prefix.store_key);
                old_store.derive()
            }
            Audience::Circle(id) => {
                assert_eq!(id, circle);
                assert_ne!(coordinate.key, old_circle_id);
                old_circle.derive()
            }
        };
        assert!(matches!(
            old.open_object_chunk(
                path.as_str(),
                aad,
                coordinate.section,
                coordinate.index,
                chunk
            ),
            Err(CryptoError::Authentication)
        ));
        offset += length;
    }
    assert!(f
        .storage
        .list(&ObjectPrefix::snapshots())
        .await
        .unwrap()
        .is_empty());
    handle.reset_store().await.unwrap();
    handle.circles().reset(circle).await.unwrap();
    let snapshots = f.storage.list(&ObjectPrefix::snapshots()).await.unwrap();
    assert_eq!(snapshots.len(), 2);
    for object in snapshots {
        let bytes = f.storage.read(&object.path).await.unwrap();
        let length = SnapshotObjectPrefix::length(&bytes).unwrap();
        let (snapshot, signature) =
            SnapshotObjectPrefix::decode_signed(&bytes[..length + 64]).unwrap();
        f.member
            .member_id()
            .verify_prefix(object.path.as_str(), &bytes[..length], &signature)
            .unwrap();
        assert!(snapshot.writes.covers(queued[0].header.position));
        let (new, old, key_path) = match snapshot.audience {
            Audience::Store => {
                assert_eq!(snapshot.key, prefix.store_key);
                (
                    new_ring.store_key(snapshot.key).unwrap().derive(),
                    old_store.derive(),
                    ObjectPath::store_key(snapshot.key, &removed.member_id()),
                )
            }
            Audience::Circle(id) => {
                assert_eq!(id, circle);
                assert!(prefix.part_keys.contains(&snapshot.key));
                (
                    new_ring.circle_key(id, snapshot.key).unwrap().derive(),
                    old_circle.derive(),
                    ObjectPath::circle_key(id, snapshot.key, &removed.member_id()),
                )
            }
        };
        assert_eq!(
            f.storage.read(&key_path).await.unwrap_err().failure(),
            StorageFailure::NotFound
        );
        let mut layout = SnapshotObjectLayout::new();
        let mut offset = length + 64;
        while offset < bytes.len() - 64 {
            let size = layout.chunk_length(&bytes[offset..]).unwrap();
            let index = layout.index();
            let chunk = layout.decode_chunk(&bytes[offset..offset + size]).unwrap();
            assert!(!new
                .open_object_chunk(object.path.as_str(), &bytes[..length], 0, index, chunk)
                .unwrap()
                .is_empty());
            assert!(matches!(
                old.open_object_chunk(object.path.as_str(), &bytes[..length], 0, index, chunk),
                Err(CryptoError::Authentication)
            ));
            offset += size;
        }
        let signature = layout.read_signature(&bytes[offset..]).unwrap();
        let mut hash = coven_crypto::ObjectHasher::new();
        hash.update(&bytes[..offset]);
        f.member
            .member_id()
            .verify_object(object.path.as_str(), &hash.finish(), &signature)
            .unwrap();
    }
    handle.close().await.unwrap();
}
