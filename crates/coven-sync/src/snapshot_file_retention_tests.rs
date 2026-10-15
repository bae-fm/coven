use super::*;
use crate::StoreLogSync;
use coven_crypto::{
    custody::{InMemoryCustody, KeySession},
    MemberIdentity, StoreKey, StoreKeyring,
};
use coven_format::store_log::StoreChange;
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix, Storage};

async fn sync(f: &Fixture) -> (StoreLogSync, MemberIdentity, crate::DeviceLogSync) {
    let member = MemberIdentity::generate(coven_foundation::id_source::MemberId(
        uuid::Uuid::from_u128(0x5f),
    ))
    .unwrap();
    let key = KeyId(uuid::Uuid::from_u128(500));
    let store_keys = Arc::new(
        KeySession::store(Arc::new(InMemoryCustody::new(StoreKeyring::new(
            StoreKey::generate(key).unwrap(),
        ))))
        .unwrap(),
    );
    let member_keys =
        Arc::new(KeySession::member(Arc::new(InMemoryCustody::new(member.clone()))).unwrap());
    let writes = crate::DeviceLogSync::new(
        f.storage.clone(),
        f.database.clone(),
        store_keys.clone(),
        member_keys.clone(),
        f.clock.clone(),
        f.send.clone(),
    );
    let mut sync = StoreLogSync::new(
        f.storage.clone(),
        f.database.clone(),
        store_keys,
        member_keys,
        f.clock.clone(),
        f.send.clone(),
        f.ids.clone(),
        f.directory.clone(),
        Arc::new(
            crate::RetryWaits::open(f.database.clone(), f.clock.clone())
                .await
                .unwrap(),
        ),
        crate::RestoreCodeCustody::Unkept,
    );
    sync.make_and_upload_entry(StoreChange::CreateStore {
        access: coven_format::MemberAccess::ProviderAccount("owner".into()),
        store: f.directory.id(),
        name: f.directory.settings().unwrap().name,
        admin: member.keys().public_keys(),
        key,
        key_hash: sync.test_key(coven_merge::Audience::Store, key),
        device_name: "Uploader".into(),
    })
    .await
    .unwrap();
    (sync, member, writes)
}

fn path(f: &Fixture, file: &FileRef) -> ObjectPath {
    f.path(file)
}

/// A fixture on one clock, storage's and the device's, that the test moves.
async fn timed_fixture() -> (Fixture, Arc<coven_foundation::clock::FixedClock>) {
    let clock = Arc::new(coven_foundation::clock::FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    ));
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock.clone()).await;
    (f, clock)
}

/// One storage day and a second on, observed, so every snapshot landed so far
/// is a mature historical witness for log deletion (§15).
async fn mature(sync: &StoreLogSync, f: &Fixture, clock: &coven_foundation::clock::FixedClock) {
    use coven_foundation::clock::Clock;
    clock.set(clock.now() + Duration::from_secs(86400 + 1));
    sync.observe_clock(&f.database.local_store_log().await.unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn an_excluded_snapshot_prevents_proving_file_absence() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, author, mut writes) = sync(&f).await;
    let file = f.uploaded("old", vec![31; CHUNK]).await;
    // A snapshot is sent once its own writes are stored (§15).
    writes.upload_writes().await.unwrap();
    sync.write_snapshot(&mut writes, Audience::Store)
        .await
        .unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files", [])?;
            Ok(())
        })
        .await
        .unwrap();
    writes.upload_writes().await.unwrap();
    let object = f
        .storage
        .list(&ObjectPrefix::snapshots(f.directory.id()))
        .await
        .unwrap()
        .remove(0);
    let mut bytes = f.storage.read(&object.path).await.unwrap();
    use coven_format::sealed_snapshot::SnapshotObjectPrefix;
    let end = SnapshotObjectPrefix::length(&bytes).unwrap();
    let mut prefix = SnapshotObjectPrefix::decode(&bytes[..end]).unwrap();
    prefix.key = KeyId(uuid::Uuid::from_u128(501));
    let clear = prefix.encode().unwrap();
    let signature = author.keys().sign_prefix(object.path.as_str(), &clear);
    bytes.splice(..end, clear);
    bytes[end..end + 64].copy_from_slice(signature.as_bytes());
    f.storage.delete(&object.path).await.unwrap();
    f.storage.create(&object.path, &bytes).await.unwrap();
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&f, &file)).await.is_ok());
    f.close().await;
}

/// §15, §16.5: a log write this device refused is not read again, so it
/// cannot help prove a file absent. Its object stays, and so does every
/// file whose absence would need it, even one no readable data names.
#[tokio::test]
async fn a_refused_log_write_blocks_proving_file_absence() {
    for refused in [false, true] {
        let (f, clock) = timed_fixture().await;
        let (mut sync, _, mut writes) = sync(&f).await;
        let old = f.uploaded("old", vec![31; CHUNK]).await;
        f.database
            .write(|sql| {
                sql.execute("DELETE FROM files WHERE id='old'", [])?;
                Ok(())
            })
            .await
            .unwrap();
        writes.upload_writes().await.unwrap();
        assert!(writes.post_positions().await.unwrap());
        sync.write_snapshot(&mut writes, Audience::Store)
            .await
            .unwrap();
        // A later write, uncovered, stays as a retained log.
        f.uploaded("kept", vec![32; CHUNK]).await;
        writes.upload_writes().await.unwrap();
        let logs = f
            .storage
            .list(&ObjectPrefix::device_logs(f.directory.id()))
            .await
            .unwrap();
        let later = logs.last().unwrap();
        if refused {
            f.storage
                .corrupt_byte(&later.path, later.size as usize - 1)
                .await
                .unwrap();
        }
        mature(&sync, &f, &clock).await;
        sync.run_retention().await.unwrap();
        assert_eq!(f.storage.read(&path(&f, &old)).await.is_ok(), refused);
        assert!(f.storage.read(&later.path).await.is_ok());
        f.close().await;
    }
}

#[tokio::test]
async fn file_references_survive_in_waiting_writes_and_kept_snapshots() {
    let (f, clock) = timed_fixture().await;
    let (mut sync, _, mut writes) = sync(&f).await;
    let old = f.uploaded("old", vec![31; CHUNK]).await;
    let kept = f.uploaded("kept", vec![32; CHUNK]).await;
    // A snapshot is sent once its own writes are stored (§15).
    writes.upload_writes().await.unwrap();
    sync.write_snapshot(&mut writes, Audience::Store)
        .await
        .unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='old'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&f, &old)).await.is_ok());
    assert!(f.storage.read(&path(&f, &kept)).await.is_ok());
    writes.upload_writes().await.unwrap();
    assert!(writes.post_positions().await.unwrap());
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&f, &old)).await.is_ok());
    sync.write_snapshot(&mut writes, Audience::Store)
        .await
        .unwrap();
    mature(&sync, &f, &clock).await;
    sync.run_retention().await.unwrap();
    let objects = f
        .storage
        .list(&ObjectPrefix::files(f.directory.id()))
        .await
        .unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].path, path(&f, &kept));
    f.close().await;
}

#[tokio::test]
async fn a_replaced_files_upload_stays_while_its_attaching_write_names_it() {
    let (f, clock) = timed_fixture().await;
    let (mut sync, _, mut writes) = sync(&f).await;
    let (old, new) = super::uploads::retargeted_upload(&f).await;
    // The waiting attaching write names A's fixed path.
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&f, &old)).await.is_ok());
    writes.upload_writes().await.unwrap();
    assert!(writes.post_positions().await.unwrap());
    sync.write_snapshot(&mut writes, Audience::Store)
        .await
        .unwrap();
    mature(&sync, &f, &clock).await;
    sync.run_retention().await.unwrap();
    // Nothing retained names A any more; B still waits in the queue.
    assert!(f
        .storage
        .list(&ObjectPrefix::files(f.directory.id()))
        .await
        .unwrap()
        .is_empty());
    let queue = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].file, new);
    f.close().await;
}

#[tokio::test]
async fn locally_protected_files_reuse_the_checked_snapshot_prefix() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, _, mut writes) = sync(&f).await;
    f.uploaded("kept", vec![32; CHUNK]).await;
    // A snapshot is sent once its own writes are stored (§15).
    writes.upload_writes().await.unwrap();
    sync.write_snapshot(&mut writes, Audience::Store)
        .await
        .unwrap();
    // The first retention after publication checks the new prefix by range
    // once; later evaluations reuse it.
    sync.run_retention().await.unwrap();
    let object = f
        .storage
        .list(&ObjectPrefix::snapshots(f.directory.id()))
        .await
        .unwrap()
        .remove(0);
    let start = f.storage.reads().await.len();
    sync.run_retention().await.unwrap();
    let reads = f.storage.reads().await;
    assert_eq!(
        reads[start..]
            .iter()
            .filter(|(path, _, _)| path == &object.path)
            .map(|(_, _, bytes)| bytes)
            .sum::<u64>(),
        0
    );
    f.close().await;
}

/// §16.5: a file uploaded after its row was deleted becomes a candidate when
/// its upload completes, and is deleted once no retained object names it. A
/// damaged retained snapshot cannot prove that, so the file stays. Retention
/// never lists `files/`.
#[tokio::test]
async fn a_late_upload_goes_once_nothing_retained_names_it() {
    for damaged in [false, true] {
        let (f, clock) = timed_fixture().await;
        let (mut sync, _, mut writes) = sync(&f).await;
        let kept = f.uploaded("kept", vec![32; CHUNK]).await;
        f.files.set_uploads_paused(true).await.unwrap();
        let late = f.attach("files", "late", vec![33; CHUNK]).await;
        f.database
            .write(|sql| {
                sql.execute("DELETE FROM files WHERE id='late'", [])?;
                Ok(())
            })
            .await
            .unwrap();
        writes.upload_writes().await.unwrap();
        assert!(writes.post_positions().await.unwrap());
        // The snapshot names only the kept file; the covered, posted writes
        // naming the late one go with the next retention.
        sync.write_snapshot(&mut writes, Audience::Store)
            .await
            .unwrap();
        mature(&sync, &f, &clock).await;
        sync.run_retention().await.unwrap();
        assert!(f
            .storage
            .list(&ObjectPrefix::device_logs(f.directory.id()))
            .await
            .unwrap()
            .is_empty());
        assert!(f.queued(&late).await);
        f.drain().await;
        assert!(!f.queued(&late).await);
        assert!(f.storage.read(&path(&f, &late)).await.is_ok());
        if damaged {
            let snapshot = f
                .storage
                .list(&ObjectPrefix::snapshots(f.directory.id()))
                .await
                .unwrap()
                .remove(0);
            f.storage
                .corrupt_byte(&snapshot.path, snapshot.size as usize - 1)
                .await
                .unwrap();
        }
        sync.run_retention().await.unwrap();
        assert_eq!(f.storage.read(&path(&f, &late)).await.is_ok(), damaged);
        assert!(f.storage.read(&path(&f, &kept)).await.is_ok());
        assert!(!f.storage.request_trace().iter().any(|request| matches!(
            request,
            coven_storage::test_utils::MemoryRequest::List(prefix)
                if prefix.as_str().contains("/files/")
        )));
        f.close().await;
    }
}

/// SD `RetentionSafety.fileDeletable`: one uploaded file is named by exactly
/// the retained references each case leaves, or by retained data this device
/// cannot read. Retention deletes it exactly when the model allows.
#[tokio::test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
async fn data_differential_file_deletion() {
    use serde_json::{json, Value};
    let kinds = [
        "rows",
        "losses",
        "snapshots",
        "logs",
        "localRows",
        "waitingWrites",
        "uploadQueues",
        "reversible",
    ];
    let mut cases = Vec::new();
    let mut deleted = Vec::new();
    for (case, naming) in [
        ("released", &[][..]),
        ("row", &["rows"][..]),
        ("waiting write", &["waitingWrites"][..]),
        ("queued upload", &["uploadQueues"][..]),
        ("retained log", &["logs"][..]),
        ("snapshot and deleting log", &["snapshots", "logs"][..]),
        ("unreadable snapshot", &[][..]),
    ] {
        let (f, clock) = timed_fixture().await;
        let (mut sync, _, mut writes) = sync(&f).await;
        // The fixture starts with uploads paused: these stay queued.
        let file = match case {
            "queued upload" | "unreadable snapshot" => {
                f.attach("files", "x", vec![34; CHUNK]).await
            }
            _ => f.uploaded("x", vec![34; CHUNK]).await,
        };
        if case == "snapshot and deleting log" {
            writes.upload_writes().await.unwrap();
            assert!(writes.post_positions().await.unwrap());
            sync.write_snapshot(&mut writes, Audience::Store)
                .await
                .unwrap();
        }
        if case != "row" {
            f.database
                .write(|sql| {
                    sql.execute("DELETE FROM files", [])?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        if case != "waiting write" && case != "row" {
            writes.upload_writes().await.unwrap();
            assert!(writes.post_positions().await.unwrap());
        }
        if matches!(case, "released" | "queued upload" | "unreadable snapshot") {
            sync.write_snapshot(&mut writes, Audience::Store)
                .await
                .unwrap();
        }
        if case == "unreadable snapshot" {
            let snapshot = f
                .storage
                .list(&ObjectPrefix::snapshots(f.directory.id()))
                .await
                .unwrap()
                .remove(0);
            f.storage
                .corrupt_byte(&snapshot.path, snapshot.size as usize - 1)
                .await
                .unwrap();
            // The upload completing releases the last local protection.
            f.drain().await;
        }
        // Covered logs go once their snapshot is a storage day old (§15).
        mature(&sync, &f, &clock).await;
        sync.run_retention().await.unwrap();
        let path = path(&f, &file);
        deleted.push(Value::Bool(f.storage.request_trace().iter().any(
            |request| matches!(request, coven_storage::test_utils::MemoryRequest::Delete(p) if *p == path),
        )));
        let references = if case == "unreadable snapshot" {
            Value::Null
        } else {
            Value::Object(
                kinds
                    .iter()
                    .map(|kind| {
                        let ids = if naming.contains(kind) {
                            json!([1])
                        } else {
                            json!([])
                        };
                        ((*kind).to_owned(), ids)
                    })
                    .collect(),
            )
        };
        cases.push(json!({ "file": 1, "references": references }));
        f.close().await;
    }
    let output = crate::tests::run_lean("COVEN_STORELOG_DATA_LEAN", &json!({ "files": cases }));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lean: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(deleted, lean);
}
