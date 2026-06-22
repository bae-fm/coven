//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `MockSyncStorage`; a second device
//! pulls and applies them through a real [`crate::database::Database`], exercising
//! the real `pull_changes` + blob plumbing.

use std::collections::HashMap;

use crate::blob::BlobPlan;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::cycle;
use crate::sync::envelope;
use crate::sync::membership::{MemberRole, MembershipAction, MembershipCoord};
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::service::{SyncCycleError, SyncService};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

#[tokio::test]
async fn pull_applies_remote_changeset_and_surfaces_row_changes() {
    let storage = MockSyncStorage::new();

    // Source device records a note as changeset seq 1.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    // Second device pulls.
    let db2 = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let (updated, result) =
        pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &NoopBlobPlan).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "First"
    );
    assert!(result
        .row_changes
        .iter()
        .any(|c| c.table == "notes" && c.pk() == Some("n1")));
}

#[tokio::test]
async fn pull_skips_changeset_from_newer_schema() {
    let storage = MockSyncStorage::new();

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Future', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION + 1);

    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobPlan,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.skipped_schema, 1);
    // The cursor must NOT advance past a genuine newer-schema changeset: it
    // becomes applicable once this app updates, and an already-running device
    // never re-bootstraps, so advancing would strand its rows forever. Leaving
    // the cursor put re-fetches seq 1 after the upgrade.
    assert_eq!(updated.get("dev1"), None);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

#[tokio::test]
async fn pull_does_not_advance_cursor_past_a_blob_failed_changeset() {
    let storage = MockSyncStorage::new();

    // Source dev1: seq 1 references a photo blob; seq 2 is a plain note.
    let db1 = open_test_db();
    let cs1 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'One', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('ph1', 'n1', 'attach', '0000000001001-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs1, SCHEMA_VERSION);
    // The photo blob is intentionally never uploaded, so seq 1's blob download
    // fails on the puller (a transient cloud unavailability, in the real world).
    let cs2 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n2', 'Two', NULL, '0000000002000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 2, &cs2, SCHEMA_VERSION);

    // The puller resolves note_photos to blobs, so seq 1's missing blob fails
    // while seq 2 (no blob) would succeed.
    let dst_photos = tempfile::tempdir().expect("dst photos");
    let plan = PhotoBlobPlan {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &plan,
    )
    .await;

    assert!(
        result.asset_downloads_failed,
        "seq 1's blob download must fail"
    );
    // The cursor must NOT jump to 2 past the blob-failed seq 1 — otherwise seq 1's
    // blob would never be re-fetched. It stays before seq 1 so the next cycle
    // resumes there.
    assert_ne!(
        updated.get("dev1"),
        Some(&2),
        "cursor must not advance past the blob-failed seq",
    );
    assert_eq!(
        updated.get("dev1"),
        None,
        "cursor stays before the blob-failed seq 1",
    );
}

#[tokio::test]
async fn blob_round_trips_through_storage_via_blob_plan() {
    let storage = MockSyncStorage::new();

    // Source: a note + a cover photo, with the photo file present locally.
    let src_photos = tempfile::tempdir().expect("src photos");
    std::fs::write(src_photos.path().join("p1"), b"PHOTOBYTES").expect("write photo");
    let src_plan = PhotoBlobPlan {
        dir: src_photos.path().to_path_buf(),
    };

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;

    // Upload the blobs the changeset references, then publish the changeset.
    // Resolve the public scope to the internal key scope exactly as the push
    // loop does — `PhotoBlobPlan` only emits Derived/Master, which pass through.
    let changes = crate::changeset::walk(&cs).expect("walk");
    for b in src_plan.blobs_to_push(&changes) {
        let data = std::fs::read(&b.local_path).expect("read photo");
        let resolved = db1
            .resolve_blob_scope(b.scope.clone())
            .await
            .expect("resolve scope");
        storage
            .put_blob(&b.namespace, &b.id, resolved, b.cloud_path.as_deref(), data)
            .await
            .expect("put_blob");
    }
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    // Destination pulls; its plan points photos at its own directory.
    let dst_photos = tempfile::tempdir().expect("dst photos");
    let dst_plan = PhotoBlobPlan {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_t, ld) = temp_library_dir();
    let (_updated, result) =
        pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &dst_plan).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let downloaded = std::fs::read(dst_photos.path().join("p1")).expect("downloaded photo");
    assert_eq!(downloaded, b"PHOTOBYTES");
}

/// A changeset that references a blob whose local file is missing must abort the
/// cycle, not skip the upload and publish the row anyway. `sync` returns the
/// outgoing changeset for the caller to push; aborting here (Err) is what keeps
/// the caller from publishing a row whose blob was never uploaded — every puller
/// would 404 on that blob forever.
#[tokio::test]
async fn sync_aborts_when_a_referenced_blob_file_is_missing() {
    let storage = MockSyncStorage::new();

    // A note + a photo row, but the photo file is deliberately never written, so
    // the push loop's existence check sees the local file as absent.
    let src_photos = tempfile::tempdir().expect("src photos");
    let src_plan = PhotoBlobPlan {
        dir: src_photos.path().to_path_buf(),
    };

    let db1 = open_test_db();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('p1', 'n1', 'attach', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    let outgoing = db1
        .take_changeset_and_suspend()
        .await
        .expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let (_t1, ld1) = temp_library_dir();
    let result = service
        .sync(
            &db1,
            &test_synced_tables(),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
            &src_plan,
        )
        .await;
    db1.resume_session().await.expect("resume");

    // `SyncResult` is not Debug; inspect only the error side for the assert message.
    let err = result.err();
    assert!(
        matches!(err, Some(SyncCycleError::BlobMissing(_))),
        "a missing local blob file must abort the cycle, got {err:?}",
    );
}

/// A blob plan that names each `note_photos` row at a readable cloud path
/// (`{note_id}/{kind}.jpg`) so the plain scheme stores it browsably, instead of
/// the content-addressed shard. The local file still lives under `dir`.
struct ReadablePhotoBlobPlan {
    dir: std::path::PathBuf,
}

impl ReadablePhotoBlobPlan {
    fn refs(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        use crate::changeset::ChangeOp;
        changes
            .iter()
            .filter(|c| c.table == "note_photos" && c.op == ChangeOp::Insert)
            .map(|c| {
                let id = c.pk().expect("note_photos pk");
                let note_id = c.col(1).expect("note_photos note_id");
                let kind = c.col(2).expect("note_photos kind");
                crate::blob::BlobRef {
                    namespace: "photos".to_string(),
                    id: id.to_string(),
                    local_path: self.dir.join(id),
                    scope: crate::blob::BlobScope::Master,
                    cloud_path: Some(format!("{note_id}/{kind}.jpg")),
                }
            })
            .collect()
    }
}

impl BlobPlan for ReadablePhotoBlobPlan {
    fn blobs_to_push(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        self.refs(changes)
    }
    fn blobs_to_pull(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        self.refs(changes)
    }
    fn blobs_in_db(
        &self,
        _conn: &rusqlite::Connection,
    ) -> rusqlite::Result<Vec<crate::blob::BlobRef>> {
        Ok(vec![])
    }
}

/// A plain-scheme home stores a changeset-driven blob at the consumer's readable
/// `cloud_path` (`photos/n1/cover.jpg`), not the content-addressed shard, and a
/// second device with the same plan pulls it from that readable key and recovers
/// the bytes. This is the changeset-push / changeset-pull half of the blob path
/// (the audio outbox was always consumer-controlled), end to end over a real
/// `CloudSyncStorage` in `BlobPathScheme::Plain`.
#[tokio::test]
async fn plain_scheme_blob_round_trips_at_the_readable_key() {
    let storage = CloudSyncStorage::new(
        Box::new(InMemoryCloudHome::new()),
        CloudCipher::Encrypted(EncryptionService::new_with_key(&[5u8; 32])),
        BlobPathScheme::Plain,
    );

    // Device A: a shared note + a cover photo whose file is present locally.
    // Driven through the real `SyncService::sync` + `push_changeset` so the
    // production blob-upload path keys the blob from its `cloud_path`.
    let plaintext = b"COVERART";
    let src_photos = tempfile::tempdir().expect("src photos");
    std::fs::write(src_photos.path().join("p1cover"), plaintext).expect("write photo");
    let src_plan = ReadablePhotoBlobPlan {
        dir: src_photos.path().to_path_buf(),
    };

    let db1 = open_test_db();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('p1cover', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    let outgoing = db1
        .take_changeset_and_suspend()
        .await
        .expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let (_t1, ld1) = temp_library_dir();
    let result = service
        .sync(
            &db1,
            &test_synced_tables(),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
            &src_plan,
        )
        .await
        .expect("sync");
    db1.resume_session().await.expect("resume");
    let outgoing = result.outgoing.expect("outgoing changeset");
    cycle::push_changeset(
        &storage,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        None,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push_changeset");

    // The blob lands at the readable key, not the hashed shard.
    assert!(
        storage
            .cloud_home()
            .exists("photos/n1/cover.jpg")
            .await
            .expect("exists at readable key"),
        "the blob must land at the readable cloud_path key",
    );
    let hashed = CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "photos", "p1cover", None)
        .expect("hashed key");
    assert!(
        !storage
            .cloud_home()
            .exists(&hashed)
            .await
            .expect("exists at hashed key"),
        "the hashed shard key must be absent under the plain scheme",
    );

    // Device B: a fresh DB and its own asset dir, same cloud + plain scheme,
    // pulls and downloads the cover from the readable key.
    let dst_photos = tempfile::tempdir().expect("dst photos");
    let dst_plan = ReadablePhotoBlobPlan {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_t2, ld) = temp_library_dir();
    let (_updated, result) =
        pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &dst_plan).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let downloaded =
        std::fs::read(dst_photos.path().join("p1cover")).expect("device B downloaded cover");
    assert_eq!(
        downloaded, plaintext,
        "device B recovers the source bytes from the readable plain-scheme key",
    );
}

/// Full encrypted blob round-trip through `CloudSyncStorage` (encrypted) over a
/// shared `CloudHome`. Device A publishes a note plus its cover photo via the real
/// `SyncService::sync`; the blob lands ciphertext at rest. Device B — a fresh DB
/// with its own asset directory but the same library key — pulls, downloads the
/// blob, decrypts it, and recovers the original bytes byte-for-byte.
#[tokio::test]
async fn encrypted_blob_round_trips_and_second_device_decrypts() {
    // One cloud and one library key, shared by both devices.
    let storage = CloudSyncStorage::new(
        Box::new(InMemoryCloudHome::new()),
        CloudCipher::Encrypted(EncryptionService::new_with_key(&[7u8; 32])),
        BlobPathScheme::Hashed,
    );

    // Device A: a note and its cover photo, the file present locally.
    let plaintext = b"COVER-ART-BYTES";
    let src_photos = tempfile::tempdir().expect("src photos");
    std::fs::write(src_photos.path().join("p1cover"), plaintext).expect("write photo");
    let src_plan = PhotoBlobPlan {
        dir: src_photos.path().to_path_buf(),
    };

    let db1 = open_test_db();
    // Write the changes, capture+suspend, then drive sync() with the captured
    // bytes — it gates, uploads blobs, and builds the envelope. sync() does not
    // put_changeset itself; the caller pushes the returned envelope.
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('p1cover', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    let outgoing = db1
        .take_changeset_and_suspend()
        .await
        .expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let (_t1, ld1) = temp_library_dir();
    let result = service
        .sync(
            &db1,
            &test_synced_tables(),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
            &src_plan,
        )
        .await
        .expect("sync");
    db1.resume_session().await.expect("resume");
    let outgoing = result.outgoing.expect("outgoing changeset");
    cycle::push_changeset(
        &storage,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        None,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push_changeset");

    // At rest the cover photo is ciphertext, not the source bytes.
    let blob_key = CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "photos", "p1cover", None)
        .expect("hashed key");
    let at_rest = storage
        .cloud_home()
        .read(&blob_key)
        .await
        .expect("blob present in cloud");
    assert_ne!(
        at_rest, plaintext,
        "blob must be encrypted at rest in the cloud"
    );

    // Device B: a fresh DB and its own asset directory, same cloud + key.
    let dst_photos = tempfile::tempdir().expect("dst photos");
    let dst_plan = PhotoBlobPlan {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_t, ld) = temp_library_dir();
    let (updated, result) =
        pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &dst_plan).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithPhoto"
    );
    let downloaded =
        std::fs::read(dst_photos.path().join("p1cover")).expect("device B downloaded photo");
    assert_eq!(
        downloaded, plaintext,
        "device B must recover the source bytes after decrypting with the shared key"
    );
}

#[tokio::test]
async fn pull_rejects_unsigned_changeset_when_chain_exists() {
    let storage = MockSyncStorage::new();

    // A membership chain exists. Coven always signs its changesets, so an
    // unsigned one here is forged; a chained library must reject it.
    let founder = UserKeypair::generate();
    let entry = founder_entry(&founder, "2026-03-01T00:00:00Z");
    let entry_bytes = serde_json::to_vec(&entry).expect("serialize founder");
    storage
        .put_membership_entry(&hex::encode(founder.public_key), 1, entry_bytes)
        .await
        .expect("put founder entry");

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobPlan,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    // An unsigned changeset in a chain library is the genuinely-bad case: nothing
    // can authorize it, so it is skipped and the cursor advances (the client must
    // not get stuck on it) and the skip is surfaced for a UI warning.
    assert_eq!(result.skipped_unauthorized, 1);
    assert_eq!(updated.get("dev1"), Some(&1));
}

/// The membership-lag case: an authorized member's changeset is pulled before the
/// entry that authorizes them shows up in a LIST. The changeset names its grant
/// coordinate, so the puller fetches that exact entry by key (strongly consistent)
/// and applies the changeset instead of dropping it forever.
#[tokio::test]
async fn pull_applies_member_changeset_whose_authorizing_entry_lags_via_grant_coord() {
    let storage = MockSyncStorage::new();

    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);

    // Chain: founder (owner) at (owner, 1); the owner then adds `member` at
    // (owner, 2).
    let founder = founder_entry(&owner, "0000000001000-0000-owner");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();
    let add_member = make_entry(
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "0000000002000-0000-owner",
    );
    storage
        .put_membership_entry(&owner_pk, 2, serde_json::to_vec(&add_member).unwrap())
        .await
        .unwrap();
    // The authorizing entry hasn't propagated to LIST yet — but a keyed GET serves
    // it (read-after-write), which is exactly what the grant coordinate enables.
    storage.hide_membership_from_list(&owner_pk, 2);

    // The member authors a signed changeset bound to its grant coordinate (owner, 2).
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromMember', NULL, '0000000003000-0000-member', '2026-01-01')",
        ],
    )
    .await;
    let grant = MembershipCoord {
        author: owner_pk.clone(),
        seq: 2,
    };
    let packed = envelope::pack_signed(
        "devMember",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:00:00Z",
        &member,
        Some(grant),
        &cs,
    );
    storage.put_changeset_packed("devMember", 1, packed);

    // The puller's cycle-start chain (from LIST) is founder-only, so the member
    // looks unauthorized — but the grant coordinate resolves the gap.
    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobPlan,
    )
    .await;

    assert_eq!(
        result.changesets_applied, 1,
        "the member's changeset must apply once its grant entry is fetched by key"
    );
    assert_eq!(result.skipped_unauthorized, 0);
    assert_eq!(updated.get("devMember"), Some(&1));
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

/// The forged case: a signed changeset by a non-member that names a grant
/// coordinate which does not exist. A keyed GET returns NotFound (authoritative),
/// so the changeset is genuinely unauthorized — skipped, cursor advanced (not
/// stuck), and surfaced.
#[tokio::test]
async fn pull_skips_signed_changeset_whose_grant_coordinate_does_not_exist() {
    let storage = MockSyncStorage::new();

    let owner = UserKeypair::generate();
    let outsider = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);

    let founder = founder_entry(&owner, "0000000001000-0000-owner");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    // An outsider signs a changeset and points its grant at an entry that does not
    // exist.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-x', '2026-01-01')",
        ],
    )
    .await;
    let bogus = MembershipCoord {
        author: owner_pk.clone(),
        seq: 99,
    };
    let packed = envelope::pack_signed(
        "devX",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:00:00Z",
        &outsider,
        Some(bogus),
        &cs,
    );
    storage.put_changeset_packed("devX", 1, packed);

    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobPlan,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.skipped_unauthorized, 1);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(
        updated.get("devX"),
        Some(&1),
        "a forged changeset is skipped (cursor advances) so the client is not stuck"
    );
}

/// The stall case the fix must NOT reintroduce: a signed changeset whose grant
/// names an entry that EXISTS and parses but is invalid (a self-signed Add by a
/// non-owner). Merging it makes chain validation fail; that must be treated as
/// unauthorized (skip + advance), never as indeterminate — otherwise a member who
/// can write a bogus membership object stalls every other device's pull forever.
#[tokio::test]
async fn pull_skips_signed_changeset_whose_grant_entry_is_invalid_without_stalling() {
    let storage = MockSyncStorage::new();

    let owner = UserKeypair::generate();
    let attacker = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let attacker_pk = hex::encode(attacker.public_key);

    // The real chain: founder only.
    let founder = founder_entry(&owner, "0000000001000-0000-owner");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    // A bogus, validly-self-signed Add the attacker plants at (attacker, 1): it
    // parses and its signature verifies, but the attacker is not an owner, so it
    // cannot authorize a write. Hidden from LIST so the cycle-start chain loads
    // clean (founder-only) and we exercise the grant-resolution path, not the
    // separate cycle-start fail-open.
    let bogus = make_entry(
        &attacker,
        MembershipAction::Add,
        &attacker,
        MemberRole::Member,
        "0000000002000-0000-attacker",
    );
    storage
        .put_membership_entry(&attacker_pk, 1, serde_json::to_vec(&bogus).unwrap())
        .await
        .unwrap();
    storage.hide_membership_from_list(&attacker_pk, 1);

    // The attacker signs a changeset and names its bogus self-Add as the grant.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000003000-0000-attacker', '2026-01-01')",
        ],
    )
    .await;
    let grant = MembershipCoord {
        author: attacker_pk.clone(),
        seq: 1,
    };
    let packed = envelope::pack_signed(
        "devAttacker",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:00:00Z",
        &attacker,
        Some(grant),
        &cs,
    );
    storage.put_changeset_packed("devAttacker", 1, packed);

    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobPlan,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.skipped_unauthorized, 1);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    // The crucial anti-stall assertion: the cursor ADVANCES past the bogus
    // changeset rather than staying put and re-failing every cycle.
    assert_eq!(
        updated.get("devAttacker"),
        Some(&1),
        "an invalid grant must skip+advance, never stall the pull"
    );
}

/// The push side binds an outgoing changeset to this device's own membership
/// grant, so a puller in the lag window can resolve it.
#[tokio::test]
async fn sync_binds_outgoing_changeset_to_our_membership_grant() {
    let storage = MockSyncStorage::new();
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);

    // A chain whose founder is this very device.
    let founder = founder_entry(&owner, "0000000001000-0000-owner");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    let db1 = open_test_db();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Mine', NULL, 1, '0000000002000-0000-dev1', '2026-01-01')",
    )
    .await;
    let outgoing = db1
        .take_changeset_and_suspend()
        .await
        .expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let (_t, ld) = temp_library_dir();
    let result = service
        .sync(
            &db1,
            &test_synced_tables(),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-03-01T00:00:00Z",
            "",
            &owner,
            &ld,
            &NoopBlobPlan,
        )
        .await
        .expect("sync");
    db1.resume_session().await.expect("resume");

    let packed = result.outgoing.expect("outgoing changeset").packed;
    let (env, _cs) = envelope::unpack(&packed).expect("unpack");
    assert_eq!(
        env.membership_grant,
        Some(MembershipCoord {
            author: owner_pk,
            seq: 1
        }),
        "the outgoing changeset must be bound to our founder grant"
    );
    assert_eq!(env.author_pubkey, Some(hex::encode(owner.public_key)));
}

mod schema_version_too_old_display {
    use crate::sync::pull::PullError;

    #[test]
    fn names_app_and_versions_and_recovery() {
        let err = PullError::SchemaVersionTooOld {
            local_version: 3,
            min_version: 5,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("Update the app"),
            "missing recovery verb: {msg}"
        );
        assert!(msg.contains("v5"), "missing required version: {msg}");
        assert!(msg.contains("v3"), "missing current version: {msg}");
    }
}
