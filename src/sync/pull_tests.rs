//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `MockSyncStorage`; a second device
//! pulls and applies them through a real [`crate::database::Database`], exercising
//! the real `pull_changes` + blob plumbing.

use std::collections::HashMap;

use crate::blob::BlobSource;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::cycle;
use crate::sync::envelope;
use crate::sync::membership::{MemberRole, MembershipAction, MembershipCoord};
use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;
use crate::sync::pull::PullError;
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
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &ld,
        &NoopBlobSource,
    )
    .await;

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
        &NoopBlobSource,
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
    let plan = PhotoBlobSource {
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
    // The blob-bearing row must NOT have been applied: with download-before-apply
    // (#111), seq 1's failed blob means seq 1 is skipped whole -- "row present,
    // blob missing" never exists. (Before #111 the row was applied and only the
    // cursor held back, so n1 was visible with no photo file on disk.)
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "seq 1's row must not be applied when its blob download fails",
    );
    // seq 2 is never reached -- the pull stops this device at the failed seq 1.
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n2'").await,
        "seq 2 is not processed past the blob-failed seq 1",
    );
    assert_eq!(result.changesets_applied, 0);
}

/// A changeset whose envelope `changeset_size` disagrees with the actual trailing
/// bytes is corrupt or tampered: it must be rejected, not applied. The size is one
/// of the fields the signature covers, so a signed changeset whose bytes were
/// altered after signing surfaces here (and at the signature check); an unsigned
/// one is caught by this gate alone. Either way the bytes failed their own
/// integrity check and the row must never land. The cursor is held back so a
/// transient on-download corruption re-fetches next cycle.
#[tokio::test]
async fn pull_rejects_changeset_whose_declared_size_mismatches_actual_bytes() {
    let storage = MockSyncStorage::new();

    // A real changeset from dev1, packed into an envelope whose `changeset_size`
    // is deliberately wrong (one byte short of the actual payload), as a truncated
    // or tampered download would be.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Corrupt', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let env = envelope::ChangesetEnvelope {
        device_id: "dev1".to_string(),
        seq: 1,
        schema_version: SCHEMA_VERSION,
        message: String::new(),
        timestamp: "2026-02-10T00:00:00Z".to_string(),
        // The lie: the envelope claims one fewer byte than the payload carries.
        changeset_size: cs.len() - 1,
        author_pubkey: None,
        membership_grant: None,
        signature: None,
    };
    storage.put_changeset_packed("dev1", 1, envelope::pack(&env, &cs));

    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a size-mismatched changeset must not be applied",
    );
    // The cursor must NOT advance: the corruption may be a transient bad download,
    // so the next cycle re-fetches this seq rather than stranding it forever.
    assert_eq!(
        updated.get("dev1"),
        None,
        "cursor must not advance past a size-mismatched changeset",
    );
    // Surfaced through the same channel as the other genuinely-bad-data, hold-the-
    // cursor cases (a walk failure), so the cycle does not report a clean idle.
    assert!(
        result.asset_downloads_failed,
        "a size-mismatched changeset must be surfaced as a bad-data hold, not swallowed",
    );
}

#[tokio::test]
async fn blob_round_trips_through_storage_via_blob_plan() {
    let storage = MockSyncStorage::new();

    // Source: a note + a cover photo, with the photo file present locally.
    let src_photos = tempfile::tempdir().expect("src photos");
    std::fs::write(src_photos.path().join("p1"), b"PHOTOBYTES").expect("write photo");
    let src_plan = PhotoBlobSource {
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
    // loop does — `PhotoBlobSource` only emits Derived/Master, which pass through.
    let changes = crate::changeset::walk(&cs).expect("walk");
    for b in changes.iter().flat_map(|c| src_plan.blobs_for_change(c)) {
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
    let dst_plan = PhotoBlobSource {
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

/// A blob source that maps each `note_photos` row to an `OnDemand` blob in the
/// `audio` namespace under the master key — the audio-style class that is uploaded
/// on push but not downloaded on pull (it streams on demand, fetched on first
/// read). The mirror of [`PhotoBlobSource`] for the other retention class.
struct AudioBlobSource {
    dir: std::path::PathBuf,
}

impl AudioBlobSource {
    fn refs(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        note_photos_refs(
            changes,
            &self.dir,
            "audio",
            &|_kind, _note_id| crate::blob::BlobScope::Master,
            crate::blob::BlobSync::OnDemand,
        )
    }
}

impl BlobSource for AudioBlobSource {
    fn blobs_for_change(&self, change: &crate::changeset::RowChange) -> Vec<crate::blob::BlobRef> {
        self.refs(std::slice::from_ref(change))
    }
    fn blobs_in_db(
        &self,
        conn: &rusqlite::Connection,
    ) -> rusqlite::Result<Vec<crate::blob::BlobRef>> {
        note_photos_refs_from_db(
            conn,
            &self.dir,
            "audio",
            &|_kind, _note_id| crate::blob::BlobScope::Master,
            crate::blob::BlobSync::OnDemand,
        )
    }
}

/// An `OnDemand` blob uploads on push but is NOT downloaded on the puller's pull:
/// the changeset's row still applies, the blob's bytes land in the cloud, but the
/// puller leaves them there (no local file) to fetch on first read. This is the
/// retention-class split — the same flow as the `Mirrored` round-trip above,
/// asserting the opposite pull outcome. The source drives the real
/// `SyncService::sync`, so the inline push upload runs for the `OnDemand` class too.
#[tokio::test]
async fn on_demand_blob_uploads_on_push_but_is_not_downloaded_on_pull() {
    let storage = MockSyncStorage::new();

    // Source: a shared note + an audio row, the audio file present locally.
    let audio_bytes = b"AUDIO-PAYLOAD";
    let src_audio = tempfile::tempdir().expect("src audio");
    std::fs::write(src_audio.path().join("audio1"), audio_bytes).expect("write audio");
    let src_plan = AudioBlobSource {
        dir: src_audio.path().to_path_buf(),
    };

    let db1 = open_test_db();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    let outgoing = db1
        .take_changeset_and_suspend()
        .await
        .expect("capture outgoing");

    // Drive the real push path: it uploads the OnDemand blob inline (same as a
    // Mirrored one — the class only changes the pull side), then returns the
    // changeset for the caller to publish.
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

    // The OnDemand blob's bytes reached the cloud on push.
    assert!(
        storage
            .get_blob("audio", "audio1", crate::blob::ResolvedScope::Master, None)
            .await
            .is_ok(),
        "the OnDemand blob uploaded on push (present in the cloud)",
    );

    // Destination pulls; its plan points audio at its own dir.
    let dst_audio = tempfile::tempdir().expect("dst audio");
    let dst_plan = AudioBlobSource {
        dir: dst_audio.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_t, ld) = temp_library_dir();
    let (updated, result) =
        pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &dst_plan).await;

    // The row applied and the cursor advanced — the OnDemand blob never blocks the
    // apply, and its absence is not a download failure.
    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithAudio",
        "the row carrying the OnDemand blob still reaches the peer",
    );
    // ...but the blob was NOT downloaded to the puller's disk: OnDemand is fetched
    // on first read, not mirrored on pull.
    assert!(
        !dst_audio.path().join("audio1").exists(),
        "an OnDemand blob must NOT be downloaded on pull — it stays in the cloud for on-demand fetch",
    );
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
    let src_plan = PhotoBlobSource {
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

/// A blob source that names each `note_photos` row at a readable cloud path
/// (`{note_id}/{kind}.jpg`) so the plain scheme stores it browsably, instead of
/// the content-addressed shard. The local file still lives under `dir`. The blobs
/// are `Mirrored`, so a pulling device downloads them.
struct ReadablePhotoBlobSource {
    dir: std::path::PathBuf,
}

impl ReadablePhotoBlobSource {
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
                    sync: crate::blob::BlobSync::Mirrored,
                }
            })
            .collect()
    }
}

impl BlobSource for ReadablePhotoBlobSource {
    fn blobs_for_change(&self, change: &crate::changeset::RowChange) -> Vec<crate::blob::BlobRef> {
        self.refs(std::slice::from_ref(change))
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
        UserKeypair::generate(),
    );

    // Device A: a shared note + a cover photo whose file is present locally.
    // Driven through the real `SyncService::sync` + `push_changeset` so the
    // production blob-upload path keys the blob from its `cloud_path`.
    let plaintext = b"COVERART";
    let src_photos = tempfile::tempdir().expect("src photos");
    std::fs::write(src_photos.path().join("p1cover"), plaintext).expect("write photo");
    let src_plan = ReadablePhotoBlobSource {
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
    let dst_plan = ReadablePhotoBlobSource {
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
        UserKeypair::generate(),
    );

    // Device A: a note and its cover photo, the file present locally.
    let plaintext = b"COVER-ART-BYTES";
    let src_photos = tempfile::tempdir().expect("src photos");
    std::fs::write(src_photos.path().join("p1cover"), plaintext).expect("write photo");
    let src_plan = PhotoBlobSource {
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
    let dst_plan = PhotoBlobSource {
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
    // A membership chain exists. Coven always signs its changesets, so an
    // unsigned one here is forged; a chained library must reject it. The mock
    // signs the head it publishes for dev1 with the founder's keypair (a current
    // member), so the head passes its authorization check and pull goes on to
    // examine — and reject — the unsigned changeset behind it.
    let founder = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(founder.clone());

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
        &NoopBlobSource,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    // An unsigned changeset carries no grant coordinate, so it is genuinely
    // unauthorized (nothing can authorize it) — not a can't-authorize-yet lag. It
    // is surfaced as a rejected-unauthorized changeset, and the cursor advances
    // past it so the device isn't stuck refetching it.
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert_eq!(result.rejected_unauthorized[0].device_id, "dev1");
    assert_eq!(result.rejected_unauthorized[0].seq, 1);
    assert_eq!(result.rejected_unauthorized[0].author, None);
    assert_eq!(updated.get("dev1"), Some(&1));
}

/// Owner anchoring (issue #95/#102): a puller with a pinned owner refuses a chain
/// whose founder is a different key — the wipe-and-refound takeover — rather than
/// adopting it and authorizing the attacker.
#[tokio::test]
async fn pull_refuses_a_chain_not_anchored_to_the_pinned_owner() {
    let storage = MockSyncStorage::new();

    // The attacker wiped membership/* and refounded themselves as Owner.
    let attacker = UserKeypair::generate();
    let forged = founder_entry(&attacker, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(
            &hex::encode(attacker.public_key),
            1,
            serde_json::to_vec(&forged).unwrap(),
        )
        .await
        .unwrap();

    // The puller has the real owner pinned (a different key).
    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key))
        .await
        .unwrap();

    let result = pull_into_result(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;
    assert!(
        matches!(result, Err(PullError::MembershipTampered(_))),
        "a chain founded by a non-owner must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// Owner anchoring (issue #104/#102): a puller with a pinned owner refuses an
/// empty membership listing — the chain was wiped — rather than falling open to
/// "no chain, accept everything."
#[tokio::test]
async fn pull_refuses_wiped_membership_when_owner_pinned() {
    let storage = MockSyncStorage::new();

    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key))
        .await
        .unwrap();

    let result = pull_into_result(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;
    assert!(
        matches!(result, Err(PullError::MembershipTampered(_))),
        "an empty chain with a pinned owner must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// `list_membership_entries` itself failing (a flaky LIST, not bad chain data) on
/// an owner-pinned library must abort the cycle, not fall open to "no chain,
/// accept everything" — the first failure mode #88 names. A real chain and a
/// changeset are staged so the old fall-open behavior would load no chain and
/// apply the changeset unvalidated; the fail-closed path must instead surface the
/// error and apply nothing.
#[tokio::test]
async fn pull_aborts_when_membership_listing_fails_on_owner_pinned_library() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let storage = MockSyncStorage::with_keypair(owner.clone());

    // A founder entry + a changeset the owner authored: without the fail-closed
    // guard the cycle would (fail to list, drop to chain=None, then) apply this.
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'X', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset(&owner_pk, 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // Membership can't even be listed: the cycle must abort rather than continue
    // with authorization silently disabled.
    storage.fail_membership_listing();

    let result = pull_into_result(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;
    assert!(
        matches!(result, Err(PullError::Storage(_))),
        "a membership-list failure on an owner-pinned library must abort the cycle, got {:?}",
        result.map(|_| ()),
    );
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "nothing is applied when membership cannot be verified",
    );
}

/// The positive case: a chain founded by the pinned owner is accepted, and a
/// changeset signed by that owner applies.
#[tokio::test]
async fn pull_accepts_a_chain_anchored_to_the_pinned_owner() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    // The owner's device is the mock: it signs the head it publishes for
    // `devOwner` with the owner keypair, so the head's author is a current member
    // and passes the head-authorization check.
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    // The owner authors a signed changeset.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromOwner', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    let packed = envelope::pack_signed(
        "devOwner",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:00:00Z",
        &owner,
        // The founder entry at (owner, 1) is what authorizes the owner to write.
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 1,
        }),
        &cs,
    );
    storage.put_changeset_packed("devOwner", 1, packed);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devOwner"), Some(&1));
}

/// Issue #84 — the membership-propagation lag, the core bug. A member's signed
/// changeset is pulled BEFORE the LIST that rebuilds the chain shows the Add that
/// authorizes them (membership entries and changesets are separate, unordered
/// object streams). The cycle-start chain does not authorize the member, so the
/// old code skipped the changeset and advanced the cursor — losing it forever.
/// Now the changeset carries the coordinate of its authorizing entry; a direct,
/// read-after-write-consistent GET resolves that entry even though the LIST lags,
/// and the changeset applies. It must NOT be lost.
#[tokio::test]
async fn pull_resolves_a_changeset_whose_authorizing_entry_lags_the_listing() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let member = UserKeypair::generate();
    // The mock signs every head with the owner key, so the member's device head is
    // owner-authored — a current member — and passes the head-authorization check
    // even while the member's own Add is still invisible to the LIST.
    let storage = MockSyncStorage::with_keypair(owner.clone());

    // Founder at (owner, 1); the owner adds the member as a Member at (owner, 2).
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();
    let add_member = make_entry(
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
    storage
        .put_membership_entry(&owner_pk, 2, serde_json::to_vec(&add_member).unwrap())
        .await
        .unwrap();
    // ...but the LIST hasn't caught up to the member's Add yet. A keyed GET of
    // (owner, 2) still resolves it — the eventual-consistency gap issue #84 closes.
    storage.hide_membership_from_listing(&owner_pk, 2);

    // The member authors a signed changeset, stamping the grant coordinate of the
    // entry that authorizes them: (owner, 2), the Add that is lagging the LIST.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromLaggingMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 2,
        }),
        &cs,
    );
    storage.put_changeset_packed("devM", 1, packed);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    // The lagging entry was fetched by coordinate and the changeset applied — not
    // dropped as non-member, and not surfaced as a rejection.
    assert_eq!(result.changesets_applied, 1);
    assert!(result.rejected_unauthorized.is_empty());
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devM"), Some(&1));
}

/// Issue #84 — the other side of the split: a genuinely unauthorized changeset
/// (here authored by a key that is NOT in the chain at all, with a grant
/// coordinate that resolves to an entry that doesn't authorize it) is judged
/// against the exact entry it names, found wanting, and SKIPPED — cursor advanced
/// so the device isn't stuck — and surfaced for a UI warning. The grant points at
/// the founder entry (owner, 1), which authorizes the owner, not the outsider, so
/// merging it still leaves the outsider unauthorized.
#[tokio::test]
async fn pull_skips_and_surfaces_a_forged_changeset_whose_grant_does_not_authorize_it() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let outsider = UserKeypair::generate();
    // Head signed by the owner (a current member) so the head passes its check and
    // pull reaches the changeset-level judgment.
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    // The outsider authors a signed changeset but, lacking any Add of their own,
    // names the founder entry (owner, 1) as their grant. The signature is valid
    // (it's their own key) but the named entry authorizes the owner, not them.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-devX', '2026-01-01')",
        ],
    )
    .await;
    let packed = envelope::pack_signed(
        "devX",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &outsider,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 1,
        }),
        &cs,
    );
    storage.put_changeset_packed("devX", 1, packed);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    // Nothing applied; the changeset is surfaced as rejected-unauthorized and the
    // cursor advances past it (the device must not stall on forged content).
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert_eq!(result.rejected_unauthorized[0].device_id, "devX");
    assert_eq!(result.rejected_unauthorized[0].seq, 1);
    assert_eq!(
        result.rejected_unauthorized[0].author,
        Some(hex::encode(outsider.public_key))
    );
    assert_eq!(updated.get("devX"), Some(&1));
}

/// Issue #84 — a removed member's changeset is skipped, not applied. The owner
/// added the member at (owner, 2) then removed them at (owner, 3); the member's
/// changeset names its (still-valid-looking) Add grant (owner, 2), but the puller
/// already holds the Remove, so merging the grant into the full chain still leaves
/// the author unauthorized. Surfaced and cursor-advanced, like any forged write.
#[tokio::test]
async fn pull_skips_a_removed_members_changeset() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();
    let add_member = make_entry(
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
    storage
        .put_membership_entry(&owner_pk, 2, serde_json::to_vec(&add_member).unwrap())
        .await
        .unwrap();
    let remove_member = make_entry(
        &owner,
        MembershipAction::Remove,
        &member,
        MemberRole::Member,
        "2026-03-01T00:03:00Z",
    );
    storage
        .put_membership_entry(&owner_pk, 3, serde_json::to_vec(&remove_member).unwrap())
        .await
        .unwrap();

    // The removed member authors a changeset stamping their old grant (owner, 2).
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromRemoved', NULL, '0000000004000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:04:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 2,
        }),
        &cs,
    );
    storage.put_changeset_packed("devM", 1, packed);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert_eq!(
        result.rejected_unauthorized[0].author,
        Some(hex::encode(member.public_key))
    );
    assert_eq!(updated.get("devM"), Some(&1));
}

/// The fail-open the auditor reproduced: a non-empty but MALFORMED chain (here a
/// founder with a corrupt signature, so `download_chain` errors) on a pinned-owner
/// library must be refused — not treated as "no chain, accept everything", which
/// would let an attacker who wipes+refounds with junk get their changesets applied.
#[tokio::test]
async fn pull_refuses_a_malformed_chain_when_owner_pinned() {
    let storage = MockSyncStorage::new();

    // A non-empty listing whose entry won't validate (broken founder signature).
    let attacker = UserKeypair::generate();
    let mut bad = founder_entry(&attacker, "2026-03-01T00:00:00Z");
    bad.signature = "00".to_string();
    storage
        .put_membership_entry(
            &hex::encode(attacker.public_key),
            1,
            serde_json::to_vec(&bad).unwrap(),
        )
        .await
        .unwrap();

    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key))
        .await
        .unwrap();

    let result = pull_into_result(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;
    assert!(
        matches!(result, Err(PullError::MembershipTampered(_))),
        "a malformed chain on a pinned-owner library must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// A head whose author is not a current member is skipped by `pull_changes` when
/// a chain exists: its changesets are never fetched and its cursor never advances,
/// even though the changeset bytes sit in the bucket. A forged head (anyone with
/// the bucket credential can write one) must not drive a per-seq fetch loop.
#[tokio::test]
async fn pull_skips_a_head_authored_by_a_non_member() {
    // The mock signs every head it publishes with `outsider`, who is not in the
    // chain — so the head it writes for `dev1` fails the membership check.
    let outsider = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(outsider);

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    // dev1 has a changeset in the bucket (its head is published by the mock,
    // signed by the non-member `outsider`).
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromForgedHead', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    // The head was skipped, so dev1 was never examined and its cursor is absent.
    assert_eq!(updated.get("dev1"), None);
    // The skipped head is also absent from the surfaced remote heads.
    assert!(!result.remote_heads.iter().any(|h| h.device_id == "dev1"));
}

/// The honored case: a head authored by a current member (here a second device
/// whose head and changeset the owner signs) is kept, and its changeset applies.
#[tokio::test]
async fn pull_honors_a_head_authored_by_a_current_member() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    // The mock is the owner's device, so the head it publishes for `devA` is
    // owner-signed — a current member.
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromMember', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
    .await;
    let packed = envelope::pack_signed(
        "devA",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:00:00Z",
        &owner,
        // The founder entry at (owner, 1) is what authorizes the owner to write.
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 1,
        }),
        &cs,
    );
    storage.put_changeset_packed("devA", 1, packed);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devA"), Some(&1));
}

/// A `min_schema_version` floor signed by a non-owner is ignored: it must NOT trip
/// `SchemaVersionTooOld` even when its version exceeds ours. The floor is an
/// owner-only control; a Member- or Follower-signed (or bucket-planted) one is a
/// freeze attempt.
#[tokio::test]
async fn pull_ignores_min_schema_version_from_a_non_owner() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let member = UserKeypair::generate();

    // The mock signs the floor with `member` (a current member, but not an owner).
    let storage = MockSyncStorage::with_keypair(member.clone());

    // Chain: owner founds, then adds `member` as a Member.
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();
    let add_member = make_entry(
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
    storage
        .put_membership_entry(&owner_pk, 2, serde_json::to_vec(&add_member).unwrap())
        .await
        .unwrap();

    // A member-signed floor above our version.
    storage
        .set_min_schema_version(SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // The pull must succeed (the non-owner floor is ignored), not error with
    // SchemaVersionTooOld.
    let result = pull_into_result(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;
    assert!(
        result.is_ok(),
        "a non-owner floor must be ignored, not trip SchemaVersionTooOld; got {:?}",
        result.map(|_| ()),
    );
}

/// A `min_schema_version` floor signed by a current owner IS honored: a version
/// above ours trips `SchemaVersionTooOld`, the same refuse-to-sync the owner
/// intends after a breaking migration.
#[tokio::test]
async fn pull_honors_min_schema_version_from_a_current_owner() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    // The mock is the owner's device, so the floor it sets is owner-signed.
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    storage
        .set_min_schema_version(SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let result = pull_into_result(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;
    assert!(
        matches!(
            result,
            Err(PullError::SchemaVersionTooOld {
                min_version,
                ..
            }) if min_version == SCHEMA_VERSION + 1
        ),
        "an owner-signed floor above our version must trip SchemaVersionTooOld; got {:?}",
        result.map(|_| ()),
    );
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
