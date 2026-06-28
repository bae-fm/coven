//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `MockSyncStorage`; a second device
//! pulls and applies them through a real [`crate::database::Database`], exercising
//! the real `pull_changes` + blob plumbing.

use std::collections::HashMap;

use crate::blob::{local_files, CacheFill, Provenance};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::CloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::cycle;
use crate::sync::envelope;
use crate::sync::membership::{MemberRole, MembershipAction, MembershipCoord};
use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;
use crate::sync::pull::PullError;
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::service::{SyncCycleError, SyncService};
use crate::sync::session::{BlobDecl, BlobScopeSpec};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

/// The common `note_photos` blob declaration: namespace `"photos"`, master scope,
/// host-provided · `CacheEager` (a cover — fetched into the cache on pull), hashed
/// scheme.
fn photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
}

fn photo_decl_with_blob_id_column() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("cloud_path")
}

/// Store `bytes` into `ld`'s local store under blob id `id`, the way a host stores a
/// host-provided cover (its Local home) before the inline push reads it to upload.
async fn store_local(ld: &crate::library_dir::LibraryDir, id: &str, bytes: &[u8]) {
    local_files::store(ld, "photos", id, bytes)
        .await
        .expect("store host-provided blob in the local store");
}

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
    let (updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

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

    // The puller declares note_photos blob-bearing, so seq 1's missing blob fails
    // while seq 2 (no blob) would succeed.
    let db2 = open_test_db_with_blob(photo_decl());
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
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
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a size-mismatched changeset must not be applied",
    );
    // The cursor ADVANCES past the bad seq. A size mismatch that survives the
    // signature check is a permanent buggy/inconsistent encoder, not a transient
    // download glitch (truncation in transit fails the signature), so holding would
    // re-fetch the same bad object every cycle and stall this device's pull forever
    // — a single bad changeset would halt the whole fleet's sync. Skipping it
    // (logged at error) keeps the fleet syncing; the row's data, if real, recovers
    // via a later snapshot from a device that produced consistent bytes.
    assert_eq!(
        updated.get("dev1"),
        Some(&1),
        "cursor advances past a permanently-bad size-mismatched changeset",
    );
}

#[tokio::test]
async fn blob_round_trips_through_storage_via_blob_plan() {
    let storage = MockSyncStorage::new();

    // Source: a note + a cover photo. The blob id is ≥4 chars so it forms the
    // `{ab}/{cd}` cache shard.
    let db1 = open_test_db_with_blob(photo_decl());
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('p1ab', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;

    // The cover blob is in the cloud (uploaded when the row was first written),
    // keyed `photos/p1ab` master-scoped as the declaration maps it.
    storage
        .put_blob(
            "photos",
            "p1ab",
            crate::blob::ResolvedScope::Master,
            None,
            b"PHOTOBYTES".to_vec(),
        )
        .await
        .expect("put_blob");
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    // Destination pulls. A `CacheEager` photo lands in the library dir's evictable
    // cache (`storage/cache/<id>`) on pull — which coven builds from the validated id.
    let db2 = open_test_db_with_blob(photo_decl());
    let (_t, ld) = temp_library_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let downloaded = std::fs::read(ld.cache_blob_path("photos", "p1ab").expect("cache path"))
        .expect("downloaded photo");
    assert_eq!(downloaded, b"PHOTOBYTES");
}

#[tokio::test]
async fn update_uploads_and_downloads_new_blob_id_and_drops_old_local_copy() {
    let storage = MockSyncStorage::new();
    let decl = photo_decl_with_blob_id_column();
    let tables = test_synced_tables_with_blob(decl.clone());

    let db1 = open_test_db_with_blob(decl.clone());
    let (_tmp1, ld1) = temp_library_dir();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01', 'oldaaaa')",
    )
    .await;
    db1.take_changeset().await.expect("drain insert changeset");
    exec(
        &db1,
        "UPDATE note_photos SET cloud_path = 'newaaaa', _updated_at = '0000000002000-0000-dev1' \
         WHERE id = 'p-row'",
    )
    .await;
    store_local(&ld1, "newaaaa", b"NEW-BLOB").await;
    let outgoing = db1.take_changeset().await.expect("capture update");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let result = service
        .sync(
            &db1,
            &tables,
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
        )
        .await
        .expect("sync update");
    let outgoing = result.outgoing.expect("outgoing update");
    assert!(
        storage.exists("photos/newaaaa").await.unwrap(),
        "push uploads the UPDATE's new blob id"
    );
    assert!(
        !storage.exists("photos/oldaaaa").await.unwrap(),
        "push must not upload the UPDATE's old blob id"
    );

    cycle::push_changeset(
        &storage,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        None,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("publish update");

    let db2 = open_test_db_with_blob(decl);
    let (_tmp2, ld2) = temp_library_dir();
    exec(
        &db2,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev2', '2026-01-01')",
    )
    .await;
    exec(
        &db2,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', '0000000001000-0000-dev2', '2026-01-01', 'oldaaaa')",
    )
    .await;
    crate::local_blob::write_atomic(
        &ld2.cache_blob_path("photos", "oldaaaa")
            .expect("old cache path"),
        b"OLD-BLOB",
    )
    .await
    .expect("seed old cache");
    db2.take_changeset()
        .await
        .expect("drain target seed changes");

    let (_updated, pull) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld2).await;
    assert_eq!(pull.changesets_applied, 1);
    assert!(
        ld2.cache_blob_path("photos", "newaaaa")
            .expect("new cache path")
            .exists(),
        "pull downloads the UPDATE's new blob id"
    );
    assert!(
        !ld2.cache_blob_path("photos", "oldaaaa")
            .expect("old cache path")
            .exists(),
        "pull cleanup drops the UPDATE's old blob id"
    );
}

#[tokio::test]
async fn update_to_null_drops_old_local_blob_copy() {
    let storage = MockSyncStorage::new();
    let decl = photo_decl_with_blob_id_column();
    let db1 = open_test_db_with_blob(decl.clone());
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01', 'oldnull')",
    )
    .await;
    db1.take_changeset().await.expect("drain insert changeset");
    let cs = capture_bytes(
        &db1,
        &[
            "UPDATE note_photos SET cloud_path = NULL, _updated_at = '0000000002000-0000-dev1' \
          WHERE id = 'p-row'",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db_with_blob(decl);
    let (_tmp, ld) = temp_library_dir();
    exec(
        &db2,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev2', '2026-01-01')",
    )
    .await;
    exec(
        &db2,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', '0000000001000-0000-dev2', '2026-01-01', 'oldnull')",
    )
    .await;
    crate::local_blob::write_atomic(
        &ld.cache_blob_path("photos", "oldnull")
            .expect("old cache path"),
        b"OLD-BLOB",
    )
    .await
    .expect("seed old cache");
    db2.take_changeset()
        .await
        .expect("drain target seed changes");

    let (_updated, pull) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;
    assert_eq!(pull.changesets_applied, 1);
    assert!(
        !ld.cache_blob_path("photos", "oldnull")
            .expect("old cache path")
            .exists(),
        "pull cleanup drops the old blob when UPDATE removes the blob id"
    );
}

/// A `CacheLazy` blob's row still crosses to the puller, but its bytes are NOT
/// downloaded on pull (it streams on demand, fetched on first read) — the opposite
/// pull outcome from the `CacheEager` round-trip above. The split is declared:
/// `note_photos` carries a user-provided · `CacheLazy` blob here.
#[tokio::test]
async fn user_provided_blob_is_not_pushed_inline_and_not_downloaded_on_pull() {
    let storage = MockSyncStorage::new();
    let audio_tables = || {
        test_synced_tables_with_blob(BlobDecl::new(
            "audio",
            Provenance::UserProvided,
            CacheFill::CacheLazy,
        ))
    };

    // Source: a shared note + an audio row, declared user-provided · CacheLazy.
    let db1 = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
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
    let outgoing = db1.take_changeset().await.expect("capture outgoing");

    // Drive the real push path. The inline push uploads only host-provided blobs, so
    // the user-provided audio is NOT uploaded here — it goes via the durable outbox in
    // the make_remote flow, not this changeset-blob upload.
    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let (_t1, ld1) = temp_library_dir();
    let result = service
        .sync(
            &db1,
            &audio_tables(),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
        )
        .await
        .expect("sync");
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

    // The user-provided blob was NOT uploaded by the inline push.
    assert!(
        storage
            .get_blob("audio", "audio1", crate::blob::ResolvedScope::Master, None)
            .await
            .is_err(),
        "the inline push must not upload a user-provided blob",
    );

    // The blob reaches the cloud by some other path (the outbox, in the real flow);
    // plant it so the puller could fetch it, to prove the pull deliberately skips it.
    storage
        .put_blob(
            "audio",
            "audio1",
            crate::blob::ResolvedScope::Master,
            None,
            b"AUDIO-PAYLOAD".to_vec(),
        )
        .await
        .expect("plant audio blob");

    // Destination pulls.
    let db2 = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let (_t, ld) = temp_library_dir();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

    // The row applied and the cursor advanced — the CacheLazy blob never blocks the
    // apply, and its absence is not a download failure.
    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithAudio",
        "the row carrying the CacheLazy blob still reaches the peer",
    );
    // ...but the blob was NOT downloaded to the puller's cache: CacheLazy is fetched
    // on first read, not eagerly on pull.
    assert!(
        !ld.pinned_blob_path("audio", "audio1").unwrap().exists()
            && !ld.cache_blob_path("audio", "audio1").unwrap().exists(),
        "a CacheLazy blob must NOT be downloaded on pull — it stays in the cloud for on-demand fetch",
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

    // A shared note + a host-provided cover row, but the cover is deliberately never
    // stored in the local store, so the inline push finds nothing in either the local
    // store or the cache.
    let db1 = open_test_db_with_blob(photo_decl());
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('p1ab', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    let outgoing = db1.take_changeset().await.expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let (_t1, ld1) = temp_library_dir();
    let result = service
        .sync(
            &db1,
            &test_synced_tables_with_blob(photo_decl()),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
        )
        .await;
    // `SyncResult` is not Debug; inspect only the error side for the assert message.
    let err = result.err();
    assert!(
        matches!(err, Some(SyncCycleError::BlobMissing(_))),
        "an unstaged blob must abort the cycle, got {err:?}",
    );
}

/// The `note_photos` declaration for the plain (browsable) scheme: the blob's
/// readable cloud key comes from the row's `cloud_path` column.
fn readable_photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_cloud_path_column("cloud_path")
}

/// A plain-scheme home stores a changeset-driven blob at the consumer's readable
/// `cloud_path` (`photos/n1/cover.jpg`), not the content-addressed shard, and a
/// second device with the same declaration pulls it from that readable key and
/// recovers the bytes. This is the changeset-push / changeset-pull half of the blob
/// path, end to end over a real `CloudSyncStorage` in `BlobPathScheme::Plain`.
#[tokio::test]
async fn plain_scheme_blob_round_trips_at_the_readable_key() {
    let storage = CloudSyncStorage::new(
        std::sync::Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Encrypted(EncryptionService::new_with_key(&[5u8; 32])),
        BlobPathScheme::Plain,
        UserKeypair::generate(),
    );

    // Device A: a shared note + a cover photo whose file is present locally.
    // Driven through the real `SyncService::sync` + `push_changeset` so the
    // production blob-upload path keys the blob from its `cloud_path`.
    let plaintext = b"COVERART";

    let db1 = open_test_db_with_blob(readable_photo_decl());
    let (_t1, ld1) = temp_library_dir();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    // The cover's readable key lives in the row's `cloud_path` column.
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, cloud_path, _updated_at, created_at) \
         VALUES ('p1cover', 'n1', 'cover', 'n1/cover.jpg', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    // The host stages the cover into the cache before the inline push reads it.
    store_local(&ld1, "p1cover", plaintext).await;
    let outgoing = db1.take_changeset().await.expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let result = service
        .sync(
            &db1,
            &test_synced_tables_with_blob(readable_photo_decl()),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
        )
        .await
        .expect("sync");
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

    // Device B: a fresh DB and its own library dir, same cloud + plain scheme,
    // pulls and downloads the cover from the readable key.
    let db2 = open_test_db_with_blob(readable_photo_decl());
    let (_t2, ld) = temp_library_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    // A `CacheEager` cover lands in B's evictable cache on pull.
    let downloaded = std::fs::read(ld.cache_blob_path("photos", "p1cover").expect("cache path"))
        .expect("device B downloaded cover");
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
        std::sync::Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Encrypted(EncryptionService::new_with_key(&[7u8; 32])),
        BlobPathScheme::Hashed,
        UserKeypair::generate(),
    );

    // Device A: a note and its cover photo, scoped to a per-library derived key.
    let plaintext = b"COVER-ART-BYTES";
    let decl = || {
        BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
            .with_scope(BlobScopeSpec::Derived("covers".to_string()))
    };

    let db1 = open_test_db_with_blob(decl());
    let (_t1, ld1) = temp_library_dir();
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
    // The host stages the cover into the cache before the inline push reads it.
    store_local(&ld1, "p1cover", plaintext).await;
    let outgoing = db1.take_changeset().await.expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let result = service
        .sync(
            &db1,
            &test_synced_tables_with_blob(decl()),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
        )
        .await
        .expect("sync");
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

    // Device B: a fresh DB and its own library dir, same cloud + key + declaration.
    let db2 = open_test_db_with_blob(decl());
    let (_t, ld) = temp_library_dir();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithPhoto"
    );
    // A `CacheEager` cover lands in B's evictable cache on pull.
    let downloaded = std::fs::read(ld.cache_blob_path("photos", "p1cover").expect("cache path"))
        .expect("device B downloaded photo");
    assert_eq!(
        downloaded, plaintext,
        "device B must recover the source bytes after decrypting with the shared key"
    );
}

/// The inline push, after uploading a host-provided blob, decides what to do with
/// the local-store copy by the blob's `CacheFill`, not its provenance: `CacheEager`
/// warms the evictable cache (the first read is a local hit), while `CacheLazy`
/// drops the local copy outright (the cloud has the bytes; a later read fetches
/// them). Either way the local store must NOT keep a Remote blob's bytes — that
/// would read as Local. Two host-provided blobs in one subtree, one of each fill,
/// prove the split is driven by fill alone.
#[tokio::test]
async fn inline_push_warms_cache_for_eager_and_drops_local_for_lazy() {
    let storage = MockSyncStorage::new();

    // Both children host-provided, differing only in fill: the photo is CacheEager,
    // the cover CacheLazy. Both inherit the `notes` gate, so a shared note carries
    // both through the inline push in one cycle.
    let eager_decl = || BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
    let lazy_decl = || BlobDecl::new("covers", Provenance::HostProvided, CacheFill::CacheLazy);

    let db1 = open_test_db_with_user_and_host_blobs(eager_decl(), lazy_decl());
    let (_t1, ld1) = temp_library_dir();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithBlobs', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('peager01', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_covers (id, note_id, _updated_at, created_at) \
         VALUES ('clazy001', 'n1', '0000000001001-0000-dev1', '2026-01-01')",
    )
    .await;
    // The host stores both blobs in the local store (their Local home) before the
    // inline push reads them to upload.
    local_files::store(&ld1, "photos", "peager01", b"EAGER-BYTES")
        .await
        .expect("store eager blob in local store");
    local_files::store(&ld1, "covers", "clazy001", b"LAZY-BYTES")
        .await
        .expect("store lazy blob in local store");
    let outgoing = db1.take_changeset().await.expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    service
        .sync(
            &db1,
            &test_synced_tables_with_user_and_host_blobs(eager_decl(), lazy_decl()),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
        )
        .await
        .expect("sync");

    // Both blobs reached the cloud — the inline push uploads regardless of fill.
    assert!(
        storage
            .get_blob(
                "photos",
                "peager01",
                crate::blob::ResolvedScope::Master,
                None
            )
            .await
            .is_ok(),
        "the eager blob must be uploaded",
    );
    assert!(
        storage
            .get_blob(
                "covers",
                "clazy001",
                crate::blob::ResolvedScope::Master,
                None
            )
            .await
            .is_ok(),
        "the lazy blob must be uploaded",
    );

    // CacheEager: warmed into the cache, gone from the local store. The first read
    // is a local cache hit.
    assert!(
        ld1.cache_blob_path("photos", "peager01").unwrap().exists(),
        "an eager blob's local copy is moved into the cache",
    );
    assert!(
        !ld1.local_blob_path("photos", "peager01").unwrap().exists(),
        "a Remote blob's bytes must not stay in the local store (would read as Local)",
    );

    // CacheLazy: dropped from the local store, NOT placed in the cache. A later read
    // fetches it from the cloud.
    assert!(
        !ld1.local_blob_path("covers", "clazy001").unwrap().exists(),
        "a lazy blob's local copy is dropped after upload",
    );
    assert!(
        !ld1.cache_blob_path("covers", "clazy001").unwrap().exists(),
        "a lazy blob is not pre-primed into the cache — it streams on first read",
    );
}

/// When a peer applies a changeset that DELETEs a blob-bearing row (a gate retract
/// or a genuine delete), it drops that blob's local copy — both cache folders and the
/// local store — or it would leak forever once the row is gone. The peer drops only
/// its own local copy; it never writes a cloud tombstone.
#[tokio::test]
async fn applying_a_blob_bearing_delete_drops_the_local_copy() {
    let storage = MockSyncStorage::new();

    // Source dev1: a note + a CacheEager cover row, the cover present in the cloud.
    let db1 = open_test_db_with_blob(photo_decl());
    let cs1 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('pdel1234', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage
        .put_blob(
            "photos",
            "pdel1234",
            crate::blob::ResolvedScope::Master,
            None,
            b"COVERBYTES".to_vec(),
        )
        .await
        .expect("plant cover");
    storage.store_changeset("dev1", 1, &cs1, SCHEMA_VERSION);

    // dev2 pulls → the CacheEager cover lands in the evictable cache.
    let db2 = open_test_db_with_blob(photo_decl());
    let (_t, ld) = temp_library_dir();
    let (cursors, _) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;
    assert!(
        ld.cache_blob_path("photos", "pdel1234").unwrap().exists(),
        "the cover lands in the evictable cache after the first pull",
    );

    // dev1 deletes the cover row; dev2 pulls the DELETE.
    let cs2 = capture_bytes(&db1, &["DELETE FROM note_photos WHERE id = 'pdel1234'"]).await;
    storage.store_changeset("dev1", 2, &cs2, SCHEMA_VERSION);
    let (_cursors, result) = pull_into(&db2, &storage, "dev2", &cursors, &ld).await;

    assert_eq!(result.changesets_applied, 1, "the DELETE changeset applied");
    assert!(
        !ld.pinned_blob_path("photos", "pdel1234").unwrap().exists()
            && !ld.cache_blob_path("photos", "pdel1234").unwrap().exists(),
        "applying the blob-bearing DELETE drops the cache copies",
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

/// Issue #86 — a changeset whose signature does not verify (forged or corrupt in
/// transit) is SKIPPED, logged at error, and surfaced as `invalid_signatures` so
/// the host can warn; the cursor advances past it (a bad signature never becomes
/// valid, so holding would stall the device). The signature check runs before the
/// authorization judgment, so a corrupt signature is reported as an invalid
/// signature, not as unauthorized.
#[tokio::test]
async fn pull_skips_and_surfaces_a_changeset_with_an_invalid_signature() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();

    // The owner (a current member) authors a changeset that WOULD be authorized,
    // then its signature is corrupted. The signature check must reject it before
    // authorization is even considered.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Tampered', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let packed = envelope::pack_signed(
        "dev1",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &owner,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 1,
        }),
        &cs,
    );
    // Corrupt the signature: an all-zero 64-byte Ed25519 signature is well-formed
    // but never verifies. Repack without re-signing so the envelope is otherwise
    // intact (the changeset_size still matches, so this reaches the signature check
    // rather than failing envelope parsing).
    let (mut env, changeset_bytes) = envelope::unpack(&packed).unwrap();
    env.signature = Some("0".repeat(128));
    storage.put_changeset_packed("dev1", 1, envelope::pack(&env, &changeset_bytes));

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
    )
    .await;

    // Nothing applied; surfaced as an invalid signature (NOT unauthorized) and the
    // cursor advances past it so the device doesn't stall on the bad object.
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert!(result.rejected_unauthorized.is_empty());
    assert_eq!(result.invalid_signatures.len(), 1);
    assert_eq!(result.invalid_signatures[0].device_id, "dev1");
    assert_eq!(result.invalid_signatures[0].seq, 1);
    assert_eq!(updated.get("dev1"), Some(&1));
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

/// A pulled blob's `id` is the primary key of a row authored by any write-capable
/// member (or anyone with the bucket credential). It is interpolated into the
/// blob's local file path, so an unconstrained `id` lets a member's row direct a
/// blob write to an attacker-chosen file outside the library directory — an
/// arbitrary file write that clobbers config/rc/binaries on every pulling device.
/// The pull must treat an `id` (or namespace/cloud_path) that could escape the
/// library directory, or that can't form a partition prefix, as bad data: refuse
/// the write, skip the row, surface it — never write outside, never panic.
mod blob_path_traversal {
    use super::*;
    use crate::blob::ResolvedScope;

    /// A blob whose `id` climbs out of the cache directory with `..` must NOT have
    /// its bytes written outside it. coven builds the destination from the id under
    /// its library cache; without the boundary check the id would resolve to a path
    /// above the cache and the downloaded bytes land there (an arbitrary-file-write
    /// RCE); the check refuses such a row as bad data, so nothing is written outside
    /// the cache and the apply is held.
    #[tokio::test]
    async fn traversal_id_does_not_write_outside_the_blob_dir() {
        let storage = MockSyncStorage::new();

        // The attacker's blob bytes, planted in the cloud under the malicious id's
        // flat mock key (the same key the puller's `get_blob` computes for it). No
        // local file is written on the source side, so nothing escapes here.
        let evil_bytes = b"OWNED".to_vec();
        storage
            .put_blob(
                "photos",
                "x/../../../PWNED",
                ResolvedScope::Master,
                None,
                evil_bytes,
            )
            .await
            .expect("plant evil blob in the cloud");

        // The source's changeset adds a note + a photo row whose id is the
        // traversal string. (The mock stored the blob above; this is the row that
        // references it.)
        let db1 = open_test_db();
        let cs = capture_bytes(
            &db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('x/../../../PWNED', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        )
        .await;
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        // The puller builds the blob's destination from the validated id under its
        // own library dir. `download_blobs` rejects the traversal id (it is not a
        // safe path token) before building any path, so nothing is written — the
        // `dir.join(id)` escape is structurally unreachable (the id validation is
        // proven by the `library_dir` unit tests).
        let db2 = open_test_db_with_blob(photo_decl());
        let (_t, ld) = temp_library_dir();
        let (updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

        // It is bad data, so the row that carries it is not applied and the cursor
        // does not advance — the same posture as any other failed-blob changeset.
        assert!(
            result.asset_downloads_failed,
            "a refused blob fails the changeset's downloads",
        );
        assert_eq!(result.changesets_applied, 0, "the bad row is not applied");
        assert_eq!(updated.get("dev1"), None, "the cursor is held for retry");
    }

    /// A blob id too short to form the `{ab}/{cd}` partition prefix (the
    /// dash-stripped id is under four chars, or splits a multi-byte char) cannot
    /// index the prefix's byte slice, so the path builder refuses it. End to end
    /// it is bad data: the row does not apply and the cursor holds. (The slice
    /// itself is proven non-panicking by the `hashed_path` unit tests in
    /// `library_dir`.)
    #[tokio::test]
    async fn unindexable_id_is_refused_not_panicked() {
        let storage = MockSyncStorage::new();

        let db1 = open_test_db();
        let cs = capture_bytes(
            &db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                // `id = "a"` dash-strips to "a", too short for the `&hex[..2]`
                // prefix slice, so the path builder refuses it.
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('a', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        )
        .await;
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        let db2 = open_test_db_with_blob(photo_decl());
        let (_t, ld) = temp_library_dir();
        // The pull completes (no panic); the unindexable row is refused.
        let (updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

        assert!(
            result.asset_downloads_failed,
            "an unindexable blob id fails the changeset's downloads instead of panicking",
        );
        assert_eq!(result.changesets_applied, 0, "the bad row is not applied");
        assert_eq!(updated.get("dev1"), None, "the cursor is held for retry");
    }

    /// A normal blob id still round-trips: the boundary check rejects only ids that
    /// could escape the cache or can't be partitioned, and a well-formed id writes
    /// its blob into the pinned cache at its partitioned `{ab}/{cd}/<id>` path.
    #[tokio::test]
    async fn normal_id_still_writes_under_the_blob_dir() {
        let storage = MockSyncStorage::new();

        storage
            .put_blob(
                "photos",
                "p1ab",
                ResolvedScope::Master,
                None,
                b"PHOTOBYTES".to_vec(),
            )
            .await
            .expect("plant blob");

        let db1 = open_test_db();
        let cs = capture_bytes(
            &db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1ab', 'n1', 'attach', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        )
        .await;
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        let db2 = open_test_db_with_blob(photo_decl());
        let (_t, ld) = temp_library_dir();
        let (updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

        assert_eq!(result.changesets_applied, 1, "a well-formed row applies");
        assert!(!result.asset_downloads_failed);
        assert_eq!(updated.get("dev1"), Some(&1));
        let written = std::fs::read(ld.cache_blob_path("photos", "p1ab").expect("cache path"))
            .expect("blob written");
        assert_eq!(
            written, b"PHOTOBYTES",
            "the blob lands in the evictable cache"
        );
    }
}
