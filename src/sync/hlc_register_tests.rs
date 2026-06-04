//! Behavioral tests for the HLC as coven's `_updated_at` register.
//!
//! Unlike the self-tests in `hlc.rs` (which prove the clock is a correct
//! clock), these assert an *external* outcome of wiring the clock to the data
//! plane: they fail if `_updated_at` is wall-clock-stamped, if the clock
//! regresses across a restart, or if revocation leaned on the deleted temporal
//! authorization gate.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use libsqlite3_sys as ffi;

use crate::db::{DbError, OutboxEntry, RawDbHandle, SyncBookkeeping};
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::sync::envelope::{self, ChangesetEnvelope};
use crate::sync::hlc::{Hlc, Timestamp, HIGHWATER_STATE_KEY};
use crate::sync::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipChain, MembershipEntry,
};
use crate::sync::pull::{pull_changes, SendDbPtr};
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::session::SyncSession;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

/// Capture a changeset's bytes after running `stmts` against `db`.
unsafe fn capture_bytes(db: *mut ffi::sqlite3, stmts: &[&str]) -> Vec<u8> {
    let session = SyncSession::start(db).expect("start session");
    for s in stmts {
        exec(db, s);
    }
    session
        .changeset()
        .expect("changeset")
        .expect("non-empty")
        .as_bytes()
        .to_vec()
}

fn temp_library_dir() -> (tempfile::TempDir, LibraryDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = LibraryDir::new(tmp.path());
    (tmp, dir)
}

/// Store a changeset signed by `author` into the mock storage, stamping the
/// envelope timestamp with `env_ts`. Mirrors the real publish path (sign then
/// pack) so pull's signature + membership checks see a genuine envelope.
fn store_signed_changeset(
    storage: &MockSyncStorage,
    device_id: &str,
    seq: u64,
    changeset_bytes: &[u8],
    author: &UserKeypair,
    env_ts: &str,
) {
    let mut env = ChangesetEnvelope {
        device_id: device_id.to_string(),
        seq,
        schema_version: SCHEMA_VERSION,
        message: String::new(),
        timestamp: env_ts.to_string(),
        changeset_size: changeset_bytes.len(),
        author_pubkey: None,
        signature: None,
    };
    envelope::sign_envelope(&mut env, author, changeset_bytes);
    let packed = envelope::pack(&env, changeset_bytes);
    storage.put_changeset_packed(device_id, seq, packed);
}

/// A signed founder (first) membership entry for `kp`.
fn founder_entry(kp: &UserKeypair, timestamp: &str) -> MembershipEntry {
    let pk_hex = hex::encode(kp.public_key);
    let mut entry = MembershipEntry {
        action: MembershipAction::Add,
        user_pubkey: pk_hex.clone(),
        role: MemberRole::Owner,
        timestamp: timestamp.to_string(),
        author_pubkey: pk_hex,
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, kp);
    entry
}

/// A signed entry where `author` adds/removes `subject` with `role`.
fn make_entry(
    author: &UserKeypair,
    action: MembershipAction,
    subject: &UserKeypair,
    role: MemberRole,
    timestamp: &str,
) -> MembershipEntry {
    let mut entry = MembershipEntry {
        action,
        user_pubkey: hex::encode(subject.public_key),
        role,
        timestamp: timestamp.to_string(),
        author_pubkey: hex::encode(author.public_key),
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, author);
    entry
}

async fn upload_chain(storage: &MockSyncStorage, entries: &[MembershipEntry]) {
    for (i, entry) in entries.iter().enumerate() {
        let bytes = serde_json::to_vec(entry).expect("serialize entry");
        storage
            .put_membership_entry(&entry.author_pubkey, (i + 1) as u64, bytes)
            .await
            .expect("put entry");
    }
}

/// The causality-under-skew guarantee. Device B edits a row *after* applying
/// A's edit of the same row; B's write must win because its HLC stamp is
/// causally greater — *even with B's wall clock set behind A's*. A plain
/// wall-clock `_updated_at` would let A win here (A's wall time is larger), so
/// this fails if `_updated_at` is not the HLC register advanced by pull.
#[tokio::test]
async fn b_edit_after_pulling_a_wins_even_with_b_clock_behind() {
    unsafe {
        init_synced_tables();
        let storage = MockSyncStorage::new();

        // A's wall clock reads far ahead of B's (A in the "future").
        let a_hlc = Hlc::with_wall_clock("dev-a".into(), || 9_000);
        let b_hlc = Hlc::with_wall_clock("dev-b".into(), || 1_000);

        // A stamps and publishes an edit of n1.
        let a_stamp = a_hlc.now().to_string();
        let db_a = open_memory_db();
        create_synced_schema(db_a);
        let cs_a = capture_bytes(
            db_a,
            &[&format!(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'A wrote this', NULL, '{a_stamp}', '2026-01-01')"
            )],
        );
        storage.store_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION);

        // B pulls A's edit and advances its HLC past every applied row's
        // `_updated_at` — exactly what the sync cycle does.
        let db_b = open_memory_db();
        create_synced_schema(db_b);
        let (_t, ld) = temp_library_dir();
        let (_cursors, pull) = pull_changes(
            SendDbPtr(db_b),
            &storage,
            "dev-b",
            &HashMap::new(),
            &ld,
            &NoopBlobPlan,
        )
        .await
        .expect("pull");

        let max_applied = pull
            .max_applied_updated_at
            .expect("pull surfaced an applied _updated_at");
        assert_eq!(max_applied.to_string(), a_stamp);
        b_hlc.update(&max_applied);

        // B now edits the same row. Its stamp must sort after A's despite B's
        // wall clock (1_000) being far behind A's (9_000).
        let b_stamp = b_hlc.now().to_string();
        assert!(
            b_stamp > a_stamp,
            "B's post-pull stamp {b_stamp} must beat A's {a_stamp} \
             despite B's wall clock being behind — this is the wall-clock-skew \
             guarantee the HLC register provides",
        );

        // And LWW agrees: applying B's edit replaces A's row.
        let cs_b = capture_bytes(
            db_b,
            &[&format!(
                "UPDATE notes SET title = 'B wrote this', _updated_at = '{b_stamp}' \
                 WHERE id = 'n1'"
            )],
        );
        storage.store_changeset("dev-b", 1, &cs_b, SCHEMA_VERSION);

        // A pulls B's edit; B wins on LWW because b_stamp > a_stamp.
        let (_c, _p) = pull_changes(
            SendDbPtr(db_a),
            &storage,
            "dev-a",
            &HashMap::new(),
            &temp_library_dir().1,
            &NoopBlobPlan,
        )
        .await
        .expect("pull into A");
        assert_eq!(
            query_text(db_a, "SELECT title FROM notes WHERE id = 'n1'"),
            "B wrote this",
            "B's causally-later edit must win the merge on A",
        );

        ffi::sqlite3_close(db_a);
        ffi::sqlite3_close(db_b);
    }
}

/// Restart seeding. A clock reconstructed from a persisted high-water mark must
/// not mint a stamp below existing rows — even when its wall clock has jumped
/// backward and even within the same millisecond as the persisted mark.
#[test]
fn reconstructed_clock_does_not_regress_below_persisted_high_water() {
    // First boot: clock reaches a high-water mark at wall=5_000.
    let hlc1 = Hlc::with_wall_clock("dev-a".into(), || 5_000);
    let last_row_stamp = hlc1.now();
    let persisted = hlc1.high_water().to_string();
    assert_eq!(persisted, last_row_stamp.to_string());

    // Restart with the wall clock jumped *backward* to 1_000 and seed from the
    // persisted mark (what `SyncManager::new` does).
    let hlc2 = Hlc::with_wall_clock("dev-a".into(), || 1_000);
    hlc2.seed(&Timestamp::parse(&persisted).expect("parse high-water"));

    let next = hlc2.now();
    assert!(
        next > last_row_stamp,
        "reconstructed clock minted {next}, which regresses below the last \
         persisted row stamp {last_row_stamp}",
    );

    // Same-millisecond restart: seed at exactly the persisted mark; the next
    // stamp still advances (counter increments).
    let hlc3 = Hlc::with_wall_clock("dev-a".into(), || 5_000);
    hlc3.seed(&Timestamp::parse(&persisted).expect("parse high-water"));
    let after_same_ms = hlc3.now();
    assert!(
        after_same_ms > last_row_stamp,
        "same-millisecond restart minted {after_same_ms}, not above {last_row_stamp}",
    );
}

/// Revocation no longer depends on a temporal authorization gate. A removed
/// member signs a changeset whose envelope timestamp falls *inside* their old
/// membership window — the exact case the deleted `can_write_at(pk, ts)` would
/// have admitted. Pull must reject it because the author is not a *current*
/// write-capable member. This proves the temporal check wasn't doing the work;
/// signatures + current membership (backed by key rotation) are.
#[tokio::test]
async fn removed_member_changeset_is_rejected_despite_in_window_timestamp() {
    unsafe {
        init_synced_tables();
        let storage = MockSyncStorage::new();

        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();

        // Chain: owner founds, adds member, then removes member.
        let entries = vec![
            founder_entry(&owner, "0000000001000-0000-owner"),
            make_entry(
                &owner,
                MembershipAction::Add,
                &member,
                MemberRole::Member,
                "0000000002000-0000-owner",
            ),
            make_entry(
                &owner,
                MembershipAction::Remove,
                &member,
                MemberRole::Member,
                "0000000004000-0000-owner",
            ),
        ];
        // Sanity: the old temporal gate WOULD have admitted a write stamped at
        // t=3000 (after Add, before Remove). The new non-temporal gate must not.
        let chain = MembershipChain::from_entries(entries.clone()).expect("valid chain");
        assert!(
            !chain.can_write_now(&hex::encode(member.public_key)),
            "removed member must not be a current writer",
        );
        upload_chain(&storage, &entries).await;

        // Member signs a changeset with an in-window envelope timestamp (t=3000).
        let db1 = open_memory_db();
        create_synced_schema(db1);
        let cs = capture_bytes(
            db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('n1', 'Stale writer', NULL, '0000000003000-0000-member', '2026-01-01')",
            ],
        );
        store_signed_changeset(
            &storage,
            "member-device",
            1,
            &cs,
            &member,
            "0000000003000-0000-member",
        );

        let db2 = open_memory_db();
        create_synced_schema(db2);
        let (updated, result) = pull_changes(
            SendDbPtr(db2),
            &storage,
            "dev2",
            &HashMap::new(),
            &temp_library_dir().1,
            &NoopBlobPlan,
        )
        .await
        .expect("pull");

        assert_eq!(
            result.changesets_applied, 0,
            "a removed member's changeset must be rejected",
        );
        assert!(!row_exists(db2, "SELECT 1 FROM notes WHERE id = 'n1'"));
        // Cursor still advances past the rejected seq.
        assert_eq!(updated.get("member-device"), Some(&1));

        ffi::sqlite3_close(db1);
        ffi::sqlite3_close(db2);
    }
}

/// A minimal in-memory `SyncDb` for exercising `SyncManager::new`'s seed read.
/// Only `get_sync_state` is wired meaningfully; the rest is unreachable on the
/// construction path under test.
struct FakeSyncDb {
    state: Mutex<HashMap<String, String>>,
}

#[async_trait]
impl SyncBookkeeping for FakeSyncDb {
    async fn get_sync_state(&self, key: &str) -> Result<Option<String>, DbError> {
        Ok(self.state.lock().unwrap().get(key).cloned())
    }
    async fn set_sync_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        self.state
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }
    async fn get_all_sync_cursors(&self) -> Result<HashMap<String, u64>, DbError> {
        Ok(HashMap::new())
    }
    async fn set_sync_cursor(&self, _device_id: &str, _seq: u64) -> Result<(), DbError> {
        Ok(())
    }
    async fn get_pending_cloud_uploads(&self) -> Result<Vec<OutboxEntry>, DbError> {
        Ok(Vec::new())
    }
    async fn get_pending_cloud_deletes(&self) -> Result<Vec<OutboxEntry>, DbError> {
        Ok(Vec::new())
    }
    async fn has_pending_cloud_uploads(&self) -> Result<bool, DbError> {
        Ok(false)
    }
    async fn remove_cloud_outbox_entry(&self, _id: i64) -> Result<(), DbError> {
        Ok(())
    }
    async fn record_cloud_upload_failure(
        &self,
        _id: i64,
        _error: &str,
        _attempted_at: &str,
    ) -> Result<(), DbError> {
        Ok(())
    }
}

#[async_trait]
impl RawDbHandle for FakeSyncDb {
    async fn raw_write_handle(&self) -> Result<*mut libsqlite3_sys::sqlite3, DbError> {
        Err(DbError("not used on the construction path".into()))
    }
}

/// `SyncManager::new` seeds the register from the persisted high-water mark, so
/// the very first host stamp after a restart does not regress below existing
/// rows — without starting sync.
#[tokio::test]
async fn manager_new_seeds_register_from_persisted_high_water() {
    use crate::clock::SystemClock;
    use crate::config::Config;
    use crate::encryption::EncryptionService;
    use crate::keys::KeyService;
    use std::sync::Arc;

    // A high-water mark far ahead of any plausible current wall millis, so a
    // freshly minted (unseeded) stamp would sort *below* it.
    let high = "9999999999000-0007-dev-a";
    let db = Arc::new(FakeSyncDb {
        state: Mutex::new(HashMap::from([(
            HIGHWATER_STATE_KEY.to_string(),
            high.to_string(),
        )])),
    });

    let config_provider = {
        // The library dir is never read on the construction path under test.
        let config = Config::with_defaults(
            "test-lib".to_string(),
            "dev-a".to_string(),
            LibraryDir::new(std::path::Path::new("/nonexistent")),
            "Test Library".to_string(),
        );
        let config = Arc::new(config);
        Arc::new(move || (*config).clone()) as crate::sync::sync_manager::ConfigProvider
    };

    let manager = crate::sync::sync_manager::SyncManager::new(
        config_provider,
        KeyService::new(true, "test-lib".to_string()),
        EncryptionService::new_with_key(&[3u8; 32]),
        db,
        Arc::new(SystemClock),
        Arc::new(NoopBlobPlan),
        None,
    )
    .await
    .expect("manager construction");

    let stamp = manager.stamp_updated_at();
    assert!(
        stamp.as_str() > high,
        "first stamp {stamp} must sort after the seeded high-water {high}",
    );
}
