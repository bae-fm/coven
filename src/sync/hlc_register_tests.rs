//! Behavioral tests for the HLC as coven's `_updated_at` register.
//!
//! Unlike the self-tests in `hlc.rs` (which prove the clock is a correct
//! clock), these assert an *external* outcome of wiring the clock to the data
//! plane: they fail if `_updated_at` is wall-clock-stamped, if the clock
//! regresses across a restart, or if revocation depended on an author-supplied
//! envelope timestamp rather than current write-capable membership.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use libsqlite3_sys as ffi;

use crate::clock::SystemClock;
use crate::db::{DbError, OutboxEntry, RawDbHandle, SyncBookkeeping};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::sync::cycle::{run_single_sync_cycle, SyncCycleOutcome};
use crate::sync::envelope::{self, ChangesetEnvelope};
use crate::sync::hlc::{Hlc, Timestamp, HIGHWATER_STATE_KEY};
use crate::sync::membership::{MemberRole, MembershipAction, MembershipChain, MembershipEntry};
use crate::sync::pull::{pull_changes, SendDbPtr};
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::register_clock::RegisterClock;
use crate::sync::session::SyncSession;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

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

async fn upload_chain(storage: &MockSyncStorage, entries: &[MembershipEntry]) {
    for (i, entry) in entries.iter().enumerate() {
        let bytes = serde_json::to_vec(entry).expect("serialize entry");
        storage
            .put_membership_entry(&entry.author_pubkey, (i + 1) as u64, bytes)
            .await
            .expect("put entry");
    }
}

/// The causality-under-skew guarantee, driven through the real sync cycle.
/// Device B runs a full [`run_single_sync_cycle`] that pulls A's edit of `n1`;
/// the cycle must advance B's HLC from the applied row's `_updated_at` so B's
/// next stamp is causally greater than A's — *even with B's wall clock set far
/// behind A's*. A plain wall-clock `_updated_at` would let A win here (A's wall
/// time is larger), so this fails if `_updated_at` is not the HLC register.
///
/// Critically, the cycle is the unit under test. Its advance source must be the
/// max applied-row `_updated_at`, not the envelope/head timestamp: the mock
/// envelope timestamp is an RFC-3339 string the HLC cannot parse, so an advance
/// from it leaves B's clock ignorant of A's stamp, B's next stamp sorts below
/// A's, and the LWW assertion below fails.
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

        // B runs a real sync cycle: it pulls A's edit into db_b and advances
        // b_hlc from the applied row's `_updated_at`. A fresh session means B has
        // no outgoing changeset, so the cycle only pulls.
        let db_b = open_memory_db();
        create_synced_schema(db_b);
        let (_t, ld) = temp_library_dir();
        let encryption = std::sync::RwLock::new(EncryptionService::new_with_key(&[3u8; 32]));
        let keypair = UserKeypair::generate();
        let bookkeeping = FakeSyncDb::new(HashMap::new(), &[]);
        let session = SyncSession::start(db_b).expect("start B session");

        let outcome = run_single_sync_cycle(
            &storage,
            "dev-b",
            &b_hlc,
            &SystemClock,
            db_b,
            session,
            &encryption,
            &keypair,
            &bookkeeping,
            &ld,
            None,
            &NoopBlobPlan,
            None,
        )
        .await;

        let result = match outcome {
            SyncCycleOutcome::Ok(result, _session) => result,
            SyncCycleOutcome::ErrWithSession(e, _) | SyncCycleOutcome::ErrNoSession(e) => {
                panic!("B's sync cycle did not complete: {e}");
            }
        };
        assert_eq!(result.changesets_applied, 1, "B must apply A's changeset");
        assert_eq!(
            query_text(db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "A wrote this",
            "A's row must be present on B after the cycle",
        );

        // B now edits the same row. The cycle must have advanced b_hlc from A's
        // applied stamp, so B's next stamp sorts after A's despite B's wall clock
        // (1_000) being far behind A's (9_000).
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

/// Revocation is enforced by current write-capable membership, not by when a
/// changeset claims to have been authored. A removed member signs a changeset
/// whose envelope timestamp falls between their Add and Remove entries — a
/// timestamp that, taken at face value, sits squarely within their membership.
/// Pull must still reject it, because the author is not a *current* write-capable
/// member. The author-signed envelope timestamp carries no authorization weight;
/// signatures plus current membership (backed by key rotation) do.
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
        // The membership check is the authorization boundary: a removed member is
        // not a current writer, regardless of any timestamp on their write.
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

/// A real-sqlite-backed `SyncDb` for exercising `RegisterClock::open`'s seed read
/// and as the bookkeeping side of a `run_single_sync_cycle`. `get_sync_state` /
/// `set_sync_state` are an in-memory map; cursors and outbox queries answer empty
/// so a cycle pulls without local pushes or outbox work. `raw_write_handle`
/// returns a live in-memory connection carrying the synced schema (and any seeded
/// rows), so the register-floor scan runs its real FFI path against actual
/// `_updated_at` rows rather than a hand-returned max.
struct FakeSyncDb {
    state: Mutex<HashMap<String, String>>,
    /// A live in-memory sqlite connection with the synced schema. The scan in
    /// `RegisterClock::open` prepares/steps `SELECT MAX(_updated_at)` against this.
    /// [`SendDbPtr`] carries `Send`; `SyncBookkeeping: Send + Sync` also requires
    /// `Sync`, which the `unsafe impl` below adds — sound because the
    /// single-threaded test serializes all access to the connection.
    db: SendDbPtr,
}

// SAFETY: access to the wrapped connection is serialized by the single-threaded
// test; see the `db` field doc.
unsafe impl Sync for FakeSyncDb {}

impl FakeSyncDb {
    /// Build over a fresh in-memory DB with the synced schema, inserting a `notes`
    /// row at each of `row_stamps` so the scan sees real on-disk `_updated_at`
    /// values. Pass an empty slice for a schema with no synced rows.
    fn new(state: HashMap<String, String>, row_stamps: &[&str]) -> Self {
        let db = unsafe {
            let db = open_memory_db();
            create_synced_schema(db);
            for (i, stamp) in row_stamps.iter().enumerate() {
                exec(
                    db,
                    &format!(
                        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                         VALUES ('n{i}', 'row {i}', NULL, '{stamp}', '2026-01-01')"
                    ),
                );
            }
            db
        };
        Self {
            state: Mutex::new(state),
            db: SendDbPtr(db),
        }
    }
}

impl Drop for FakeSyncDb {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_close(self.db.0) };
    }
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
        Ok(self.db.0)
    }
}

/// Open a seeded [`RegisterClock`] over `db` for the construction-path seed
/// tests. This is the real unit: it reads the persisted high-water mark and runs
/// the raw-handle `SELECT MAX(_updated_at)` scan to seed its floor.
async fn open_clock_over(db: std::sync::Arc<FakeSyncDb>) -> RegisterClock {
    RegisterClock::open("dev-a".to_string(), db.as_ref())
        .await
        .expect("register clock open")
}

/// `RegisterClock::open` seeds the register from the persisted high-water mark, so
/// the very first host stamp after a restart does not regress below existing
/// rows.
#[tokio::test]
async fn register_clock_seeds_from_persisted_high_water() {
    init_synced_tables();
    // A high-water mark far ahead of any plausible current wall millis, so a
    // freshly minted (unseeded) stamp would sort *below* it. No synced rows on
    // disk, so the high-water mark is the only floor.
    let high = "9999999999000-0007-dev-a";
    let db = std::sync::Arc::new(FakeSyncDb::new(
        HashMap::from([(HIGHWATER_STATE_KEY.to_string(), high.to_string())]),
        &[],
    ));

    let clock = open_clock_over(db).await;

    let stamp = clock.updated_at_stamper().stamp();
    assert!(
        stamp.as_str() > high,
        "first stamp {stamp} must sort after the seeded high-water {high}",
    );
}

/// The on-disk register is the authoritative seed floor — not just the flushed
/// high-water mark. Local stamps minted between cycles are written to synced-row
/// `_updated_at` but the high-water flush happens only at cycle end, so on an
/// offline restart the persisted high-water can lag the device's own rows. The
/// clock must seed from `max(persisted high-water, on-disk MAX(_updated_at))`, or
/// the first post-restart stamp sorts below the device's own un-flushed rows and
/// loses LWW to them — self-data-loss.
///
/// This drives the real seeding path: a real `notes` row carrying a far-future
/// `_updated_at` sits in coven's in-memory test DB, and `RegisterClock::open` runs
/// its own raw-handle `SELECT MAX(_updated_at)` scan over it (no host-supplied
/// max). Removing that scan makes the clock seed only from the absent high-water
/// mark and mint a stamp below the row — so this fails.
#[tokio::test]
async fn register_clock_seeds_from_on_disk_rows_above_high_water() {
    init_synced_tables();
    // No persisted high-water (never flushed), but a synced row exists far ahead
    // of any plausible wall millis. Seeding from high-water alone would leave the
    // clock unseeded and mint a stamp below this row.
    let row_stamp = "9999999999000-0011-dev-a";
    let db = std::sync::Arc::new(FakeSyncDb::new(HashMap::new(), &[row_stamp]));

    let clock = open_clock_over(db).await;

    let stamp = clock.updated_at_stamper().stamp();
    assert!(
        stamp.as_str() > row_stamp,
        "first stamp {stamp} must sort after the on-disk row {row_stamp}; \
         seeding from the flushed high-water alone misses un-flushed local rows",
    );
}

/// The host's injected [`UpdatedAtStamper`] and a second stamper from the same
/// [`RegisterClock`] mint from one shared, advancing clock — the whole point of
/// handing the host a handle rather than letting its db carry a separate clock.
///
/// Two external outcomes, neither a restatement of `Hlc::now`'s self-tests:
/// - **Sharing:** the clock seeds the register past a far-future on-disk row at
///   open (the same forward push advance-on-pull performs). A stamper obtained
///   *after* open must mint above that floor — it would not if it wrapped a
///   fresh, unseeded clock instead of the register's `Arc<Hlc>`.
/// - **Monotonic across handles:** stamps interleaved between two stampers
///   strictly increase regardless of which mints them, which holds only if both
///   draw from the same clock.
#[tokio::test]
async fn stampers_share_one_advancing_clock() {
    init_synced_tables();
    // A synced row far ahead of any plausible wall millis seeds the register at
    // open, standing in for an advance-on-pull push of the shared clock.
    let seeded_floor = "9999999999000-0005-dev-a";
    let db = std::sync::Arc::new(FakeSyncDb::new(HashMap::new(), &[seeded_floor]));

    let clock = open_clock_over(db).await;

    // A stamper obtained after the seed reflects it: it wraps the register's
    // seeded clock, not a fresh one.
    let stamper = clock.updated_at_stamper();
    let s1 = stamper.stamp();
    assert!(
        s1.as_str() > seeded_floor,
        "stamper minted {s1} below the seeded floor {seeded_floor}; it is not \
         sharing the register's clock",
    );

    // Interleave two handles; every stamp must strictly outrank the last, which
    // holds only if both advance one shared clock.
    let stamper2 = clock.updated_at_stamper();
    let s2 = stamper2.stamp();
    let s3 = stamper.stamp();
    let s4 = stamper2.stamp();
    assert!(s2 > s1, "stamper2 {s2} must outrank prior stamper {s1}");
    assert!(s3 > s2, "stamper {s3} must outrank prior stamper2 {s2}");
    assert!(s4 > s3, "stamper2 {s4} must outrank prior stamper {s3}");
}
