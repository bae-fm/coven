//! Behavioral tests for the HLC as coven's `_updated_at` register.
//!
//! Unlike the self-tests in `hlc.rs` (which prove the clock is a correct clock),
//! these assert an *external* outcome of wiring the clock to the data plane: they
//! fail if `_updated_at` is wall-clock-stamped, if the clock regresses across a
//! restart, or if revocation depended on an author-supplied envelope timestamp
//! rather than current write-capable membership. They drive a real
//! [`crate::database::Database`] (with an injected, wall-clock-controlled `Hlc`)
//! so the register lives where production puts it: inside the owned connection.

use std::collections::HashMap;
use std::sync::Arc;

use crate::clock::SystemClock;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::envelope::{self, ChangesetEnvelope};
use crate::sync::hlc::{Hlc, Timestamp, HIGHWATER_STATE_KEY};
use crate::sync::membership::{MemberRole, MembershipAction, MembershipChain, MembershipEntry};
use crate::sync::pull::pull_changes;
use crate::sync::push::SCHEMA_VERSION;
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
        membership_grant: None,
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
/// behind A's*. A plain wall-clock `_updated_at` would let A win here.
///
/// The cycle is the unit under test: its advance source must be the max
/// applied-row `_updated_at`, not the envelope/head timestamp (which the HLC
/// cannot parse).
#[tokio::test]
async fn b_edit_after_pulling_a_wins_even_with_b_clock_behind() {
    let storage = MockSyncStorage::new();

    // A's wall clock reads far ahead of B's (A in the "future").
    let a_hlc = Hlc::with_wall_clock("dev-a".into(), || 9_000);
    let b_hlc = Arc::new(Hlc::with_wall_clock("dev-b".into(), || 1_000));

    // A stamps and publishes an edit of n1.
    let a_stamp = a_hlc.now().to_string();
    let db_a = open_test_db();
    let cs_a = capture_bytes(
        &db_a,
        &[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A wrote this', NULL, '{a_stamp}', '2026-01-01')"
        )],
    )
    .await;
    storage.store_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION);

    // B runs a real sync cycle over its own Database (clock = b_hlc): it pulls A's
    // edit and advances b_hlc from the applied row's `_updated_at`.
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    let (_t, ld) = temp_library_dir();
    let encryption = std::sync::RwLock::new(CloudCipher::Encrypted(
        EncryptionService::new_with_key(&[3u8; 32]),
    ));
    let keypair = UserKeypair::generate();

    let result = run_single_sync_cycle(
        &storage,
        "test-lib",
        "dev-b",
        &b_hlc,
        &SystemClock,
        &db_b,
        &encryption,
        &keypair,
        &ld,
        None,
        &NoopBlobSource,
        None,
    )
    .await
    .expect("B's sync cycle completes");

    assert_eq!(result.changesets_applied, 1, "B must apply A's changeset");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "A wrote this",
        "A's row must be present on B after the cycle",
    );

    // B now edits the same row. The cycle advanced b_hlc from A's applied stamp,
    // so B's next stamp sorts after A's despite B's wall clock being far behind.
    let b_stamp = b_hlc.now().to_string();
    assert!(
        b_stamp > a_stamp,
        "B's post-pull stamp {b_stamp} must beat A's {a_stamp} despite B's wall \
         clock being behind — the wall-clock-skew guarantee the HLC register provides",
    );

    // And LWW agrees: applying B's edit replaces A's row.
    let cs_b = capture_bytes(
        &db_b,
        &[&format!(
            "UPDATE notes SET title = 'B wrote this', _updated_at = '{b_stamp}' \
             WHERE id = 'n1'"
        )],
    )
    .await;
    storage.store_changeset("dev-b", 1, &cs_b, SCHEMA_VERSION);

    // A pulls B's edit; B wins on LWW because b_stamp > a_stamp.
    pull_changes(
        &db_a,
        &test_synced_tables(),
        &storage,
        "dev-a",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await
    .expect("pull into A");
    assert_eq!(
        query_text(&db_a, "SELECT title FROM notes WHERE id = 'n1'").await,
        "B wrote this",
        "B's causally-later edit must win the merge on A",
    );
}

/// Restart seeding. A clock reconstructed from a persisted high-water mark must
/// not mint a stamp below existing rows — even when its wall clock has jumped
/// backward and even within the same millisecond as the persisted mark.
#[test]
fn reconstructed_clock_does_not_regress_below_persisted_high_water() {
    let hlc1 = Hlc::with_wall_clock("dev-a".into(), || 5_000);
    let last_row_stamp = hlc1.now();
    let persisted = hlc1.high_water().to_string();
    assert_eq!(persisted, last_row_stamp.to_string());

    // Restart with the wall clock jumped *backward* and seed from the mark.
    let hlc2 = Hlc::with_wall_clock("dev-a".into(), || 1_000);
    hlc2.seed(&Timestamp::parse(&persisted).expect("parse high-water"));
    let next = hlc2.now();
    assert!(
        next > last_row_stamp,
        "reconstructed clock minted {next}, which regresses below {last_row_stamp}",
    );

    // Same-millisecond restart: seed at exactly the persisted mark; still advances.
    let hlc3 = Hlc::with_wall_clock("dev-a".into(), || 5_000);
    hlc3.seed(&Timestamp::parse(&persisted).expect("parse high-water"));
    let after_same_ms = hlc3.now();
    assert!(
        after_same_ms > last_row_stamp,
        "same-millisecond restart minted {after_same_ms}, not above {last_row_stamp}",
    );
}

/// Revocation is enforced by current membership, not by when a changeset claims
/// to have been authored. A removed member signs a changeset whose envelope
/// timestamp falls between their Add and Remove entries; pull must still reject it
/// because the author is not a current member. The removed member's device signs
/// its own head, so the rejection now lands at the head check (a removed member's
/// head is skipped before its changesets are even fetched) — strictly stronger
/// than the changeset-level check, and still keyed on *current* membership rather
/// than the (spoofable) envelope timestamp.
#[tokio::test]
async fn removed_member_changeset_is_rejected_despite_in_window_timestamp() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    // The mock signs the head it publishes for `member-device` with the member's
    // own keypair, the way a real device signs its own head.
    let storage = MockSyncStorage::with_keypair(member.clone());

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
    let chain = MembershipChain::from_entries(entries.clone()).expect("valid chain");
    assert!(
        !chain.can_write_now(&hex::encode(member.public_key)),
        "removed member must not be a current writer",
    );
    upload_chain(&storage, &entries).await;

    // Member signs a changeset with an in-window envelope timestamp (t=3000).
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Stale writer', NULL, '0000000003000-0000-member', '2026-01-01')",
        ],
    )
    .await;
    store_signed_changeset(
        &storage,
        "member-device",
        1,
        &cs,
        &member,
        "0000000003000-0000-member",
    );

    let db2 = open_test_db();
    let (updated, result) = pull_changes(
        &db2,
        &test_synced_tables(),
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await
    .expect("pull");

    assert_eq!(
        result.changesets_applied, 0,
        "a removed member's changeset must be rejected",
    );
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    // The removed member's head is skipped, so its changeset is never fetched and
    // its cursor is never advanced (nothing was examined to advance past).
    assert_eq!(updated.get("member-device"), None);
}

/// `Database::open` seeds the register from the persisted high-water mark, so the
/// very first host stamp after a restart does not regress below existing rows.
#[tokio::test]
async fn register_seeds_from_persisted_high_water() {
    // A high-water mark far ahead of any plausible wall millis. No synced rows on
    // disk, so the high-water mark is the only floor.
    let high = "9999999999000-0007-dev-a";
    let db = open_test_db_with_hlc(Arc::new(Hlc::new("dev-a".into())), |conn| {
        conn.execute(
            "INSERT INTO sync_state (key, value) VALUES (?1, ?2)",
            (HIGHWATER_STATE_KEY, high),
        )
        .map(|_| ())
        .map_err(crate::database::DbError::from)
    });

    let stamp = db.hlc().now().to_string();
    assert!(
        stamp.as_str() > high,
        "first stamp {stamp} must sort after the seeded high-water {high}",
    );
}

/// The on-disk register is the authoritative seed floor — not just the flushed
/// high-water mark. The clock must seed from `max(persisted high-water, on-disk
/// MAX(_updated_at))`, or the first post-restart stamp sorts below the device's
/// own un-flushed rows and loses LWW to them.
#[tokio::test]
async fn register_seeds_from_on_disk_rows_above_high_water() {
    let row_stamp = "9999999999000-0011-dev-a";
    let db = open_test_db_with_hlc(Arc::new(Hlc::new("dev-a".into())), |conn| {
        conn.execute(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n0', 'row', NULL, ?1, '2026-01-01')",
            [row_stamp],
        )
        .map(|_| ())
        .map_err(crate::database::DbError::from)
    });

    let stamp = db.hlc().now().to_string();
    assert!(
        stamp.as_str() > row_stamp,
        "first stamp {stamp} must sort after the on-disk row {row_stamp}; \
         seeding from the flushed high-water alone misses un-flushed local rows",
    );
}

/// The [`UpdatedAtStamper`] `Database::open` returns mints from the same seeded,
/// advancing clock the database holds — the point of handing the host a stamper
/// rather than letting its writes carry a separate clock. A synced row far ahead
/// of any plausible wall millis seeds the register at open (standing in for an
/// advance-on-pull push); the returned stamper must mint above that floor and be
/// strictly monotonic.
#[tokio::test]
async fn returned_stamper_shares_seeded_clock() {
    let seeded_floor = "9999999999000-0005-dev-a";
    let (db, stamper) = crate::database::Database::open_with_hlc(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        Arc::new(Hlc::new("dev-a".into())),
        |conn| {
            create_synced_schema(conn)?;
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n0', 'row', NULL, ?1, '2026-01-01')",
                [seeded_floor],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        },
    )
    .expect("open db");

    let s1 = stamper.stamp();
    assert!(
        s1.as_str() > seeded_floor,
        "stamper minted {s1} below the seeded floor {seeded_floor}; it is not \
         sharing the database's seeded clock",
    );
    // Monotonic across the stamper and the database's own clock handle.
    let s2 = db.hlc().now().to_string();
    let s3 = stamper.stamp();
    assert!(s2 > s1, "clock {s2} must outrank prior stamper {s1}");
    assert!(s3 > s2, "stamper {s3} must outrank prior clock {s2}");
}

/// A grossly-future incoming `_updated_at` must neither win last-writer-wins nor
/// ratchet the receiver's HLC. A peer whose wall clock is broken (or a buggy
/// client) can stamp a row years ahead; as a raw string that value beats every
/// honest stamp and, once applied, drags the receiver's clock up to it so every
/// later local write inherits the skew. The receiver bounds an incoming stamp to
/// its own wall time plus a generous offline allowance and refuses one beyond it.
///
/// Receiver B's wall clock is pinned at a known millis. A pre-existing local row
/// carries an honest stamp at that time; A's incoming row carries a stamp ten
/// years in the future. The honest local row must survive the merge, and B's HLC
/// must stay near wall time rather than jumping ten years ahead.
#[tokio::test]
async fn grossly_future_incoming_neither_wins_lww_nor_ratchets_hlc() {
    let storage = MockSyncStorage::new();

    // B's wall clock is pinned at t = 1_700_000_000_000 (a 2023 millis).
    let b_wall: u64 = 1_700_000_000_000;
    let b_hlc = Arc::new(Hlc::with_wall_clock("dev-b".into(), move || b_wall));

    // B already holds an honest local edit of n1, stamped at its own wall time.
    let b_local_stamp = format!("{b_wall:013}-0000-dev-b");
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    exec(
        &db_b,
        &format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'B honest', NULL, '{b_local_stamp}', '2026-01-01')"
        ),
    )
    .await;

    // A publishes a competing edit of n1 stamped ten years in the future — far
    // beyond any legitimate offline skew.
    let a_future_ms = b_wall + 10 * 365 * 24 * 60 * 60 * 1000;
    let a_future_stamp = format!("{a_future_ms:013}-0000-dev-a");
    let db_a = open_test_db();
    let cs_a = capture_bytes(
        &db_a,
        &[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A far future', NULL, '{a_future_stamp}', '2026-01-01')"
        )],
    )
    .await;
    storage.store_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION);

    // B pulls A's changeset.
    let (_cursors, result) = pull_into(
        &db_b,
        &storage,
        "dev-b",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    // (a) LWW: the grossly-future row must NOT win — B's honest local edit stands.
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "B honest",
        "a grossly-future incoming _updated_at won LWW over an honest local stamp",
    );

    // (b) clock ratchet: the pull must not carry the far-future stamp as the value
    // to advance the HLC past — it stays unset because the only applied row's stamp
    // was rejected as grossly-future.
    assert!(
        result.max_applied_updated_at.is_none(),
        "a grossly-future incoming stamp was collected to ratchet the HLC: {:?}",
        result.max_applied_updated_at,
    );

    // And the live clock is not skewed: B's next stamp sorts near its wall time,
    // far below the ten-years-future incoming value.
    let next = b_hlc.now().to_string();
    assert!(
        next.as_str() < a_future_stamp.as_str(),
        "B's clock ratcheted past a grossly-future incoming stamp: next={next} \
         incoming={a_future_stamp}",
    );
}

/// A legitimately-skewed incoming stamp — within the offline allowance — still
/// applies and wins normally. Devices are offline for long stretches, so a stamp
/// days ahead of the receiver's wall clock is honest and must not be rejected.
/// Receiver B is at wall time; A's incoming edit is stamped a few days ahead
/// (well inside the allowance). A's edit must win LWW and advance B's clock.
#[tokio::test]
async fn legitimately_skewed_incoming_still_wins_and_advances() {
    let storage = MockSyncStorage::new();

    let b_wall: u64 = 1_700_000_000_000;
    let b_hlc = Arc::new(Hlc::with_wall_clock("dev-b".into(), move || b_wall));

    // B holds an honest local edit at its own wall time.
    let b_local_stamp = format!("{b_wall:013}-0000-dev-b");
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    exec(
        &db_b,
        &format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'B honest', NULL, '{b_local_stamp}', '2026-01-01')"
        ),
    )
    .await;

    // A's edit is stamped three days ahead — a plausible cross-device clock spread,
    // well within the offline-skew allowance.
    let a_ms = b_wall + 3 * 24 * 60 * 60 * 1000;
    let a_stamp = format!("{a_ms:013}-0000-dev-a");
    let db_a = open_test_db();
    let cs_a = capture_bytes(
        &db_a,
        &[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A skewed', NULL, '{a_stamp}', '2026-01-01')"
        )],
    )
    .await;
    storage.store_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION);

    let (_cursors, result) = pull_into(
        &db_b,
        &storage,
        "dev-b",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobSource,
    )
    .await;

    // A's causally-later (within-allowance) edit wins.
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "A skewed",
        "a legitimately-skewed incoming edit failed to win LWW",
    );

    // And it advances B's clock past the applied stamp, so B's next write sorts
    // after A's.
    assert_eq!(
        result.max_applied_updated_at,
        Some(Timestamp::parse(&a_stamp).unwrap()),
        "a within-allowance applied stamp was not collected to advance the HLC",
    );
    if let Some(max) = &result.max_applied_updated_at {
        b_hlc.advance_past(max);
    }
    let next = b_hlc.now().to_string();
    assert!(
        next.as_str() > a_stamp.as_str(),
        "B's clock did not advance past a legitimately-skewed applied stamp: \
         next={next} applied={a_stamp}",
    );
}

/// Regression: a sync cycle that errors mid-cycle must still leave host-write
/// capture working. The `Database` (and its actor) outlive the cycle and are
/// reused every cycle, so if a failure could leave capture off, every later host
/// write would go un-recorded and never sync. The fix makes this structural —
/// capture is never suspended across the cycle (only an apply briefly disables it,
/// and that closure can't be reached on this failure path) — so there is no
/// suspended state a mid-cycle abort could strand.
///
/// We force the failure by blocking every `INSERT` into `sync_state` with a
/// trigger that `RAISE(ABORT)`s. The cycle's top-of-cycle reads (plain `SELECT`s)
/// still succeed, but the first bookkeeping persist fails, so the cycle returns
/// `Err`. Capture must still record a subsequent host write into the next
/// captured changeset.
#[tokio::test]
async fn cycle_error_mid_cycle_still_captures_host_writes() {
    // A db whose `sync_state` rejects every INSERT, so a `set_sync_state` inside
    // the cycle fails. Reads (the seed scan, the cycle's top-of-cycle loads) are
    // SELECTs and remain fine.
    let (db, _stamper) = crate::database::Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        "dev-self".to_string(),
        |conn| {
            create_synced_schema(conn)?;
            conn.execute_batch(
                "CREATE TRIGGER block_sync_state_insert BEFORE INSERT ON sync_state \
                 BEGIN SELECT RAISE(ABORT, 'forced set_sync_state failure'); END;",
            )
            .map_err(crate::database::DbError::from)
        },
    )
    .expect("open db with sync_state-blocking trigger");

    // A local insert so the cycle has an outgoing changeset to stage — the first
    // `set_sync_state("staged_seq", ...)` is what the trigger aborts. (Even with no
    // outgoing, the unconditional HLC high-water persist would abort; either way the
    // cycle fails after capture.)
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'First', NULL, 1, '0000000001000-0000-dev-self', '2026-01-01')",
    )
    .await;

    let storage = MockSyncStorage::new();
    let (_t, ld) = temp_library_dir();
    let encryption = std::sync::RwLock::new(CloudCipher::Encrypted(
        EncryptionService::new_with_key(&[7u8; 32]),
    ));
    let keypair = UserKeypair::generate();
    let hlc = db.hlc();

    let result = run_single_sync_cycle(
        &storage,
        "test-lib",
        "dev-self",
        &hlc,
        &SystemClock,
        &db,
        &encryption,
        &keypair,
        &ld,
        None,
        &NoopBlobSource,
        None,
    )
    .await;

    assert!(
        result.is_err(),
        "the blocked set_sync_state must make the cycle fail mid-span"
    );

    // Capture must still be live despite the mid-cycle failure: a fresh host write
    // is recorded into the next changeset. If a failure could leave capture off,
    // this capture would come back empty.
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n2', 'After failure', NULL, 1, '0000000002000-0000-dev-self', '2026-01-01')",
    )
    .await;
    let next = db
        .take_changeset()
        .await
        .expect("capture after the failed cycle");

    let changes = crate::changeset::walk(&next).expect("walk next changeset");
    assert!(
        changes
            .iter()
            .any(|c| c.table == "notes" && c.pk() == Some("n2")),
        "the post-failure host write was not captured — the cycle left capture off \
         (the leak this test guards against)"
    );
}
