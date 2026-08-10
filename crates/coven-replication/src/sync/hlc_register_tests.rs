//! Behavioral tests for the HLC as coven's `_updated_at` register.
//!
//! Unlike the self-tests in `hlc.rs` (which prove the clock is a correct clock),
//! these assert an *external* outcome of wiring the clock to the data plane: they
//! fail if `_updated_at` is wall-clock-stamped, if the clock regresses across a
//! restart, or if revocation depended on an author-supplied transport timestamp
//! rather than current write-capable membership. They drive a real
//! [`coven_database::Database`] (with an injected, wall-clock-controlled `Hlc`)
//! so the register lives where production puts it: inside the owned connection.

use std::sync::Arc;

use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_protocol::hlc::{Hlc, Timestamp, HIGHWATER_STATE_KEY};
use coven_protocol::membership::MemberRole;
/// The synthetic test db opens with a single migration, so its
/// [`coven_database::Database::schema_version`] is 1. Changesets are stored at
/// that version.
const SCHEMA_VERSION: u32 = 1;

use crate::sync::test_helpers::*;

/// The causality-under-skew guarantee, driven through the Store pull path.
/// Device B's join bootstrap pulls A's edit of `n1`; applying that row must
/// advance B's HLC from its `_updated_at` so B's
/// next stamp is causally greater than A's — *even with B's wall clock set far
/// behind A's*. A plain wall-clock `_updated_at` would let A win here.
///
/// The pull is the unit under test: its advance source must be the max
/// applied-row `_updated_at`, not a transport/head timestamp (which the HLC
/// cannot parse).
#[tokio::test]
async fn b_edit_after_pulling_a_wins_even_with_b_clock_behind() {
    // A's wall clock reads far ahead of B's (A in the "future").
    let a_hlc = Arc::new(Hlc::new(
        "dev-a".into(),
        coven_foundation::clock::clock_from_millis(|| 9_000),
    ));
    let b_hlc = Arc::new(Hlc::new(
        "dev-b".into(),
        coven_foundation::clock::clock_from_millis(|| 1_000),
    ));

    // A stamps and publishes an edit of n1 through the Store outbox so its
    // registration, commit, and local materialized frontier advance together
    // without publishing a snapshot that already contains the row.
    let a_stamp = a_hlc.now().to_string();
    let db_a = open_test_db_with_hlc(a_hlc.clone(), |_conn| Ok(()));
    let store_database_a = coven_database::StoreDatabase::new(&db_a.database);
    let keypair = UserKeypair::generate();
    let storage = TestStore::create(
        &db_a,
        "test-store",
        keypair.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact HLC test Store");
    let (_a_temp, a_store_dir) = temp_store_dir();
    storage
        .open_into(&db_a)
        .await
        .expect("open exact test Store");
    db_a.database
        .execute_test_host_write(&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at, shared) \
             VALUES ('n1', 'A wrote this', NULL, '{a_stamp}', '2026-01-01', 1)"
        ))
        .await;
    assert_eq!(
        store_database_a.pending_writes().await.unwrap().len(),
        1,
        "A's shared insert must enter the Store outbox"
    );
    assert!(
        storage
            .publish_pending(&db_a, &a_store_dir)
            .await
            .expect("publish A's initial edit"),
        "A's Store outbox must publish one write"
    );
    assert!(
        store_database_a.pending_writes().await.unwrap().is_empty(),
        "A's Store publication must finish its initial edit"
    );

    // B's join bootstrap pulls the signed cut into its own SyntheticStoreFixture (clock =
    // b_hlc), including A's edit, before it activates the local registration.
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    let (_t, ld) = temp_store_dir();
    storage
        .activate_joined_device(&db_a, &db_b, &keypair, "0000000001000-0000-dev-b")
        .await
        .expect("install B's active exact device fixture");
    let active_device_count = db_a
        .database
        .test_sql(|database| {
            database.table_row_count(coven_database::DatabaseTestTable::named(
                "store_device_registration_activations",
            ))
        })
        .await
        .expect("count A's activated devices");
    assert_eq!(active_device_count, 2, "A must activate B's registration");
    assert_eq!(
        db_b.database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "A wrote this",
        "A's row must be present when B's registration activates",
    );

    // B now edits the same row. The pull advanced b_hlc from A's applied stamp,
    // so B's next stamp sorts after A's despite B's wall clock being far behind.
    let b_stamp = b_hlc.now().to_string();
    assert!(
        b_stamp > a_stamp,
        "B's post-pull stamp {b_stamp} must beat A's {a_stamp} despite B's wall \
         clock being behind — the wall-clock-skew guarantee the HLC register provides",
    );

    // And LWW agrees: applying B's edit replaces A's row.
    db_b.database
        .execute_test_host_write(&format!(
            "UPDATE notes SET title = 'B wrote this', _updated_at = '{b_stamp}' \
             WHERE id = 'n1'"
        ))
        .await;
    assert!(
        storage
            .publish_pending(&db_b, &ld)
            .await
            .expect("publish B's post-pull edit"),
        "B's Store outbox must publish its post-pull edit"
    );

    // A pulls B's edit; B wins on LWW because b_stamp > a_stamp.
    let (_, first_pull) = storage.pull_into(&db_a, &temp_store_dir().1).await;
    assert!(
        first_pull.held_positions.is_empty(),
        "A held B's post-pull edit: {:?}",
        first_pull.held_positions
    );
    let (_, second_pull) = storage.pull_into(&db_a, &temp_store_dir().1).await;
    assert!(
        second_pull.held_positions.is_empty(),
        "A held B's post-activation stream: {:?}",
        second_pull.held_positions
    );
    assert_eq!(
        db_a.database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "B wrote this",
        "B's causally-later edit must win the merge on A; pulls: {first_pull:?}, {second_pull:?}",
    );
}

/// The register advances as each changeset applies, inside the pull — not only at
/// the end of the cycle. A host write landing after a changeset is applied but
/// before the cycle finishes mints its `_updated_at` off the shared clock, so that
/// stamp must already sort above every row the pull has applied. With B's wall
/// clock far behind A's, the pull's per-changeset advance is the only thing that
/// can lift B's next stamp above A's applied row.
///
/// The unit under test is `pull_changes` (its row-and-position commit advances the
/// register): after it applies A's changeset, B's shared clock — which the
/// host write path stamps off — must already outrank A's row, with no cycle
/// wrapping the pull.
#[tokio::test]
async fn pull_advances_register_as_each_changeset_applies() {
    // A's wall clock reads far ahead of B's, so only the pull's advance can lift
    // B's clock above A's applied stamp.
    let a_hlc = Hlc::new(
        "dev-a".into(),
        coven_foundation::clock::clock_from_millis(|| 9_000),
    );
    let b_hlc = Arc::new(Hlc::new(
        "dev-b".into(),
        coven_foundation::clock::clock_from_millis(|| 1_000),
    ));

    let a_stamp = a_hlc.now().to_string();
    let db_a = open_test_db();
    let storage = TestStore::create(
        &db_a,
        "test-store",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact test Store for the test database");
    let cs_a = db_a
        .database
        .capture_test_changeset(&[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A wrote this', NULL, '{a_stamp}', '2026-01-01')"
        )])
        .await;
    storage
        .publish_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION)
        .await
        .expect("publish A changeset");

    // B pulls A's Store commit directly — no cycle wraps it, so
    // the only advance that can fire is the per-changeset one.
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    let (_positions, result) = storage.pull_into(&db_b, &temp_store_dir().1).await;
    assert_eq!(result.changesets_applied, 1, "B must apply A's changeset");

    // A host write on B now mints off the shared clock the pull advanced. It must
    // already sort above A's applied row, despite B's wall clock being far behind.
    let host_stamp = b_hlc.now().to_string();
    assert!(
        host_stamp > a_stamp,
        "a host write after the pull applied A's row (stamp {a_stamp}) minted \
         {host_stamp}, which sorts below it — the register did not advance as the \
         changeset applied, so a causally-later local edit would lose LWW",
    );
}

#[tokio::test]
async fn a_host_write_queued_after_remote_commit_stamps_past_the_committed_row() {
    let remote_hlc = Hlc::new(
        "dev-a".into(),
        coven_foundation::clock::clock_from_millis(|| 9_000),
    );
    let remote_stamp = remote_hlc.now().to_string();
    let source = open_test_db();
    let storage = Arc::new(
        TestStore::create(
            &source,
            "test-store",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exact test Store for the test database"),
    );
    let changeset = source
        .database
        .capture_test_changeset(&[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('remote-boundary', 'Remote', NULL, '{remote_stamp}', '2026-01-01')"
        )])
        .await;
    let expected_commit = storage
        .publish_changeset("dev-a", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish remote changeset");
    let expected_stream = expected_commit.coord.stream_id.to_string();

    let local_hlc = Arc::new(Hlc::new(
        "dev-b".into(),
        coven_foundation::clock::clock_from_millis(|| 1_000),
    ));
    let target = open_test_db_with_hlc(local_hlc, |_conn| Ok(()));
    let (_tmp, store_dir) = temp_store_dir();
    let (commit_reached, _resume_pull) =
        target
            .database
            .arm_test_pause(coven_database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id: expected_stream.clone(),
                seq: 1,
            });
    let pull_db = target.clone();
    let pull_storage = storage.clone();
    let pull_store_dir = store_dir.clone();
    let pull = tokio::spawn(async move { pull_storage.pull_into(&pull_db, &pull_store_dir).await });

    commit_reached.notified().await;
    let queued_stamp = coven_database::StoreDatabase::new(&target.database).stamp();
    let host_stamp = coven_database::StoreDatabase::new(&target.database)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                         VALUES ('local-boundary', 'Local', NULL, ?1, '2026-01-01')",
                [queued_stamp.as_str()],
            )
            .map_err(coven_database::DbError::from)?;
            Ok::<String, coven_database::DbError>(queued_stamp)
        })
        .await
        .expect("queued host write commits")
        .value;
    pull.abort();
    let _ = pull.await;

    assert!(
        host_stamp > remote_stamp,
        "host stamp {host_stamp} must sort after the already-committed remote row \
         {remote_stamp}",
    );
    assert!(
        target
            .database
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'remote-boundary'")
            .await
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&target.database)
            .materialized_frontier()
            .await
            .expect("read materialized frontier")
            .get(&expected_stream),
        Some(&expected_commit),
        "cancellation after the commit leaves its row and position durable",
    );
}

/// Restart seeding. A clock reconstructed from a persisted high-water mark must
/// not mint a stamp below existing rows — even when its wall clock has jumped
/// backward and even within the same millisecond as the persisted mark.
#[test]
fn reconstructed_clock_does_not_regress_below_persisted_high_water() {
    let hlc1 = Hlc::new(
        "dev-a".into(),
        coven_foundation::clock::clock_from_millis(|| 5_000),
    );
    let last_row_stamp = hlc1.now();
    let persisted = hlc1.high_water().to_string();
    assert_eq!(persisted, last_row_stamp.to_string());

    // Restart with the wall clock jumped *backward* and seed from the mark.
    let hlc2 = Hlc::new(
        "dev-a".into(),
        coven_foundation::clock::clock_from_millis(|| 1_000),
    );
    hlc2.seed(&Timestamp::parse(&persisted).expect("parse high-water"));
    let next = hlc2.now();
    assert!(
        next > last_row_stamp,
        "reconstructed clock minted {next}, which regresses below {last_row_stamp}",
    );

    // Same-millisecond restart: seed at exactly the persisted mark; still advances.
    let hlc3 = Hlc::new(
        "dev-a".into(),
        coven_foundation::clock::clock_from_millis(|| 5_000),
    );
    hlc3.seed(&Timestamp::parse(&persisted).expect("parse high-water"));
    let after_same_ms = hlc3.now();
    assert!(
        after_same_ms > last_row_stamp,
        "same-millisecond restart minted {after_same_ms}, not above {last_row_stamp}",
    );
}

/// Revocation is enforced by current membership, not by when a changeset was
/// committed. A member publishes a changeset while their grant is active, then an
/// owner removes them before another device pulls it. The pull must reject the
/// earlier commit because its author lacks a current membership grant.
#[tokio::test]
async fn removed_member_changeset_is_rejected_despite_in_window_timestamp() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let owner_db = open_test_db();
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = TestStoreFixture::create(
        &owner_db,
        "test-store",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact revocation test Store");
    let encryption = EncryptionService::from_key([42; 32]);
    storage
        .invite_member(
            &owner_db,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("invite exact member identity");

    let receiver_db = open_test_db();
    storage
        .open_into(&receiver_db)
        .await
        .expect("open exact Store on receiving device");
    let member_db = open_test_db();
    storage
        .activate_joined_device(&owner_db, &member_db, &member, "0000000002500-0000-member")
        .await
        .expect("install member's active exact device fixture");
    let member_device_id = member_db
        .database
        .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read member device id")
        .expect("member device registration is active");

    member_db
        .database
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Stale writer', NULL, 1, '0000000003000-0000-member', '2026-01-01')",
        )
        .await;
    let (_member_temp, member_store_dir) = temp_store_dir();
    let member_store = crate::sync::store::Store::load(
        coven_database::StoreDatabase::new(&member_db.database),
        cloud_storage,
        member_store_dir,
        member.clone(),
    )
    .await
    .expect("load member Store");
    let mut writer = member_store
        .authorize_writer()
        .await
        .expect("authorize member writer while their grant is active");
    assert!(
        writer
            .prepare_pending_store_write()
            .await
            .expect("prepare member commit while grant is active"),
        "member write must prepare while its grant is active",
    );
    writer
        .drain_store_writes()
        .await
        .expect("publish member commit while grant is active");

    let custody = TestCustody::default();
    custody.set_initial_key([42; 32]);
    storage
        .remove_member(
            &owner_db,
            &owner,
            &pubkey_hex(&member),
            &encryption,
            &custody,
        )
        .await
        .expect("remove exact member identity");

    let (updated, _result) = storage.pull_into(&receiver_db, &temp_store_dir().1).await;

    assert!(
        !receiver_db
            .database
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(updated.get(&member_device_id), None);
}

/// `SyntheticStoreFixture::open` seeds the register from the persisted high-water mark, so the
/// very first host stamp after a restart does not regress below existing rows.
#[tokio::test]
async fn register_seeds_from_persisted_high_water() {
    // A high-water mark far ahead of any plausible wall millis. No synced rows on
    // disk, so the high-water mark is the only floor.
    let high = "9999999999000-0007-dev-a";
    let temp = tempfile::tempdir().expect("create register restart directory");
    let path = temp.path().join("register.sqlite");
    let migrations = test_migrations();
    let before_restart = coven_database::SyntheticStoreFixture::open(
        &path,
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "dev-a".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &migrations,
    )
    .expect("open register database before restart");
    before_restart
        .database
        .set_protocol_state(HIGHWATER_STATE_KEY, high)
        .await
        .expect("persist high-water before restart");
    drop(before_restart);

    let db = coven_database::SyntheticStoreFixture::open_with_hlc(
        &path,
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        Arc::new(Hlc::new(
            "dev-a".into(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
        )),
        &migrations,
    )
    .expect("reopen register database with persisted high-water");

    let stamp = coven_database::StoreDatabase::new(&db.database).stamp();
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
    let row_stamp = "9000000000000-0011-dev-a";
    let db = open_test_db_with_hlc(
        Arc::new(Hlc::new(
            "dev-a".into(),
            coven_foundation::clock::clock_from_millis(|| 9_000_000_000_000),
        )),
        move |conn| {
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n0', 'row', NULL, ?1, '2026-01-01')",
                [row_stamp],
            )
            .map(|_| ())
            .map_err(coven_database::DbError::from)
        },
    );

    let stamp = coven_database::StoreDatabase::new(&db.database).stamp();
    assert!(
        stamp.as_str() > row_stamp,
        "first stamp {stamp} must sort after the on-disk row {row_stamp}; \
         seeding from the flushed high-water alone misses un-flushed local rows",
    );
}

/// Restart seeding uses the greatest synced-row register stamp an honest device
/// could have produced, so a grossly-future row already on disk cannot drag the
/// clock past every later local write.
#[tokio::test]
async fn restart_does_not_seed_past_grossly_future_synced_row() {
    let wall: u64 = 1_700_000_000_000;
    let honest = format!("{wall:013}-0000-dev-a");
    let poison_ms = wall + 60 * 24 * 60 * 60 * 1000;
    let poison = format!("{poison_ms:013}-0000-dev-b");
    let honest_seed = honest.clone();
    let poison_seed = poison.clone();
    let db = open_test_db_with_hlc(
        Arc::new(Hlc::new(
            "dev-local".into(),
            coven_foundation::clock::clock_from_millis(move || wall),
        )),
        move |conn| {
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('honest', 'row', NULL, ?1, '2026-01-01')",
                [honest_seed.as_str()],
            )
            .map_err(coven_database::DbError::from)?;
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('poison', 'row', NULL, ?1, '2026-01-01')",
                [poison_seed.as_str()],
            )
            .map(|_| ())
            .map_err(coven_database::DbError::from)
        },
    );

    let stamp = coven_database::StoreDatabase::new(&db.database).stamp();
    assert!(
        stamp.as_str() > honest.as_str(),
        "first stamp {stamp} must sort after honest on-disk row {honest}",
    );
    assert!(
        stamp.as_str() < poison.as_str(),
        "first stamp {stamp} must not seed past grossly-future on-disk row {poison}",
    );
}

#[tokio::test]
async fn restart_seeds_past_within_bound_synced_row() {
    let wall: u64 = 1_700_000_000_000;
    let within_ms = wall + 60 * 60 * 1000;
    let within = format!("{within_ms:013}-0000-dev-a");
    let within_seed = within.clone();
    let db = open_test_db_with_hlc(
        Arc::new(Hlc::new(
            "dev-local".into(),
            coven_foundation::clock::clock_from_millis(move || wall),
        )),
        move |conn| {
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('within', 'row', NULL, ?1, '2026-01-01')",
                [within_seed.as_str()],
            )
            .map(|_| ())
            .map_err(coven_database::DbError::from)
        },
    );

    let stamp = coven_database::StoreDatabase::new(&db.database).stamp();
    assert!(
        stamp.as_str() > within.as_str(),
        "first stamp {stamp} must sort after within-bound on-disk row {within}",
    );
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
    // B's wall clock is pinned at t = 1_700_000_000_000 (a 2023 millis).
    let b_wall: u64 = 1_700_000_000_000;
    let b_hlc = Arc::new(Hlc::new(
        "dev-b".into(),
        coven_foundation::clock::clock_from_millis(move || b_wall),
    ));

    // B already holds an honest local edit of n1, stamped at its own wall time.
    let b_local_stamp = format!("{b_wall:013}-0000-dev-b");
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    db_b.database
        .execute_test_sql(&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'B honest', NULL, '{b_local_stamp}', '2026-01-01')"
        ))
        .await;

    // A publishes a competing edit of n1 stamped ten years in the future — far
    // beyond any legitimate offline skew.
    let a_future_ms = b_wall + 10 * 365 * 24 * 60 * 60 * 1000;
    let a_future_stamp = format!("{a_future_ms:013}-0000-dev-a");
    let db_a = open_test_db();
    let storage = TestStore::create(
        &db_a,
        "test-store",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact test Store for the test database");
    let cs_a = db_a
        .database
        .capture_test_changeset(&[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A far future', NULL, '{a_future_stamp}', '2026-01-01')"
        )])
        .await;
    storage
        .publish_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION)
        .await
        .expect("publish grossly-future changeset");

    // B pulls A's changeset.
    let (_positions, _result) = storage.pull_into(&db_b, &temp_store_dir().1).await;

    // (a) LWW: the grossly-future row must NOT win — B's honest local edit stands.
    assert_eq!(
        db_b.database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "B honest",
        "a grossly-future incoming _updated_at won LWW over an honest local stamp",
    );

    // (b) clock ratchet: the pull advances the register itself, so the live clock
    // is the observable — it must not be skewed. B's next stamp sorts near its own
    // wall time, far below the ten-years-future incoming value.
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
    let b_wall: u64 = 1_700_000_000_000;
    let b_hlc = Arc::new(Hlc::new(
        "dev-b".into(),
        coven_foundation::clock::clock_from_millis(move || b_wall),
    ));

    // B holds an honest local edit at its own wall time.
    let b_local_stamp = format!("{b_wall:013}-0000-dev-b");
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    db_b.database
        .execute_test_sql(&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'B honest', NULL, '{b_local_stamp}', '2026-01-01')"
        ))
        .await;

    // A's edit is stamped three days ahead — a plausible cross-device clock spread,
    // well within the offline-skew allowance.
    let a_ms = b_wall + 3 * 24 * 60 * 60 * 1000;
    let a_stamp = format!("{a_ms:013}-0000-dev-a");
    let db_a = open_test_db();
    let storage = TestStore::create(
        &db_a,
        "test-store",
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact test Store for the test database");
    let cs_a = db_a
        .database
        .capture_test_changeset(&[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A skewed', NULL, '{a_stamp}', '2026-01-01')"
        )])
        .await;
    storage
        .publish_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION)
        .await
        .expect("publish within-bound changeset");

    let (_positions, _result) = storage.pull_into(&db_b, &temp_store_dir().1).await;

    // A's causally-later (within-allowance) edit wins.
    assert_eq!(
        db_b.database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "A skewed",
        "a legitimately-skewed incoming edit failed to win LWW",
    );

    // And the pull advances B's clock past the applied stamp, so B's next write
    // sorts after A's.
    let next = b_hlc.now().to_string();
    assert!(
        next.as_str() > a_stamp.as_str(),
        "B's clock did not advance past a legitimately-skewed applied stamp: \
         next={next} applied={a_stamp}",
    );
}

/// Regression: a sync cycle that errors mid-cycle must still leave host-write
/// capture working. The `SyntheticStoreFixture` and its connection thread outlive the cycle and
/// are reused every cycle. Capture is structural — each host write records into the
/// pending-changeset journal inside its own transaction, and the journal already
/// holds any change a failed cycle didn't manage to push — so there is no
/// cross-call capture state a mid-cycle abort could strand.
///
/// We force the failure after open by blocking every `INSERT` into
/// `protocol_state` with a trigger that `RAISE(ABORT)`s. The cycle's top-of-cycle
/// reads (plain `SELECT`s) still succeed, but the first bookkeeping persist fails,
/// so the cycle returns `Err`. A subsequent host write must still journal into the
/// next drained changeset.
#[tokio::test]
async fn cycle_error_mid_cycle_still_captures_host_writes() {
    // Open normally, then make `protocol_state` reject every INSERT so a
    // `set_protocol_state` inside the cycle fails. Reads remain available.
    let migrations = test_migrations();
    let db = coven_database::SyntheticStoreFixture::open(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "dev-self".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &migrations,
    )
    .expect("open database before installing protocol_state fault");
    let keypair = UserKeypair::generate();
    let storage = TestStore::create(
        &db,
        "test-store",
        keypair.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store before installing protocol_state fault");
    let device = storage
        .open_into(&db)
        .await
        .expect("bind the founder device before installing protocol_state fault");
    db.database
        .test_sql(|database| database.install_protocol_state_insert_failure_trigger())
        .await
        .expect("install protocol_state fault");

    // A local insert gives the cycle a pending Store write. The trigger also blocks
    // the unconditional HLC high-water persist, so the cycle fails after the write
    // commits to the ledger.
    db.database
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'First', NULL, 1, '0000000001000-0000-dev-self', '2026-01-01')",
        )
        .await;

    let (_t, ld) = temp_store_dir();
    let result = device.run_cycle(&ld, None).await;

    assert!(
        result.is_err(),
        "the blocked set_protocol_state must make the cycle fail mid-span"
    );

    // Capture must still be live despite the mid-cycle failure: a fresh host write
    // journals and is drained into the next changeset (alongside n1, which the
    // failed cycle never pushed). If a failure could leave capture off, this drain
    // would not carry n2.
    let next = db
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n2', 'After failure', NULL, 1, '0000000002000-0000-dev-self', '2026-01-01')",
        ])
        .await;

    let changes = coven_database::walk_changeset(&next).expect("walk next changeset");
    assert!(
        changes
            .iter()
            .any(|c| c.table == "notes" && c.pk() == Some("n2")),
        "the post-failure host write was not captured — the cycle left capture off \
         (the leak this test guards against)"
    );
}
