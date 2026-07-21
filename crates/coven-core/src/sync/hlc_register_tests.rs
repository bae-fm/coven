//! Behavioral tests for the HLC as coven's `_updated_at` register.
//!
//! Unlike the self-tests in `hlc.rs` (which prove the clock is a correct clock),
//! these assert an *external* outcome of wiring the clock to the data plane: they
//! fail if `_updated_at` is wall-clock-stamped, if the clock regresses across a
//! restart, or if revocation depended on an author-supplied transport timestamp
//! rather than current write-capable membership. They drive a real
//! [`crate::database::Database`] (with an injected, wall-clock-controlled `Hlc`)
//! so the register lives where production puts it: inside the owned connection.

use std::sync::Arc;

use crate::clock::SystemClock;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::sync::cloud_storage::{CloudCipher, PendingRotation};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::{Hlc, Timestamp, HIGHWATER_STATE_KEY};
use crate::sync::membership::MemberRole;
/// The synthetic test db opens with a single migration, so its
/// [`crate::database::Database::schema_version`] is 1. Changesets are stored at
/// that version.
const SCHEMA_VERSION: u32 = 1;

async fn create_store(db: &crate::database::Database) -> TestStore {
    TestStore::create(db, "test-store", UserKeypair::generate())
        .await
        .expect("create exact test Store for the test database")
}
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
    let a_hlc = Arc::new(Hlc::with_wall_clock("dev-a".into(), || 9_000));
    let b_hlc = Arc::new(Hlc::with_wall_clock("dev-b".into(), || 1_000));

    // A stamps and publishes an edit of n1 through the Store outbox so its
    // registration, commit, and local materialized frontier advance together
    // without publishing a snapshot that already contains the row.
    let a_stamp = a_hlc.now().to_string();
    let db_a = open_test_db_with_hlc(a_hlc.clone(), |_conn| Ok(()));
    let keypair = UserKeypair::generate();
    let storage = TestStore::create(&db_a, "test-store", keypair.clone())
        .await
        .expect("create exact HLC test Store");
    let (_a_temp, a_store_dir) = temp_store_dir();
    storage
        .open_into(&db_a)
        .await
        .expect("open exact test Store");
    host_exec(
        &db_a,
        &format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at, shared) \
             VALUES ('n1', 'A wrote this', NULL, '{a_stamp}', '2026-01-01', 1)"
        ),
    )
    .await;
    assert_eq!(
        db_a.pending_writes().await.unwrap().len(),
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
        db_a.pending_writes().await.unwrap().is_empty(),
        "A's Store publication must finish its initial edit"
    );

    // B's join bootstrap pulls the signed cut into its own Database (clock =
    // b_hlc), including A's edit, before it activates the local registration.
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    let (_t, ld) = temp_store_dir();
    install_active_device_fixture(&storage, &db_a, &db_b, &keypair, "0000000001000-0000-dev-b")
        .await
        .expect("install B's active exact device fixture");
    let active_device_count = db_a
        .call(|connection| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM store_device_registration_activations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(crate::database::DbError::from)
        })
        .await
        .expect("count A's activated devices");
    assert_eq!(active_device_count, 2, "A must activate B's registration");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
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
    host_exec(
        &db_b,
        &format!(
            "UPDATE notes SET title = 'B wrote this', _updated_at = '{b_stamp}' \
             WHERE id = 'n1'"
        ),
    )
    .await;
    assert!(
        storage
            .publish_pending(&db_b, &ld)
            .await
            .expect("publish B's post-pull edit"),
        "B's Store outbox must publish its post-pull edit"
    );

    // A pulls B's edit; B wins on LWW because b_stamp > a_stamp.
    let (_, first_pull) = pull_into(&db_a, &storage, &temp_store_dir().1).await;
    assert!(
        first_pull.held_positions.is_empty(),
        "A held B's post-pull edit: {:?}",
        first_pull.held_positions
    );
    let (_, second_pull) = pull_into(&db_a, &storage, &temp_store_dir().1).await;
    assert!(
        second_pull.held_positions.is_empty(),
        "A held B's post-activation stream: {:?}",
        second_pull.held_positions
    );
    assert_eq!(
        query_text(&db_a, "SELECT title FROM notes WHERE id = 'n1'").await,
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
    let a_hlc = Hlc::with_wall_clock("dev-a".into(), || 9_000);
    let b_hlc = Arc::new(Hlc::with_wall_clock("dev-b".into(), || 1_000));

    let a_stamp = a_hlc.now().to_string();
    let db_a = open_test_db();
    let storage = create_store(&db_a).await;
    let cs_a = capture_bytes(
        &db_a,
        &[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A wrote this', NULL, '{a_stamp}', '2026-01-01')"
        )],
    )
    .await;
    storage
        .publish_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION)
        .await
        .expect("publish A changeset");

    // B pulls A's Store commit directly — no cycle wraps it, so
    // the only advance that can fire is the per-changeset one.
    let db_b = open_test_db_with_hlc(b_hlc.clone(), |_conn| Ok(()));
    let (_positions, result) = pull_into(&db_b, &storage, &temp_store_dir().1).await;
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
    let remote_hlc = Hlc::with_wall_clock("dev-a".into(), || 9_000);
    let remote_stamp = remote_hlc.now().to_string();
    let source = open_test_db();
    let storage = Arc::new(create_store(&source).await);
    let changeset = capture_bytes(
        &source,
        &[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('remote-boundary', 'Remote', NULL, '{remote_stamp}', '2026-01-01')"
        )],
    )
    .await;
    let expected_commit = storage
        .publish_changeset("dev-a", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish remote changeset");
    let expected_stream = match expected_commit.coord {
        crate::sync::store_commit::StoreCommitCoord::MergeConcurrent { stream_id, .. } => {
            stream_id.to_string()
        }
        crate::sync::store_commit::StoreCommitCoord::Serial { .. } => {
            panic!("test Store must use MergeConcurrent commits")
        }
    };

    let local_hlc = Arc::new(Hlc::with_wall_clock("dev-b".into(), || 1_000));
    let target = open_test_db_with_hlc(local_hlc, |_conn| Ok(()));
    let (_tmp, store_dir) = temp_store_dir();
    let (commit_reached, _resume_pull) =
        target.arm_test_pause(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
            device_id: expected_stream.clone(),
            seq: 1,
        });
    let pull_db = target.clone();
    let pull_storage = storage.clone();
    let pull_store_dir = store_dir.clone();
    let pull =
        tokio::spawn(
            async move { pull_into(&pull_db, pull_storage.as_ref(), &pull_store_dir).await },
        );

    commit_reached.notified().await;
    let tables = target.synced_tables().to_vec();
    let stamper = target.stamper();
    let write_id = target.new_write_id();
    let host_stamp = target
        .call(move |conn| {
            crate::database::Database::run_internal_store_write_transaction_on(
                conn,
                &tables,
                crate::WritePolicy::MergeConcurrent,
                None,
                write_id,
                |tx| {
                    let stamp = stamper.stamp();
                    tx.execute(
                        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                         VALUES ('local-boundary', 'Local', NULL, ?1, '2026-01-01')",
                        [stamp.as_str()],
                    )
                    .map_err(crate::database::DbError::from)?;
                    Ok::<String, crate::database::DbError>(stamp)
                },
            )
        })
        .await
        .expect("queued host write commits");
    pull.abort();
    let _ = pull.await;

    assert!(
        host_stamp > remote_stamp,
        "host stamp {host_stamp} must sort after the already-committed remote row \
         {remote_stamp}",
    );
    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'remote-boundary'").await);
    assert_eq!(
        target
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

/// Revocation is enforced by current membership, not by when a changeset was
/// committed. A member publishes a changeset while their grant is active, then an
/// owner removes them before another device pulls it. The pull must reject the
/// earlier commit because its author lacks a current membership grant.
#[tokio::test]
async fn removed_member_changeset_is_rejected_despite_in_window_timestamp() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = TestStore::create(&owner_db, "test-store", owner.clone())
        .await
        .expect("create exact revocation test Store");
    let encryption = EncryptionService::from_key([42; 32]);
    crate::sync::membership_ops::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::with_wall_clock("owner".to_string(), || 2_000),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &encryption,
        "test-store",
        "Test Store",
        &owner_db,
    )
    .await
    .expect("invite exact member identity");

    let receiver_db = open_test_db();
    storage
        .open_into(&receiver_db)
        .await
        .expect("open exact Store on receiving device");
    let member_db = open_test_db();
    install_active_device_fixture(
        &storage,
        &owner_db,
        &member_db,
        &member,
        "0000000002500-0000-member",
    )
    .await
    .expect("install member's active exact device fixture");
    let member_device_id = member_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read member device id")
        .expect("member device registration is active");

    host_exec(
        &member_db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Stale writer', NULL, 1, '0000000003000-0000-member', '2026-01-01')",
    )
    .await;
    let membership = crate::sync::pull::load_cycle_membership(&storage.storage, &member_db)
        .await
        .expect("load membership while member grant is active");
    let (_member_temp, member_store_dir) = temp_store_dir();
    assert!(
        crate::sync::store_outbound::prepare_pending_store_write(
            &member_db,
            &storage.storage,
            &member_device_id,
            "0000000003000-0000-member",
            &member,
            &member_store_dir,
            membership.chain.as_ref(),
        )
        .await
        .expect("prepare member commit while grant is active"),
        "member write must prepare while its grant is active",
    );
    crate::sync::store_outbound::drain_store_writes(&member_db, &storage.storage)
        .await
        .expect("publish member commit while grant is active");

    let custody = TestCustody::default();
    custody.set_initial_key([42; 32]);
    let cipher = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    crate::sync::membership_ops::remove_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::with_wall_clock("owner".to_string(), || 4_000),
        &pubkey_hex(&member),
        "test-store",
        &encryption,
        &custody,
        cipher.as_ref(),
        pending_rotation.as_ref(),
        &owner_db,
    )
    .await
    .expect("remove exact member identity");

    let (updated, _result) = pull_into(&receiver_db, &storage, &temp_store_dir().1).await;

    assert!(!row_exists(&receiver_db, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get(&member_device_id), None);
}

/// `Database::open` seeds the register from the persisted high-water mark, so the
/// very first host stamp after a restart does not regress below existing rows.
#[tokio::test]
async fn register_seeds_from_persisted_high_water() {
    // A high-water mark far ahead of any plausible wall millis. No synced rows on
    // disk, so the high-water mark is the only floor.
    let high = "9999999999000-0007-dev-a";
    let temp = tempfile::tempdir().expect("create register restart directory");
    let path = temp.path().join("register.sqlite");
    let migrations = test_migrations();
    let (before_restart, _stamper) = crate::database::Database::open(
        &path,
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::MergeConcurrent,
        "dev-a".to_string(),
        &migrations,
    )
    .expect("open register database before restart");
    before_restart
        .set_protocol_state(HIGHWATER_STATE_KEY, high)
        .await
        .expect("persist high-water before restart");
    drop(before_restart);

    let (db, _stamper) = crate::database::Database::open_with_hlc(
        &path,
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::MergeConcurrent,
        Arc::new(Hlc::new("dev-a".into())),
        &migrations,
    )
    .expect("reopen register database with persisted high-water");

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
    let row_stamp = "9000000000000-0011-dev-a";
    let db = open_test_db_with_hlc(
        Arc::new(Hlc::with_wall_clock("dev-a".into(), || 9_000_000_000_000)),
        move |conn| {
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n0', 'row', NULL, ?1, '2026-01-01')",
                [row_stamp],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        },
    );

    let stamp = db.hlc().now().to_string();
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
        Arc::new(Hlc::with_wall_clock("dev-local".into(), move || wall)),
        move |conn| {
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('honest', 'row', NULL, ?1, '2026-01-01')",
                [honest_seed.as_str()],
            )
            .map_err(crate::database::DbError::from)?;
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('poison', 'row', NULL, ?1, '2026-01-01')",
                [poison_seed.as_str()],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        },
    );

    let stamp = db.hlc().now().to_string();
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
        Arc::new(Hlc::with_wall_clock("dev-local".into(), move || wall)),
        move |conn| {
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('within', 'row', NULL, ?1, '2026-01-01')",
                [within_seed.as_str()],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        },
    );

    let stamp = db.hlc().now().to_string();
    assert!(
        stamp.as_str() > within.as_str(),
        "first stamp {stamp} must sort after within-bound on-disk row {within}",
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
    let seeded_floor = "9000000000000-0005-dev-a";
    let migrations = vec![crate::migration::Migration::run(
        1,
        "test-schema",
        move |conn| {
            create_synced_schema(conn)?;
            conn.execute(
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n0', 'row', NULL, ?1, '2026-01-01')",
                [seeded_floor],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        },
    )];
    let (db, stamper) = crate::database::Database::open_with_hlc(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::MergeConcurrent,
        Arc::new(Hlc::with_wall_clock("dev-a".into(), || 9_000_000_000_000)),
        &migrations,
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
    let storage = create_store(&db_a).await;
    let cs_a = capture_bytes(
        &db_a,
        &[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A far future', NULL, '{a_future_stamp}', '2026-01-01')"
        )],
    )
    .await;
    storage
        .publish_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION)
        .await
        .expect("publish grossly-future changeset");

    // B pulls A's changeset.
    let (_positions, _result) = pull_into(&db_b, &storage, &temp_store_dir().1).await;

    // (a) LWW: the grossly-future row must NOT win — B's honest local edit stands.
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
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
    let storage = create_store(&db_a).await;
    let cs_a = capture_bytes(
        &db_a,
        &[&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A skewed', NULL, '{a_stamp}', '2026-01-01')"
        )],
    )
    .await;
    storage
        .publish_changeset("dev-a", 1, &cs_a, SCHEMA_VERSION)
        .await
        .expect("publish within-bound changeset");

    let (_positions, _result) = pull_into(&db_b, &storage, &temp_store_dir().1).await;

    // A's causally-later (within-allowance) edit wins.
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
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
/// capture working. The `Database` and its connection thread outlive the cycle and
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
    let (db, _stamper) = crate::database::Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::MergeConcurrent,
        "dev-self".to_string(),
        &migrations,
    )
    .expect("open database before installing protocol_state fault");
    let keypair = UserKeypair::generate();
    let storage = TestStore::create(&db, "test-store", keypair.clone())
        .await
        .expect("create exact Store before installing protocol_state fault");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read exact local device id")
        .expect("founder device registration is active");
    exec(
        &db,
        "CREATE TRIGGER block_protocol_state_insert BEFORE INSERT ON protocol_state \
         BEGIN SELECT RAISE(ABORT, 'forced set_protocol_state failure'); END;",
    )
    .await;

    // A local insert gives the cycle a pending Store write. The trigger also blocks
    // the unconditional HLC high-water persist, so the cycle fails after the write
    // commits to the ledger.
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'First', NULL, 1, '0000000001000-0000-dev-self', '2026-01-01')",
    )
    .await;

    let (_t, ld) = temp_store_dir();
    let encryption = std::sync::RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [42; 32],
    )));
    let hlc = db.hlc();

    let result = run_single_sync_cycle(
        &storage.storage,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &encryption,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await;

    assert!(
        result.is_err(),
        "the blocked set_protocol_state must make the cycle fail mid-span"
    );

    // Capture must still be live despite the mid-cycle failure: a fresh host write
    // journals and is drained into the next changeset (alongside n1, which the
    // failed cycle never pushed). If a failure could leave capture off, this drain
    // would not carry n2.
    let next = capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n2', 'After failure', NULL, 1, '0000000002000-0000-dev-self', '2026-01-01')",
        ],
    )
    .await;

    let changes = crate::changeset::walk(&next).expect("walk next changeset");
    assert!(
        changes
            .iter()
            .any(|c| c.table == "notes" && c.pk() == Some("n2")),
        "the post-failure host write was not captured — the cycle left capture off \
         (the leak this test guards against)"
    );
}
