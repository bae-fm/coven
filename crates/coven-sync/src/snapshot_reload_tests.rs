//! A circle readable again reloads what this device skipped (§14.4).

use crate::device_log_sync::tests::circles::{household, identity};
use crate::device_log_sync::tests::{rows, sql, storage, STORE_ID};
use crate::store_log_sync::tests::Device;
use crate::SyncError;
use coven_database::{Database, DbError, SnapshotError, StoreLogState};
use coven_format::store_log::{MemberRole, StoreChange};
use coven_foundation::id_source::{CircleId, KeyId};
use coven_merge::{Audience, WriteId};
use coven_storage::Storage;
use std::collections::BTreeSet;
use std::time::{Duration, UNIX_EPOCH};
use uuid::Uuid;

const GIFTS: CircleId = CircleId(Uuid::from_u128(10));

async fn try_sql(db: &Database, statement: &'static str) -> Result<(), DbError> {
    db.write(move |context| {
        context.execute_batch(statement)?;
        Ok(())
    })
    .await
}

async fn pins(db: &Database) -> Vec<String> {
    db.read(|sql| Ok(sql.query("SELECT title FROM pins ORDER BY title", [], |r| r.get(0))?))
        .await
        .unwrap()
}

async fn reports_gifts(db: &Database) -> bool {
    db.pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .iter()
        .any(|record| record.subject == crate::PendingSubject::Audience(Audience::Circle(GIFTS)))
}

async fn passed(device: &Device, write: WriteId) -> bool {
    device
        .db
        .sync_state(Vec::new())
        .await
        .unwrap()
        .positions
        .covers(write)
}

/// Ben leaves Gifts, and Ana's write 2 adds a note and a Gifts pin. Ben applies
/// the note, so his position passes write 2. When Ana readmits him, that
/// position is no proof he read the pin: he reloads Gifts from storage, and
/// while write 2 is missing the reload fails as a whole. Removed again before
/// it commits, he waits for no reload; readmitted, he waits again.
#[tokio::test]
async fn a_rejoined_member_reloads_the_part_its_passed_position_skipped() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    let [ana, ben, _] = devices.as_mut_slice() else {
        panic!("three members")
    };
    let membership = |add| {
        let (circle, member) = (GIFTS, identity(4).member());
        if add {
            StoreChange::AddCircleMember { circle, member }
        } else {
            StoreChange::RemoveCircleMember { circle, member }
        }
    };
    sql(&ana.db, "INSERT INTO notes VALUES('first','public','one'); INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','before leaving')").await;
    ana.writes.upload_writes().await.unwrap();
    ben.writes.download_writes().await.unwrap();
    assert_eq!(pins(&ben.db).await, ["before leaving"]);
    ana.sync
        .make_and_upload_entry(membership(false))
        .await
        .unwrap();
    ana.sync
        .test_rotate(Audience::Circle(GIFTS), KeyId(Uuid::from_u128(11)))
        .await;
    sql(&ana.db, "INSERT INTO notes VALUES('second','public','two'); INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000002','00000000-0000-0000-0000-00000000000a','while away')").await;
    let skipped = ana.writes.upload_writes().await.unwrap();
    assert_eq!(skipped.len(), 1);
    ben.sync.sync_store_log().await.unwrap();
    ben.writes.download_writes().await.unwrap();
    assert!(passed(ben, skipped[0]).await);
    assert_eq!(pins(&ben.db).await, ["before leaving"]);
    assert_eq!(rows(&ben.db).await.len(), 2);

    // The skipped write is gone, and no Gifts snapshot covers it.
    let path = crate::write_seal::path(STORE_ID, skipped[0]);
    let bytes = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    ana.sync
        .make_and_upload_entry(membership(true))
        .await
        .unwrap();
    let failed = ben.sync.sync_store_log().await;
    assert!(
        matches!(&failed, Err(SyncError::Database(DbError::Snapshot(SnapshotError::MissingWrites { missing }))) if *missing == [skipped[0]]),
        "{failed:?}"
    );
    assert_eq!(
        ben.db.reloading_audiences().await.unwrap(),
        BTreeSet::from([Audience::Circle(GIFTS)])
    );
    assert!(reports_gifts(&ben.db).await);
    // The failed reload left the old rows and positions; Gifts refuses writes
    // while the store stays writable.
    assert_eq!(pins(&ben.db).await, ["before leaving"]);
    assert!(passed(ben, skipped[0]).await);
    assert!(matches!(
        try_sql(&ben.db, "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000003','00000000-0000-0000-0000-00000000000a','refused')").await,
        Err(DbError::AudienceReloading(Audience::Circle(GIFTS)))
    ));
    try_sql(&ben.db, "INSERT INTO notes VALUES('third','local','three')")
        .await
        .unwrap();

    // Replay that leaves Gifts unreadable withdraws the requirement and its
    // pending record; Gifts refuses Ben's writes as a circle he is not in.
    ana.sync
        .make_and_upload_entry(membership(false))
        .await
        .unwrap();
    ben.sync.sync_store_log().await.unwrap();
    assert!(ben.db.reloading_audiences().await.unwrap().is_empty());
    assert!(!reports_gifts(&ben.db).await);
    assert!(matches!(
        try_sql(&ben.db, "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000003','00000000-0000-0000-0000-00000000000a','refused')").await,
        Err(DbError::NotInCircle(GIFTS))
    ));
    ana.sync
        .make_and_upload_entry(membership(true))
        .await
        .unwrap();
    assert!(matches!(
        ben.sync.sync_store_log().await,
        Err(SyncError::Database(DbError::Snapshot(
            SnapshotError::MissingWrites { .. }
        )))
    ));
    assert_eq!(
        ben.db.reloading_audiences().await.unwrap(),
        BTreeSet::from([Audience::Circle(GIFTS)])
    );
    assert!(reports_gifts(&ben.db).await);

    storage.create(&path, &bytes).await.unwrap();
    ben.elapse_retry_delays();
    ben.sync.sync_store_log().await.unwrap();
    assert!(ben.db.reloading_audiences().await.unwrap().is_empty());
    assert_eq!(pins(&ben.db).await, ["before leaving", "while away"]);
    assert_eq!(
        rows(&ben.db)
            .await
            .into_iter()
            .map(|(id, _, _)| id)
            .collect::<Vec<_>>(),
        ["first", "second", "third"]
    );
    try_sql(&ben.db, "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000003','00000000-0000-0000-0000-00000000000a','after')").await.unwrap();
}

/// Ben makes Plans. Ana, unaware, removes Ben, which drops his creation, so
/// Plans is absent when Carol skips Ben's pin: she holds no key for it. Ben then
/// adds Carol and, before reading Ana's removal, removes Ana. Ana's removal
/// would now leave no admin, so it drops and Plans returns with Carol in it:
/// Carol reloads the pin her position already passed.
#[tokio::test]
async fn a_circle_whose_creation_dropped_reloads_when_it_returns() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    let [ana, ben, carol] = devices.as_mut_slice() else {
        panic!("three members")
    };
    let plans = CircleId(Uuid::from_u128(20));
    ana.sync
        .make_and_upload_entry(StoreChange::ChangeRole {
            member: identity(5).member(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    for device in [&mut *ben, &mut *carol] {
        device.sync.sync_store_log().await.unwrap();
    }
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(10));
    let key_hash = ben
        .sync
        .test_key(Audience::Circle(plans), KeyId(Uuid::from_u128(20)));
    ben.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: plans,
            name: "Plans".into(),
            key: KeyId(Uuid::from_u128(20)),
            key_hash,
        })
        .await
        .unwrap();
    ana.clock.set(UNIX_EPOCH + Duration::from_secs(20));
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: identity(4).member(),
        })
        .await
        .unwrap();
    carol.sync.sync_store_log().await.unwrap();
    let log = carol.db.store_log().await.unwrap();
    assert!(!log.replay.state.circles.contains_key(&plans));
    assert!(!log.replay.state.circle_deletions.contains_key(&plans));
    sql(&ben.db, "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000004','00000000-0000-0000-0000-000000000014','plan')").await;
    let written = ben.writes.upload_writes().await.unwrap();
    carol.writes.download_writes().await.unwrap();
    assert!(passed(carol, written[0]).await);
    assert!(pins(&carol.db).await.is_empty());

    ben.clock.set(UNIX_EPOCH + Duration::from_secs(11));
    for change in [
        StoreChange::AddCircleMember {
            circle: plans,
            member: identity(5).member(),
        },
        StoreChange::RemoveMember {
            member: identity(3).member(),
        },
    ] {
        ben.sync.make_and_upload_entry(change).await.unwrap();
    }
    carol.sync.sync_store_log().await.unwrap();
    let state = carol.db.store_log().await.unwrap().replay.state;
    assert!(state.reads_circle(plans, &identity(5).member()));
    assert!(carol.db.reloading_audiences().await.unwrap().is_empty());
    assert_eq!(pins(&carol.db).await, ["plan"]);
}

mod histories {
    use crate::replay::tests::*;
    use coven_database::{StoreLog, StoreLogState};
    use coven_format::store_log::MemberRole::{Admin, Member};
    use coven_foundation::id_source::CircleId;
    use std::collections::BTreeSet;

    /// Gifts' member Ben leaves and is readmitted.
    pub(super) fn rejoin() -> History {
        let mut h = gifts();
        h.all(0, 0, leave(0, 1));
        h.all(0, 0, join(0, 1));
        h
    }

    /// Ana removes Ben while Ben deletes Gifts; the removal wins, so the
    /// deletion drops and Gifts returns to Ana.
    pub(super) fn deletion() -> History {
        let mut h = gifts();
        let past: Vec<_> = (0..h.entries.len()).collect();
        h.push(0, 0, &past, remove(1));
        h.push(1, 1, &past, delete(0));
        h
    }

    /// Ben makes Gifts and adds Carol, then removes Ana. Ana, unaware of all
    /// three, removes Ben. Without Ben's removal hers beats his creation; with
    /// it, hers would leave no admin and drops, returning Gifts.
    pub(super) fn creation() -> History {
        let mut h = household(Admin, Member);
        let base: Vec<_> = (0..h.entries.len()).collect();
        h.all(1, 1, make(0, "Gifts"));
        h.all(1, 1, join(0, 2));
        h.all(1, 1, remove(0));
        h.push(0, 0, &base, remove(1));
        h
    }

    /// The other installation misses Notes' creation. Gifts' first creation
    /// lands at the deadline or past it; another creation names the same id.
    pub(super) fn late_creation(landing: u64) -> (History, Vec<u64>) {
        let mut h = History::new();
        h.all(0, 0, device(1));
        h.all(0, 0, make(1, "Notes"));
        h.push(0, 1, &[0, 1], make(0, "Gifts"));
        h.all(0, 0, make(0, "Gifts"));
        (h, vec![0, 1, 2, landing, landing + 1])
    }

    /// Ben rejoins Gifts and leaves it again; no reload commits in between.
    pub(super) fn leave_again() -> History {
        let mut h = rejoin();
        h.all(0, 0, leave(0, 1));
        h
    }

    /// Ben rejoins Gifts and Ana deletes it; no reload commits in between.
    pub(super) fn delete_after_return() -> History {
        let mut h = rejoin();
        h.all(0, 0, delete(0));
        h
    }

    /// Each batch's prior log and resulting replay state.
    pub(super) fn transitions(
        h: &History,
        stored: &[u64],
        batches: &[Vec<usize>],
    ) -> Vec<(StoreLog, StoreLogState)> {
        let publications = crate::tests::publication_times(h, stored);
        let mut log = StoreLog::default();
        let mut result = Vec::new();
        for batch in batches {
            let mut replay = crate::replay::ReplayBatch::new(&log);
            for &index in batch {
                replay.push_checked(h.entries[index].clone(), &publications);
            }
            let (entries, replay) = replay.finish().unwrap();
            let before = log.clone();
            log.entries.extend(entries);
            log.entries.sort_by_key(|e| e.entry.timestamp);
            log.replay = replay;
            result.push((before, log.replay.state.clone()));
        }
        result
    }

    pub(super) fn returning(h: &History, batches: &[Vec<usize>], m: u8) -> Vec<BTreeSet<CircleId>> {
        let stored: Vec<_> = (0..h.entries.len() as u64).collect();
        transitions(h, &stored, batches)
            .iter()
            .map(|(before, after)| before.returning_circles(after, &member(m)))
            .collect()
    }

    pub(super) fn singles(order: &[usize]) -> Vec<Vec<usize>> {
        order.iter().map(|&i| vec![i]).collect()
    }
}

#[test]
fn returning_circles_need_an_earlier_creation_and_new_readability() {
    use crate::replay::tests::circle;
    use histories::*;
    let gifts = BTreeSet::from([circle(0)]);
    let none = BTreeSet::new();
    // Ana's own new circle has nothing skipped; Ben joining an existing one,
    // and rejoining it after leaving, reloads.
    let order: Vec<_> = (0..7).collect();
    assert_eq!(
        returning(&rejoin(), &singles(&order), 0),
        vec![none.clone(); 7]
    );
    assert_eq!(
        returning(&rejoin(), &singles(&order), 1),
        [&none, &none, &none, &none, &gifts, &none, &gifts].map(Clone::clone)
    );
    // Leaving and rejoining in one batch never made Gifts unreadable here.
    assert_eq!(
        returning(&rejoin(), &[(0..4).collect(), vec![4], vec![5, 6]], 1),
        [none.clone(), gifts.clone(), none.clone()]
    );
    // The deletion arrives first, then the removal that drops it.
    let deleted = returning(&deletion(), &singles(&[0, 1, 2, 3, 4, 6, 5]), 0);
    assert_eq!(deleted[5], none);
    assert_eq!(deleted[6], gifts);
    // Gifts is absent after Ana's removal drops its creation, then returns.
    for m in [1, 2] {
        let absent = returning(&creation(), &singles(&[0, 1, 2, 3, 4, 8, 5, 6, 7]), m);
        assert_eq!(absent[..8], vec![none.clone(); 8]);
        assert_eq!(absent[8], gifts);
        // With its creation in the same batch, Gifts has nothing skipped.
        assert_eq!(
            returning(&creation(), &[(0..9).collect()], m),
            [BTreeSet::new()]
        );
    }
}

#[test]
fn a_late_creation_cannot_make_another_creation_reload() {
    use crate::replay::tests::{circle, member};
    for landing in [32, 33] {
        let (h, stored) = histories::late_creation(landing);
        let batches = histories::singles(&[0, 1, 2, 3, 4]);
        let transitions = histories::transitions(&h, &stored, &batches);
        let (before, after) = &transitions[4];
        assert_eq!(before.entries[3].check.landed_too_late(), landing == 33);
        assert_eq!(
            before.replay.state.reads_circle(circle(0), &member(0)),
            landing == 32
        );
        assert!(after.reads_circle(circle(0), &member(0)));
        assert!(before.returning_circles(after, &member(0)).is_empty());
    }
}

/// Compare each batch's returning circles with `CurrentData.needsReload`
/// (§14.4) over the same checked histories, including creations first received
/// in the batch, which have no skipped part and need no reload. With no reload
/// committing, the circles still waiting after each batch must match those
/// `CurrentData.circleReplay` leaves reloading: the database records a
/// returning circle and drops it once the circle is unreadable
/// (`entry_effects::replay`).
#[test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
fn data_differential_circle_returns() {
    use crate::tests::{input, orders, run_lean};
    use serde_json::{json, Value};
    let number = |circle: &CircleId| u64::try_from(circle.0.as_u128()).unwrap();
    let mut inputs = Vec::new();
    let mut expected = Vec::new();
    let mut withdrawn = 0;
    let histories = [
        histories::rejoin(),
        histories::deletion(),
        histories::creation(),
        histories::leave_again(),
        histories::delete_after_return(),
    ]
    .into_iter()
    .map(|h| {
        let stored = (0..h.entries.len() as u64).collect();
        (h, stored)
    })
    .chain([histories::late_creation(32), histories::late_creation(33)]);
    for (h, stored) in histories {
        for order in orders(&h) {
            for width in [1, 2, h.entries.len()] {
                let batches: Vec<Vec<usize>> = order.chunks(width).map(<[usize]>::to_vec).collect();
                let mut received = Vec::new();
                let mut all = Vec::new();
                for batch in &batches {
                    all.extend(batch);
                    received.push(all.clone());
                }
                let mut results = Vec::new();
                let mut required = vec![BTreeSet::new(); 5];
                for (before, after) in histories::transitions(&h, &stored, &batches) {
                    let (mut pairs, mut waiting) = (Vec::new(), Vec::new());
                    for (m, required) in (0..5).zip(&mut required) {
                        let member = crate::replay::tests::member(m);
                        let returning = before.returning_circles(&after, &member);
                        for circle in &returning {
                            pairs.push(json!([m, number(circle)]));
                        }
                        required.extend(returning);
                        let readable = after.readable_circles(&member);
                        let waited = required.len();
                        required.retain(|circle| readable.contains(circle));
                        withdrawn += waited - required.len();
                        for circle in required.iter() {
                            waiting.push(json!([m, number(circle)]));
                        }
                    }
                    results.push((pairs, waiting));
                }
                inputs.push(input(&h, &stored, &received));
                expected.push(results);
            }
        }
    }
    let output = run_lean("COVEN_STORELOG_DATA_LEAN", &json!({"reloads": inputs}));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    #[derive(serde::Deserialize)]
    struct Batch {
        returns: Vec<Value>,
        waiting: Vec<Value>,
    }
    let actual: Vec<Vec<Batch>> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual.len(), expected.len());
    let sorted = |pairs: &[Value]| {
        let mut pairs = pairs.to_vec();
        pairs.sort_by_key(Value::to_string);
        pairs
    };
    let mut reloads = 0;
    for ((actual, expected), input) in actual.iter().zip(&expected).zip(&inputs) {
        assert_eq!(actual.len(), expected.len(), "circle returns: {input}");
        for (lean, (returns, waiting)) in actual.iter().zip(expected) {
            reloads += lean.returns.len();
            assert_eq!(
                sorted(&lean.returns),
                sorted(returns),
                "circle returns: {input}"
            );
            assert_eq!(
                sorted(&lean.waiting),
                sorted(waiting),
                "waiting reloads: {input}"
            );
        }
    }
    assert!(reloads > 0 && withdrawn > 0);
}

#[test]
fn reads_circle_needs_store_and_circle_membership() {
    use crate::replay::tests::{circle, member};
    let state: StoreLogState =
        crate::replay::tests::replay(&histories::rejoin().prefix(5).entries).state;
    assert!(state.reads_circle(circle(0), &member(1)));
    assert!(!state.reads_circle(circle(0), &member(2)));
    assert!(!state.reads_circle(circle(1), &member(0)));
}

/// storelog-data `ReloadWait.choose` and `Choice.reason`: Ben's catalog of
/// the store's snapshots and `Catalog::take_current` choose what his reload
/// loads, and a reload that waits records its reason on its operation. Each
/// case lists snapshots by Ana's and Ben's devices with random signed
/// positions. Some prefixes are signed by another member, so this version
/// refuses them; some bodies carry Ben's refusal; some are sealed under a
/// key his log does not accept; some prefixes are in a newer format. Bodies
/// are never opened: a wait is compared on the reload's record, a load only
/// on the snapshot chosen.
#[tokio::test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
async fn data_differential_reload_choice() {
    use crate::store_log_sync::tests::{device, key, member, store};
    use crate::tests::run_lean;
    use crate::{PendingReason, PendingRecord, PendingSubject, Prerequisite, RequiredUpdate};
    use coven_format::sealed_snapshot::SnapshotObjectPrefix;
    use coven_format::value::{EntryPositions, StorageTime, WritePositions};
    use coven_foundation::id_source::DeviceId;
    use coven_storage::ObjectPath;
    use serde_json::{json, Value};
    use std::num::NonZeroU64;
    use std::sync::Arc;

    const LANDED: u64 = 1_760_400_000;
    let clock = Arc::new(coven_foundation::clock::FixedClock::new(
        UNIX_EPOCH + Duration::from_secs(LANDED),
    ));
    let storage = Arc::new(
        coven_storage::test_utils::MemoryStorage::builder()
            .transfer_limits(65536, 65536)
            .clock(clock)
            .build()
            .unwrap(),
    );
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    let mut ben = device(storage.clone(), 2, member(1), store(1)).await;
    ben.sync().await;
    let own = ben.db.local_store_log().await.unwrap().device;
    ben.sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: own,
            name: own.0.to_string(),
            replaces: None,
        })
        .await
        .unwrap();
    let log = ben.db.local_store_log().await.unwrap().log;
    let devices = [ana.db.local_store_log().await.unwrap().device, own];
    let other = member(2);
    let snapshot = |device: DeviceId, number: u64| {
        ObjectPath::snapshot(
            store(1),
            Audience::Store,
            device,
            NonZeroU64::new(number).unwrap(),
        )
    };
    let id_json = |path: &ObjectPath| {
        let id = path.snapshot_id().unwrap();
        json!([id.device.0, id.number])
    };
    let record_json = |reason: &PendingReason| match reason {
        PendingReason::Waits(Prerequisite::Object(path)) => {
            let id = path.snapshot_id().unwrap();
            json!([
                "waits",
                ["object", format!("{}/{}", id.device.0, id.number)]
            ])
        }
        PendingReason::KeyUnavailable {
            audience: Audience::Store,
            key,
        } => json!(["keyUnavailable", 0, key.0.as_u128()]),
        PendingReason::UpdateRequired(RequiredUpdate::CovenFormat { version }) => {
            json!(["updateRequired", version])
        }
        other => panic!("unexpected reload record {other:?}"),
    };
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound
    };
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for case in 0..48u64 {
        let mut listed = Vec::new();
        let mut paths = Vec::new();
        let mut refused_bodies = Vec::new();
        for k in 0..1 + next(4) {
            let path = snapshot(devices[next(2) as usize], case * 8 + k + 1);
            let mut positions = Vec::new();
            for device in devices {
                if next(3) > 0 {
                    positions.push((device, 1 + next(4)));
                }
            }
            if positions.is_empty() {
                positions.push((devices[0], 1));
            }
            // 0: another member signs the prefix; 1-3: Ben refused the
            // body; 4-5: a key Ben does not accept; 6: a newer format.
            let kind = next(20);
            let prefix = SnapshotObjectPrefix {
                audience: Audience::Store,
                key: if (4..=5).contains(&kind) {
                    key(99)
                } else {
                    key(1)
                },
                caught_at: StorageTime::new(LANDED, 0).unwrap(),
                raise: None,
                writes: WritePositions(
                    positions
                        .iter()
                        .map(|(device, number)| WriteId {
                            device: *device,
                            number: *number,
                        })
                        .collect(),
                ),
                store_log: EntryPositions(Vec::new()),
            };
            let mut bytes = prefix.encode().unwrap();
            let signer = if kind == 0 { &other } else { &ana.member };
            let signature = signer.keys().sign_prefix(path.as_str(), &bytes);
            bytes.extend_from_slice(signature.as_bytes());
            bytes.extend_from_slice(&[0; 32]);
            if kind == 6 {
                bytes[2] = 2;
            }
            storage.create(&path, &bytes).await.unwrap();
            if (1..=3).contains(&kind) {
                refused_bodies.push(path.clone());
            }
            let id = path.snapshot_id().unwrap();
            listed.push(json!({
                "path": [id.device.0, id.number],
                "writes": positions.iter().map(|(d, n)| json!([d.0, n])).collect::<Vec<_>>(),
                "format": if kind == 6 { 2 } else { coven_format::FORMAT_VERSION },
                "prefixRefused": kind == 0,
                "bodyRefused": (1..=3).contains(&kind),
                "keyHeld": !(4..=5).contains(&kind),
                "key": if (4..=5).contains(&kind) { 99 } else { 1 },
            }));
            paths.push(path);
        }
        // A body refusal is recorded once its prefix verified and was retained.
        ben.sync
            .snapshot_candidates(&Audience::Store, &log)
            .await
            .unwrap();
        for path in &refused_bodies {
            ben.db
                .record_pending(PendingRecord {
                    subject: PendingSubject::Snapshot(path.snapshot_id().unwrap()),
                    reason: PendingReason::Refused(crate::Refusal::Decryption { cause: None }),
                    reported_by: own,
                })
                .await
                .unwrap();
        }
        let mut catalog = ben
            .sync
            .snapshot_candidates(&Audience::Store, &log)
            .await
            .unwrap();
        let choice = match catalog.update {
            Some(RequiredUpdate::CovenFormat { version }) => json!(["update", version]),
            Some(other) => panic!("unexpected update {other:?}"),
            None => match catalog.take_current(&log) {
                Ok(Some(candidate)) => json!(["load", id_json(&candidate.object.path)]),
                Ok(None) => json!(["logs"]),
                Err(PendingReason::Waits(Prerequisite::Object(path))) => {
                    json!(["waits", id_json(&path), "body"])
                }
                Err(PendingReason::KeyUnavailable { .. }) => {
                    let newest = catalog
                        .verified
                        .iter()
                        .find(|snapshot| snapshot.follows_boundary(&log))
                        .unwrap();
                    json!(["waits", id_json(&newest.object.path), "key"])
                }
                Err(other) => panic!("unexpected wait {other:?}"),
            },
        };
        // A waiting reload records that reason on its own operation.
        let record = if matches!(choice[0].as_str(), Some("update" | "waits")) {
            assert!(ben.sync.reload_from_snapshots().await.is_err());
            let id = ben.sync.pending_reload().await.unwrap().unwrap();
            let pending = ben
                .db
                .operation_pending::<PendingRecord>(id)
                .await
                .unwrap()
                .unwrap();
            ben.db.finish_operation(id).await.unwrap();
            record_json(&pending.reason)
        } else {
            Value::Null
        };
        cases.push(json!({
            "audience": 0,
            "supported": coven_format::FORMAT_VERSION,
            "listed": listed,
        }));
        actual.push(json!({"choice": choice, "record": record}));
        for path in &paths {
            storage.delete(path).await.unwrap();
        }
    }
    let output = run_lean("COVEN_STORELOG_DATA_LEAN", &json!({"choices": cases}));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    for ((lean, rust), case) in expected.iter().zip(&actual).zip(&cases) {
        assert_eq!(lean, rust, "reload choice: {case}");
    }
    assert_eq!(expected.len(), actual.len());
    let kinds = |name: &str, why: Value| {
        actual
            .iter()
            .filter(|outcome| outcome["choice"][0] == name && outcome["choice"][2] == why)
            .count()
    };
    assert!(
        kinds("load", Value::Null) > 0
            && kinds("waits", json!("body")) > 0
            && kinds("waits", json!("key")) > 0
            && kinds("update", Value::Null) > 0,
        "{actual:?}"
    );
}
