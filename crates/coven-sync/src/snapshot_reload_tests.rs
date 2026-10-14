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
/// while write 2 is missing the reload fails as a whole.
#[tokio::test]
async fn a_rejoined_member_reloads_the_part_its_passed_position_skipped() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    let [ana, ben, _] = devices.as_mut_slice() else {
        panic!("three members")
    };
    sql(&ana.db, "INSERT INTO notes VALUES('first','public','one'); INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','before leaving')").await;
    ana.writes.upload_writes().await.unwrap();
    ben.writes.download_writes().await.unwrap();
    assert_eq!(pins(&ben.db).await, ["before leaving"]);
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: GIFTS,
            member: identity(4).member_id(),
        })
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
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle: GIFTS,
            member: identity(4).member_id(),
        })
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
    assert!(ben
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .iter()
        .any(|record| record.subject == crate::PendingSubject::Audience(Audience::Circle(GIFTS))));
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

    storage.create(&path, &bytes).await.unwrap();
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
            member: identity(5).member_id(),
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
            member: identity(4).member_id(),
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
            member: identity(5).member_id(),
        },
        StoreChange::RemoveMember {
            member: identity(3).member_id(),
        },
    ] {
        ben.sync.make_and_upload_entry(change).await.unwrap();
    }
    carol.sync.sync_store_log().await.unwrap();
    let state = carol.db.store_log().await.unwrap().replay.state;
    assert!(state.reads_circle(plans, &identity(5).member_id()));
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

    /// Each batch's prior log and resulting replay state.
    pub(super) fn transitions(
        h: &History,
        batches: &[Vec<usize>],
    ) -> Vec<(StoreLog, StoreLogState)> {
        let stored: Vec<_> = (0..h.entries.len() as u64).collect();
        let publications = crate::tests::publication_times(h, &stored);
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
        transitions(h, batches)
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

/// Compare each batch's returning circles with `CurrentData.needsReload`
/// (§14.4) over the same checked histories. Lean also reports circles whose
/// creation arrives in that batch, which have no skipped part; Rust reloads
/// exactly the others.
#[test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
fn data_differential_circle_returns() {
    use crate::tests::{input, orders, run_lean};
    use serde_json::{json, Value};
    let mut inputs = Vec::new();
    let mut expected = Vec::new();
    for h in [
        histories::rejoin(),
        histories::deletion(),
        histories::creation(),
    ] {
        let stored: Vec<_> = (0..h.entries.len() as u64).collect();
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
                for (before, after) in histories::transitions(&h, &batches) {
                    let mut pairs = Vec::new();
                    for m in 0..5 {
                        for circle in
                            before.returning_circles(&after, &crate::replay::tests::member(m))
                        {
                            pairs.push(json!([m, u64::try_from(circle.0.as_u128()).unwrap()]));
                        }
                    }
                    results.push(pairs);
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
    let actual: Vec<Vec<Vec<Value>>> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual.len(), expected.len());
    let (mut reloads, mut new_circles) = (0, 0);
    for ((actual, expected), input) in actual.iter().zip(&expected).zip(&inputs) {
        assert_eq!(actual.len(), expected.len(), "circle returns: {input}");
        for (lean, rust) in actual.iter().zip(expected) {
            let mut skipped: Vec<_> = lean
                .iter()
                .filter(|pair| pair[2] == json!(true))
                .map(|pair| json!([pair[0], pair[1]]))
                .collect();
            new_circles += lean.len() - skipped.len();
            reloads += skipped.len();
            let mut rust = rust.clone();
            skipped.sort_by_key(Value::to_string);
            rust.sort_by_key(Value::to_string);
            assert_eq!(skipped, rust, "circle returns: {input}");
        }
    }
    assert!(reloads > 0 && new_circles > 0);
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
