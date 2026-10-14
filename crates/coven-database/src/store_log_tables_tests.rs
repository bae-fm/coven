use crate::pending::tests::Record as PendingRecord;
use crate::store_log::tests::{checked, circle_history};
use crate::tests::TestStore;
use crate::{
    Database, DbError, DropReason, EntryOutcome, RemovalRule, Replacement, RowIdentity, SyncedTable,
};
use coven_format::value::{EntryId, Value};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{RowId, RowState, Timestamp};
use std::time::Duration;

async fn open(store: &TestStore) -> Database {
    store
        .schema(
        vec![SyncedTable::new("notes", RowIdentity::SharedKey).key_columns(["id", "audience"]).audience_column("audience")],
        "CREATE TABLE notes(id TEXT NOT NULL,audience TEXT NOT NULL,title TEXT,PRIMARY KEY(id,audience))",
        )
        .await
        .unwrap()
}

fn states(db: &Database) -> Vec<RowState<Value>> {
    db.inspect_writer_schema(|db, schema| {
        let visible = crate::write_rows::AppView::after(db, schema);
        let store = crate::merge_store::MergeStore::new(db, &visible);
        db.query(
            "SELECT DISTINCT table_name,key,audience FROM _coven_rows ORDER BY table_name,key,audience",
            [],
            crate::row_queries::read_identity,
        ).unwrap().into_iter().map(|row: RowId| store.row(&row).unwrap().state).collect()
    })
}

async fn causes(db: &Database, entry: EntryId, count: usize) {
    let losses = db.lost_values().await.unwrap();
    assert_eq!(losses.len(), count);
    for loss in losses {
        assert_eq!(
            loss.replaced_by,
            Replacement::Rules(vec![RemovalRule::DeletedCircle { entry }])
        );
    }
}

#[tokio::test]
async fn replacing_and_dropping_circle_causes_preserves_generations_and_setters() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let source = TestStore::with_ids(&ids);
    let target = TestStore::with_ids(&ids);
    let author = open(&source).await;
    let db = open(&target).await;
    let circle = CircleId(uuid::Uuid::from_u128(2));
    let history = circle_history(circle);
    for (entry, replay) in &history[..4] {
        for db in [&author, &db] {
            db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
                .await
                .unwrap();
        }
    }
    source.join_circle(&author, circle);
    for id in ["existing", "arriving"] {
        author
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO notes VALUES(?1,?2,?1)",
                    (id, circle.to_string()),
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }
    let expected = states(&author);
    let writes = crate::write::tests::records(&author);
    assert_eq!(
        db.apply_downloaded(writes[0].clone().into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
    let initial = states(&db);
    let (deletion, mut replay) = history[4].clone();
    db.apply_store_log::<PendingRecord>(checked(deletion.clone()), replay.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(states(&db), initial);
    causes(&db, deletion.position, 1).await;
    assert_eq!(
        db.apply_downloaded(writes[1].clone().into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
    assert_eq!(states(&db), expected);
    causes(&db, deletion.position, 2).await;

    // The database receives the checked entries and replay chosen by sync.
    // A different concurrent deletion can remain when the first drops.
    let mut replacement = deletion.clone();
    replacement.position.device = DeviceId(u64::MAX - 2);
    replacement.timestamp = Timestamp::new(7, 0, replacement.position.device).unwrap();
    replay.entries.insert(
        deletion.position,
        EntryOutcome::Dropped(DropReason::BeatenBy(replacement.position)),
    );
    replay
        .entries
        .insert(replacement.position, EntryOutcome::Kept);
    replay
        .state
        .circle_deletions
        .insert(circle, replacement.position);
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| {
        sql.fail_at("refuse", "BEFORE UPDATE ON _coven_lost", "retain old cause")
    });
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(
            checked(replacement.clone()),
            replay.clone(),
            Vec::new()
        )
        .await,
        Err(crate::DbError::Sqlite(_))
    ));
    assert_eq!(db.store_log().await.unwrap(), before);
    causes(&db, deletion.position, 2).await;
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    db.apply_store_log::<PendingRecord>(checked(replacement.clone()), replay.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(states(&db), expected);
    causes(&db, replacement.position, 2).await;
    db.close().await.unwrap();
    let db = open(&target).await;
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    causes(&db, replacement.position, 2).await;

    replay.entries.insert(
        replacement.position,
        EntryOutcome::Dropped(DropReason::NotAllowed),
    );
    replay.state.circle_deletions.clear();
    replay.state.circles = history[3].1.state.circles.clone();
    db.apply_store_log::<PendingRecord>(checked(replacement), replay, Vec::new())
        .await
        .unwrap();
    assert_eq!(states(&db), expected);
    assert!(db.lost_values().await.unwrap().is_empty());
    assert_eq!(crate::write::tests::count(&db, "notes"), 2);
    assert!(crate::write::tests::records(&db).is_empty());
    author.close().await.unwrap();
    db.close().await.unwrap();
}

#[test]
fn horizon_encoding_preserves_provider_ticks_and_rejects_noncanonical_fractions() {
    for (duration, expected) in [
        (Duration::ZERO, vec![0; 12]),
        (
            Duration::new(0x0102_0304_0506_0708, 0x0506_0708),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 5, 6, 7, 8],
        ),
        (
            Duration::MAX,
            vec![255, 255, 255, 255, 255, 255, 255, 255, 59, 154, 201, 255],
        ),
    ] {
        assert_eq!(super::encode_horizon(duration), expected);
        assert_eq!(super::decode_horizon(&expected).unwrap(), duration);
    }
    for bytes in [
        vec![],
        vec![0; 11],
        vec![0; 13],
        [vec![0; 8], 1_000_000_000u32.to_be_bytes().to_vec()].concat(),
        [vec![0; 8], u32::MAX.to_be_bytes().to_vec()].concat(),
    ] {
        assert!(matches!(
            super::decode_horizon(&bytes),
            Err(DbError::DamagedDatabase)
        ));
    }
}

#[tokio::test]
async fn horizon_persists_and_failed_or_stale_transactions_preserve_it() {
    use std::time::UNIX_EPOCH;
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let absent = db.local_store_log().await.unwrap().log;
    assert_eq!(absent.horizon, None);
    db.advance_store_log_horizon(absent.clone(), Duration::ZERO)
        .await
        .unwrap();
    db.advance_store_log_horizon(absent, Duration::ZERO)
        .await
        .unwrap();
    let zero = db.local_store_log().await.unwrap().log;
    assert_eq!(zero.horizon, Some(Duration::ZERO));
    let mut different = zero.clone();
    different.entries.push(crate::ReplayEntry {
        entry: coven_format::test_utils::store_log(),
        stored_at: UNIX_EPOCH,
        check: crate::StoreLogCheck::Allowed,
    });
    assert!(matches!(
        db.advance_store_log_horizon(different, Duration::ZERO)
            .await,
        Err(DbError::StoreLogEntriesChanged)
    ));
    db.inspect_writer(|sql| {
        sql.fail_at(
            "reject_horizon",
            "BEFORE UPDATE ON _coven_finality",
            "horizon failed",
        )
    });
    db.advance_store_log_horizon(zero.clone(), Duration::ZERO)
        .await
        .unwrap();
    let later = Duration::from_secs(1) - Duration::from_nanos(1);
    assert!(db
        .advance_store_log_horizon(zero.clone(), later)
        .await
        .is_err());
    assert_eq!(db.local_store_log().await.unwrap().log, zero);
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER reject_horizon").unwrap());
    db.advance_store_log_horizon(zero.clone(), later)
        .await
        .unwrap();
    let saved = db.local_store_log().await.unwrap().log;
    assert_eq!(saved.horizon, Some(later));
    db.advance_store_log_horizon(zero, Duration::ZERO)
        .await
        .unwrap();
    db.close().await.unwrap();
    let reopened = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(reopened.local_store_log().await.unwrap().log, saved);
}

// The store identity is read back from the kept creation entry (§9).
mod identity {
    use crate::store_log::{tests as log_tests, *};
    use crate::{
        pending::tests::Record, tests::TestStore, DbError, StoreLogCheck, StoreLogSealing,
    };
    use coven_format::store_log::StoreChange;
    use coven_foundation::id_source::{CircleId, KeyId, StoreId};
    use coven_merge::Audience;

    #[tokio::test]
    async fn replay_cannot_replace_the_creation_identity() {
        let directory = TestStore::new();
        let db = directory.builder(vec![], vec![]).open().await.unwrap();
        let (creation, replay) =
            log_tests::circle_history(CircleId(uuid::Uuid::from_u128(2)))[0].clone();
        for wrong_id in [false, true] {
            let mut changed = replay.clone();
            let identity = changed.state.store.as_mut().unwrap();
            if wrong_id {
                identity.id = StoreId(uuid::Uuid::from_u128(99));
            } else {
                identity.name = "Conflicting name".into();
            }
            assert!(matches!(
                db.apply_store_log::<Record>(log_tests::checked(creation.clone()), changed)
                    .await,
                Err(DbError::StoreLogEntriesChanged)
            ));
            assert!(db.local_store_log().await.unwrap().log.entries.is_empty());
            assert!(db.store_log().await.unwrap().entries.is_empty());
        }
    }

    #[tokio::test]
    async fn checked_creation_supplies_identity_after_a_rotation_and_reopening() {
        let directory = TestStore::new();
        let db = directory.builder(vec![], vec![]).open().await.unwrap();
        assert!(db.store_log().await.unwrap().replay.state.store.is_none());
        let (creation, mut replay) =
            log_tests::circle_history(CircleId(uuid::Uuid::from_u128(2)))[0].clone();
        let identity = replay.state.store.clone().unwrap();
        db.apply_store_log::<Record>(log_tests::checked(creation.clone()), replay.clone())
            .await
            .unwrap();
        let mut rotation = creation;
        rotation.position.number = 2;
        rotation.timestamp = coven_merge::Timestamp::new(2, 0, rotation.position.device).unwrap();
        let key = KeyId(uuid::Uuid::from_u128(u128::MAX));
        rotation.change = StoreChange::RotateKey {
            audience: Audience::Store,
            key,
            key_hash: [99; 32],
        };
        replay.entries.insert(rotation.position, EntryOutcome::Kept);
        db.apply_store_log::<Record>(log_tests::checked(rotation), replay.clone())
            .await
            .unwrap();
        db.close().await.unwrap();
        let db = directory.builder(vec![], vec![]).open().await.unwrap();
        let saved = db.store_log().await.unwrap();
        assert_eq!(saved.replay, replay);
        let saved = saved.replay.state.store.unwrap();
        assert_eq!(saved.id, identity.id);
        assert_eq!(saved.name, identity.name);
    }

    #[tokio::test]
    async fn rejected_creation_does_not_supply_an_identity() {
        let directory = TestStore::new();
        let db = directory.builder(vec![], vec![]).open().await.unwrap();
        let (creation, _) =
            log_tests::circle_history(CircleId(uuid::Uuid::from_u128(2)))[0].clone();
        let mut replay = StoreLogReplay::default();
        replay.entries.insert(
            creation.position,
            EntryOutcome::Dropped(DropReason::NotAllowed),
        );
        let mut checked = log_tests::checked(creation);
        checked.check = StoreLogCheck::NotAllowed;
        db.apply_store_log::<Record>(checked, replay).await.unwrap();
        db.close().await.unwrap();
        let db = directory.builder(vec![], vec![]).open().await.unwrap();
        assert!(db.store_log().await.unwrap().replay.state.store.is_none());
    }

    #[tokio::test]
    async fn a_creation_disagreeing_with_settings_is_neither_reserved_nor_applied() {
        let directory = TestStore::new();
        let db = directory.builder(vec![], vec![]).open().await.unwrap();
        let (mut creation, mut replay) =
            log_tests::circle_history(CircleId(uuid::Uuid::from_u128(2)))[0].clone();
        let StoreChange::CreateStore { name, .. } = &mut creation.change else {
            unreachable!("the first fixture entry creates the store")
        };
        *name = "Another name".into();
        replay.state.store.as_mut().unwrap().name = "Another name".into();
        let mismatch = |result: Result<(), DbError>| {
            matches!(result, Err(DbError::StoreSettingsMismatch { store, name })
                if store == directory.id() && name == "Another name")
        };
        assert!(mismatch(
            db.prepare_store_log(
                creation.author.clone(),
                creation.change.clone(),
                |_, _, _| -> Result<StoreLogSealing, DbError> {
                    panic!("a mismatched creation cannot reach sealing")
                },
            )
            .await
            .map(drop)
        ));
        assert!(db.local_store_log().await.unwrap().upload.is_none());
        assert!(mismatch(
            db.apply_store_log::<Record>(log_tests::checked(creation), replay)
                .await
        ));
        let log = db.local_store_log().await.unwrap().log;
        assert!(log.entries.is_empty() && log.replay.state.store.is_none());
    }
}
