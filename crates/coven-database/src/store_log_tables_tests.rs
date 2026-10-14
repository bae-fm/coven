use crate::pending::tests::Record as PendingRecord;
use crate::store_log::tests::{checked, circle_history};
use crate::tests::TestStore;
use crate::{
    Database, DropReason, EntryOutcome, RemovalRule, Replacement, RowIdentity, SyncedTable,
};
use coven_format::value::{EntryId, Value};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{RowId, RowState, Timestamp};

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
            db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone())
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
    db.apply_store_log::<PendingRecord>(checked(deletion.clone()), replay.clone())
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
        db.apply_store_log::<PendingRecord>(checked(replacement.clone()), replay.clone())
            .await,
        Err(crate::DbError::Sqlite(_))
    ));
    assert_eq!(db.store_log().await.unwrap(), before);
    causes(&db, deletion.position, 2).await;
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    db.apply_store_log::<PendingRecord>(checked(replacement.clone()), replay.clone())
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
    db.apply_store_log::<PendingRecord>(checked(replacement), replay)
        .await
        .unwrap();
    assert_eq!(states(&db), expected);
    assert!(db.lost_values().await.unwrap().is_empty());
    assert_eq!(crate::write::tests::count(&db, "notes"), 2);
    assert!(crate::write::tests::records(&db).is_empty());
    author.close().await.unwrap();
    db.close().await.unwrap();
}
