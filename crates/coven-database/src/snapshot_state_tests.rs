use crate::snapshot_write::tests::{decode, frames, id, load_one};
use crate::tests::contents;
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql};
use coven_format::loss::{LossCause, LossValues};
use coven_format::snapshot::{SnapshotEncoder, SnapshotRecord};
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use coven_merge::Audience;
use std::{
    io::Cursor,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

async fn measured_load(source: &crate::Database, target: &crate::Database) -> usize {
    let before = target.inspect_writer(|db| db.merge_loads().values().sum::<usize>());
    crate::snapshot_write::tests::assert_loaded_losses(source, target).await;
    target.inspect_writer(|db| db.merge_loads().values().sum::<usize>()) - before
}

#[tokio::test]
async fn active_loss_records_require_their_merge_history_and_removed_rows_can_return() {
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title INTEGER NOT NULL,body INTEGER NOT NULL,CHECK(title<=body))";
    let ids = SequentialIds::new();
    let stores: Vec<_> = (0..6).map(|_| TestStore::with_ids(&ids)).collect();
    let mut databases = Vec::new();
    for (millis, store) in [1000, 2000, 3000, 4000, 2500, 4000]
        .into_iter()
        .zip(&stores)
    {
        databases.push(
            store
                .builder(notes(), vec![crate::Migration::sql(1, "bounds", SCHEMA)])
                .clock(Arc::new(FixedClock::new(
                    UNIX_EPOCH + Duration::from_millis(millis),
                )))
                .open()
                .await
                .unwrap(),
        );
    }
    let [a, b, c, target, extra, larger_target] = databases.as_slice() else {
        unreachable!()
    };
    sql(a, "INSERT INTO notes VALUES('n',1,3)").await.unwrap();
    let initial = records(a)[0].clone();
    for database in [b, c, extra] {
        database
            .apply_downloaded(initial.clone().into())
            .await
            .unwrap();
    }
    sql(b, "UPDATE notes SET title=2,body=4").await.unwrap();
    sql(c, "UPDATE notes SET body=1").await.unwrap();
    let losing = records(b)[0].clone();
    for change in [losing.clone(), records(c)[0].clone()] {
        a.apply_downloaded(change.into()).await.unwrap();
    }
    assert_eq!(count(a, "notes"), 0);
    assert_eq!(a.lost_values().await.unwrap().len(), 2);
    let snapshot = frames(a, Audience::Store).await;
    let (header, snapshot_records) = decode(&snapshot);
    let original = contents(target);
    for damage in 0..7 {
        let mut header = header.clone();
        let mut records = snapshot_records.clone();
        match damage {
            0 => {
                records.retain(|r| !matches!(r, SnapshotRecord::Merge(_)));
                header.counts[3] -= 1;
            }
            1..=3 => {
                let loss = records
                    .iter_mut()
                    .find_map(|r| match r {
                        SnapshotRecord::Loss(loss)
                            if matches!(loss.values, LossValues::Cell { .. }) =>
                        {
                            Some(loss)
                        }
                        _ => None,
                    })
                    .unwrap();
                match damage {
                    1 => loss.generation = 3,
                    2 => loss.cause = LossCause::Write(losing.header.position),
                    3 => {
                        let LossValues::Cell { cell, .. } = &mut loss.values else {
                            unreachable!()
                        };
                        cell.write = initial.header.position;
                    }
                    _ => unreachable!(),
                }
            }
            4..=5 => {
                let loss = records
                    .iter_mut()
                    .find_map(|r| match r {
                        SnapshotRecord::Loss(loss) if matches!(loss.values, LossValues::Row(_)) => {
                            Some(loss)
                        }
                        _ => None,
                    })
                    .unwrap();
                if damage == 4 {
                    loss.generation = 3;
                } else {
                    let LossValues::Row(cells) = &mut loss.values else {
                        unreachable!()
                    };
                    cells.remove("title");
                }
            }
            6 => {
                records.retain(|r| !matches!(r,SnapshotRecord::Loss(loss) if matches!(loss.values,LossValues::Row(_))));
                header.counts[4] -= 1;
            }
            _ => unreachable!(),
        }
        if damage == 1 {
            let index = records.iter().position(|r| matches!(r, SnapshotRecord::Loss(loss) if matches!(loss.values, LossValues::Cell { .. }))).unwrap();
            let cell = records.remove(index);
            records.push(cell);
        }
        let (mut encoder, mut plaintext) = SnapshotEncoder::start(header).unwrap();
        for record in records {
            plaintext.extend(encoder.record(record).unwrap());
        }
        plaintext.extend(encoder.finish().unwrap());
        assert!(
            load_one(
                target,
                id(Audience::Store),
                snapshot.prefix.clone(),
                Cursor::new(plaintext)
            )
            .await
            .is_err(),
            "damage {damage}"
        );
        assert_eq!(contents(target), original, "damage {damage}");
    }
    let reads = measured_load(a, target).await;
    sql(extra, "UPDATE notes SET body=2").await.unwrap();
    let additional = records(extra)[0].clone();
    a.apply_downloaded(additional.clone().into()).await.unwrap();
    assert_eq!(a.lost_values().await.unwrap().len(), 3);
    assert_eq!(
        measured_load(a, larger_target).await,
        reads,
        "additional losses must not reload row history"
    );
    target.apply_downloaded(additional.into()).await.unwrap();
    sql(c, "UPDATE notes SET body=5").await.unwrap();
    let restoring = records(c).pop().unwrap();
    for database in [a, target, larger_target] {
        database
            .apply_downloaded(restoring.clone().into())
            .await
            .unwrap();
        assert_eq!(count(database, "notes"), 1);
    }
    assert!(a
        .lost_values()
        .await
        .unwrap()
        .iter()
        .all(|loss| matches!(loss.lost, crate::Lost::Cell(_))));
    crate::snapshot_write::tests::assert_loaded_losses(a, target).await;
    for database in databases {
        database.close().await.unwrap();
    }
}
