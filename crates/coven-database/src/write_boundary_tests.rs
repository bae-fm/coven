use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{ApplyOutcome, DownloadedPart, DownloadedWrite, EntryId, Replacement};
use coven_format::value::WritePositions;
use coven_foundation::id_source::{DeviceId, SequentialIds};
use coven_merge::{Audience, WriteId};

#[tokio::test]
async fn schema_boundaries_are_independent_for_each_audience() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let record = records(&db).remove(0);
    let circle = Audience::Circle(coven_foundation::id_source::CircleId(
        uuid::Uuid::from_u128(1),
    ));
    let boundary = crate::WriteBoundary::SchemaChange {
        audience: circle.clone(),
        version: 2,
        included: WritePositions(Vec::new()),
    };
    assert!(boundary
        .excludes(&record.header, &record.parts[0])
        .is_none());
    let mut part = record.parts[0].clone();
    part.audience = circle.clone();
    assert!(matches!(
        boundary.excludes(&record.header, &part),
        Some(coven_format::snapshot_rows::LostWriteCause::SchemaChange(2))
    ));
    assert!(db
        .apply_breaking_change(circle, 2, WritePositions(Vec::new()))
        .await
        .unwrap());
    assert!(db
        .apply_breaking_change(Audience::Store, 2, WritePositions(Vec::new()))
        .await
        .unwrap());
    assert_eq!(count(&db, "_coven_applied_boundaries"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn reset_successors_can_read_post_reset_writes_from_another_device() {
    let ids = SequentialIds::new();
    let stores = [
        TestStore::with_ids(&ids),
        TestStore::with_ids(&ids),
        TestStore::with_ids(&ids),
    ];
    let a = stores[0].schema(notes(), NOTES).await.unwrap();
    let b = stores[1].schema(notes(), NOTES).await.unwrap();
    let receiver = stores[2].schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','snapshot','')")
        .await
        .unwrap();
    let initial = records(&a).remove(0);
    for db in [&b, &receiver] {
        db.apply_downloaded(initial.clone().into()).await.unwrap();
    }
    receiver
        .apply_reset(
            EntryId {
                device: DeviceId(99),
                number: 1,
            },
            Audience::Store,
            WritePositions(vec![initial.header.position]),
        )
        .await
        .unwrap();
    sql(&a, "UPDATE notes SET title='post reset'")
        .await
        .unwrap();
    let first = records(&a).remove(1);
    b.apply_downloaded(first.clone().into()).await.unwrap();
    sql(&b, "UPDATE notes SET title='read post reset'")
        .await
        .unwrap();
    let second = records(&b).remove(0);
    assert!(second.header.had_read.covers(first.header.position));
    for write in [first, second] {
        assert_eq!(
            receiver.apply_downloaded(write.into()).await.unwrap(),
            ApplyOutcome::Applied
        );
    }
    assert!(receiver.lost_values().await.unwrap().is_empty());
    assert_eq!(
        receiver
            .read(
                |sql| Ok(sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
            )
            .await
            .unwrap(),
        "read post reset"
    );
    for db in [a, b, receiver] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn repeated_boundaries_keep_their_original_coverage_and_application_order() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','old','')")
        .await
        .unwrap();
    sql(&source, "UPDATE notes SET title='excluded'")
        .await
        .unwrap();
    let writes = records(&source);
    let entry = EntryId {
        device: DeviceId(99),
        number: 1,
    };
    for reset_first in [false, true] {
        let store = TestStore::with_ids(&ids);
        let db = store.schema(notes(), NOTES).await.unwrap();
        db.apply_downloaded(DownloadedWrite {
            header: writes[0].header.clone(),
            parts: vec![DownloadedPart::Skipped(Audience::Store)],
        })
        .await
        .unwrap();
        for reset in [reset_first, !reset_first] {
            if reset {
                assert!(db
                    .apply_reset(
                        entry,
                        Audience::Store,
                        WritePositions(vec![WriteId {
                            device: DeviceId(99),
                            number: 1
                        }])
                    )
                    .await
                    .unwrap());
            } else {
                assert!(db
                    .apply_breaking_change(coven_merge::Audience::Store, 2, WritePositions(vec![]))
                    .await
                    .unwrap());
            }
        }
        db.close().await.unwrap();
        let db = store.schema(notes(), NOTES).await.unwrap();
        // A repeated effect cannot change its coverage or move it after another.
        assert!(!db
            .apply_reset(
                entry,
                Audience::Store,
                WritePositions(vec![writes[1].header.position])
            )
            .await
            .unwrap());
        assert!(!db
            .apply_breaking_change(
                coven_merge::Audience::Store,
                2,
                WritePositions(vec![writes[1].header.position])
            )
            .await
            .unwrap());
        db.apply_downloaded(writes[1].clone().into()).await.unwrap();
        assert_eq!(count(&db, "_coven_applied_boundaries"), 2);
        assert_eq!(count(&db, "notes"), 0);
        let losses = db.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        assert_eq!(
            losses[0].replaced_by,
            if reset_first {
                Replacement::Reset(entry)
            } else {
                Replacement::SchemaChange { version: 2 }
            }
        );
        db.close().await.unwrap();
    }
    source.close().await.unwrap();
}
