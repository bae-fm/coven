//! Which records a write deletes (§17.2): the write removing a file's last
//! reference deletes its record, a late write naming it then loses, and a
//! file other devices' writes leave with no reference goes in this device's
//! next write.

use crate::{
    file_write::tests::{
        complete_uploads, has_record, local_count, namespaces, open, stage, tables, SCHEMA,
    },
    tests::TestStore,
    write::tests::records,
    Database, DbError, Migration, Provenance,
};
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

fn rows(db: &Database) -> Vec<String> {
    db.inspect_writer(|sql| {
        sql.query("SELECT id FROM files ORDER BY id", [], |r| r.get(0))
            .unwrap()
    })
}

fn candidates(db: &Database) -> i64 {
    local_count(db, "_coven_file_candidates")
}

/// Publish each of `from`'s waiting writes in a slot of its own, and apply
/// each on `to` in the same slot, so both devices count the same slots.
async fn share(from: &Database, to: &Database) -> Vec<coven_format::write::WriteRecord> {
    let mut shared = Vec::new();
    for record in records(from) {
        crate::tests::publish_through(from, record.header.position.number)
            .await
            .unwrap();
        to.receive(record.clone().into()).await.unwrap();
        shared.push(record);
    }
    shared
}

/// Ana deletes the last row naming a file while Ben, who hasn't seen that,
/// points another row at it. Ana's write deleted the file's record, so on
/// every device Ben's row is taken out like a child of a deleted row, and its
/// lost value names a file nobody has: restoring it fails. Ana's stored
/// bytes stay until her delete is published.
#[tokio::test]
async fn a_late_write_naming_a_deleted_file_loses_everywhere() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let mut databases = Vec::new();
    for store in [&a_store, &b_store] {
        databases.push(
            store
                .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
                .namespaces(namespaces(Provenance::AppProvided))
                .clock(clock.clone())
                .open()
                .await
                .unwrap(),
        );
    }
    let a = databases.remove(0);
    let b = databases.remove(0);
    let file = stage(&a, b"photo".to_vec(), true).await.unwrap();
    let mut shared = share(&a, &b).await;
    complete_uploads(&a).await;
    shared.extend(share(&a, &b).await);
    assert!(has_record(&b, file));
    clock.set(UNIX_EPOCH + Duration::from_secs(2));
    a.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(!has_record(&a, file));
    let deletion = records(&a).pop().unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(3));
    b.write(move |sql| {
        sql.execute("INSERT INTO files(id,file) VALUES('8',?1)", [file])?;
        Ok(())
    })
    .await
    .unwrap();
    let late = records(&b).pop().unwrap();
    // Ana's bytes stay while her delete waits to publish.
    assert_eq!(candidates(&a), 0);
    share(&a, &b).await;
    assert_eq!(candidates(&a), 1);
    share(&b, &a).await;
    for db in [&a, &b] {
        assert!(rows(db).is_empty());
        assert!(!has_record(db, file));
        let lost = db.lost_values().await.unwrap();
        assert_eq!(lost.len(), 1, "{lost:?}");
    }
    // Every order of arrival ends the same.
    for order in [
        [deletion.clone(), late.clone()],
        [late.clone(), deletion.clone()],
    ] {
        let store = TestStore::with_ids(&ids);
        let c = store
            .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
            .namespaces(namespaces(Provenance::AppProvided))
            .open()
            .await
            .unwrap();
        for record in shared.iter().cloned().chain(order) {
            c.receive(record.into()).await.unwrap();
        }
        assert!(rows(&c).is_empty());
        assert!(!has_record(&c, file));
        assert_eq!(c.lost_values().await.unwrap().len(), 1);
        c.close().await.unwrap();
    }
    // Restoring Ben's row names a file this device has no record of.
    let error = b
        .write(move |sql| {
            sql.execute("INSERT INTO files(id,file) VALUES('8',?1)", [file])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::FileUnknown { .. }), "{error:?}");
    assert_eq!(candidates(&a), 1);
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// Ana and Ben each remove one of a file's two references, neither seeing
/// the other's: each write leaves the other reference, so neither deletes
/// the record. Applying both leaves it with none, and the device's next own
/// write deletes it.
#[tokio::test]
async fn a_file_others_leave_without_references_goes_in_this_devices_next_write() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = open(&a_store, Provenance::AppProvided).await;
    let b = open(&b_store, Provenance::AppProvided).await;
    let file = a
        .write_with_files::<_, _, _, _, crate::DbError>(
            |batch| Ok(batch.put_file("files", b"shared".to_vec(), crate::Place::Cloud)),
            |sql, file| {
                sql.execute(
                    "INSERT INTO files(id,file) VALUES('one',?1),('two',?1)",
                    [file],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    share(&a, &b).await;
    complete_uploads(&a).await;
    share(&a, &b).await;
    a.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='one'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    b.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='two'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    share(&a, &b).await;
    share(&b, &a).await;
    for db in [&a, &b] {
        assert!(rows(db).is_empty());
        assert!(has_record(db, file));
        assert_eq!(local_count(db, "_coven_unreferenced_files"), 1);
        db.settle_files().await.unwrap();
        assert!(!has_record(db, file));
        assert_eq!(local_count(db, "_coven_unreferenced_files"), 0);
        let settled = records(db).pop().unwrap();
        assert_eq!(settled.parts[0].rows[0].row.table, "coven_files");
    }
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// Ana and Ben each point row 7 at a new file, neither seeing the other's
/// write. Ben's is later, so Ana's file survives on both devices only as a
/// lost value. When Ana dismisses it, nothing references her file any more,
/// and the dismissal's own write deletes its record.
#[tokio::test]
async fn dismissing_a_files_last_lost_reference_deletes_its_record() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let mut databases = Vec::new();
    for store in [&a_store, &b_store] {
        databases.push(
            store
                .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
                .namespaces(namespaces(Provenance::AppProvided))
                .clock(clock.clone())
                .open()
                .await
                .unwrap(),
        );
    }
    let a = databases.remove(0);
    let b = databases.remove(0);
    let first = stage(&a, b"first".to_vec(), true).await.unwrap();
    share(&a, &b).await;
    clock.set(UNIX_EPOCH + Duration::from_secs(2));
    let ana = stage(&a, b"ana".to_vec(), false).await.unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let ben = stage(&b, b"ben".to_vec(), false).await.unwrap();
    share(&a, &b).await;
    share(&b, &a).await;
    for db in [&a, &b] {
        assert!(!has_record(db, first));
        assert!(has_record(db, ana));
        assert!(has_record(db, ben));
    }
    let lost = a.lost_values().await.unwrap();
    assert_eq!(lost.len(), 1, "{lost:?}");
    a.dismiss_lost_values(&lost).await.unwrap();
    assert!(!has_record(&a, ana));
    assert!(has_record(&a, ben));
    let dismissal = records(&a).pop().unwrap();
    let rows: Vec<_> = dismissal
        .parts
        .iter()
        .flat_map(|part| &part.rows)
        .map(|change| (change.row.table.as_str(), &change.change.operation))
        .collect();
    assert!(
        matches!(
            rows.as_slice(),
            [("coven_files", coven_merge::Operation::Delete)]
        ),
        "{rows:?}"
    );
    share(&a, &b).await;
    assert!(!has_record(&b, ana));
    a.close().await.unwrap();
    b.close().await.unwrap();
}
