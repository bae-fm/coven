//! What installing a snapshot leaves its rows protecting: the same file
//! references, stored paths and stored files the snapshot's device keeps,
//! whether a row's protection is written with its table's or one row at a
//! time, on a new device and on one that had rows.
use crate::database::file_retention::tests::{files_table, insert, stored, write, FILES};
use crate::snapshot_reload::tests::{everything, publish};
use crate::snapshot_write::tests::{frames, id, load_one};
use crate::tests::TestStore;
use crate::write::tests::count;
use crate::Database;
use coven_format::value::Value;
use coven_format::write::{WriteDisposition, WriteHeader, WritePart, WriteRecord};
use coven_foundation::id_source::{DeviceId, FileId, SequentialIds};
use coven_merge::{Audience, Timestamp, WriteId};
use std::io::Cursor;

fn file(n: u128) -> FileId {
    FileId(uuid::Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0000 | n))
}

/// `device`'s first write, at `time`, recording `records`, each at its
/// coordinates and with its content hash, and the rows of `t` that
/// reference them.
fn recorded(
    device: u64,
    time: u64,
    records: &[(FileId, Value, Value)],
    rows: &[(&str, Value)],
) -> WriteRecord {
    let id = WriteId { device: DeviceId(device), number: 1 };
    let mut changes: Vec<_> = records
        .iter()
        .map(|(file, coordinates, hash)| {
            insert(
                "coven_files",
                &file.0.to_string(),
                vec![
                    ("id", Value::Text(file.0.to_string())),
                    (
                        "audience",
                        Value::Text(crate::write_encoding::audience_text(&Audience::Store)),
                    ),
                    ("size", Value::Integer(1)),
                    ("hash", hash.clone()),
                    ("coordinates", coordinates.clone()),
                    ("namespace", Value::Text("files".into())),
                ],
            )
        })
        .collect();
    changes.extend(rows.iter().map(|(key, reference)| {
        insert("t", key, vec![("id", Value::Text((*key).into())), ("file", reference.clone())])
    }));
    // A part's rows are in row order.
    changes.sort_by(|x, y| (&x.row.table, &x.row.key).cmp(&(&y.row.table, &y.row.key)));
    WriteRecord {
        header: WriteHeader {
            position: id,
            timestamp: Timestamp::new(time, 0, id.device).unwrap(),
            read_through: 0,
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: Audience::Store,
            dismissals: vec![],
            executions: vec![],
            rows: changes,
        }],
    }
}

/// What the device's rows protect: each file reference, stored path,
/// stored file, file waiting for its record's delete, original and upload.
fn protection(db: &Database) -> Vec<(String, Vec<Vec<crate::types::Value>>)> {
    db.inspect_writer(|db| {
        everything(db)
            .into_iter()
            .filter(|(table, _)| {
                [
                    "_coven_file_references",
                    "_coven_row_file_refs",
                    "_coven_stored_files",
                    "_coven_unreferenced_files",
                    "_coven_user_files",
                    "_coven_upload_files",
                ]
                .contains(&table.as_str())
            })
            .collect()
    })
}

/// Bringing every row's protection up to date one row at a time, from its
/// whole merge state and losses, changes nothing an install left.
fn assert_protection_settled(db: &Database) {
    db.inspect_writer_schema(|writer, schema| {
        writer
            .discarded_transaction(|writer| {
                let before = everything(writer);
                let keys = writer.query(
                    "SELECT table_name,key FROM _coven_rows WHERE table_name IN ('t','coven_files')
                     UNION SELECT table_name,key FROM _coven_lost
                     UNION SELECT table_name,key FROM _coven_file_references
                     UNION SELECT table_name,key FROM _coven_row_file_refs",
                    [],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )?;
                for key in keys {
                    crate::file_protection::refresh_received(writer, schema, &key)?;
                }
                assert_eq!(everything(writer), before);
                Ok::<_, crate::DbError>(())
            })
            .unwrap();
    });
}

async fn write_sql(db: &Database, sql: String) {
    db.write(move |context| {
        context.execute_batch(&sql)?;
        Ok(())
    })
    .await
    .unwrap();
}

/// Device 2 records a stored file, which two rows of `t` reference, one on
/// its own device, which a third references, and another stored file, which
/// a fifth references; a fourth references none. Devices 4 and 5 each record
/// one more over bytes of their own, unaware of the other, so its record
/// keeps a lost value. The source clears the third row's reference, which
/// deletes the file it named, and deletes the fourth row. Then it receives
/// two devices' writes, each deleting one of the rows referencing the first
/// file, neither seeing the other, so that file has no reference left and
/// waits for its record's delete.
///
/// A new device, and one that had the earlier rows, each install the
/// source's snapshot and protect what the source does. The earlier device
/// had also applied device 6's write, which the source never did: it
/// recorded a file for the fifth row, which device 2's later write kept as a
/// lost value. The snapshot drops that write, and the fifth row, shown as
/// before, no longer protects that file, and a request to keep it whole
/// there ends with its record. Only the device that had the first
/// file's references applies their removal: it, like the source, keeps the
/// file waiting for its record's delete, and the new device, which never
/// referenced it, has nothing to wait for.
#[tokio::test]
async fn installed_rows_protect_what_the_snapshots_device_does() {
    let ids = SequentialIds::new();
    let [a, b, c, d, e] = [(); 5].map(|()| TestStore::with_ids(&ids));
    let mut devices = Vec::new();
    for store in [&a, &b, &c, &d, &e] {
        devices.push(store.schema(vec![files_table()], FILES).await.unwrap());
    }
    let Ok::<[Database; 5], _>([a, b, c, d, e]) = devices.try_into() else {
        unreachable!("five devices opened")
    };
    let (shared, local, kept, dropped) = (file(1), file(2), file(3), file(4));
    let reference = |file: FileId| Value::Text(file.0.to_string());
    let hash = || Value::Blob(vec![3; 32]);
    let received = [
        recorded(
            2,
            2,
            &[
                (shared, stored(2, file(11), "ab"), hash()),
                (local, Value::Text("device 2".into()), Value::Null),
                (kept, stored(2, file(13), "ab"), hash()),
            ],
            &[
                ("one", reference(shared)),
                ("two", reference(shared)),
                ("three", reference(local)),
                ("four", Value::Null),
                ("five", reference(kept)),
            ],
        ),
        write(4, 3, stored(4, file(14), "cd")),
        write(5, 4, stored(5, file(15), "ef")),
    ];
    for db in [&a, &c, &d, &e] {
        for record in received.clone() {
            db.receive(record.into()).await.unwrap();
        }
    }
    c.receive(
        recorded(
            6,
            1,
            &[(dropped, stored(6, file(16), "ab"), hash())],
            &[("five", reference(dropped))],
        )
        .into(),
    )
    .await
    .unwrap();
    assert!(!a.lost_values().await.unwrap().is_empty());
    write_sql(&a, "UPDATE t SET file=NULL WHERE id='three'; DELETE FROM t WHERE id='four'".into())
        .await;
    write_sql(&d, "DELETE FROM t WHERE id='one'".into()).await;
    write_sql(&e, "DELETE FROM t WHERE id='two'".into()).await;
    publish(&d, &[&a]).await;
    publish(&e, &[&a]).await;
    let unreferenced = |db: &Database| {
        db.inspect_writer(|db| {
            db.query("SELECT file FROM _coven_unreferenced_files", [], |r| r.get::<_, String>(0))
                .unwrap()
        })
    };
    assert_eq!(unreferenced(&a), [shared.0.to_string()]);
    let references = |db: &Database| {
        db.inspect_writer(|db| {
            db.query(
                "SELECT file FROM _coven_file_references WHERE key=?1 ORDER BY file",
                [coven_format::key::encode_key(&[Value::Text("five".into())]).unwrap()],
                |r| r.get::<_, String>(0),
            )
            .unwrap()
        })
    };
    assert_eq!(references(&c), [kept.0.to_string(), dropped.0.to_string()]);
    c.inspect_writer(|db| crate::file_pins::request(db, dropped).unwrap());
    assert_eq!(count(&c, "_coven_pin_requests"), 1);
    let snapshot = frames(&a, Audience::Store).await;
    for (device, db, waiting) in [("new", &b, vec![]), ("earlier", &c, unreferenced(&a))] {
        load_one(db, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(snapshot.concat()))
            .await
            .unwrap();
        for ((table, installed), (_, source)) in protection(db).into_iter().zip(protection(&a)) {
            if table != "_coven_unreferenced_files" {
                assert_eq!(installed, source, "{device} device's {table}");
            }
        }
        assert_eq!(unreferenced(db), waiting, "{device} device");
        assert_protection_settled(db);
    }
    assert_eq!(references(&c), [kept.0.to_string()]);
    assert_eq!(count(&c, "_coven_pin_requests"), 0);
    for db in [a, b, c, d, e] {
        db.close().await.unwrap();
    }
}
