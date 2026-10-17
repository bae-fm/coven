use crate::{
    tests::TestStore,
    write::tests::{records, sql},
    ApplyOutcome, Database, Migration, Replacement, RowIdentity, SyncedTable,
};
use coven_foundation::id_source::SequentialIds;
use coven_merge::Audience;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,value INTEGER NOT NULL)";

fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

fn migrations(version: u32, converters: [bool; 2], calls: &Arc<Mutex<Vec<u32>>>) -> Vec<Migration> {
    let mut steps = vec![Migration::addition(1, "notes", SCHEMA)];
    for (number, statement) in [
        (2, "CREATE INDEX notes_value ON notes(value)"),
        (3, "ALTER TABLE notes ADD COLUMN color TEXT"),
        (4, "CREATE INDEX notes_color ON notes(color)"),
        (5, "ALTER TABLE notes ADD COLUMN extra TEXT"),
    ] {
        if number > version {
            break;
        }
        let migration = if number % 2 == 0 {
            let old_writes = if converters[(number / 2 - 1) as usize] {
                let calls = calls.clone();
                crate::OldWrites::Convert(Arc::new(move |row| {
                    calls.lock().unwrap().push(number);
                    for column in &mut row.columns {
                        if column.name == "value" {
                            for value in column.old.iter_mut().chain(&mut column.new) {
                                let crate::types::Value::Integer(value) = value else {
                                    panic!("integer fixture value")
                                };
                                *value = *value * 10 + i64::from(number);
                            }
                        }
                    }
                    Ok(())
                }))
            } else {
                crate::OldWrites::Drop
            };
            Migration::breaking(number, "change", statement, old_writes)
        } else {
            Migration::addition(number, "change", statement)
        };
        steps.push(migration);
    }
    steps
}

async fn result(db: &Database) -> Value {
    let values: Vec<i64> = db
        .read(|sql| Ok(sql.query("SELECT value FROM notes", [], |row| row.get(0))?))
        .await
        .unwrap();
    let losses = db.lost_values().await.unwrap();
    if values.is_empty() {
        assert_eq!(losses.len(), 1);
        let Replacement::SchemaChange { version } = losses[0].replaced_by else {
            panic!("schema loss")
        };
        let crate::Lost::Row(cells) = &losses[0].lost else {
            panic!("whole-row loss")
        };
        let crate::types::Value::Integer(value) =
            cells.iter().find(|c| c.column == "value").unwrap().value
        else {
            panic!("original integer")
        };
        json!({"applied": false, "values": [value], "loss": version})
    } else {
        assert_eq!(values.len(), 1);
        assert!(losses.is_empty());
        json!({"applied": true, "values": values, "loss": null})
    }
}

async fn compare(runner: Option<&std::ffi::OsStr>) {
    // Drop and convert choices below the write do not affect admission.
    // The final cases also make the order of two retained callbacks observable.
    for (version, raise, converters, expected, expected_calls) in [
        (2, 3, [false, false], Some(5), vec![]),
        (2, 3, [true, false], Some(5), vec![]),
        (1, 3, [false, false], None, vec![]),
        (1, 3, [true, false], Some(52), vec![2]),
        (3, 3, [false, false], Some(5), vec![]),
        (1, 5, [true, true], Some(524), vec![2, 4]),
        (1, 5, [true, false], None, vec![]),
        (1, 5, [false, true], None, vec![]),
        (2, 5, [false, true], Some(54), vec![4]),
        (4, 5, [false, false], Some(5), vec![]),
    ] {
        for streamed in [false, true] {
            let ids = SequentialIds::new();
            let source_store = TestStore::with_ids(&ids);
            let receiver_store = TestStore::with_ids(&ids);
            let calls = Arc::new(Mutex::new(Vec::new()));
            let source = source_store
                .builder(tables(), migrations(version, converters, &calls))
                .open()
                .await
                .unwrap();
            sql(&source, "INSERT INTO notes(id,value) VALUES('ben',5)")
                .await
                .unwrap();
            let originals = records(&source);
            let ben = originals.last().unwrap();
            assert_eq!(ben.header.schema_version, version);
            // Run the breaking update, then the addition-only update separately.
            let receiver = receiver_store
                .builder(tables(), migrations(raise - 1, converters, &calls))
                .open()
                .await
                .unwrap();
            receiver.close().await.unwrap();
            let receiver = receiver_store
                .builder(tables(), migrations(raise, converters, &calls))
                .open()
                .await
                .unwrap();
            assert_eq!(
                receiver
                    .applied_migrations()
                    .unwrap()
                    .last()
                    .unwrap()
                    .change,
                crate::MigrationChange::Addition
            );
            receiver
                .install_schema_fixture(Audience::Store, raise, 0)
                .await
                .unwrap();
            for write in &originals {
                let outcome = if streamed {
                    receiver
                        .receive_stream(crate::snapshot_write::tests::stream(write), || Ok(()))
                        .await
                } else {
                    receiver.receive(write.clone().into()).await
                };
                assert_eq!(outcome.unwrap(), ApplyOutcome::Applied);
            }
            let actual = result(&receiver).await;
            let wanted = match expected {
                Some(value) => json!({"applied": true, "values": [value], "loss": null}),
                None => json!({"applied": false, "values": [5], "loss": raise}),
            };
            assert_eq!(
                actual, wanted,
                "version {version}, raise {raise}, converters {converters:?}, streamed {streamed}"
            );
            assert_eq!(*calls.lock().unwrap(), expected_calls);
            assert_eq!(records(&source), originals);
            assert_eq!(
                receiver
                    .checked_write_header(ben.header.position)
                    .await
                    .unwrap()
                    .map(|frame| frame.header),
                Some(ben.header.clone())
            );
            if let Some(runner) = runner {
                let steps: Vec<_> = (2..=raise).map(|number| json!({
                    "version": number, "breaking": number % 2 == 0,
                    "declaration": if number % 2 != 0 { "addition" } else if converters[(number / 2 - 1) as usize] { "convert" } else { "drop" },
                    "digit": if number % 2 == 0 && converters[(number / 2 - 1) as usize] { Some(number) } else { None },
                })).collect();
                let input =
                    json!({"version": version, "raise": raise, "steps": steps, "values": [5]});
                assert_eq!(crate::tests::lean_model(runner, input), actual);
            }
            receiver.close().await.unwrap();
            let receiver = receiver_store
                .builder(tables(), migrations(raise, converters, &calls))
                .open()
                .await
                .unwrap();
            assert_eq!(result(&receiver).await, actual);
            assert_eq!(
                receiver.receive(ben.clone().into()).await.unwrap(),
                ApplyOutcome::AlreadyApplied
            );
            assert_eq!(*calls.lock().unwrap(), expected_calls);
            receiver.close().await.unwrap();
            source.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn additions_after_a_breaking_raise_preserve_admission_and_conversion() {
    compare(None).await;
}

#[tokio::test]
#[ignore = "requires COVEN_CONVERSION_LEAN; scripts/check.sh builds conversionRunner"]
async fn conversion_chain_matches_lean() {
    let runner = std::env::var_os("COVEN_CONVERSION_LEAN").expect("built conversionRunner");
    compare(Some(&runner)).await;
    crate::migration::tests::declarations::compare(Some(&runner)).await;
}
