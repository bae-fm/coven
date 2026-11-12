use crate::DbError;
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
                            let crate::types::Value::Integer(value) = &mut column.value else {
                                panic!("integer fixture value")
                            };
                            *value = *value * 10 + i64::from(number);
                        }
                    }
                    Ok(())
                }))
            } else {
                crate::OldWrites::Drop
            };
            Migration::breaking(number, "change", statement, old_writes, Vec::new())
        } else {
            Migration::addition(number, "change", statement)
        };
        steps.push(migration);
    }
    steps
}

async fn result(db: &Database) -> Value {
    let values: Vec<i64> = db
        .read(|sql| Ok::<_, DbError>(sql.query("SELECT value FROM notes", [], |row| row.get(0))?))
        .await
        .unwrap();
    let losses = db.lost_values().await.unwrap();
    if values.is_empty() {
        assert_eq!(losses.len(), 1);
        let Replacement::SchemaChange { version } = losses[0].replaced_by else {
            panic!("schema loss")
        };
        let crate::Lost::Row(cells) = &losses[0].lost else { panic!("whole-row loss") };
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
            sql(&source, "INSERT INTO notes(id,value) VALUES('ben',5)").await.unwrap();
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
                receiver.applied_migrations().unwrap().last().unwrap().change,
                crate::MigrationChange::Addition
            );
            receiver.install_schema_fixture(Audience::Store, raise, 0).await.unwrap();
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

/// An old-schema write converts on receipt after a raise whether its author
/// had tried to send it or not, and the author's queued record keeps its
/// bytes and sealing keys (§18.6).
#[tokio::test]
async fn an_attempted_write_arriving_after_the_raise_converts_like_an_untried_one() {
    let mut outcomes = Vec::new();
    for attempted in [false, true] {
        let ids = SequentialIds::new();
        let source_store = TestStore::with_ids(&ids);
        let receiver_store = TestStore::with_ids(&ids);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let source = source_store
            .builder(tables(), migrations(1, [true, false], &calls))
            .open()
            .await
            .unwrap();
        sql(&source, "INSERT INTO notes(id,value) VALUES('ben',5)").await.unwrap();
        if attempted {
            crate::upload::tests::attempt(&source, 9).await.unwrap();
        }
        let queued = |db: &Database| {
            db.inspect_writer(|db| {
                db.query(
                    "SELECT u.record,k.sealing_keys FROM _coven_uploads u
                     LEFT JOIN _coven_upload_keys k USING(device,number) ORDER BY u.number",
                    [],
                    |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<Vec<u8>>>(1)?)),
                )
                .unwrap()
            })
        };
        let originals = records(&source);
        let queue = queued(&source);
        assert_eq!(queue[0].1.is_some(), attempted);

        let receiver = receiver_store
            .builder(tables(), migrations(3, [true, false], &calls))
            .open()
            .await
            .unwrap();
        receiver.install_schema_fixture(Audience::Store, 3, 0).await.unwrap();
        for write in &originals {
            assert_eq!(
                receiver.receive(write.clone().into()).await.unwrap(),
                ApplyOutcome::Applied
            );
        }
        outcomes.push((result(&receiver).await, calls.lock().unwrap().clone()));
        assert_eq!(records(&source), originals);
        assert_eq!(queued(&source), queue);
        receiver.close().await.unwrap();
        source.close().await.unwrap();
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(outcomes[0].0, json!({"applied": true, "values": [52], "loss": null}));
    assert_eq!(outcomes[0].1, [2]);
}

#[tokio::test]
#[ignore = "requires COVEN_CONVERSION_LEAN; scripts/check.sh builds conversionRunner"]
async fn conversion_chain_matches_lean() {
    let runner = std::env::var_os("COVEN_CONVERSION_LEAN").expect("built conversionRunner");
    compare(Some(&runner)).await;
    crate::migration::tests::declarations::compare(Some(&runner)).await;
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_conversion_schemas SELECT i,x'00',x'00',x'00',x'00' FROM fixture;",
    )
    .unwrap();
}

mod data_sources {
    //! Ben's late old-schema writes 9 and 10 after a raise (§18.6): the data
    //! rule runs on original changes before any converter (E13).
    use crate::{
        tests::TestStore,
        write::tests::{records, sql},
        *,
    };
    use coven_format::write::WriteRecord;
    use coven_foundation::id_source::SequentialIds;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const NOTES: &str =
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL)";
    fn tables() -> Vec<SyncedTable> {
        vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
    }
    fn initial() -> Migration {
        Migration::addition(1, "notes", NOTES)
    }
    fn rename(
        version: u32,
        from: &'static str,
        to: &'static str,
        calls: Option<Arc<AtomicUsize>>,
    ) -> Migration {
        let sql: &'static str =
            Box::leak(format!("ALTER TABLE notes RENAME COLUMN {from} TO {to}").into_boxed_str());
        let old_writes = match calls {
            Some(calls) => crate::OldWrites::Convert(Arc::new(move |row| {
                calls.fetch_add(1, Ordering::SeqCst);
                row.rename_column(from, to);
                Ok(())
            })),
            None => crate::OldWrites::Drop,
        };
        Migration::breaking(version, "rename", sql, old_writes, tables())
    }
    async fn open(store: &TestStore, migrations: Vec<Migration>) -> Database {
        store.builder(tables(), migrations).open().await.unwrap()
    }
    async fn names(db: &Database, column: &'static str) -> Vec<(String, String)> {
        db.read(move |sql| {
            Ok::<_, DbError>(sql.query(
                &format!("SELECT id,{column} FROM notes ORDER BY id"),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap()
    }
    fn source(write: &WriteRecord, id: &str) -> crate::ChangeSource {
        crate::ChangeSource {
            write: write.header.position,
            table: "notes".into(),
            key: RowKey(vec![types::Value::Text(id.into())]),
            audience: coven_merge::Audience::Store,
        }
    }

    /// Ben's 9 creates a note and is excluded by an explicit drop choice;
    /// his 10 edits that note and renames an unrelated playlist. With the
    /// converter, 10's note edit is a frozen dependency loss naming 9 and its
    /// playlist rename converts and applies; the callback never sees the note.
    #[tokio::test]
    async fn a_change_built_on_an_excluded_write_stays_frozen_beside_a_converted_one() {
        for setter_only in [false, true] {
            let ids = SequentialIds::new();
            let (ben_store, receiver_store) =
                (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
            let ben = ben_store.schema(tables(), NOTES).await.unwrap();
            sql(&ben, "INSERT INTO notes VALUES('p','playlist','b'),('n','covered note','b')")
                .await
                .unwrap();
            if setter_only {
                // 9 replaces the covered note's title: its generation survives.
                sql(&ben, "UPDATE notes SET title='excluded title' WHERE id='n'").await.unwrap();
            } else {
                sql(&ben, "DELETE FROM notes WHERE id='n'").await.unwrap();
                sql(&ben, "INSERT INTO notes VALUES('n','new note','b')").await.unwrap();
            }
            ben.write(|sql| {
                sql.execute("UPDATE notes SET title='edited' WHERE id='n'", [])?;
                sql.execute("UPDATE notes SET title='renamed playlist' WHERE id='p'", [])?;
                Ok(())
            })
            .await
            .unwrap();
            let writes = records(&ben);
            // The receiver applied Ben's first write before its app migrated;
            // the kept raise covers it.
            let receiver = open(&receiver_store, vec![initial()]).await;
            receiver.receive(writes[0].clone().into()).await.unwrap();
            receiver.close().await.unwrap();
            let receiver =
                open(&receiver_store, vec![initial(), rename(2, "title", "name", None)]).await;
            receiver.exclude_later_writes(2).await;
            for write in &writes[1..writes.len() - 1] {
                receiver.receive(write.clone().into()).await.unwrap();
            }
            receiver.close().await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let receiver = open(
                &receiver_store,
                vec![initial(), rename(2, "title", "name", Some(calls.clone()))],
            )
            .await;
            let ten = writes.last().unwrap();
            assert_eq!(receiver.receive(ten.clone().into()).await.unwrap(), ApplyOutcome::Applied);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let note = if setter_only {
                vec![source(&writes[1], "n")]
            } else {
                vec![source(&writes[2], "n"), source(&writes[2], "n")]
            };
            let dependency = receiver
                .lost_values()
                .await
                .unwrap()
                .into_iter()
                .filter_map(|loss| match loss.replaced_by {
                    Replacement::DataDependency { write, sources } => Some((write, sources)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(dependency, [(ten.header.position, note)]);
            // Neither excluded write applied: the covered note keeps its value.
            let rows = names(&receiver, "name").await;
            assert_eq!(
                rows,
                [
                    ("n".to_owned(), "covered note".to_owned()),
                    ("p".to_owned(), "renamed playlist".to_owned())
                ]
            );
            // Reopening keeps the committed interpretation.
            receiver.close().await.unwrap();
            let receiver = open(
                &receiver_store,
                vec![initial(), rename(2, "title", "name", Some(calls.clone()))],
            )
            .await;
            assert_eq!(names(&receiver, "name").await, rows);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            receiver.close().await.unwrap();
            ben.close().await.unwrap();
        }
    }

    /// Ben's 9 is excluded on a device that then takes a snapshot; a device
    /// loading it receives Ben's 10 late and still treats 9 as a source that
    /// doesn't count, so 10's edit of the same note is a frozen dependency
    /// loss while its playlist change converts (§16.1, §9.5).
    #[tokio::test]
    async fn a_loaded_snapshot_keeps_an_excluded_writes_verdict_for_later_dependents() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store, loader_store) =
            (TestStore::with_ids(&ids), TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let calls = Arc::new(AtomicUsize::new(0));
        // The receiver drops late old-schema writes; the loader converts them.
        let migrations = |old_writes| {
            vec![
                initial(),
                Migration::breaking(
                    2,
                    "index",
                    "CREATE INDEX notes_title ON notes(title)",
                    old_writes,
                    tables(),
                ),
            ]
        };
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('p','playlist','b'),('n','covered note','b')")
            .await
            .unwrap();
        sql(&ben, "UPDATE notes SET title='excluded title' WHERE id='n'").await.unwrap();
        ben.write(|sql| {
            sql.execute("UPDATE notes SET title='edited' WHERE id='n'", [])?;
            sql.execute("UPDATE notes SET title='renamed playlist' WHERE id='p'", [])?;
            Ok(())
        })
        .await
        .unwrap();
        let writes = records(&ben);
        let receiver = open(&receiver_store, vec![initial()]).await;
        receiver.receive(writes[0].clone().into()).await.unwrap();
        receiver.close().await.unwrap();
        let receiver = open(&receiver_store, migrations(crate::OldWrites::Drop)).await;
        receiver.exclude_later_writes(2).await;
        receiver.receive(writes[1].clone().into()).await.unwrap();
        let snapshot =
            crate::snapshot_write::tests::frames(&receiver, coven_merge::Audience::Store).await;
        open(&loader_store, vec![initial()]).await.close().await.unwrap();
        let counted = calls.clone();
        let converting = crate::OldWrites::Convert(Arc::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
        let loader = open(&loader_store, migrations(converting)).await;
        loader.exclude_later_writes(2).await;
        loader
            .load_snapshots::<crate::pending::tests::Record, std::io::Cursor<Vec<u8>>>(
                crate::tests::reload(
                    &loader,
                    vec![snapshot.input()],
                    vec![],
                    Arc::new(crate::test_utils::TestAuthorship),
                )
                .await,
                None,
            )
            .await
            .unwrap();
        let ten = &writes[2];
        assert_eq!(loader.receive(ten.clone().into()).await.unwrap(), ApplyOutcome::Applied);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let dependency = loader
            .lost_values()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|loss| match loss.replaced_by {
                Replacement::DataDependency { write, sources } => Some((write, sources)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(dependency, [(ten.header.position, vec![source(&writes[1], "n")])]);
        assert_eq!(
            names(&loader, "title").await,
            [
                ("n".to_owned(), "covered note".to_owned()),
                ("p".to_owned(), "renamed playlist".to_owned())
            ]
        );
        for db in [ben, receiver, loader] {
            db.close().await.unwrap();
        }
    }

    /// A late write converts through every raise between its version and the
    /// winning one, in version order; a covered write never reaches a callback.
    #[tokio::test]
    async fn repeated_raises_convert_in_version_order_and_skip_covered_writes() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('n','covered','b')").await.unwrap();
        sql(&ben, "UPDATE notes SET title='late'").await.unwrap();
        // An empty delete still converts as a delete of its row.
        sql(&ben, "DELETE FROM notes").await.unwrap();
        let mut writes = records(&ben);
        writes[2].parts[0].rows[0].old.clear();
        let calls = Arc::new(AtomicUsize::new(0));
        let receiver = open(&receiver_store, vec![initial()]).await;
        receiver.receive(writes[0].clone().into()).await.unwrap();
        receiver.close().await.unwrap();
        let receiver = open(
            &receiver_store,
            vec![
                initial(),
                rename(2, "title", "heading", Some(calls.clone())),
                rename(3, "heading", "name", Some(calls.clone())),
            ],
        )
        .await;
        receiver.exclude_later_writes(3).await;
        assert_eq!(
            receiver.receive(writes[0].clone().into()).await.unwrap(),
            ApplyOutcome::AlreadyApplied
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        receiver.receive(writes[1].clone().into()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(names(&receiver, "name").await, [("n".into(), "late".into())]);
        receiver.receive(writes[2].clone().into()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert!(names(&receiver, "name").await.is_empty());
        assert!(receiver.lost_values().await.unwrap().is_empty());
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }

    /// Ben's 9 creates a note and an explicit drop choice excludes it. A later
    /// raise to version 4, whose converter would accept the write, keeps the
    /// frozen loss with its values and cause; receiving 9 again neither calls
    /// the converter nor revives the note (§18.6).
    #[tokio::test]
    async fn a_frozen_loss_keeps_its_values_and_cause_through_a_later_raise() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('n','title','b')").await.unwrap();
        let nine = records(&ben).remove(0);
        let receiver =
            open(&receiver_store, vec![initial(), rename(2, "title", "name", None)]).await;
        receiver.exclude_later_writes(2).await;
        assert_eq!(receiver.receive(nine.clone().into()).await.unwrap(), ApplyOutcome::Applied);
        let frozen = receiver.lost_values().await.unwrap();
        assert!(
            matches!(&frozen[..], [loss] if loss.replaced_by == Replacement::SchemaChange { version: 2 }),
            "{frozen:?}"
        );
        receiver.close().await.unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let receiver = open(
            &receiver_store,
            vec![
                initial(),
                rename(2, "title", "name", None),
                Migration::addition(3, "color", "ALTER TABLE notes ADD COLUMN color TEXT"),
                rename(4, "name", "heading", Some(calls.clone())),
            ],
        )
        .await;
        receiver.exclude_later_writes(4).await;
        assert_eq!(receiver.lost_values().await.unwrap(), frozen);
        assert_eq!(receiver.receive(nine.into()).await.unwrap(), ApplyOutcome::AlreadyApplied);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(names(&receiver, "heading").await.is_empty());
        assert_eq!(receiver.lost_values().await.unwrap(), frozen);
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }

    /// Ben's writes 1 and 2 are excluded by a drop choice. His migrated app
    /// then writes 4, after a marker, with both earlier writes among its own
    /// predecessors: the writes it recorded as read order it, and never
    /// exclude it, so every receiver applies it with no data-dependency
    /// loss (§18.6).
    #[tokio::test]
    async fn recorded_reads_order_a_write_but_never_exclude_it() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('a','first','b')").await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('b','second','b')").await.unwrap();
        ben.close().await.unwrap();
        let ben = open(&ben_store, vec![initial(), rename(2, "title", "name", None)]).await;
        sql(&ben, "INSERT INTO notes(id,name,body) VALUES('z','third','b')").await.unwrap();
        let writes = records(&ben);
        let numbers: Vec<_> = writes.iter().map(|w| w.header.position.number).collect();
        assert_eq!(numbers, [1, 2, 3, 4]);
        assert_eq!(writes[3].header.schema_version, 2);

        let receiver =
            open(&receiver_store, vec![initial(), rename(2, "title", "name", None)]).await;
        receiver.exclude_later_writes(2).await;
        for write in &writes {
            assert_eq!(
                receiver.receive(write.clone().into()).await.unwrap(),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(names(&receiver, "name").await, [("z".to_owned(), "third".to_owned())]);
        let losses = receiver.lost_values().await.unwrap();
        assert_eq!(losses.len(), 2, "{losses:?}");
        assert!(losses
            .iter()
            .all(|loss| loss.replaced_by == Replacement::SchemaChange { version: 2 }));
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }

    /// Note n keeps its generation from a covered write, its title set by the
    /// excluded 9. Ben's 10 edits that title and renames a playlist: the
    /// title edit is frozen on its excluded setter, the rename converts. A
    /// later edit of the playlist applies; a later edit of the frozen title
    /// loses with it (§18.6).
    #[tokio::test]
    async fn the_replaced_setter_decides_and_later_changes_follow_each_part() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('p','playlist','b'),('n','covered note','b')")
            .await
            .unwrap();
        sql(&ben, "UPDATE notes SET title='excluded title' WHERE id='n'").await.unwrap();
        ben.write(|sql| {
            sql.execute("UPDATE notes SET title='edited' WHERE id='n'", [])?;
            sql.execute("UPDATE notes SET title='renamed playlist' WHERE id='p'", [])?;
            Ok(())
        })
        .await
        .unwrap();
        sql(&ben, "UPDATE notes SET title='playlist again' WHERE id='p'").await.unwrap();
        sql(&ben, "UPDATE notes SET title='built on the frozen edit' WHERE id='n'").await.unwrap();
        let writes = records(&ben);
        assert_eq!(writes.len(), 5);

        let receiver = open(&receiver_store, vec![initial()]).await;
        receiver.receive(writes[0].clone().into()).await.unwrap();
        receiver.close().await.unwrap();
        let receiver =
            open(&receiver_store, vec![initial(), rename(2, "title", "name", None)]).await;
        receiver.exclude_later_writes(2).await;
        receiver.receive(writes[1].clone().into()).await.unwrap();
        receiver.close().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let receiver =
            open(&receiver_store, vec![initial(), rename(2, "title", "name", Some(calls.clone()))])
                .await;
        for write in &writes[2..] {
            assert_eq!(
                receiver.receive(write.clone().into()).await.unwrap(),
                ApplyOutcome::Applied
            );
        }
        // Only the playlist's changes reach the converter.
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            names(&receiver, "name").await,
            [
                ("n".to_owned(), "covered note".to_owned()),
                ("p".to_owned(), "playlist again".to_owned())
            ]
        );
        let dependencies: Vec<_> = receiver
            .lost_values()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|loss| match loss.replaced_by {
                Replacement::DataDependency { write, sources } => Some((write, sources)),
                _ => None,
            })
            .collect();
        assert_eq!(
            dependencies,
            [
                (writes[2].header.position, vec![source(&writes[1], "n")]),
                (writes[4].header.position, vec![source(&writes[2], "n")]),
            ]
        );
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }
}
