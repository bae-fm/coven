use super::*;
use crate::tests::TestStore;
use crate::write::tests::{records, sql};
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::write::{Half, RowExecution};
use coven_foundation::id_source::CircleId;

const SCHEMA: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id) ON DELETE CASCADE ON UPDATE CASCADE, body TEXT);
CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id) ON DELETE SET NULL, body TEXT);
CREATE TABLE marks(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id));
CREATE TRIGGER unmark AFTER DELETE ON tracks WHEN NOT coven_applying() BEGIN DELETE FROM marks WHERE track=old.id; END;
CREATE TRIGGER audit AFTER DELETE ON reviews WHEN NOT coven_applying() BEGIN UPDATE notes SET body='review gone' WHERE id='audited'; END;";

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("tracks", RowIdentity::SharedKey).shared_trigger("unmark"),
        SyncedTable::new("reviews", RowIdentity::SharedKey).shared_trigger("audit"),
        SyncedTable::new("notes", RowIdentity::SharedKey),
        SyncedTable::new("marks", RowIdentity::SharedKey),
    ]
}

async fn database(store: &TestStore) -> Database {
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    sql(
        &db,
        "INSERT INTO tracks VALUES('t','Song');
         INSERT INTO reviews VALUES('r','t','good');
         INSERT INTO notes VALUES('n','t','listen');
         INSERT INTO notes VALUES('audited',NULL,'');
         INSERT INTO marks VALUES('m','t');",
    )
    .await
    .unwrap();
    db
}

fn row(table: &str, key: &str) -> RowId {
    crate::tests::store_row(table, key)
}

/// A queued write's steps by row, and the cause its judgement found for each
/// foreign-key action, by the action's row and sequence. Every other step is
/// direct (§9.6).
pub(crate) struct Classified {
    pub(crate) steps: BTreeMap<RowId, Vec<Step>>,
    pub(crate) causes: BTreeMap<(RowId, u32), (RowId, u32)>,
}

impl Classified {
    /// The last write the device queued or refused.
    pub(crate) fn last(db: &Database) -> Self {
        let (record, judgement) = db.inspect_writer(|c| c.take_judged().unwrap());
        let steps = record
            .parts
            .iter()
            .flat_map(|part| part.records())
            .map(|r| (r.row().clone(), r.execution().steps.clone()))
            .collect();
        let causes = judgement
            .rows
            .iter()
            .flat_map(|(row, checked)| {
                checked.actions.iter().map(move |(sequence, chain)| {
                    ((row.clone(), *sequence), chain.last().unwrap().clone())
                })
            })
            .collect();
        Self { steps, causes }
    }

    /// Each of the row's steps' cause, or `None` for a direct step.
    fn origin(&self, table: &str, key: &str) -> Vec<Option<(RowId, u32)>> {
        let row = row(table, key);
        self.steps[&row]
            .iter()
            .map(|step| self.causes.get(&(row.clone(), step.sequence)).cloned())
            .collect()
    }
}

fn cause(table: &str, key: &str, sequence: u32) -> Option<(RowId, u32)> {
    Some((row(table, key), sequence))
}

/// Deleting a track cascades to its review and sets its note's reference
/// to NULL; a trigger's own deletes and updates stay direct, even one fired
/// by the cascade (§9.6).
#[tokio::test]
async fn foreign_key_actions_name_their_cause_and_trigger_writes_stay_direct() {
    let store = TestStore::new();
    let db = database(&store).await;
    sql(&db, "DELETE FROM tracks WHERE id='t'").await.unwrap();
    let operations = Classified::last(&db);
    let track = &operations.steps[&row("tracks", "t")];
    assert_eq!(track.len(), 1);
    assert!(matches!(track[0].change.operation, Operation::Delete));
    let deleted = track[0].sequence;
    assert_eq!(operations.origin("tracks", "t"), [None]);
    assert_eq!(operations.origin("reviews", "r"), [cause("tracks", "t", deleted)]);
    assert_eq!(operations.origin("notes", "n"), [cause("tracks", "t", deleted)]);
    assert_eq!(operations.origin("marks", "m"), [None]);
    assert_eq!(operations.origin("notes", "audited"), [None]);
    // SET NULL writes exactly the reference column, from generation one.
    let note = &operations.steps[&row("notes", "n")][0];
    let Operation::Update(columns) = &note.change.operation else {
        panic!("SET NULL is an update")
    };
    assert_eq!(note.change.generation, 1);
    assert_eq!(columns.keys().collect::<Vec<_>>(), ["track"]);
    assert_eq!(columns["track"].value, Value::Null);
    assert!(columns["track"].parents.is_empty());
    db.close().await.unwrap();
}

/// A primary-key update is a delete and an insert at one position; the
/// child's ON UPDATE CASCADE names the delete half as its cause.
#[tokio::test]
async fn a_key_update_is_two_halves_at_one_position() {
    let store = TestStore::new();
    let db = database(&store).await;
    // The mark's and note's references have no ON UPDATE action.
    sql(
        &db,
        "DELETE FROM marks; UPDATE notes SET track=NULL; UPDATE tracks SET id='u' WHERE id='t'",
    )
    .await
    .unwrap();
    let operations = Classified::last(&db);
    let old = &operations.steps[&row("tracks", "t")][0];
    let new = &operations.steps[&row("tracks", "u")][0];
    assert_eq!(old.sequence, new.sequence);
    assert!(old.half == Half::OldKey && matches!(old.change.operation, Operation::Delete));
    assert!(new.half == Half::Whole && matches!(new.change.operation, Operation::Insert(_)));
    assert_eq!(operations.origin("reviews", "r"), [cause("tracks", "t", old.sequence)]);
    let review = &operations.steps[&row("reviews", "r")][0];
    let Operation::Update(columns) = &review.change.operation else {
        panic!("ON UPDATE CASCADE is an update")
    };
    assert_eq!(columns["track"].value, Value::Text("u".into()));
    // The new reference names the parent's new row, as it is now.
    assert_eq!(columns["track"].parents.values().next().unwrap().row, row("tracks", "u"));
    db.close().await.unwrap();
}

/// Statements that revisit a row keep every step; a row inserted and
/// deleted again in one write has steps and no net change (§9.6, D5).
#[tokio::test]
async fn revisited_and_cancelled_rows_keep_each_operation() {
    let store = TestStore::new();
    let db = database(&store).await;
    sql(
        &db,
        "UPDATE reviews SET body='better' WHERE id='r';
         UPDATE reviews SET body='best' WHERE id='r';
         INSERT INTO tracks VALUES('gone','Brief');
         DELETE FROM tracks WHERE id='gone';",
    )
    .await
    .unwrap();
    let operations = Classified::last(&db);
    assert_eq!(operations.origin("reviews", "r"), [None, None]);
    let record = records(&db).pop().unwrap();
    let review =
        record.parts[0].rows.iter().find(|change| change.row == row("reviews", "r")).unwrap();
    assert_eq!(review.execution.steps.len(), 2);
    assert!(review.execution.steps[0].sequence < review.execution.steps[1].sequence);
    let cancelled: Vec<&RowExecution> = record.parts[0]
        .executions
        .iter()
        .filter(|execution| execution.row == row("tracks", "gone"))
        .collect();
    assert_eq!(cancelled.len(), 1);
    let steps = &cancelled[0].execution.steps;
    assert!(matches!(steps[0].change.operation, Operation::Insert(_)));
    assert!(matches!(steps[1].change.operation, Operation::Delete));
    assert_eq!(steps[1].change.generation, 1);
    assert!(record.parts[0].rows.iter().all(|change| change.row != row("tracks", "gone")));
    db.close().await.unwrap();
}

pub(crate) mod lean {
    // Real SQLite foreign-key actions against `ForeignKey.consequence` and
    // `evaluate` (B18): each action the author's judgement classifies, rebuilt
    // from its schema reference and its causes' own steps, must be the
    // model's exact consequence.

    use super::*;
    use crate::test_utils::tests::run_lean;
    use crate::write_rows::{column_name, key_columns, target_columns};
    use crate::write_schema::WriteSchema;
    use serde_json::{json, Value as Json};
    use std::collections::BTreeSet;

    /// Dense numbers for rows, columns and scalars, as the runner models them.
    #[derive(Default)]
    pub(crate) struct Ids {
        pub(crate) rows: BTreeMap<RowId, usize>,
        columns: BTreeMap<(String, String), usize>,
        values: Vec<Value>,
    }

    impl Ids {
        fn row(&mut self, row: &RowId) -> usize {
            let next = self.rows.len();
            *self.rows.entry(row.clone()).or_insert(next)
        }
        fn column(&mut self, table: &str, column: &str) -> usize {
            let next = self.columns.len();
            *self.columns.entry((table.to_owned(), column.to_owned())).or_insert(next)
        }
        fn value(&mut self, value: &Value) -> Option<usize> {
            if *value == Value::Null {
                return None;
            }
            Some(match self.values.iter().position(|v| v == value) {
                Some(index) => index,
                None => {
                    self.values.push(value.clone());
                    self.values.len() - 1
                }
            })
        }
    }

    fn action(rule: &str) -> &'static str {
        match rule.to_ascii_uppercase().as_str() {
            "CASCADE" => "cascade",
            "SET NULL" => "setNull",
            "RESTRICT" => "restrict",
            _ => "noAction",
        }
    }

    fn step_of(operations: &Classified, (row, sequence): &(RowId, u32)) -> Step {
        operations.steps[row].iter().find(|step| step.sequence == *sequence).unwrap().clone()
    }

    /// The parent key's values by column, from its encoded key.
    fn key_values(schema: &WriteSchema, row: &RowId) -> BTreeMap<String, Value> {
        let table = schema.table(&row.table);
        key_columns(table)
            .into_iter()
            .map(|c| c.name.clone())
            .zip(coven_format::key::decode_key(&row.key).unwrap())
            .collect()
    }

    /// A step as the model's effect: a delete has no cells; a key update's old
    /// half is an update of the key columns to the new half's values.
    fn effect(
        ids: &mut Ids,
        schema: &WriteSchema,
        operations: &Classified,
        row: &RowId,
        step: &Step,
        order: Option<&[String]>,
    ) -> Json {
        let id = ids.row(row);
        let (kind, cells) = match &step.change.operation {
            Operation::Delete if step.half == Half::OldKey => {
                let (new_row, _) = operations
                    .steps
                    .iter()
                    .flat_map(|(r, steps)| steps.iter().map(move |s| (r, s)))
                    .find(|(r, s)| s.sequence == step.sequence && r.table == row.table && *r != row)
                    .expect("the key update's insert half");
                let old = key_values(schema, row);
                let cells: Vec<_> = key_values(schema, new_row)
                    .into_iter()
                    .filter(|(c, v)| old.get(c) != Some(v))
                    .map(|(c, v)| json!({"column": ids.column(&row.table, &c), "value": ids.value(&v)}))
                    .collect();
                (1, cells)
            }
            Operation::Delete => (2, Vec::new()),
            Operation::Insert(columns) | Operation::Update(columns) => {
                let names: Vec<String> = match order {
                    Some(order) => order.to_vec(),
                    None => columns.keys().cloned().collect(),
                };
                let cells = names
                    .iter()
                    .map(|c| {
                        json!({"column": ids.column(&row.table, c), "value": ids.value(&columns[c].value)})
                    })
                    .collect();
                (if matches!(step.change.operation, Operation::Insert(_)) { 0 } else { 1 }, cells)
            }
        };
        json!({"row": id, "generation": step.change.generation, "kind": kind, "cells": cells})
    }

    /// The runner input for every foreign-key action of one write.
    fn input(schema: &WriteSchema, operations: &Classified) -> (Json, usize) {
        let (input, _) = effects(schema, operations, false);
        let count = input["derived"].as_array().unwrap().len();
        (input, count)
    }

    /// Every foreign-key action of one write with its references and the
    /// direct roots it derives from; with `every_direct`, also every other
    /// row a direct step changed.
    pub(crate) fn effects(
        schema: &WriteSchema,
        operations: &Classified,
        every_direct: bool,
    ) -> (Json, Ids) {
        let mut ids = Ids::default();
        let mut references = Vec::new();
        let mut direct = Vec::new();
        let mut derived = Vec::new();
        let mut roots = BTreeSet::new();
        for ((row, sequence), cause) in &operations.causes {
            let step = step_of(operations, &(row.clone(), *sequence));
            let parent_step = step_of(operations, cause);
            let (parent, _) = cause;
            let child_table = schema.table(&row.table);
            let parent_table = schema.table(&parent.table);
            let key = child_table
                .foreign_keys
                .iter()
                .find(|key| key.target.eq_ignore_ascii_case(&parent.table))
                .expect("the action's foreign key");
            let columns: Vec<String> =
                key.columns.iter().map(|c| column_name(child_table, c).to_owned()).collect();
            let targets = target_columns(parent_table, key);
            let old = key_values(schema, parent);
            references.push(json!({
                "parent": ids.row(parent), "parentGeneration": parent_step.change.generation,
                "child": ids.row(row), "childGeneration": step.change.generation,
                "columns": columns.iter().map(|c| ids.column(&row.table, c)).collect::<Vec<_>>(),
                "oldKey": targets.iter().map(|t| ids.value(&old[t])).collect::<Vec<_>>(),
                "onDelete": action(&key.on_delete), "onUpdate": action(&key.on_update),
                "childValues": [],
                "parentColumns": targets.iter().map(|t| ids.column(&parent.table, t)).collect::<Vec<_>>(),
            }));
            // The chain of causes back to the direct root.
            let mut path = vec![row.clone()];
            let mut at = cause.clone();
            loop {
                path.push(at.0.clone());
                match operations.causes.get(&at) {
                    Some(cause) => at = cause.clone(),
                    None => break,
                }
            }
            if roots.insert(at.clone()) {
                let root = step_of(operations, &at);
                direct.push(effect(&mut ids, schema, operations, &at.0, &root, None));
            }
            path.reverse();
            let proposed = effect(&mut ids, schema, operations, row, &step, Some(&columns));
            derived.push(json!({
                "path": path.iter().map(|r| ids.row(r)).collect::<Vec<_>>(),
                "effect": proposed,
            }));
        }
        if every_direct {
            let mut present: BTreeSet<RowId> = roots.iter().map(|(row, _)| row.clone()).collect();
            for (row, steps) in &operations.steps {
                // One effect per row: its first direct step.
                let first = steps
                    .iter()
                    .find(|step| !operations.causes.contains_key(&(row.clone(), step.sequence)));
                if let Some(step) = first {
                    if present.insert(row.clone()) {
                        direct.push(effect(&mut ids, schema, operations, row, step, None));
                    }
                }
            }
        }
        (
            json!({"mode": "effects", "references": references, "direct": direct, "derived": derived}),
            ids,
        )
    }

    const ACTIONS: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT);
    CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id) ON DELETE CASCADE ON UPDATE CASCADE, body TEXT);
    CREATE TABLE comments(id TEXT NOT NULL PRIMARY KEY, review TEXT REFERENCES reviews(id) ON DELETE CASCADE ON UPDATE SET NULL);
    CREATE TABLE albums(artist TEXT NOT NULL, title TEXT NOT NULL, PRIMARY KEY(artist, title));
    CREATE TABLE songs(id TEXT NOT NULL PRIMARY KEY, artist TEXT, album TEXT,
      FOREIGN KEY(artist, album) REFERENCES albums(artist, title) ON DELETE SET NULL ON UPDATE CASCADE);";

    fn action_tables() -> Vec<SyncedTable> {
        ["tracks", "reviews", "comments", "albums", "songs"]
            .into_iter()
            .map(|table| match table {
                "albums" => {
                    SyncedTable::new(table, RowIdentity::SharedKey).key_columns(["artist", "title"])
                }
                table => SyncedTable::new(table, RowIdentity::SharedKey),
            })
            .collect()
    }

    #[tokio::test]
    #[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh supplies the merge runner"]
    async fn captured_actions_are_the_models_consequences() {
        let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
        let store = TestStore::new();
        let db = store.schema(action_tables(), ACTIONS).await.unwrap();
        let seed = "DELETE FROM songs; DELETE FROM albums; DELETE FROM comments; DELETE FROM reviews; DELETE FROM tracks;
            INSERT INTO tracks VALUES('t','Song'),('s','Other');
            INSERT INTO reviews VALUES('r','t','good'),('q','t','fine'),('x','s','meh');
            INSERT INTO comments VALUES('c','r'),('d','q');
            INSERT INTO albums VALUES('ana','first'),('ana','second');
            INSERT INTO songs VALUES('one','ana','first'),('two','ana','first'),('three','ana','second');";
        // Each statement's actions: a transitive cascade, a referenced-key
        // update cascading on to a SET NULL, compound updates and SET NULL, and
        // several actions within one write.
        let cases = [
            "DELETE FROM tracks WHERE id='t'",
            "UPDATE tracks SET id='u' WHERE id='t'",
            "UPDATE albums SET title='renamed' WHERE title='first'",
            "DELETE FROM albums WHERE title='first'",
            "DELETE FROM tracks WHERE id='s'; UPDATE reviews SET id='p' WHERE id='r'; DELETE FROM albums",
        ];
        let mut compared = 0;
        for case in cases {
            sql(&db, seed).await.unwrap();
            sql(&db, case).await.unwrap();
            let operations = Classified::last(&db);
            let (input, count) = db.inspect_writer_schema(|_, schema| input(schema, &operations));
            assert!(count > 0, "{case} causes foreign-key actions");
            let result = run_lean(&runner, &input);
            let derived = input["derived"].as_array().unwrap();
            for (proposed, lean) in derived.iter().zip(result.as_array().unwrap()) {
                assert_eq!(lean["effect"], proposed["effect"], "{case}: {proposed}");
                assert_eq!(lean["verifies"], true, "{case}: {proposed}");
                compared += 1;
            }
        }
        assert!(compared >= 10, "{compared} actions compared");
        db.close().await.unwrap();
    }
}

/// A row whose own update changes its audience moves: a delete of its old
/// audience's row and an insert of the new one's, both halves at the
/// update's position, and a later update continues from that insert. No
/// foreign key acts on the delete (§15.4, D5).
#[tokio::test]
async fn a_move_is_two_halves_at_one_position() {
    const NOTE: &str = "00000000-0000-4000-8000-0000000000a1";
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")
            ],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, body TEXT)",
        )
        .await
        .unwrap();
    let circle = CircleId(uuid::Uuid::from_u128(10));
    store.join_circle(&db, circle);
    sql(&db, "INSERT INTO notes VALUES('00000000-0000-4000-8000-0000000000a1','store','draft')")
        .await
        .unwrap();
    let audience = circle.to_string();
    db.write(move |sql| {
        sql.execute("UPDATE notes SET audience=?1, body='moved' WHERE id=?2", [&audience, NOTE])?;
        sql.execute("UPDATE notes SET body='later' WHERE id=?1", [NOTE])?;
        Ok(())
    })
    .await
    .unwrap();
    let operations = Classified::last(&db);
    let old = row("notes", NOTE);
    let new = RowId { audience: Audience::Circle(circle), ..old.clone() };
    let [delete] = &operations.steps[&old][..] else { panic!("one step in the store") };
    let [insert, later] = &operations.steps[&new][..] else { panic!("two steps in the circle") };
    assert_eq!(delete.sequence, insert.sequence);
    assert_eq!([delete.half, insert.half, later.half], [Half::Move, Half::Move, Half::Whole]);
    assert_eq!((delete.change.generation, insert.change.generation), (1, 0));
    assert_eq!(delete.change.operation, Operation::Delete);
    let Operation::Insert(columns) = &insert.change.operation else { panic!("the move inserts") };
    assert_eq!(columns["body"].value, Value::Text("moved".into()));
    assert_eq!(columns["audience"].value, Value::Text(circle.to_string()));
    assert_eq!(later.change.generation, 1);
    assert!(operations.causes.is_empty());
    db.close().await.unwrap();
}

const ALBUM: &str = "00000000-0000-4000-8000-0000000000a2";
const OLD_TRACK: &str = "00000000-0000-4000-8000-0000000000a3";
const NEW_TRACK: &str = "00000000-0000-4000-8000-0000000000a4";

/// Albums whose audience their tracks follow, one album in the store with
/// one track, on a device in a circle.
async fn albums(store: &TestStore, deferred: bool) -> (Database, CircleId) {
    let schema = if deferred {
        "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL);
         CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY,
           album TEXT NOT NULL REFERENCES albums(id) DEFERRABLE INITIALLY DEFERRED, body TEXT)"
    } else {
        "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL);
         CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY,
           album TEXT NOT NULL REFERENCES albums(id), body TEXT)"
    };
    let db = store
        .schema(
            vec![
                SyncedTable::new("albums", RowIdentity::IndependentUuid)
                    .audience_column("audience"),
                SyncedTable::new("tracks", RowIdentity::IndependentUuid).audience_from("album"),
            ],
            schema,
        )
        .await
        .unwrap();
    let circle = CircleId(uuid::Uuid::from_u128(10));
    store.join_circle(&db, circle);
    sql(
        &db,
        "INSERT INTO albums VALUES('00000000-0000-4000-8000-0000000000a2','store');
         INSERT INTO tracks VALUES('00000000-0000-4000-8000-0000000000a3',
           '00000000-0000-4000-8000-0000000000a2','draft')",
    )
    .await
    .unwrap();
    (db, circle)
}

/// Each step's position, half, generation and whether it deletes.
fn shape(steps: &[Step]) -> Vec<(u32, Half, u64, bool)> {
    steps
        .iter()
        .map(|step| {
            let delete = step.change.operation == Operation::Delete;
            (step.sequence, step.half, step.change.generation, delete)
        })
        .collect()
}

/// The album's move carries its tracks at its own position: the track that
/// was there before the write, and the one the write inserted before the
/// move, each a delete of its store row and an insert of its circle row. A
/// later update of a carried track continues from that insert (§15.4, D5).
#[tokio::test]
async fn a_move_carries_the_rows_that_follow_its_audience() {
    let store = TestStore::new();
    let (db, circle) = albums(&store, false).await;
    let audience = circle.to_string();
    db.write(move |sql| {
        sql.execute("INSERT INTO tracks VALUES(?1,?2,'new')", [NEW_TRACK, ALBUM])?;
        sql.execute("UPDATE albums SET audience=?1 WHERE id=?2", [audience.as_str(), ALBUM])?;
        sql.execute("UPDATE tracks SET body='later' WHERE id=?1", [OLD_TRACK])?;
        Ok(())
    })
    .await
    .unwrap();
    let operations = Classified::last(&db);
    let moved = |table: &str, key: &str| {
        let stored = row(table, key);
        let carried = RowId { audience: Audience::Circle(circle), ..stored.clone() };
        (shape(&operations.steps[&stored]), shape(&operations.steps[&carried]))
    };
    assert_eq!(
        moved("albums", ALBUM),
        (vec![(1, Half::Move, 1, true)], vec![(1, Half::Move, 0, false)])
    );
    assert_eq!(
        moved("tracks", OLD_TRACK),
        (
            vec![(1, Half::Move, 1, true)],
            vec![(1, Half::Move, 0, false), (2, Half::Whole, 1, false)]
        )
    );
    assert_eq!(
        moved("tracks", NEW_TRACK),
        (
            vec![(0, Half::Whole, 0, false), (1, Half::Move, 1, true)],
            vec![(1, Half::Move, 0, false)]
        )
    );
    assert!(operations.causes.is_empty());
    db.close().await.unwrap();
}

/// A track whose album the write deletes and adds again in the circle,
/// under a deferred key, stays where it was while it has no album, and
/// follows the album at its insert (§15.4, D5).
#[tokio::test]
async fn an_insert_carries_the_rows_still_naming_its_key() {
    let store = TestStore::new();
    let (db, circle) = albums(&store, true).await;
    let audience = circle.to_string();
    db.write(move |sql| {
        sql.execute("DELETE FROM albums WHERE id=?1", [ALBUM])?;
        sql.execute("UPDATE tracks SET body='orphan' WHERE id=?1", [OLD_TRACK])?;
        sql.execute("INSERT INTO albums VALUES(?1,?2)", [ALBUM, audience.as_str()])?;
        Ok(())
    })
    .await
    .unwrap();
    let operations = Classified::last(&db);
    let stored = row("tracks", OLD_TRACK);
    let carried = RowId { audience: Audience::Circle(circle), ..stored.clone() };
    assert_eq!(
        shape(&operations.steps[&stored]),
        vec![(1, Half::Whole, 1, false), (2, Half::Move, 1, true)]
    );
    assert_eq!(shape(&operations.steps[&carried]), vec![(2, Half::Move, 0, false)]);
    db.close().await.unwrap();
}

/// What recording a write costs per row of a table whose audience follows a
/// reference (§15.4), as that table grows: each write's wall and CPU time
/// and SQL statements per row, its full-scan steps, every statement's runs
/// and virtual machine steps per row, and the machine's load. A measurement,
/// not a check: run it with `--ignored --nocapture` on two commits and
/// compare the statements; wall time follows the machine's load.
mod write_cost {
    use crate::sqlite::tests::ProfiledStatement;
    use crate::tests::TestStore;
    use crate::{RowIdentity, SyncedTable};
    use coven_foundation::id_source::CircleId;
    use std::collections::BTreeMap;
    use std::time::Instant;

    const ALBUMS: usize = 100;
    const CHUNK: usize = 10_000;

    fn album(i: usize) -> String {
        format!("00000000-0000-4000-8000-a{i:011x}")
    }

    fn track(i: usize) -> String {
        format!("00000000-0000-4000-8000-b{i:011x}")
    }

    /// The test process's CPU time so far, which other processes' load leaves
    /// out, in microseconds.
    fn cpu() -> f64 {
        let pid = std::process::id().to_string();
        let out =
            std::process::Command::new("/bin/ps").args(["-o", "cputime=", "-p", &pid]).output();
        let text = String::from_utf8_lossy(&out.unwrap().stdout).trim().to_owned();
        // [[days-]hours:]minutes:seconds.hundredths
        text.split(':').fold(0.0, |total, part| total * 60.0 + part.parse::<f64>().unwrap()) * 1e6
    }

    fn load() -> String {
        let out =
            std::process::Command::new("/usr/sbin/sysctl").args(["-n", "vm.loadavg"]).output();
        out.map_or_else(|_| "?".into(), |out| String::from_utf8_lossy(&out.stdout).trim().into())
    }

    /// Run one write of `rows` rows and print what it cost per row.
    async fn measure(
        db: &crate::Database,
        label: &str,
        rows: usize,
        write: impl FnOnce(crate::SqlContext<'_, '_>) -> Result<(), crate::DbError> + Send + 'static,
    ) {
        let (started, cpu_started) = (Instant::now(), cpu());
        db.write(write).await.unwrap();
        let elapsed = started.elapsed();
        let cpu = (cpu() - cpu_started) / rows as f64;
        let statements = db.inspect_writer(|writer| writer.profiled_statements());
        let fullscan: i64 = statements.iter().map(|s| i64::from(s.fullscan_steps)).sum();
        println!(
            "{label}: {rows} rows, {:.1} µs/row, {cpu:.1} CPU µs/row, {:.2} statements/row, \
             {fullscan} full-scan steps, load {}",
            elapsed.as_secs_f64() * 1e6 / rows as f64,
            statements.len() as f64 / rows as f64,
            load(),
        );
        for (sql, (runs, vm, scans)) in heaviest(&statements) {
            let sql: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
            let sql = sql.chars().take(110).collect::<String>();
            println!(
                "    {:>6.2}/row {:>8.1} vm/row {scans:>8} scans  {sql}",
                runs as f64 / rows as f64,
                vm as f64 / rows as f64
            );
        }
    }

    /// Each statement's executions, virtual machine steps and full-scan steps in
    /// the write, most steps first.
    fn heaviest(statements: &[ProfiledStatement]) -> Vec<(String, (i64, i64, i64))> {
        let mut by_sql: BTreeMap<String, (i64, i32, i32, i64)> = BTreeMap::new();
        for statement in statements {
            let entry =
                by_sql.entry(statement.sql.clone()).or_insert((0, statement.vm_steps, 0, 0));
            entry.0 += 1;
            entry.2 = statement.vm_steps;
            entry.3 += i64::from(statement.fullscan_steps);
        }
        let mut out: Vec<_> = by_sql
            .into_iter()
            .map(|(sql, (runs, first, last, scans))| (sql, (runs, i64::from(last - first), scans)))
            .collect();
        out.sort_by_key(|(_, (_, vm, _))| -vm);
        out
    }

    #[tokio::test]
    #[ignore = "measurement: run with --ignored --nocapture"]
    async fn write_cost_per_row_as_the_table_grows() {
        let store = TestStore::new();
        let db = store
            .schema(
                vec![
                    SyncedTable::new("albums", RowIdentity::IndependentUuid)
                        .audience_column("audience"),
                    SyncedTable::new("tracks", RowIdentity::IndependentUuid).audience_from("album"),
                ],
                "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL);
                 CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY,
                   album TEXT NOT NULL REFERENCES albums(id) ON DELETE CASCADE, title TEXT)",
            )
            .await
            .unwrap();
        let circle = CircleId(uuid::Uuid::from_u128(11));
        store.join_circle(&db, circle);
        db.write(|sql| {
            for i in 0..ALBUMS {
                sql.execute("INSERT INTO albums VALUES(?1,'store')", [album(i)])?;
            }
            Ok(())
        })
        .await
        .unwrap();
        let mut audience = "store".to_owned();
        for chunk in 0..100_000 / CHUNK {
            let first = chunk * CHUNK;
            measure(&db, &format!("insert tracks {first}.."), CHUNK, move |sql| {
                for i in first..first + CHUNK {
                    sql.execute(
                        "INSERT INTO tracks VALUES(?1,?2,'t')",
                        [track(i), album(i % ALBUMS)],
                    )?;
                }
                Ok(())
            })
            .await;
            let rows = first + CHUNK;
            if ![20_000, 50_000, 100_000].contains(&rows) {
                continue;
            }
            println!("--- the table holds {rows} tracks");
            measure(&db, "update 1000 titles", 1000, move |sql| {
                for i in 0..1000 {
                    sql.execute(
                        "UPDATE tracks SET title='u' WHERE id=?1",
                        [track(i * rows / 1000)],
                    )?;
                }
                Ok(())
            })
            .await;
            audience = if audience == "store" { circle.to_string() } else { "store".to_owned() };
            let to = audience.clone();
            measure(&db, "move every album, carrying every track", rows, move |sql| {
                for i in 0..ALBUMS {
                    sql.execute("UPDATE albums SET audience=?1 WHERE id=?2", [&to, &album(i)])?;
                }
                Ok(())
            })
            .await;
        }
        db.close().await.unwrap();
    }
}
