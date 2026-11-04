use super::*;

/// The tables `sql` writes, in every statement.
fn written_tables(sql: &str) -> Vec<String> {
    statements(sql)
        .into_iter()
        .flat_map(|statement| statement.writes.into_iter().map(|write| write.table))
        .collect()
}

#[test]
fn each_writing_statement_names_its_table() {
    for (sql, tables) in [
        ("INSERT INTO notes (body) VALUES (?1)", &["notes"][..]),
        ("insert or ignore into main.Notes values (1)", &["notes"]),
        ("REPLACE INTO \"notes\" VALUES (1)", &["notes"]),
        ("UPDATE OR ROLLBACK notes SET body = ?1", &["notes"]),
        ("DELETE FROM [notes] WHERE id = ?1", &["notes"]),
        ("WITH gone AS (SELECT id FROM tags) DELETE FROM `notes` WHERE id IN gone", &["notes"]),
        (
            "INSERT INTO notes VALUES (1) ON CONFLICT (id) DO UPDATE SET body = excluded.body",
            &["notes"],
        ),
    ] {
        assert_eq!(written_tables(sql), tables, "{sql}");
    }
}

#[test]
fn a_trigger_writes_what_its_body_writes_not_what_it_watches() {
    let sql = "CREATE TRIGGER notes_listed AFTER UPDATE OF body ON notes
        BEGIN
            INSERT INTO note_listing (id) VALUES (new.id);
            DELETE FROM note_search WHERE id = old.id;
        END;
        CREATE TRIGGER notes_gone BEFORE DELETE ON notes BEGIN SELECT 1; END";
    assert_eq!(written_tables(sql), ["note_listing", "note_search"]);
}

#[test]
fn reads_strings_and_comments_write_nothing() {
    for sql in [
        "SELECT body FROM notes JOIN tags ON tags.note = notes.id",
        "SELECT replace(body, 'a', 'b') FROM notes",
        "SELECT 'INSERT INTO notes' FROM tags -- DELETE FROM notes",
        "/* UPDATE notes SET body = 1 */ SELECT 1",
    ] {
        assert_eq!(written_tables(sql), Vec::<String>::new(), "{sql}");
    }
}

/// A trigger's body is one statement with the trigger, its `CASE … END`
/// included; the `;` after the trigger's own `END` ends it.
#[test]
fn statements_split_at_semicolons_outside_trigger_bodies() {
    let sql = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
CREATE INDEX IF NOT EXISTS notes_body ON main.notes(body);
CREATE TRIGGER IF NOT EXISTS notes_listed AFTER INSERT ON notes BEGIN
    UPDATE listing SET shown = CASE WHEN new.body IS NULL THEN 0 ELSE 1 END;
    DELETE FROM search WHERE id = new.id;
END;
INSERT INTO notes (body) VALUES ('BEGIN; END;')";
    let statements = statements(sql)
        .into_iter()
        .map(|statement| {
            (
                line_of(sql, statement.at),
                statement.creation,
                statement
                    .writes
                    .into_iter()
                    .map(|write| (write.table, line_of(sql, write.at)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        statements,
        [
            (1, Some(Creation::Table("notes".into())), vec![]),
            (2, Some(Creation::Index("notes".into())), vec![]),
            (
                3,
                Some(Creation::Trigger { table: "notes".into(), instead_of: None }),
                vec![("listing".into(), 4), ("search".into(), 5)]
            ),
            (7, None, vec![("notes".into(), 7)]),
        ]
    );
}

/// Temporary objects belong to the connection that makes them.
#[test]
fn temporary_objects_create_nothing_of_the_schema() {
    for sql in [
        "CREATE TEMP TABLE staged (id INTEGER)",
        "CREATE TEMPORARY TABLE staged (id INTEGER)",
        "CREATE TABLE temp.staged (id INTEGER)",
        "CREATE INDEX temp.staged_id ON staged(id)",
        "CREATE TEMP TRIGGER staged_added AFTER INSERT ON main.notes BEGIN SELECT 1; END",
        "CREATE TEMP VIEW staged_listing AS SELECT * FROM notes",
    ] {
        assert_eq!(statements(sql)[0].creation, None, "{sql}");
    }
}

/// A view is schema, and an `INSTEAD OF` trigger on one names the operation
/// it stands in for.
#[test]
fn views_and_the_operations_their_triggers_stand_in_for() {
    assert_eq!(
        statements("CREATE VIEW IF NOT EXISTS Listing AS SELECT * FROM notes")[0].creation,
        Some(Creation::View("listing".into()))
    );
    for (sql, operation) in [
        ("CREATE TRIGGER t INSTEAD OF INSERT ON listing BEGIN SELECT 1; END", Operation::Insert),
        (
            "CREATE TRIGGER IF NOT EXISTS t INSTEAD OF UPDATE OF body ON main.listing BEGIN SELECT 1; END",
            Operation::Update,
        ),
        ("CREATE TRIGGER t INSTEAD OF DELETE ON listing BEGIN SELECT 1; END", Operation::Delete),
    ] {
        assert_eq!(
            statements(sql)[0].creation,
            Some(Creation::Trigger { table: "listing".into(), instead_of: Some(operation) }),
            "{sql}"
        );
    }
}

/// Each write says how it writes: `REPLACE` inserts.
#[test]
fn each_write_names_its_operation() {
    let operations = |sql: &str| {
        statements(sql)
            .into_iter()
            .flat_map(|statement| statement.writes.into_iter().map(|write| write.operation))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        operations(
            "INSERT INTO a VALUES (1); REPLACE INTO a VALUES (1); UPDATE a SET b = 1; DELETE FROM a"
        ),
        [Operation::Insert, Operation::Insert, Operation::Update, Operation::Delete]
    );
    // One statement writing a table two ways writes it once each way.
    assert_eq!(
        operations("CREATE TRIGGER t AFTER INSERT ON a BEGIN DELETE FROM b; INSERT INTO b VALUES (1); DELETE FROM b; END"),
        [Operation::Delete, Operation::Insert]
    );
}

#[test]
fn unique_indexes_and_virtual_tables_are_schema() {
    assert_eq!(
        statements("CREATE UNIQUE INDEX notes_id ON \"Notes\"(id)")[0].creation,
        Some(Creation::Index("notes".into()))
    );
    assert_eq!(
        statements("CREATE VIRTUAL TABLE IF NOT EXISTS note_search USING fts5(body)")[0].creation,
        Some(Creation::Table("note_search".into()))
    );
}
