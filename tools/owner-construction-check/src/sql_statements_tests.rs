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
                Some(Creation::Trigger("notes".into())),
                vec![("listing".into(), 4), ("search".into(), 5)]
            ),
            (7, None, vec![("notes".into(), 7)]),
        ]
    );
}

/// Temporary objects belong to the connection that makes them; views are
/// no tables.
#[test]
fn temporary_objects_and_views_create_nothing_of_the_schema() {
    for sql in [
        "CREATE TEMP TABLE staged (id INTEGER)",
        "CREATE TEMPORARY TABLE staged (id INTEGER)",
        "CREATE TABLE temp.staged (id INTEGER)",
        "CREATE INDEX temp.staged_id ON staged(id)",
        "CREATE TEMP TRIGGER staged_added AFTER INSERT ON main.notes BEGIN SELECT 1; END",
        "CREATE VIEW IF NOT EXISTS listing AS SELECT * FROM notes",
    ] {
        assert_eq!(statements(sql)[0].creation, None, "{sql}");
    }
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
