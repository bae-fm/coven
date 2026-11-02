use super::*;

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
