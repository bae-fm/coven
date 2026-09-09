use super::*;

#[path = "replay_projection_relationship_tests.rs"]
mod relationships;

fn projection_connection(parent_id: &str) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_in_memory().expect("open projection database");
    connection
        .execute_batch(&format!(
            "PRAGMA foreign_keys = ON;
                 CREATE TABLE parents (
                     id TEXT PRIMARY KEY,
                     code TEXT NOT NULL UNIQUE
                 );
                 CREATE TABLE children (
                     id TEXT PRIMARY KEY,
                     parent_code TEXT NOT NULL
                         REFERENCES parents(code) ON DELETE CASCADE
                 );
                 INSERT INTO parents VALUES ('{parent_id}', 'stable-code');
                 INSERT INTO children VALUES ('child', 'stable-code');"
        ))
        .expect("create projection rows");
    connection
}

#[test]
fn projection_install_restores_unchanged_child_removed_by_parent_cascade() {
    let source = projection_connection("replacement-parent");
    let mut target = projection_connection("old-parent");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(
        &source,
        &transaction,
        &["parents".to_string(), "children".to_string()],
    )
    .expect("replace projection tables");
    transaction.commit().expect("commit projection install");

    assert_eq!(
        target
            .query_row("SELECT COUNT(*) FROM children", [], |row| row
                .get::<_, i64>(0))
            .expect("count installed children"),
        1,
    );
}

fn update_cascade_projection(parent_rows: &str, child_parent: &str) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_in_memory().expect("open projection database");
    connection
        .execute_batch(&format!(
            "PRAGMA foreign_keys = ON;
                 CREATE TABLE parents (
                     id TEXT PRIMARY KEY,
                     code TEXT NOT NULL UNIQUE
                 );
                 CREATE TABLE children (
                     id TEXT PRIMARY KEY,
                     parent_code TEXT NOT NULL
                         REFERENCES parents(code) ON UPDATE CASCADE
                 );
                 {parent_rows}
                 INSERT INTO children VALUES ('child', '{child_parent}');"
        ))
        .expect("create update-cascade projection rows");
    connection
}

#[test]
fn projection_install_is_exact_after_parent_update_cascades() {
    let source =
        update_cascade_projection("INSERT INTO parents VALUES ('0', 'C'), ('1', 'B');", "B");
    let mut target =
        update_cascade_projection("INSERT INTO parents VALUES ('0', 'B'), ('1', 'A');", "A");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(
        &source,
        &transaction,
        &["children".to_string(), "parents".to_string()],
    )
    .expect("replace projection tables");
    transaction.commit().expect("commit projection install");

    assert_eq!(
        target
            .query_row(
                "SELECT parent_code FROM children WHERE id = 'child'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read installed child"),
        "B",
    );
}

#[test]
fn projection_install_does_not_depend_on_unique_value_update_order() {
    let source =
        update_cascade_projection("INSERT INTO parents VALUES ('0', 'B'), ('1', 'C');", "B");
    let mut target =
        update_cascade_projection("INSERT INTO parents VALUES ('0', 'A'), ('1', 'B');", "B");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(
        &source,
        &transaction,
        &["children".to_string(), "parents".to_string()],
    )
    .expect("replace projection tables");
    transaction.commit().expect("commit projection install");

    assert_eq!(
        target
            .query_row(
                "SELECT group_concat(id || ':' || code, ',') FROM parents ORDER BY id",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read installed parents"),
        "0:B,1:C",
    );
}

#[test]
fn cyclic_unique_projection_installs_without_replaying_host_triggers() {
    fn connection(rows: &str) -> rusqlite::Connection {
        let connection = rusqlite::Connection::open_in_memory().expect("open projection database");
        connection
            .execute_batch(&format!(
                "CREATE TABLE rows (
                         id TEXT PRIMARY KEY,
                         code TEXT NOT NULL UNIQUE CHECK (length(code) = 1)
                     );
                     {rows}"
            ))
            .expect("create projection rows");
        connection
    }
    let source = connection("INSERT INTO rows VALUES ('0', 'B'), ('1', 'A');");
    let mut target = connection("INSERT INTO rows VALUES ('0', 'A'), ('1', 'B');");
    target
        .execute_batch(
            "CREATE TABLE local_audit (
                     row_id TEXT NOT NULL,
                     old_code TEXT NOT NULL,
                     new_code TEXT NOT NULL
                 );
                 CREATE TRIGGER audit_code
                 AFTER UPDATE OF code ON rows
                 BEGIN
                     INSERT INTO local_audit VALUES (NEW.id, OLD.code, NEW.code);
                 END;",
        )
        .expect("create local update audit");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(&source, &transaction, &["rows".to_string()])
        .expect("install exact cyclic UNIQUE projection");
    transaction.commit().expect("commit projection install");

    assert_eq!(
        target
            .query_row(
                "SELECT group_concat(id || ':' || code, ',') FROM rows ORDER BY id",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read installed rows"),
        "0:B,1:A",
    );
    assert_eq!(
        target
            .query_row("SELECT COUNT(*) FROM local_audit", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count duplicate trigger effects"),
        0,
    );
}

#[test]
fn projection_install_does_not_repeat_captured_trigger_effects() {
    fn connection(rows: &str) -> rusqlite::Connection {
        let connection = rusqlite::Connection::open_in_memory().expect("open projection database");
        connection
            .execute_batch(&format!(
                "CREATE TABLE parents (
                         id TEXT PRIMARY KEY,
                         code TEXT NOT NULL UNIQUE ON CONFLICT FAIL
                     );
                     {rows}"
            ))
            .expect("create projection rows");
        connection
    }
    let source = connection("INSERT INTO parents VALUES ('0', 'B'), ('1', 'C');");
    let mut target = connection("INSERT INTO parents VALUES ('0', 'A'), ('1', 'B');");
    target
        .execute_batch(
            "CREATE TABLE local_audit (row_id TEXT NOT NULL);
                 CREATE TRIGGER audit_parent_code
                 BEFORE UPDATE OF code ON parents
                 BEGIN
                     INSERT INTO local_audit VALUES (OLD.id);
                 END;",
        )
        .expect("create local update audit");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(&source, &transaction, &["parents".to_string()])
        .expect("replace projection tables");
    transaction.commit().expect("commit projection install");

    assert_eq!(
        target
            .query_row("SELECT COUNT(*) FROM local_audit", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count duplicate update effects"),
        0,
    );
}

fn self_referencing_projection(parent_title: &str) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_in_memory().expect("open projection database");
    connection
        .execute_batch(&format!(
            "PRAGMA foreign_keys = ON;
                 CREATE TABLE nodes (
                     id TEXT PRIMARY KEY,
                     parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE,
                     title TEXT NOT NULL
                 );
                 INSERT INTO nodes VALUES ('parent', NULL, '{parent_title}');
                 INSERT INTO nodes VALUES ('child', 'parent', 'Child');"
        ))
        .expect("create self-referencing projection rows");
    connection
}

#[test]
fn projection_install_restores_unchanged_self_referencing_child() {
    let source = self_referencing_projection("New parent");
    let mut target = self_referencing_projection("Old parent");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(&source, &transaction, &["nodes".to_string()])
        .expect("replace projection table");
    transaction.commit().expect("commit projection install");

    assert_eq!(
        target
            .query_row("SELECT title FROM nodes WHERE id = 'child'", [], |row| {
                row.get::<_, String>(0)
            },)
            .expect("read restored child"),
        "Child",
    );
}

fn local_dependent_projection(parent_title: &str) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_in_memory().expect("open projection database");
    connection
        .execute_batch(&format!(
            "PRAGMA foreign_keys = ON;
                 CREATE TABLE parents (
                     id TEXT PRIMARY KEY,
                     title TEXT NOT NULL
                 );
                 CREATE TABLE local_records (
                     id TEXT PRIMARY KEY,
                     parent_id TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE
                 );
                 INSERT INTO parents VALUES ('parent', '{parent_title}');
                 INSERT INTO local_records VALUES ('local-record', 'parent');"
        ))
        .expect("create projection with local dependent");
    connection
}

#[test]
fn projection_install_preserves_unprojected_dependents_of_changed_rows() {
    let source = local_dependent_projection("New parent");
    let mut target = local_dependent_projection("Old parent");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(&source, &transaction, &["parents".to_string()])
        .expect("replace projection table");
    transaction.commit().expect("commit projection install");

    assert_eq!(
        target
            .query_row("SELECT COUNT(*) FROM local_records", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count local dependents"),
        1,
    );
}

#[test]
fn projection_install_cascades_real_deletions_to_device_local_rows() {
    let source = local_dependent_projection("Parent");
    source
        .execute("DELETE FROM parents", [])
        .expect("remove projected parent");
    let mut target = local_dependent_projection("Parent");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(&source, &transaction, &["parents".to_string()])
        .expect("install parent deletion");
    assert_eq!(
        transaction
            .query_row("SELECT COUNT(*) FROM local_records", [], |row| row
                .get::<_, i64>(0))
            .expect("count device-local children"),
        0,
    );
    assert!(!transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get::<_, bool>(0)
        )
        .expect("validate installed foreign keys"));
    transaction.commit().expect("commit parent deletion");
}

#[test]
fn projection_install_preserves_local_descendants_of_reparented_rows() {
    fn connection(reparent: bool) -> rusqlite::Connection {
        let connection = local_dependent_projection("Parent");
        connection
            .execute_batch(
                "CREATE TABLE local_details (
                id TEXT PRIMARY KEY,
                record_id TEXT NOT NULL REFERENCES local_records(id) ON DELETE CASCADE
             );
             INSERT INTO local_details VALUES ('detail', 'local-record');
             INSERT INTO parents VALUES ('replacement', 'Replacement');",
            )
            .expect("create local grandchild");
        if reparent {
            connection
                .execute_batch(
                    "UPDATE local_records SET parent_id = 'replacement';
                 DELETE FROM parents WHERE id = 'parent';",
                )
                .expect("reparent the projected child before deleting its former parent");
        }
        connection
    }
    let source = connection(true);
    let mut target = connection(false);
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(
        &source,
        &transaction,
        &["parents".to_string(), "local_records".to_string()],
    )
    .expect("install reparented child and parent deletion");
    assert_eq!(
        transaction
            .query_row(
                "SELECT record_id FROM local_details WHERE id = 'detail'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("retain the surviving child's local data"),
        "local-record",
    );
    transaction.commit().expect("commit reparented projection");
}

#[test]
fn projection_install_keeps_local_references_with_their_surviving_parent() {
    fn connection() -> rusqlite::Connection {
        let connection = rusqlite::Connection::open_in_memory().expect("open projection database");
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
             CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT NOT NULL UNIQUE);
             CREATE TABLE local_records (
                id TEXT PRIMARY KEY,
                parent_code TEXT NOT NULL REFERENCES parents(code)
                    ON UPDATE CASCADE ON DELETE CASCADE
             );
             INSERT INTO parents VALUES ('one', 'A'), ('two', 'B');
             INSERT INTO local_records VALUES ('local-one', 'A'), ('local-two', 'B');",
            )
            .expect("create parents with local references");
        connection
    }
    let source = connection();
    source
        .execute_batch(
            "UPDATE parents SET code = 'temporary' WHERE id = 'one';
         UPDATE parents SET code = 'A' WHERE id = 'two';
         UPDATE parents SET code = 'B' WHERE id = 'one';",
        )
        .expect("swap parent keys");
    let mut target = connection();
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_connection_on(&source, &transaction, &["parents".to_string()])
        .expect("install swapped parent keys");
    let references = crate::query_mapped_rows(
        &transaction,
        "SELECT id, parent_code FROM local_records ORDER BY id",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )
    .expect("read retained local references");
    assert_eq!(
        references,
        [
            ("local-one".into(), "B".into()),
            ("local-two".into(), "A".into())
        ]
    );
    transaction.commit().expect("commit parent key swap");
}
