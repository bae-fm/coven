use super::*;

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
    let source = ReplayProjection {
        connection: projection_connection("replacement-parent"),
        store_dir: crate::synthetic_store::test_store_dir(),
    };
    let mut target = projection_connection("old-parent");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_projection_on(
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
    let source = ReplayProjection {
        connection: update_cascade_projection(
            "INSERT INTO parents VALUES ('0', 'C'), ('1', 'B');",
            "B",
        ),
        store_dir: crate::synthetic_store::test_store_dir(),
    };
    let mut target =
        update_cascade_projection("INSERT INTO parents VALUES ('0', 'B'), ('1', 'A');", "A");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_projection_on(
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
    let source = ReplayProjection {
        connection: update_cascade_projection(
            "INSERT INTO parents VALUES ('0', 'B'), ('1', 'C');",
            "B",
        ),
        store_dir: crate::synthetic_store::test_store_dir(),
    };
    let mut target =
        update_cascade_projection("INSERT INTO parents VALUES ('0', 'A'), ('1', 'B');", "B");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_projection_on(
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
fn cyclic_unique_projection_fails_without_exposing_intermediate_rows() {
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
    let source = ReplayProjection {
        connection: connection("INSERT INTO rows VALUES ('0', 'B'), ('1', 'A');"),
        store_dir: crate::synthetic_store::test_store_dir(),
    };
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

    let error = replace_tables_from_projection_on(&source, &transaction, &["rows".to_string()])
        .expect_err("cyclic unique projection must fail atomically");
    assert!(
        error.to_string().contains("UNIQUE constraint failed"),
        "error preserves the SQLite constraint: {error}"
    );
    transaction
        .rollback()
        .expect("roll back failed projection install");

    assert_eq!(
        target
            .query_row(
                "SELECT group_concat(id || ':' || code, ',') FROM rows ORDER BY id",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read unchanged rows"),
        "0:A,1:B",
    );
    assert_eq!(
        target
            .query_row("SELECT COUNT(*) FROM local_audit", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count rolled-back trigger effects"),
        0,
    );
}

#[test]
fn deferred_unique_attempt_rolls_back_trigger_effects() {
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
    let source = ReplayProjection {
        connection: connection("INSERT INTO parents VALUES ('0', 'B'), ('1', 'C');"),
        store_dir: crate::synthetic_store::test_store_dir(),
    };
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

    replace_tables_from_projection_on(&source, &transaction, &["parents".to_string()])
        .expect("replace projection tables");
    transaction.commit().expect("commit projection install");

    assert_eq!(
        target
            .query_row("SELECT COUNT(*) FROM local_audit", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count successful update effects"),
        2,
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
    let source = ReplayProjection {
        connection: self_referencing_projection("New parent"),
        store_dir: crate::synthetic_store::test_store_dir(),
    };
    let mut target = self_referencing_projection("Old parent");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_projection_on(&source, &transaction, &["nodes".to_string()])
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
    let source = ReplayProjection {
        connection: local_dependent_projection("New parent"),
        store_dir: crate::synthetic_store::test_store_dir(),
    };
    let mut target = local_dependent_projection("Old parent");
    let transaction = target.transaction().expect("begin projection install");

    replace_tables_from_projection_on(&source, &transaction, &["parents".to_string()])
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
