use super::*;
use coven_protocol::synced_schema::RowIdentity;

#[test]
fn losing_column_merge_keeps_the_exact_winning_stamp() {
    let conn = Connection::open_in_memory().unwrap();
    crate::apply_coven_schema(&conn).unwrap();
    conn.execute_batch("CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, body TEXT, title TEXT, _updated_at TEXT NOT NULL) STRICT; INSERT INTO notes VALUES ('one', 'base', 'base', '0000000001000-0000-owner')").unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    let mut session = rusqlite::session::Session::new(&tx).unwrap();
    session.attach(Some("notes")).unwrap();
    tx.execute_batch("UPDATE notes SET body = 'incoming', _updated_at = '2000-0-owner'")
        .unwrap();
    let mut bytes = Vec::new();
    session.changeset_strm(&mut bytes).unwrap();
    drop(session);
    tx.rollback().unwrap();
    conn.execute_batch("UPDATE notes SET title = 'peer', _updated_at = '0000000002000-0000-owner'")
        .unwrap();
    let (_temp, dir) = coven_foundation::store_dir::temp_store_dir();
    let applied = resolve_and_apply_changeset(
        &conn,
        &dir,
        &bytes,
        &[SyncedTable::new("notes", RowIdentity::SharedKey)],
        4000,
    )
    .unwrap();
    assert!(applied.constraint_conflict_tables.is_empty());
    let row: (String, String, String) = conn
        .query_row("SELECT body, title, _updated_at FROM notes", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(
        row,
        (
            "incoming".into(),
            "peer".into(),
            "0000000002000-0000-owner".into()
        )
    );
}

#[test]
fn losing_column_merge_without_surviving_values_does_not_run_an_update_trigger() {
    let conn = Connection::open_in_memory().unwrap();
    crate::apply_coven_schema(&conn).unwrap();
    conn.execute_batch("CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, body TEXT, _updated_at TEXT NOT NULL) STRICT; INSERT INTO notes VALUES ('one', 'base', '0000000001000-0000-owner')").unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    let mut session = rusqlite::session::Session::new(&tx).unwrap();
    session.attach(Some("notes")).unwrap();
    tx.execute_batch(
        "UPDATE notes SET body = 'incoming', _updated_at = '0000000002000-0000-owner'",
    )
    .unwrap();
    let mut bytes = Vec::new();
    session.changeset_strm(&mut bytes).unwrap();
    drop(session);
    tx.rollback().unwrap();
    conn.execute_batch("UPDATE notes SET body = 'peer', _updated_at = '0000000003000-0000-peer'; CREATE TABLE updates (id INTEGER); CREATE TRIGGER record_update AFTER UPDATE ON notes BEGIN INSERT INTO updates VALUES (1); END;").unwrap();
    let (_temp, dir) = coven_foundation::store_dir::temp_store_dir();
    let applied = resolve_and_apply_changeset(
        &conn,
        &dir,
        &bytes,
        &[SyncedTable::new("notes", RowIdentity::SharedKey)],
        4000,
    )
    .unwrap();
    assert!(applied.constraint_conflict_tables.is_empty());
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM updates", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        conn.query_row("SELECT body FROM notes", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "peer"
    );
}

#[test]
fn losing_column_merge_preserves_winning_columns_and_sqlite_value_types() {
    for value in [
        Value::Null,
        Value::Text("001".into()),
        Value::Blob(vec![0, 255]),
        Value::Integer(7),
        Value::Real(1.25),
    ] {
        let conn = Connection::open_in_memory().unwrap();
        crate::apply_coven_schema(&conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, value ANY, conflicting TEXT, untouched TEXT, _updated_at TEXT NOT NULL) STRICT;
             INSERT INTO notes VALUES ('one', 'base', 'base', 'base', '0000000001000-0000-owner');",
        ).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        let mut session = rusqlite::session::Session::new(&tx).unwrap();
        session.attach(Some("notes")).unwrap();
        tx.execute("UPDATE notes SET value = ?1, conflicting = 'source', _updated_at = '0000000002000-0000-owner'", [&value]).unwrap();
        let mut bytes = Vec::new();
        session.changeset_strm(&mut bytes).unwrap();
        drop(session);
        tx.rollback().unwrap();
        conn.execute_batch("UPDATE notes SET conflicting = 'peer', untouched = 'peer only', _updated_at = '0000000003000-0000-peer'").unwrap();
        let (_temp, dir) = coven_foundation::store_dir::temp_store_dir();
        let applied = resolve_and_apply_changeset(
            &conn,
            &dir,
            &bytes,
            &[SyncedTable::new("notes", RowIdentity::SharedKey)],
            4000,
        )
        .unwrap();
        assert!(applied.constraint_conflict_tables.is_empty());
        let actual: (Value, String, String, String) = conn
            .query_row(
                "SELECT value, conflicting, untouched, _updated_at FROM notes",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            actual,
            (
                value,
                "peer".into(),
                "peer only".into(),
                "0000000003000-0000-peer".into()
            )
        );
    }
}

#[test]
fn losing_column_merge_rejects_combined_constraint_and_rolls_back_other_rows() {
    let conn = Connection::open_in_memory().unwrap();
    crate::apply_coven_schema(&conn).unwrap();
    conn.execute_batch(
        "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL, _updated_at TEXT NOT NULL, CHECK(length(title) + length(body) <= 10)) STRICT;
         INSERT INTO notes VALUES ('one', 'four', 'four', '0000000001000-0000-owner');",
    ).unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    let mut session = rusqlite::session::Session::new(&tx).unwrap();
    session.attach(Some("notes")).unwrap();
    tx.execute_batch("UPDATE notes SET title = 'longer', _updated_at = '0000000002000-0000-owner'; INSERT INTO notes VALUES ('two', 'short', 'body', '0000000002000-0000-owner')").unwrap();
    let mut bytes = Vec::new();
    session.changeset_strm(&mut bytes).unwrap();
    drop(session);
    tx.rollback().unwrap();
    conn.execute_batch("UPDATE notes SET body = 'longer', _updated_at = '0000000003000-0000-peer'")
        .unwrap();
    let (_temp, dir) = coven_foundation::store_dir::temp_store_dir();
    let applied = resolve_and_apply_changeset(
        &conn,
        &dir,
        &bytes,
        &[SyncedTable::new("notes", RowIdentity::SharedKey)],
        4000,
    );
    let error = match applied {
        Err(error) => error,
        Ok(_) => panic!("combined constraint must reject the changeset"),
    };
    assert!(
        matches!(error, DbError::Sqlite(ref source) if source.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation)),
        "{error}"
    );
    let rows: Vec<(String, String, String, String)> = crate::query_mapped_rows(
        &conn,
        "SELECT id, title, body, _updated_at FROM notes ORDER BY id",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )
    .unwrap();
    assert_eq!(
        rows,
        [(
            "one".into(),
            "four".into(),
            "longer".into(),
            "0000000003000-0000-peer".into()
        )]
    );
}

#[test]
fn losing_column_merge_applies_unique_swaps_as_one_changeset() {
    for peer_row in ["one", "two"] {
        let conn = Connection::open_in_memory().unwrap();
        crate::apply_coven_schema(&conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                id TEXT PRIMARY KEY NOT NULL,
                title TEXT NOT NULL UNIQUE,
                body TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO notes VALUES
                ('one', 'first', 'original body', '0000000001000-0000-owner'),
                ('two', 'second', 'original body', '0000000001000-0000-owner');",
        )
        .unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        let mut session = rusqlite::session::Session::new(&tx).unwrap();
        session.attach(Some("notes")).unwrap();
        tx.execute_batch(
            "UPDATE notes SET title = 'temporary' WHERE id = 'one';
             UPDATE notes SET title = 'first', _updated_at = '0000000002000-0000-owner' WHERE id = 'two';
             UPDATE notes SET title = 'second', _updated_at = '0000000002000-0000-owner' WHERE id = 'one';",
        ).unwrap();
        let mut bytes = Vec::new();
        session.changeset_strm(&mut bytes).unwrap();
        drop(session);
        tx.rollback().unwrap();
        conn.execute(
            "UPDATE notes SET body = 'peer body', _updated_at = '0000000003000-0000-peer' WHERE id = ?1",
            [peer_row],
        ).unwrap();
        let (_temp, dir) = coven_foundation::store_dir::temp_store_dir();
        let result = resolve_and_apply_changeset(
            &conn,
            &dir,
            &bytes,
            &[SyncedTable::new("notes", RowIdentity::SharedKey)],
            4000,
        )
        .expect("merge both titles before checking uniqueness");
        assert!(result.constraint_conflict_tables.is_empty());
        let rows: Vec<(String, String, String, String)> = crate::query_mapped_rows(
            &conn,
            "SELECT id, title, body, _updated_at FROM notes ORDER BY id",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
        for (id, title, body, stamp) in rows {
            assert_eq!(title, if id == "one" { "second" } else { "first" });
            assert_eq!(
                body,
                if id == peer_row {
                    "peer body"
                } else {
                    "original body"
                }
            );
            assert_eq!(
                stamp,
                if id == peer_row {
                    "0000000003000-0000-peer"
                } else {
                    "0000000002000-0000-owner"
                }
            );
        }
    }
}
