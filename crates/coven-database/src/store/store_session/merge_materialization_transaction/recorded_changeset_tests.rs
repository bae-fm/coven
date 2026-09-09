use super::*;
use crate::store::store_session::replay_sql::ReplaySql;
use coven_protocol::synced_schema::RowIdentity;
use coven_protocol::write::{WriteId, WriteRebaseConflictReason};

struct RecordedEditFixture {
    conn: Connection,
    schema: Arc<TableSchema>,
    dir: coven_foundation::store_dir::StoreDir,
    _temp: tempfile::TempDir,
}

impl RecordedEditFixture {
    fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        crate::apply_coven_schema(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", true).unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                id TEXT PRIMARY KEY NOT NULL,
                title TEXT NOT NULL UNIQUE,
                body TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO notes VALUES ('one', 'original', 'body', '0000000001000-0000-local');",
        )
        .unwrap();
        let tables = [SyncedTable::new("notes", RowIdentity::SharedKey)];
        let schema = Arc::new(TableSchema::from_db(&conn, &tables).unwrap());
        let temp = tempfile::tempdir().unwrap();
        let dir = coven_foundation::store_dir::StoreDir::new_ephemeral(temp.path());
        Self {
            conn,
            schema,
            dir,
            _temp: temp,
        }
    }

    fn record(&self, sql: &str) -> Vec<u8> {
        let tx = self.conn.unchecked_transaction().unwrap();
        let mut session = rusqlite::session::Session::new(&tx).unwrap();
        for table in self.schema.synced_tables() {
            session.attach(Some(table.name())).unwrap();
        }
        tx.execute_batch(sql).unwrap();
        let mut bytes = Vec::new();
        session.changeset_strm(&mut bytes).unwrap();
        drop(session);
        tx.rollback().unwrap();
        bytes
    }

    fn apply(&self, bytes: &[u8]) -> Result<(), DbError> {
        let changeset = ValidatedChangeset::new(bytes, self.schema.clone())?;
        let tx = self.conn.unchecked_transaction()?;
        ReplaySql::begin(&tx)?.run(|| {
            MergeMaterializationTransaction::from_store(
                crate::store::store_session::StoreTransaction::new(&tx, &self.dir),
            )
            .apply_recorded_changeset(
                changeset,
                &WriteId::from_generated("recorded-write".into()),
                &Timestamp::new(4000, 0, "local".into()),
            )
        })?;
        assert!(!tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get::<_, bool>(0)
        )?);
        tx.commit()?;
        Ok(())
    }

    fn row(&self) -> (String, Option<String>, String) {
        self.conn
            .query_row(
                "SELECT title, body, _updated_at FROM notes WHERE id = 'one'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }
}

#[test]
fn recorded_edit_preserves_unrelated_peer_column_and_uses_new_stamp() {
    let fixture = RecordedEditFixture::new();
    let bytes = fixture.record("UPDATE notes SET title = 'local title', _updated_at = '0000000002000-0000-local' WHERE id = 'one'");
    fixture.conn.execute_batch("UPDATE notes SET body = 'peer body', _updated_at = '0000000003000-0000-peer' WHERE id = 'one'").unwrap();
    fixture.apply(&bytes).unwrap();
    assert_eq!(
        fixture.row(),
        (
            "local title".into(),
            Some("peer body".into()),
            "0000000004000-0000-local".into()
        )
    );
}

#[test]
fn recorded_edit_accepts_already_applied_value() {
    let fixture = RecordedEditFixture::new();
    let bytes = fixture.record(
        "UPDATE notes SET body = NULL, _updated_at = '0000000002000-0000-local' WHERE id = 'one'",
    );
    fixture.conn.execute_batch("UPDATE notes SET body = NULL, _updated_at = '0000000003000-0000-peer' WHERE id = 'one'").unwrap();
    fixture.apply(&bytes).unwrap();
    assert_eq!(
        fixture.row(),
        ("original".into(), None, "0000000004000-0000-local".into())
    );
}

#[test]
fn recorded_edit_reports_conflicts_without_committing_rows() {
    let cases = [
        (
            "UPDATE notes SET title = 'local title', _updated_at = '0000000002000-0000-local' WHERE id = 'one'",
            "DELETE FROM notes",
            WriteRebaseConflictReason::MissingTarget,
        ),
        (
            "UPDATE notes SET title = 'local title', _updated_at = '0000000002000-0000-local' WHERE id = 'one'",
            "UPDATE notes SET title = 'peer title'",
            WriteRebaseConflictReason::ChangedColumn {
                column: "title".into(),
            },
        ),
        (
            "DELETE FROM notes WHERE id = 'one'",
            "UPDATE notes SET body = 'peer body'",
            WriteRebaseConflictReason::ChangedColumn {
                column: "body".into(),
            },
        ),
        (
            "DELETE FROM notes WHERE id = 'one'",
            "DELETE FROM notes",
            WriteRebaseConflictReason::MissingTarget,
        ),
        (
            "INSERT INTO notes VALUES ('two', 'local title', 'body', '0000000002000-0000-local')",
            "INSERT INTO notes VALUES ('two', 'peer title', 'body', '0000000003000-0000-peer')",
            WriteRebaseConflictReason::IdentityCollision,
        ),
    ];
    for (edit, peer, reason) in cases {
        let fixture = RecordedEditFixture::new();
        let bytes = fixture.record(edit);
        fixture.conn.execute_batch(peer).unwrap();
        let before: Vec<(String, String, Option<String>, String)> =
            crate::query_mapped_rows(&fixture.conn, "SELECT * FROM notes ORDER BY id", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .unwrap();
        let error = fixture.apply(&bytes).unwrap_err();
        let DbError::WriteRebaseConflict(conflict) = error else {
            panic!("expected typed conflict: {error}")
        };
        assert_eq!(conflict.write_id.as_str(), "recorded-write");
        assert_eq!(conflict.affected_rows.len(), 1);
        assert_eq!(conflict.affected_rows[0].table, "notes");
        assert_eq!(conflict.reason, reason);
        let after: Vec<(String, String, Option<String>, String)> =
            crate::query_mapped_rows(&fixture.conn, "SELECT * FROM notes ORDER BY id", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .unwrap();
        assert_eq!(before, after);
    }
}

#[test]
fn recorded_edit_constraint_failure_rolls_back_the_whole_write() {
    let fixture = RecordedEditFixture::new();
    let bytes = fixture.record("UPDATE notes SET body = 'local body', _updated_at = '0000000002000-0000-local' WHERE id = 'one'; INSERT INTO notes VALUES ('two', 'conflicting title', 'body', '0000000002000-0000-local')");
    fixture.conn.execute_batch("INSERT INTO notes VALUES ('peer', 'conflicting title', 'body', '0000000003000-0000-peer')").unwrap();
    let before = fixture.row();
    let error = fixture.apply(&bytes).unwrap_err();
    assert!(
        matches!(error, DbError::WriteRebaseConflict(ref conflict) if matches!(conflict.reason, WriteRebaseConflictReason::Constraint { .. })),
        "{error}"
    );
    assert_eq!(
        error.write_rebase_conflict().unwrap().affected_rows,
        [coven_protocol::write::AffectedRow {
            table: "notes".into(),
            primary_key: "two".into(),
        }],
    );
    assert_eq!(fixture.row(), before);
    assert_eq!(
        fixture
            .conn
            .query_row("SELECT COUNT(*) FROM notes WHERE id = 'two'", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn recorded_delete_ignores_only_the_timestamp_change() {
    let fixture = RecordedEditFixture::new();
    let bytes = fixture.record("DELETE FROM notes WHERE id = 'one'");
    fixture
        .conn
        .execute_batch("UPDATE notes SET _updated_at = '0000000003000-0000-peer'")
        .unwrap();
    fixture.apply(&bytes).unwrap();
    assert_eq!(
        fixture
            .conn
            .query_row("SELECT COUNT(*) FROM notes", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn recorded_edit_suffix_rolls_back_an_already_applied_edit() {
    let fixture = RecordedEditFixture::new();
    let first = fixture.record("UPDATE notes SET body = 'local body', _updated_at = '0000000002000-0000-local' WHERE id = 'one'");
    let second = fixture.record(
        "INSERT INTO notes VALUES ('two', 'conflicting title', 'body', '0000000002000-0000-local')",
    );
    fixture.conn.execute_batch("INSERT INTO notes VALUES ('peer', 'conflicting title', 'body', '0000000003000-0000-peer')").unwrap();
    let before = fixture.row();
    let result = (|| -> Result<(), DbError> {
        let tx = fixture.conn.unchecked_transaction()?;
        let replay = MergeMaterializationTransaction::from_store(
            crate::store::store_session::StoreTransaction::new(&tx, &fixture.dir),
        );
        ReplaySql::begin(&tx)?.run(|| {
            replay.apply_recorded_changeset(
                ValidatedChangeset::new(first, fixture.schema.clone())?,
                &WriteId::from_generated("first".into()),
                &Timestamp::new(4000, 0, "local".into()),
            )?;
            assert_eq!(fixture.row().1.as_deref(), Some("local body"));
            replay.apply_recorded_changeset(
                ValidatedChangeset::new(second, fixture.schema.clone())?,
                &WriteId::from_generated("second".into()),
                &Timestamp::new(4000, 1, "local".into()),
            )
        })?;
        tx.commit()?;
        Ok(())
    })();
    let DbError::WriteRebaseConflict(conflict) = result.unwrap_err() else {
        panic!("expected constraint conflict")
    };
    assert_eq!(conflict.write_id.as_str(), "second");
    assert!(matches!(
        conflict.reason,
        WriteRebaseConflictReason::Constraint { .. }
    ));
    assert_eq!(fixture.row(), before);
}

#[test]
fn recorded_insert_is_captured_with_the_replacement_stamp() {
    let fixture = RecordedEditFixture::new();
    let bytes = fixture.record(
        "INSERT INTO notes VALUES ('two', 'local title', NULL, '0000000002000-0000-local')",
    );
    let mut capture = rusqlite::session::Session::new(&fixture.conn).unwrap();
    capture.attach(Some("notes")).unwrap();
    fixture.apply(&bytes).unwrap();
    let mut actual = Vec::new();
    capture.changeset_strm(&mut actual).unwrap();
    let incoming = incoming_rows(&actual, &fixture.schema).unwrap();
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0].row_id, "two");
    assert_eq!(
        incoming[0].row_stamp.as_deref(),
        Some("0000000004000-0000-local")
    );
    assert_ne!(actual, bytes);
}

#[test]
fn recorded_edit_replays_unique_value_swaps_and_preserves_peer_columns() {
    let mut fixture = RecordedEditFixture::new();
    fixture.conn.execute_batch("CREATE TABLE comments (id TEXT PRIMARY KEY NOT NULL, note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE, _updated_at TEXT NOT NULL) STRICT; INSERT INTO comments VALUES ('child-one', 'one', '0000000001000-0000-local')").unwrap();
    fixture.schema = Arc::new(
        TableSchema::from_db(
            &fixture.conn,
            &[
                SyncedTable::new("notes", RowIdentity::SharedKey),
                SyncedTable::new("comments", RowIdentity::SharedKey),
            ],
        )
        .unwrap(),
    );
    fixture
        .conn
        .execute_batch(
            "INSERT INTO notes VALUES ('two', 'second', 'second body', '0000000001000-0000-local')",
        )
        .unwrap();
    fixture
        .conn
        .execute_batch(
            "INSERT INTO comments VALUES ('child-two', 'two', '0000000001000-0000-local')",
        )
        .unwrap();
    let bytes = fixture.record("UPDATE notes SET title = 'temporary' WHERE id = 'one'; UPDATE notes SET title = 'original', _updated_at = '0000000002000-0000-local' WHERE id = 'two'; UPDATE notes SET title = 'second', _updated_at = '0000000002000-0000-local' WHERE id = 'one'");
    fixture.conn.execute_batch("UPDATE notes SET body = 'peer body', _updated_at = '0000000003000-0000-peer' WHERE id = 'one'").unwrap();
    fixture.apply(&bytes).unwrap();
    assert_eq!(
        fixture.row(),
        (
            "second".into(),
            Some("peer body".into()),
            "0000000004000-0000-local".into()
        )
    );
    let second: (String, String, String) = fixture
        .conn
        .query_row(
            "SELECT title, body, _updated_at FROM notes WHERE id = 'two'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        second,
        (
            "original".into(),
            "second body".into(),
            "0000000004000-0000-local".into()
        )
    );
    let children: Vec<(String, String)> = crate::query_mapped_rows(
        &fixture.conn,
        "SELECT id, note_id FROM comments ORDER BY id",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap();
    assert_eq!(
        children,
        [
            ("child-one".into(), "one".into()),
            ("child-two".into(), "two".into())
        ]
    );
}

#[test]
fn recorded_edit_reports_replacement_stamp_constraints_without_committing() {
    let mut fixture = RecordedEditFixture::new();
    fixture.conn.execute_batch("ALTER TABLE notes ADD COLUMN stamp_check INTEGER CHECK(_updated_at != '0000000004000-0000-local')").unwrap();
    fixture.schema = Arc::new(
        TableSchema::from_db(
            &fixture.conn,
            &[SyncedTable::new("notes", RowIdentity::SharedKey)],
        )
        .unwrap(),
    );
    let bytes = fixture.record("UPDATE notes SET title = 'local title', _updated_at = '0000000002000-0000-local' WHERE id = 'one'");
    let before = fixture.row();
    let error = fixture.apply(&bytes).unwrap_err();
    assert!(
        matches!(error, DbError::WriteRebaseConflict(ref conflict) if matches!(conflict.reason, WriteRebaseConflictReason::Constraint { .. })),
        "{error}"
    );
    assert_eq!(fixture.row(), before);
}

#[test]
fn recorded_edit_does_not_repeat_captured_trigger_effects() {
    let mut fixture = RecordedEditFixture::new();
    fixture.conn.execute_batch("CREATE TABLE audits (id TEXT PRIMARY KEY NOT NULL, note_id TEXT NOT NULL, _updated_at TEXT NOT NULL) STRICT; CREATE TRIGGER audit_title AFTER UPDATE OF title ON notes BEGIN INSERT INTO audits VALUES ('audit-' || (SELECT COUNT(*) + 1 FROM audits), NEW.id, NEW._updated_at); END;").unwrap();
    fixture.schema = Arc::new(
        TableSchema::from_db(
            &fixture.conn,
            &[
                SyncedTable::new("notes", RowIdentity::SharedKey),
                SyncedTable::new("audits", RowIdentity::SharedKey),
            ],
        )
        .unwrap(),
    );
    let bytes = fixture.record("UPDATE notes SET title = 'local title', _updated_at = '0000000002000-0000-local' WHERE id = 'one'");
    fixture.apply(&bytes).unwrap();
    let audit_ids: Vec<String> = crate::query_mapped_rows(
        &fixture.conn,
        "SELECT id FROM audits ORDER BY id",
        [],
        |row| row.get(0),
    )
    .unwrap();
    assert_eq!(audit_ids, ["audit-1"]);
}

#[test]
fn recorded_edit_preserves_the_owners_foreign_key_deferral() {
    let fixture = RecordedEditFixture::new();
    let bytes = fixture.record("UPDATE notes SET title = 'local title', _updated_at = '0000000002000-0000-local' WHERE id = 'one'");
    let tx = fixture.conn.unchecked_transaction().unwrap();
    tx.pragma_update(None, "defer_foreign_keys", true).unwrap();
    ReplaySql::begin(&tx)
        .unwrap()
        .run(|| {
            MergeMaterializationTransaction::from_store(
                crate::store::store_session::StoreTransaction::new(&tx, &fixture.dir),
            )
            .apply_recorded_changeset(
                ValidatedChangeset::new(bytes, fixture.schema.clone()).unwrap(),
                &WriteId::from_generated("recorded-write".into()),
                &Timestamp::new(4000, 0, "local".into()),
            )
        })
        .unwrap();
    assert!(tx
        .pragma_query_value(None, "defer_foreign_keys", |row| row.get::<_, bool>(0))
        .unwrap());
    tx.commit().unwrap();
}

fn assert_combined_column_constraint(
    invariant: &str,
) -> Box<coven_protocol::write::WriteRebaseConflict> {
    let mut fixture = RecordedEditFixture::new();
    fixture.conn.execute_batch("UPDATE notes SET title = 'four', body = 'four'; INSERT INTO notes VALUES ('two', 'second', 'body', '0000000001000-0000-local')").unwrap();
    fixture.conn.execute_batch(invariant).unwrap();
    fixture.schema = Arc::new(
        TableSchema::from_db(
            &fixture.conn,
            &[SyncedTable::new("notes", RowIdentity::SharedKey)],
        )
        .unwrap(),
    );
    let bytes = fixture.record(
        "UPDATE notes SET body = 'memo', _updated_at = '0000000002000-0000-local' WHERE id = 'two'; UPDATE notes SET title = 'longer', _updated_at = '0000000002000-0000-local' WHERE id = 'one'",
    );
    fixture.conn.execute_batch(
        "UPDATE notes SET body = 'longer', _updated_at = '0000000003000-0000-peer' WHERE id = 'one'",
    ).unwrap();
    let before = fixture.row();
    let error = fixture
        .apply(&bytes)
        .expect_err("individually valid edits cannot bypass the combined constraint");
    let DbError::WriteRebaseConflict(conflict) = error else {
        panic!("expected attributed write conflict: {error}");
    };
    assert!(matches!(
        conflict.reason,
        WriteRebaseConflictReason::Constraint { .. }
    ));
    assert_eq!(conflict.write_id.as_str(), "recorded-write");
    assert!(conflict
        .affected_rows
        .iter()
        .any(|row| row.table == "notes" && row.primary_key == "one"));
    assert_eq!(fixture.row(), before);
    assert_eq!(
        fixture
            .conn
            .query_row("SELECT body FROM notes WHERE id = 'two'", [], |row| row
                .get::<_, String>(
                0
            ))
            .unwrap(),
        "body"
    );
    conflict
}

#[test]
fn recorded_edit_rejects_combined_columns_that_violate_a_check_constraint() {
    let conflict = assert_combined_column_constraint(
        "ALTER TABLE notes ADD COLUMN length_check INTEGER CHECK(length(title) + length(body) <= 10)",
    );
    assert_eq!(
        conflict
            .affected_rows
            .iter()
            .map(|row| (row.table.as_str(), row.primary_key.as_str()))
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([("notes", "one"), ("notes", "two")]),
    );
}

#[test]
fn recorded_edit_preserves_captured_trigger_rows_without_reexecuting_host_statements() {
    let mut fixture = RecordedEditFixture::new();
    fixture
        .conn
        .execute_batch(
            "UPDATE notes SET title = 'four', body = 'four';
         CREATE TABLE audits (
             id TEXT PRIMARY KEY NOT NULL,
             captured_body TEXT NOT NULL,
             _updated_at TEXT NOT NULL
         ) STRICT;
         CREATE TRIGGER validate_lengths BEFORE UPDATE ON notes
         WHEN length(NEW.title) + length(NEW.body) > 10
         BEGIN SELECT RAISE(ABORT, 'combined text too long'); END;
         CREATE TRIGGER audit_title AFTER UPDATE OF title ON notes
         BEGIN
             INSERT INTO audits VALUES (
                 'audit-' || (SELECT COUNT(*) + 1 FROM audits),
                 NEW.body,
                 NEW._updated_at
             );
         END;",
        )
        .unwrap();
    fixture.schema = Arc::new(
        TableSchema::from_db(
            &fixture.conn,
            &[
                SyncedTable::new("notes", RowIdentity::SharedKey),
                SyncedTable::new("audits", RowIdentity::SharedKey),
            ],
        )
        .unwrap(),
    );
    let edit = "UPDATE notes SET title = 'longer', _updated_at = '0000000002000-0000-local' WHERE id = 'one'";
    let bytes = fixture.record(edit);
    fixture.conn.execute_batch(
        "UPDATE notes SET body = 'longer', _updated_at = '0000000003000-0000-peer' WHERE id = 'one'",
    ).unwrap();
    let before = fixture.row();
    let error = fixture.conn.execute_batch(edit).expect_err(
        "rerunning the host statement has different semantics from applying its captured effects",
    );
    assert!(error.to_string().contains("combined text too long"));
    assert_eq!(fixture.row(), before);

    fixture
        .apply(&bytes)
        .expect("replay the recorded row effects");
    assert_eq!(
        fixture.row(),
        (
            "longer".into(),
            Some("longer".into()),
            "0000000004000-0000-local".into(),
        ),
    );
    let audits: Vec<(String, String, String)> = crate::query_mapped_rows(
        &fixture.conn,
        "SELECT id, captured_body, _updated_at FROM audits ORDER BY id",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap();
    assert_eq!(
        audits,
        [(
            "audit-1".into(),
            "four".into(),
            "0000000004000-0000-local".into(),
        )],
        "the captured trigger row survives once with its captured value",
    );
    let after = fixture.row();
    let error = fixture
        .conn
        .execute_batch(edit)
        .expect_err("normal host writes still execute the validation trigger after replay");
    assert!(error.to_string().contains("combined text too long"));
    assert_eq!(fixture.row(), after);
}
