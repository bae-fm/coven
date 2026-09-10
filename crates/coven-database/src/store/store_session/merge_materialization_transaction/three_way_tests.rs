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

fn assert_blob_content_merges_as_one_value(publication_order: [usize; 2], same_content: bool) {
    use coven_protocol::blob::{CacheFill, Provenance};
    use coven_protocol::store_commit::ObjectHash;
    use coven_protocol::synced_schema::BlobDecl;

    let conn = Connection::open_in_memory().expect("open merge database");
    crate::apply_coven_schema(&conn).expect("install Coven schema");
    conn.execute_batch(
        "CREATE TABLE files (
             id TEXT PRIMARY KEY NOT NULL,
             blob_id TEXT NOT NULL,
             size INTEGER NOT NULL,
             hash TEXT NOT NULL,
             title TEXT NOT NULL,
             cloud_path TEXT,
             _updated_at TEXT NOT NULL
         ) STRICT;",
    )
    .expect("create blob-bearing table");
    conn.execute(
        "INSERT INTO files VALUES ('one', 'base', 4, ?1, 'base title', NULL, '0000000001000-0000-base')",
        [ObjectHash::digest(b"aaaa").to_string()],
    )
    .expect("insert shared base");
    let tables = [
        SyncedTable::new("files", RowIdentity::SharedKey).carries_blob(
            BlobDecl::new("files", Provenance::HostProvided, CacheFill::CacheEager)
                .with_id_column("blob_id")
                .with_cloud_path_column("cloud_path"),
        ),
    ];
    let declarations =
        crate::BlobDecls::from_tables(&conn, &tables).expect("resolve content declaration");
    let gates = crate::Gates::from_tables(&conn, &tables).expect("resolve publication gates");
    let newer_hash = ObjectHash::digest(b"bbbb").to_string();
    let older_hash = if same_content {
        newer_hash.clone()
    } else {
        ObjectHash::digest(b"cccccccc").to_string()
    };
    let mut changesets = Vec::new();
    for (blob_id, size, hash, title, path, stamp) in [
        (
            if same_content { "newer" } else { "older" },
            if same_content { 4 } else { 8 },
            &older_hash,
            "independent title",
            same_content.then_some("files/newer"),
            "0000000002000-0000-older",
        ),
        (
            "newer",
            4,
            &newer_hash,
            "base title",
            None,
            "0000000003000-0000-newer",
        ),
    ] {
        let tx = conn
            .unchecked_transaction()
            .expect("begin independent write");
        let mut session = rusqlite::session::Session::new(&tx).expect("capture write");
        session.attach(Some("files")).expect("attach blob table");
        tx.execute(
            "UPDATE files SET blob_id = ?1, size = ?2, hash = ?3, title = ?4, cloud_path = ?5, _updated_at = ?6",
            rusqlite::params![blob_id, size, hash, title, path, stamp],
        )
        .expect("replace blob content");
        let mut bytes = Vec::new();
        session.changeset_strm(&mut bytes).expect("extract write");
        let bytes = declarations
            .complete_blob_changeset(&tx, &bytes)
            .expect("capture complete content tuple");
        let partitioned =
            crate::partition_outbound(&tx, &bytes, &crate::RoutingChanges::empty(), &gates)
                .expect("partition actual captured content");
        assert!(partitioned.moves.is_empty());
        let [partition] = partitioned.partitions.as_slice() else {
            panic!("expected one Store partition")
        };
        changesets.push(partition.changeset.clone());
        drop(session);
        tx.rollback().expect("restore common base");
    }
    let (_temp, dir) = coven_foundation::store_dir::temp_store_dir();
    for index in publication_order {
        let applied = resolve_and_apply_changeset(&conn, &dir, &changesets[index], &tables, 4000)
            .expect("apply shared blob edit");
        assert!(applied.constraint_conflict_tables.is_empty());
        assert!(!applied.had_fk_violations);
    }
    let actual: (String, i64, String, String, Option<String>, String) = conn
        .query_row(
            "SELECT blob_id, size, hash, title, cloud_path, _updated_at FROM files WHERE id = 'one'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .expect("read merged content and metadata");
    assert_eq!(
        actual,
        (
            "newer".into(),
            4,
            newer_hash,
            "independent title".into(),
            None,
            "0000000003000-0000-newer".into(),
        ),
        "publication order {publication_order:?} must preserve one blob's content facts"
    );
}

#[test]
fn an_older_blob_edit_cannot_replace_part_of_newer_content() {
    assert_blob_content_merges_as_one_value([1, 0], false);
}

#[test]
fn a_newer_blob_edit_replaces_the_complete_older_content() {
    assert_blob_content_merges_as_one_value([0, 1], false);
}

#[test]
fn a_blob_tuple_must_match_the_whole_base_before_merging() {
    assert_blob_content_merges_as_one_value([1, 0], true);
}
