use super::*;
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::synced_schema::{BlobDecl, RowIdentity};
use rusqlite::session::Session;

fn capture_update_bytes(conn: &Connection, sql: &str) -> Vec<u8> {
    let mut session = Session::new(conn).expect("create session");
    session.attach(Some("files")).expect("attach files");
    conn.execute(sql, []).expect("update file row");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract changeset");
    changeset
}

fn capture_update(conn: &Connection, sql: &str) -> RowChange {
    crate::walk_changeset(&capture_update_bytes(conn, sql))
        .expect("walk changeset")
        .into_iter()
        .next()
        .expect("captured update")
}

fn write_once_decl(id_column: Option<&str>) -> BlobDecl {
    let decl = BlobDecl::new("files", Provenance::HostProvided, CacheFill::CacheEager).write_once();
    match id_column {
        Some(column) => decl.with_id_column(column),
        None => decl,
    }
}

#[test]
fn unrelated_update_does_not_repoint_a_primary_key_blob() {
    let conn = Connection::open_in_memory().expect("open connection");
    conn.execute_batch(
        "CREATE TABLE files (
             id TEXT PRIMARY KEY,
             title TEXT NOT NULL,
             size INTEGER NOT NULL,
             hash TEXT NOT NULL
         );
         INSERT INTO files VALUES ('blob-a', 'before', 1, 'hash-a');",
    )
    .expect("create file row");
    let declarations = BlobDecls::from_tables(
        &conn,
        &[SyncedTable::new("files", RowIdentity::IndependentUuid)
            .carries_blob(write_once_decl(None))],
    )
    .expect("resolve declarations");

    let change = capture_update(&conn, "UPDATE files SET title = 'after'");

    let blob = declarations
        .ref_from_change(&change)
        .expect("read unrelated update")
        .expect("unchanged blob reference remains available");
    assert_eq!(blob.id, "blob-a");
}

#[test]
fn changing_a_write_once_blob_column_is_rejected() {
    let conn = Connection::open_in_memory().expect("open connection");
    conn.execute_batch(
        "CREATE TABLE files (
             id TEXT PRIMARY KEY,
             blob_id TEXT NOT NULL,
             size INTEGER NOT NULL,
             hash TEXT NOT NULL
         );
         INSERT INTO files VALUES ('row-a', 'blob-a', 1, 'hash-a');",
    )
    .expect("create file row");
    let declarations = BlobDecls::from_tables(
        &conn,
        &[SyncedTable::new("files", RowIdentity::IndependentUuid)
            .carries_blob(write_once_decl(Some("blob_id")))],
    )
    .expect("resolve declarations");

    let change = capture_update(&conn, "UPDATE files SET blob_id = 'blob-b'");

    assert!(matches!(
        declarations.ref_from_change(&change),
        Err(BlobDeclError::WriteOnceBlobRepointed { blob_id, .. })
            if blob_id == "blob-b"
    ));
    assert!(matches!(
        declarations.publication_blob_from_change(&conn, &change),
        Err(BlobDeclError::WriteOnceBlobRepointed { blob_id, .. })
            if blob_id == "blob-b"
    ));
}

#[test]
fn metadata_publication_captures_an_unchanged_non_primary_key_blob() {
    for replacement in [BlobReplacement::Replaceable, BlobReplacement::WriteOnce] {
        let conn = Connection::open_in_memory().expect("open connection");
        conn.execute_batch(
            "CREATE TABLE files (
                 id TEXT PRIMARY KEY,
                 blob_id TEXT,
                 title TEXT NOT NULL,
                 size INTEGER NOT NULL,
                 hash TEXT NOT NULL,
                 cloud_path TEXT,
                 _updated_at TEXT NOT NULL
             );
             INSERT INTO files VALUES (
                 'row-a', 'blob-a', 'before', 17, 'hash-a', 'files/blob-a', 'stamp-a'
             );",
        )
        .expect("create file row");
        let declaration = BlobDecl::new("files", Provenance::HostProvided, CacheFill::CacheEager)
            .with_id_column("blob_id")
            .with_cloud_path_column("cloud_path");
        let declaration = match replacement {
            BlobReplacement::Replaceable => declaration,
            BlobReplacement::WriteOnce => declaration.write_once(),
        };
        let declarations = BlobDecls::from_tables(
            &conn,
            &[SyncedTable::new("files", RowIdentity::IndependentUuid).carries_blob(declaration)],
        )
        .expect("resolve declarations");
        let bytes = capture_update_bytes(
            &conn,
            "UPDATE files SET title = 'after', _updated_at = 'stamp-b'",
        );
        let bytes = declarations
            .complete_blob_changeset(&conn, &bytes)
            .expect("preserve metadata update");
        let changes = crate::walk_changeset(&bytes).expect("decode metadata update");
        let [change] = changes.as_slice() else {
            panic!("expected metadata update")
        };
        assert_eq!(change.col(1), None, "SQLite omits the unchanged blob ID");
        assert!(!change.column_changed(1));

        let publication = declarations
            .publication_blob_from_change(&conn, change)
            .expect("capture metadata publication")
            .expect("unchanged blob remains part of the publication");
        assert_eq!(publication.table, "files");
        assert_eq!(publication.row_id, "row-a");
        assert_eq!(publication.row_stamp, "stamp-b");
        assert_eq!(publication.column, "blob_id");
        assert_eq!(publication.blob.id, "blob-a");
        assert_eq!(publication.blob.cloud_path.as_deref(), Some("files/blob-a"));
        assert_eq!(publication.plaintext_size, 17);
        assert_eq!(publication.plaintext_hash, "hash-a");
    }
}

#[test]
fn publication_capture_distinguishes_a_null_blob_from_an_omitted_id() {
    let conn = Connection::open_in_memory().expect("open connection");
    conn.execute_batch(
        "CREATE TABLE files (
             id TEXT PRIMARY KEY,
             blob_id TEXT,
             title TEXT NOT NULL,
             size INTEGER NOT NULL,
             hash TEXT NOT NULL,
             _updated_at TEXT NOT NULL
         );
         INSERT INTO files VALUES ('row-a', 'blob-a', 'before', 1, 'hash-a', 'stamp-a');",
    )
    .expect("create file row");
    let declarations = BlobDecls::from_tables(
        &conn,
        &[
            SyncedTable::new("files", RowIdentity::IndependentUuid).carries_blob(
                BlobDecl::new("files", Provenance::HostProvided, CacheFill::CacheEager)
                    .with_id_column("blob_id"),
            ),
        ],
    )
    .expect("resolve declarations");

    let cleared = capture_update(
        &conn,
        "UPDATE files SET blob_id = NULL, _updated_at = 'stamp-b'",
    );
    assert!(cleared.column_changed(1));
    assert!(declarations
        .publication_blob_from_change(&conn, &cleared)
        .expect("capture explicit null")
        .is_none());

    let metadata = capture_update(
        &conn,
        "UPDATE files SET title = 'after', _updated_at = 'stamp-c'",
    );
    assert!(!metadata.column_changed(1));
    assert!(declarations
        .publication_blob_from_change(&conn, &metadata)
        .expect("capture metadata on a row without a blob")
        .is_none());

    let introduced = capture_update(
        &conn,
        "UPDATE files SET blob_id = 'blob-b', _updated_at = 'stamp-d'",
    );
    conn.execute("UPDATE files SET blob_id = 'blob-c'", [])
        .expect("repoint transaction row after capture");
    assert!(matches!(
        declarations.publication_blob_from_change(&conn, &introduced),
        Err(BlobDeclError::PublicationBlobMismatch {
            changed_blob_id, row_blob_id, ..
        }) if changed_blob_id == "blob-b" && row_blob_id == "blob-c"
    ));
}

#[test]
fn completing_a_write_once_blob_edit_preserves_unchanged_identity() {
    for id_column in ["id", "blob_id"] {
        let conn = Connection::open_in_memory().expect("open declaration database");
        conn.execute_batch(
            "CREATE TABLE files (
                 id TEXT PRIMARY KEY,
                 blob_id TEXT NOT NULL,
                 size INTEGER NOT NULL,
                 hash TEXT NOT NULL,
                 path TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             );
             INSERT INTO files VALUES ('row-a', 'blob-a', 7, 'hash-a', 'before', 'stamp-a');",
        )
        .expect("create write-once blob");
        let declaration = BlobDecl::new("files", Provenance::HostProvided, CacheFill::CacheEager)
            .with_id_column(id_column)
            .with_cloud_path_column("path")
            .write_once();
        let declarations = BlobDecls::from_tables(
            &conn,
            &[SyncedTable::new("files", RowIdentity::SharedKey).carries_blob(declaration)],
        )
        .expect("resolve declaration");
        let raw = capture_update_bytes(
            &conn,
            "UPDATE files SET path = 'after', _updated_at = 'stamp-b'",
        );
        let bytes = declarations
            .complete_blob_changeset(&conn, &raw)
            .expect("capture complete tuple");
        let changes = crate::walk_changeset(&bytes).expect("decode complete tuple");
        let [change] = changes.as_slice() else {
            panic!("expected one update")
        };
        let id_index = if id_column == "id" { 0 } else { 1 };
        assert!(!change.column_changed(id_index));
        assert!(!change.column_changed(2));
        assert_eq!(change.col(2), Some("7"));
        assert!(change.column_changed(4));
        let captured = declarations
            .publication_blob_from_change(&conn, change)
            .expect("unchanged write-once identity is allowed")
            .expect("blob is present");
        assert_eq!(
            captured.blob.id,
            if id_column == "id" { "row-a" } else { "blob-a" }
        );
        assert_eq!(captured.blob.cloud_path.as_deref(), Some("after"));
        unsafe {
            crate::gate::for_each_change(&bytes, |iter, _| {
                let (old, new, _) = crate::gate::update_values(iter)?;
                assert_eq!(old[0], Some(rusqlite::types::Value::Text("row-a".into())));
                assert_eq!(new[0], None, "SQLite UPDATE primary key remains Old-only");
                assert_eq!(old[2], new[2]);
                Ok(())
            })
            .expect("inspect exact update cells");
        }
    }
}
