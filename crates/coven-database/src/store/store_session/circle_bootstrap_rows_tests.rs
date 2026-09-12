use std::collections::BTreeMap;

use rusqlite::types::Value;

use super::*;
use coven_protocol::circle::{CircleBootstrapRef, CircleId};
use coven_protocol::store_commit::{CommitFrontier, ObjectHash, SnapshotImageRef};
use coven_protocol::synced_schema::RowIdentity;

const HOST_SCHEMA: &str = "CREATE TABLE documents (
         id TEXT PRIMARY KEY,
         body TEXT,
         size INTEGER,
         weight REAL,
         data BLOB,
         _updated_at TEXT NOT NULL
     ) STRICT;";

fn declared_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("documents", RowIdentity::IndependentUuid)]
}

/// A database carrying the host schema every test here declares, at schema
/// version 7.
fn host_database() -> Connection {
    let connection = Connection::open_in_memory().expect("open bootstrap rows database");
    connection
        .execute_batch(HOST_SCHEMA)
        .expect("create the host schema");
    crate::apply_coven_schema(&connection).expect("create the Coven tables");
    connection
        .pragma_update(None, "user_version", 7)
        .expect("state the schema version");
    connection
}

fn reference_for(connection: &Connection, rows: &[u8]) -> CircleBootstrapRef {
    let image_hash = ObjectHash::digest(rows);
    CircleBootstrapRef {
        coverage: CommitFrontier(BTreeMap::new()),
        schema_version: connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read the schema version"),
        sync_routing_hash: crate::SyncRoutingContract::from_connection(
            connection,
            &declared_tables(),
        )
        .expect("read the routing contract")
        .hash(),
        image: SnapshotImageRef {
            image_hash,
            object: coven_protocol::objects::ExactObjectRef::new(
                coven_protocol::objects::ObjectSlot::logical(
                    "circle-bootstrap.changeset".to_string(),
                )
                .expect("bootstrap slot"),
                rows.len() as u64,
                image_hash,
            ),
        },
        blobs: Vec::new(),
    }
}

fn verify(
    receiver: &Connection,
    rows: &[u8],
    reference: &CircleBootstrapRef,
) -> Result<StagedCircleRows, SnapshotImageError> {
    verify_circle_bootstrap_rows(
        receiver,
        rows,
        reference,
        CircleId::from_bytes([3; 16]),
        &declared_tables(),
        None,
    )
}

fn document_rows(connection: &Connection) -> Vec<Vec<Value>> {
    let mut statement = connection
        .prepare("SELECT id, body, size, weight, data, _updated_at FROM documents ORDER BY id")
        .expect("read documents");
    let rows = statement
        .query_map([], |row| {
            (0..6)
                .map(|column| row.get::<_, Value>(column))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("map documents")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect documents");
    rows
}

/// One changeset stating `changes` against the host schema, built outside the
/// capture so a test can state what no honest capture would.
fn changeset(changes: impl FnOnce(&Connection)) -> Vec<u8> {
    let connection = Connection::open_in_memory().expect("open changeset source");
    connection
        .execute_batch(HOST_SCHEMA)
        .expect("create the changeset schema");
    connection
        .execute_batch(
            "CREATE TABLE strangers (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
        )
        .expect("create the undeclared table");
    connection
        .execute(
            "INSERT INTO documents VALUES ('01890a5d-ac96-774b-bcce-b302099c3f74',
             'body', NULL, NULL, NULL, '0000000001000-0000-owner')",
            [],
        )
        .expect("seed the changeset row");
    let mut session =
        rusqlite::session::Session::new(&connection).expect("open the changeset session");
    session.attach(None::<&str>).expect("attach every table");
    changes(&connection);
    let mut stated = Vec::new();
    session
        .changeset_strm(&mut stated)
        .expect("state the changeset");
    stated
}

#[test]
fn a_bootstrap_states_every_cell_type_byte_for_byte() {
    let source = host_database();
    source
        .execute(
            "INSERT INTO documents VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                "01890a5d-ac96-774b-bcce-b302099c3f74",
                "body text",
                42_i64,
                1.5_f64,
                vec![0_u8, 1, 255],
                "0000000001000-0000-owner",
            ],
        )
        .expect("insert a fully typed row");
    source
        .execute(
            "INSERT INTO documents VALUES (?1, NULL, NULL, NULL, NULL, ?2)",
            rusqlite::params![
                "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                "0000000001001-0000-owner",
            ],
        )
        .expect("insert an all-null row");
    let rows = crate::gate::full_state_rows(
        &source,
        &circle_projection_tables(&source, &declared_tables()).expect("projection tables"),
    )
    .expect("state the bootstrap rows");

    let receiver = host_database();
    let reference = reference_for(&receiver, &rows);
    let staged = verify(&receiver, &rows, &reference).expect("verify the bootstrap rows");

    assert_eq!(
        document_rows(&staged.scratch),
        document_rows(&source),
        "every cell arrives as the value it left as"
    );
}

#[test]
fn an_empty_circle_states_an_empty_bootstrap_that_installs_no_rows() {
    let source = host_database();
    let rows = crate::gate::full_state_rows(
        &source,
        &circle_projection_tables(&source, &declared_tables()).expect("projection tables"),
    )
    .expect("state the empty bootstrap");
    assert!(rows.is_empty(), "an empty Circle states no changes");

    let receiver = host_database();
    let reference = reference_for(&receiver, &rows);
    let staged = verify(&receiver, &rows, &reference).expect("verify the empty bootstrap");
    assert!(document_rows(&staged.scratch).is_empty());
}

#[test]
fn a_bootstrap_refuses_an_update() {
    let rows = changeset(|connection| {
        connection
            .execute(
                "UPDATE documents SET body = 'edited', _updated_at = '0000000002000-0000-owner'",
                [],
            )
            .expect("state an update");
    });
    let receiver = host_database();
    let reference = reference_for(&receiver, &rows);

    let error = verify(&receiver, &rows, &reference)
        .map(|_| ())
        .expect_err("an update is not a bootstrap");
    assert!(
        error
            .to_string()
            .contains("carries a non-insert change to documents"),
        "{error}"
    );
}

#[test]
fn a_bootstrap_refuses_a_delete() {
    let rows = changeset(|connection| {
        connection
            .execute("DELETE FROM documents", [])
            .expect("state a delete");
    });
    let receiver = host_database();
    let reference = reference_for(&receiver, &rows);

    let error = verify(&receiver, &rows, &reference)
        .map(|_| ())
        .expect_err("a delete is not a bootstrap");
    assert!(
        error
            .to_string()
            .contains("carries a non-insert change to documents"),
        "{error}"
    );
}

#[test]
fn a_bootstrap_refuses_an_undeclared_table() {
    let rows = changeset(|connection| {
        connection
            .execute(
                "INSERT INTO strangers VALUES ('stranger', '0000000001000-0000-owner')",
                [],
            )
            .expect("state an undeclared row");
    });
    let receiver = host_database();
    let reference = reference_for(&receiver, &rows);

    let error = verify(&receiver, &rows, &reference)
        .map(|_| ())
        .expect_err("an undeclared table is not projected");
    assert!(
        error
            .to_string()
            .contains("names undeclared table strangers"),
        "{error}"
    );
}

#[test]
fn a_bootstrap_refuses_a_repeated_row() {
    let stated = changeset(|connection| {
        connection
            .execute(
                "INSERT INTO documents VALUES ('2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7',
                 'body', NULL, NULL, NULL, '0000000001001-0000-owner')",
                [],
            )
            .expect("state an inserted row");
    });
    let mut rows = stated.clone();
    rows.extend_from_slice(&stated);
    let receiver = host_database();
    let reference = reference_for(&receiver, &rows);

    let error = verify(&receiver, &rows, &reference)
        .map(|_| ())
        .expect_err("a row is stated once");
    assert!(
        error
            .to_string()
            .contains("repeats row documents.2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7"),
        "{error}"
    );
}

#[test]
fn a_bootstrap_refuses_another_schema_version() {
    let receiver = host_database();
    let rows = crate::gate::full_state_rows(
        &receiver,
        &circle_projection_tables(&receiver, &declared_tables()).expect("projection tables"),
    )
    .expect("state the bootstrap rows");
    let mut reference = reference_for(&receiver, &rows);
    reference.schema_version = 6;

    let error = verify(&receiver, &rows, &reference)
        .map(|_| ())
        .expect_err("another schema version is not installable");
    assert!(
        error
            .to_string()
            .contains("Circle bootstrap schema is 6, this database is 7"),
        "{error}"
    );
}

#[test]
fn a_bootstrap_refuses_another_routing_contract() {
    let receiver = host_database();
    let rows = crate::gate::full_state_rows(
        &receiver,
        &circle_projection_tables(&receiver, &declared_tables()).expect("projection tables"),
    )
    .expect("state the bootstrap rows");
    let mut reference = reference_for(&receiver, &rows);
    reference.sync_routing_hash = ObjectHash::digest(b"another routing contract");

    let error = verify(&receiver, &rows, &reference)
        .map(|_| ())
        .expect_err("another routing contract is not installable");
    assert!(
        error
            .to_string()
            .contains("routing contract differs from its signed hash"),
        "{error}"
    );
}

#[test]
fn a_bootstrap_refuses_a_blob_its_reference_does_not_bind() {
    let declaration = coven_protocol::synced_schema::BlobDecl::new(
        "photos",
        coven_protocol::blob::Provenance::HostProvided,
        coven_protocol::blob::CacheFill::CacheEager,
    )
    .with_id_column("photo_id");
    let blob_tables =
        vec![SyncedTable::new("photos", RowIdentity::IndependentUuid).carries_blob(declaration)];
    let blob_database = || {
        let connection = Connection::open_in_memory().expect("open blob database");
        connection
            .execute_batch(
                "CREATE TABLE photos (
                     id TEXT PRIMARY KEY,
                     photo_id TEXT,
                     size INTEGER,
                     hash TEXT,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
            )
            .expect("create the blob schema");
        crate::apply_coven_schema(&connection).expect("create the Coven tables");
        connection
            .pragma_update(None, "user_version", 7)
            .expect("state the schema version");
        connection
    };

    let source = blob_database();
    source
        .execute(
            "INSERT INTO photos VALUES (?1, 'photo-a', 7, ?2, ?3)",
            rusqlite::params![
                "01890a5d-ac96-774b-bcce-b302099c3f74",
                "00".repeat(32),
                "0000000001000-0000-owner",
            ],
        )
        .expect("insert a blob-bearing row");
    let rows = crate::gate::full_state_rows(
        &source,
        &circle_projection_tables(&source, &blob_tables).expect("projection tables"),
    )
    .expect("state the bootstrap rows");

    let receiver = blob_database();
    let image_hash = ObjectHash::digest(&rows);
    let reference = CircleBootstrapRef {
        coverage: CommitFrontier(BTreeMap::new()),
        schema_version: 7,
        sync_routing_hash: crate::SyncRoutingContract::from_connection(&receiver, &blob_tables)
            .expect("read the routing contract")
            .hash(),
        image: SnapshotImageRef {
            image_hash,
            object: coven_protocol::objects::ExactObjectRef::new(
                coven_protocol::objects::ObjectSlot::logical(
                    "circle-bootstrap.changeset".to_string(),
                )
                .expect("bootstrap slot"),
                rows.len() as u64,
                image_hash,
            ),
        },
        blobs: Vec::new(),
    };

    let error = verify_circle_bootstrap_rows(
        &receiver,
        &rows,
        &reference,
        CircleId::from_bytes([3; 16]),
        &blob_tables,
        None,
    )
    .map(|_| ())
    .expect_err("a blob its reference does not bind is not installable");
    assert!(
        error
            .to_string()
            .contains("blob closure does not exactly cover its image rows"),
        "{error}"
    );
}

#[test]
fn an_incomplete_foreign_key_rolls_the_whole_install_back() {
    let related_tables = vec![
        SyncedTable::new("documents", RowIdentity::IndependentUuid),
        SyncedTable::new("paragraphs", RowIdentity::IndependentUuid),
    ];
    let related_schema = "CREATE TABLE documents (
             id TEXT PRIMARY KEY,
             _updated_at TEXT NOT NULL
         ) STRICT;
         CREATE TABLE paragraphs (
             id TEXT PRIMARY KEY,
             document_id TEXT NOT NULL REFERENCES documents(id),
             _updated_at TEXT NOT NULL
         ) STRICT;";
    let related_database = || {
        let connection = Connection::open_in_memory().expect("open related database");
        connection
            .execute_batch(related_schema)
            .expect("create the related schema");
        crate::apply_coven_schema(&connection).expect("create the Coven tables");
        connection
            .pragma_update(None, "foreign_keys", true)
            .expect("enforce foreign keys");
        connection
    };

    // A payload stating a paragraph whose document it leaves out.
    let source = related_database();
    source
        .pragma_update(None, "foreign_keys", false)
        .expect("stage an unreferenced child");
    source
        .execute(
            "INSERT INTO paragraphs VALUES ('2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7',
             '01890a5d-ac96-774b-bcce-b302099c3f74', '0000000001000-0000-owner')",
            [],
        )
        .expect("insert the orphaned paragraph");
    let rows = crate::gate::full_state_rows(
        &source,
        &circle_projection_tables(&source, &related_tables).expect("projection tables"),
    )
    .expect("state the bootstrap rows");

    let receiver = related_database();
    let staged = StagedCircleRows::stage(&receiver, &rows, &related_tables)
        .expect("stage the incomplete bootstrap rows");
    let transaction = receiver
        .unchecked_transaction()
        .expect("begin the install transaction");
    staged
        .install_on(
            &transaction,
            &related_tables,
            &crate::tests::fixtures::test_commit_ref(),
            CircleId::from_bytes([3; 16]),
            &reference_for(&receiver, &rows),
        )
        .expect("copy the rows under deferred foreign keys");

    let committed = transaction.commit();

    assert!(
        committed.is_err(),
        "an install whose foreign keys are incomplete cannot commit"
    );
    assert_eq!(
        receiver
            .query_row("SELECT COUNT(*) FROM paragraphs", [], |row| row
                .get::<_, i64>(0))
            .expect("count installed paragraphs"),
        0,
        "the refused install leaves no rows behind"
    );
}
