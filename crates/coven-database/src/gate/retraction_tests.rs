use super::outbound::gate_outbound;
use super::{partition_outbound, Gates, RoutingChanges};
use coven_foundation::changeset::ChangeOp;
use coven_protocol::circle::Audience;
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};
use rusqlite::session::Session;
use rusqlite::Connection;

fn capture_and_gate(connection: &Connection, tables: &[SyncedTable], statement: &str) -> Vec<u8> {
    let mut session = Session::new(connection).expect("create capture session");
    for table in tables {
        session.attach(Some(table.name())).expect("attach table");
    }
    connection
        .execute_batch(statement)
        .expect("execute captured write");
    let mut captured = Vec::new();
    session
        .changeset_strm(&mut captured)
        .expect("extract captured write");
    let gates = Gates::from_tables(connection, tables).expect("build gates");
    gate_outbound(connection, &captured, &gates).expect("gate captured write")
}

fn capture_store_partition(
    connection: &Connection,
    tables: &[SyncedTable],
    statement: &str,
) -> Vec<u8> {
    let mut session = Session::new(connection).expect("create capture session");
    for table in tables {
        session.attach(Some(table.name())).expect("attach table");
    }
    connection
        .execute_batch(statement)
        .expect("execute captured write");
    let mut captured = Vec::new();
    session
        .changeset_strm(&mut captured)
        .expect("extract captured write");
    let gates = Gates::from_tables(connection, tables).expect("build gates");
    partition_outbound(connection, &captured, &RoutingChanges::empty(), &gates)
        .expect("partition captured write")
        .partitions
        .into_iter()
        .find(|partition| partition.audience == Audience::Store)
        .expect("Store partition")
        .changeset
}

#[test]
fn withdrawal_delete_carries_the_pre_write_row() {
    let connection = Connection::open_in_memory().expect("open database");
    connection
        .execute_batch(
            "CREATE TABLE records (
                 id TEXT PRIMARY KEY,
                 body TEXT NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO records VALUES
                 ('record', 'public body', 1, '0000000001000-0000-test');",
        )
        .expect("create records");
    let tables = vec![SyncedTable::new("records", RowIdentity::SharedKey).gated_by("shared")];

    let bytes = capture_and_gate(
        &connection,
        &tables,
        "UPDATE records
         SET body = 'private body', shared = 0,
             _updated_at = '0000000002000-0000-test'
         WHERE id = 'record'",
    );
    let changes = crate::walk_changeset(&bytes).expect("walk Store partition");
    let deletion = changes
        .iter()
        .find(|change| change.table == "records" && change.pk() == Some("record"))
        .expect("withdrawal deletion");

    assert_eq!(deletion.op, ChangeOp::Delete);
    assert_eq!(deletion.col(1), Some("public body"));
    assert_eq!(
        connection
            .query_row("SELECT body FROM records WHERE id = 'record'", [], |row| {
                row.get::<_, String>(0)
            },)
            .expect("read private row"),
        "private body",
    );
}

#[test]
fn reparent_and_withdraw_does_not_retract_an_independent_private_root() {
    let connection = Connection::open_in_memory().expect("open database");
    connection
        .execute_batch(
            "CREATE TABLE containers (
                 id TEXT PRIMARY KEY,
                 body TEXT NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE records (
                 id TEXT PRIMARY KEY,
                 container_id TEXT NOT NULL REFERENCES containers(id),
                 body TEXT NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO containers VALUES
                 ('public-container', 'public', 1, '0000000001000-0000-test'),
                 ('private-container', 'private', 0, '0000000001000-0000-test');
             INSERT INTO records VALUES
                 ('record', 'public-container', 'public', 1, '0000000001000-0000-test');",
        )
        .expect("create records");
    let tables = vec![
        SyncedTable::new("containers", RowIdentity::SharedKey).gated_by("shared"),
        SyncedTable::new("records", RowIdentity::SharedKey).gated_by("shared"),
    ];

    let bytes = capture_and_gate(
        &connection,
        &tables,
        "UPDATE records
         SET container_id = 'private-container', shared = 0,
             _updated_at = '0000000002000-0000-test'
         WHERE id = 'record'",
    );
    let changes = crate::walk_changeset(&bytes).expect("walk Store partition");

    assert!(changes.iter().any(|change| {
        change.table == "records" && change.pk() == Some("record") && change.op == ChangeOp::Delete
    }));
    assert!(changes.iter().all(|change| {
        change.table != "containers" || change.pk() != Some("private-container")
    }));
}

#[test]
fn ancestor_withdrawal_carries_the_pre_write_row() {
    let connection = Connection::open_in_memory().expect("open database");
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE containers (
                 id TEXT PRIMARY KEY,
                 body TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE records (
                 id TEXT PRIMARY KEY,
                 container_id TEXT NOT NULL REFERENCES containers(id),
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO containers VALUES
                 ('container', 'public body', '0000000001000-0000-test');
             INSERT INTO records VALUES
                 ('record', 'container', 1, '0000000001000-0000-test');",
        )
        .expect("create records");
    let tables = vec![
        SyncedTable::new("containers", RowIdentity::SharedKey).gated_by_descendants(),
        SyncedTable::new("records", RowIdentity::SharedKey).gated_by("shared"),
    ];

    let bytes = capture_store_partition(
        &connection,
        &tables,
        "DELETE FROM records WHERE id = 'record';
         UPDATE containers
         SET body = 'private body', _updated_at = '0000000002000-0000-test'
         WHERE id = 'container';",
    );
    let changes = crate::walk_changeset(&bytes).expect("walk Store partition");
    let deletion = changes
        .iter()
        .find(|change| change.table == "containers" && change.pk() == Some("container"))
        .expect("ancestor withdrawal deletion");

    assert_eq!(deletion.op, ChangeOp::Delete);
    assert_eq!(deletion.col(1), Some("public body"));
    assert_eq!(
        connection
            .query_row(
                "SELECT body FROM containers WHERE id = 'container'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read private ancestor"),
        "private body",
    );
}
