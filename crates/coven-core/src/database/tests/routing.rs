use super::super::*;
use crate::blob::BLOB_TOMBSTONE_GRACE;

use super::fixtures::*;

#[tokio::test]
async fn fresh_open_requires_each_make_remote_intent_to_name_retain_pinned() {
    let (db, _stamper) = Database::open(
        Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::MergeConcurrent,
        "test-device".to_string(),
        &[],
    )
    .expect("open database");

    let column = db
        .call(|conn| {
            let mut stmt = conn
                .prepare("PRAGMA table_info(blob_make_remote_intents)")
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(DbError::from)?;
            for row in rows {
                let (name, notnull, default_value) = row.map_err(DbError::from)?;
                if name == "retain_pinned" {
                    return Ok(Some((notnull, default_value)));
                }
            }
            Ok(None)
        })
        .await
        .expect("read make_remote intent schema")
        .expect("retain_pinned column exists");

    assert_eq!(column.0, 1, "retain_pinned must be NOT NULL");
    assert_eq!(
        column.1, None,
        "retain_pinned must be supplied by every make_remote intent",
    );
}

#[tokio::test]
async fn serial_pending_branch_survives_reopen_with_exact_base_and_inverses() {
    let temp = tempfile::tempdir().expect("temporary Store");
    let path = temp.path().join("serial.db");
    let tables = vec![SyncedTable::new(
        "notes",
        crate::sync::session::RowIdentity::SharedKey,
    )];
    let migrations = vec![notes_migration()];
    let (db, _) = Database::open(
        &path,
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::Serial,
        "serial-device".to_string(),
        &migrations,
    )
    .expect("open serial Store");
    for (write_id, sql) in [
        (
            "serial-write-1",
            "INSERT INTO notes VALUES ('n1', 'first', '0000000001000-0000-serial')",
        ),
        (
            "serial-write-2",
            "UPDATE notes SET body = 'second', _updated_at = '0000000002000-0000-serial' WHERE id = 'n1'",
        ),
    ] {
        let tables = tables.clone();
        let write_id = WriteId::from_generated(write_id.to_string());
        db.call(move |conn| {
            Database::run_internal_store_write_transaction_on(
                conn,
                &tables,
                crate::WritePolicy::Serial,
                None,
                write_id,
                |tx| tx.execute_batch(sql).map_err(DbError::from),
            )
        })
        .await
        .expect("commit provisional serial write");
    }
    drop(db);

    let (reopened, _) = Database::open(
        &path,
        tables,
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::Serial,
        "serial-device".to_string(),
        &migrations,
    )
    .expect("reopen serial Store");
    let rows = reopened
        .call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT write_id, inverse_changeset, base
                     FROM store_writes ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            Ok(rows)
        })
        .await
        .expect("read reopened branch");
    assert_eq!(rows.len(), 2);
    for (write_id, inverse, base) in rows {
        assert!(!inverse.is_empty(), "{write_id} retains its inverse");
        let base: StoreWriteBase = serde_json::from_str(&base).expect("serial base");
        assert_eq!(
            base,
            StoreWriteBase::Serial {
                branch_id: PendingBranchId::from_first_write(WriteId::from_generated(
                    "serial-write-1".to_string(),
                )),
                base: None,
            }
        );
    }
}

async fn capture_scoped_write_then_reopen(
    policy: WritePolicy,
    name: &str,
) -> (
    tempfile::TempDir,
    Database,
    Vec<(String, Option<String>, Vec<u8>)>,
) {
    let temp = tempfile::tempdir().expect("temporary scoped Store");
    let path = temp.path().join(format!("{name}.db"));
    let tables = vec![
        SyncedTable::new("accounts", crate::sync::session::RowIdentity::SharedKey)
            .scoped_by("audience"),
    ];
    let migrations = vec![Migration::sql(
        1,
        "accounts",
        "CREATE TABLE accounts (
            id TEXT PRIMARY KEY,
            audience TEXT,
            _updated_at TEXT NOT NULL
         ) STRICT;",
    )];
    let (db, _) = Database::open(
        &path,
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        policy,
        format!("{name}-device"),
        &migrations,
    )
    .expect("open scoped Store");
    crate::sync::test_helpers::TestStore::create(&db, name, crate::keys::UserKeypair::generate())
        .await
        .expect("install exact scoped Store authority");
    let circle_label = format!("{name}-circle");
    let (circle_id, _control) = db
        .call(move |conn| {
            Ok(crate::sync::test_helpers::install_test_active_circle(
                conn,
                &circle_label,
                policy,
            ))
        })
        .await
        .expect("install active Circle current state");
    let circle_id = circle_id.to_string();

    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let write_id = db.new_write_id();
    let routing =
        (policy == WritePolicy::MergeConcurrent).then(|| EncryptionService::from_key([7; 32]));
    let capture_tables = tables.clone();
    let capture_circle_id = circle_id.clone();
    db.call(move |conn| {
        Database::run_store_write_transaction_on(
            conn,
            &capture_tables,
            &gates,
            &blob_decls,
            policy,
            routing.as_ref(),
            write_id,
            |tx| {
                tx.execute(
                    "INSERT INTO accounts (id, audience, _updated_at)
                     VALUES ('store-account', NULL, '0000000001000-0000-restart')",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO accounts (id, audience, _updated_at)
                     VALUES ('circle-account', ?1, '0000000001001-0000-restart')",
                    [&capture_circle_id],
                )?;
                tx.execute(
                    "INSERT INTO accounts (id, audience, _updated_at)
                     VALUES ('local-account', 'local', '0000000001002-0000-restart')",
                    [],
                )?;
                Ok::<_, DbError>(())
            },
        )
    })
    .await
    .expect("capture Store and Circle partitions");
    let expected = db
        .call(|conn| {
            conn.prepare(
                "SELECT audience, control_coord, changeset
                 FROM store_write_partitions
                 ORDER BY CASE audience WHEN 'store' THEN 0 WHEN 'local' THEN 2 ELSE 1 END,
                          audience, control_coord",
            )
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(DbError::from)
        })
        .await
        .expect("read exact persisted audience partitions");
    assert_eq!(expected.len(), 3);
    assert!(expected.iter().any(|(audience, control, changeset)| {
        audience == "local" && control.is_none() && !changeset.is_empty()
    }));
    drop(db);

    let (reopened, _) = Database::open(
        &path,
        tables,
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        policy,
        format!("{name}-device"),
        &migrations,
    )
    .expect("reopen scoped Store");
    (temp, reopened, expected)
}

fn assert_prepared_partitions(
    actual: &PreparedStoreWritePartitions,
    expected: &[(String, Option<String>, Vec<u8>)],
) {
    let actual = actual
        .iter()
        .map(|partition| {
            let audience = match partition.audience {
                crate::sync::circle::Audience::Store => "store".to_string(),
                crate::sync::circle::Audience::Circle(circle) => circle.to_string(),
                crate::sync::circle::Audience::Local => "local".to_string(),
            };
            (
                audience,
                partition.control.as_ref().map(|control| {
                    let parsed =
                        serde_json::from_str(control.stored_json()).expect("parse stored control");
                    assert_eq!(control.coordinate(), &parsed);
                    control.stored_json().to_string()
                }),
                partition.changeset.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn merge_preparation_reloads_exact_scoped_partitions_after_restart() {
    let (_temp, reopened, expected) =
        capture_scoped_write_then_reopen(WritePolicy::MergeConcurrent, "merge-restart").await;
    let prepared = reopened
        .prepare_store_write()
        .await
        .expect("prepare restarted Merge write")
        .expect("pending Merge write");

    assert_prepared_partitions(&prepared.partitions, &expected);
}

#[tokio::test]
async fn serial_preparation_reloads_exact_scoped_partitions_after_restart() {
    let (_temp, reopened, expected) =
        capture_scoped_write_then_reopen(WritePolicy::Serial, "serial-restart").await;
    let branch = reopened
        .reserve_serial_store_branch()
        .await
        .expect("reserve restarted Serial branch")
        .expect("pending Serial branch");
    assert_eq!(branch.writes.len(), 1);

    assert_prepared_partitions(&branch.writes[0].partitions, &expected);
}

#[tokio::test]
async fn preparation_rejects_a_local_partition_with_circle_control() {
    let (_temp, reopened, _expected) =
        capture_scoped_write_then_reopen(WritePolicy::MergeConcurrent, "controlled-local-restart")
            .await;
    reopened
        .call(|conn| {
            conn.pragma_update(None, "ignore_check_constraints", true)
                .map_err(DbError::from)?;
            conn.execute(
                "UPDATE store_write_partitions
                 SET control_coord = '{}'
                 WHERE audience = 'local'",
                [],
            )
            .map_err(DbError::from)?;
            conn.pragma_update(None, "ignore_check_constraints", false)
                .map_err(DbError::from)
        })
        .await
        .expect("plant controlled Local partition");

    let error = match reopened.prepare_store_write().await {
        Err(error) => error,
        Ok(_) => panic!("controlled Local partition must fail preparation"),
    };
    assert!(error
        .to_string()
        .contains("Local partition carries a Circle control"));
}

async fn assert_local_only_scoped_write(policy: WritePolicy, name: &str) {
    let (_temp, db, _) = capture_scoped_write_then_reopen(policy, name).await;
    let tables = vec![
        SyncedTable::new("accounts", crate::sync::session::RowIdentity::SharedKey)
            .scoped_by("audience"),
    ];
    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let write_id = db.new_write_id();
    let routing =
        (policy == WritePolicy::MergeConcurrent).then(|| EncryptionService::from_key([7; 32]));
    let receipt = db
        .call(move |conn| {
            Database::run_store_write_transaction_on(
                conn,
                &tables,
                &gates,
                &blob_decls,
                policy,
                routing.as_ref(),
                write_id,
                |tx| {
                    tx.execute(
                        "INSERT INTO accounts (id, audience, _updated_at)
                         VALUES ('second-local-account', 'local', '0000000002000-0000-local')",
                        [],
                    )?;
                    Ok::<_, DbError>(())
                },
            )
        })
        .await
        .expect("capture local-only partition");

    assert_eq!(receipt.status, WriteStatus::LocalOnly);
    assert_eq!(receipt.pending_branch_id, None);
    assert_eq!(
        db.write_status(&receipt.write_id)
            .await
            .expect("reload local-only status"),
        WriteStatus::LocalOnly
    );
    assert!(db
        .pending_writes()
        .await
        .expect("list pending writes")
        .iter()
        .all(|write| write.write_id != receipt.write_id));
    let stored_write_id = receipt.write_id.clone();
    db.call(move |conn| {
        let (affected_rows, store_changeset): (String, Vec<u8>) = conn
            .query_row(
                "SELECT affected_rows, changeset FROM store_writes WHERE write_id = ?1",
                [stored_write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        assert_eq!(affected_rows, "[]");
        assert!(store_changeset.is_empty());
        let partition: (String, Option<String>, Vec<u8>) = conn
            .query_row(
                "SELECT audience, control_coord, changeset
                 FROM store_write_partitions WHERE write_id = ?1",
                [stored_write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)?;
        assert_eq!(partition.0, "local");
        assert_eq!(partition.1, None);
        assert!(!partition.2.is_empty());
        Ok(())
    })
    .await
    .expect("verify durable local-only journal");
}

#[tokio::test]
async fn merge_local_only_scoped_write_is_not_pending() {
    assert_local_only_scoped_write(WritePolicy::MergeConcurrent, "merge-local-only").await;
}

#[tokio::test]
async fn serial_local_only_scoped_write_is_not_pending() {
    assert_local_only_scoped_write(WritePolicy::Serial, "serial-local-only").await;
}
