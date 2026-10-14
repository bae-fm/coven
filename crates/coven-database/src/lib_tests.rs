use crate::{CovenError, DatabaseBuilder, DbError, Migration, SyncedTable};
use coven_foundation::{
    files::{StoreDir, StoreLayout},
    id_source::{IdSource, SequentialIds, StoreId},
};

pub(crate) struct TestStore {
    directory: StoreDir,
    _temporary: tempfile::TempDir,
}

/// The store name in every test store's settings; fixture creation entries use it.
pub(crate) const STORE_NAME: &str = "database tests";

/// The id of [`TestStore::new`]'s store, which fixture creation entries name.
pub(crate) fn store_id() -> StoreId {
    StoreId(SequentialIds::new().new_id())
}

/// The format crate's fixture entry, a creation naming [`TestStore::new`]'s store.
pub(crate) fn store_log() -> coven_format::store_log::StoreLogEntry {
    let mut entry = coven_format::test_utils::store_log();
    let coven_format::store_log::StoreChange::CreateStore { store, name, .. } = &mut entry.change
    else {
        unreachable!("the format fixture creates a store")
    };
    *store = store_id();
    *name = STORE_NAME.into();
    entry
}

impl TestStore {
    pub(crate) fn new() -> Self {
        Self::with_ids(&SequentialIds::new())
    }

    pub(crate) fn with_ids(ids: &dyn IdSource) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(temporary.path().to_owned());
        // Every test store is a device of the one fixture store that fixture
        // creation entries name. Drawing an id first keeps device ids unchanged.
        ids.new_id();
        let directory = layout
            .create_store_dir(store_id(), STORE_NAME, ids)
            .unwrap();
        Self {
            directory,
            _temporary: temporary,
        }
    }

    pub(crate) fn id(&self) -> StoreId {
        self.directory.id()
    }

    pub(crate) fn database_path(&self) -> std::path::PathBuf {
        self.directory.database_path()
    }

    /// Supply committed membership for tests of local writes, without replaying a log.
    pub(crate) fn join_circle(
        &self,
        database: &crate::Database,
        circle: coven_foundation::id_source::CircleId,
    ) {
        let device = self.directory.settings().unwrap().device_id;
        let keys = crate::store_log::tests::keys(1);
        database.inspect_writer(|db| {
            db.transaction(|db| {
                db.internal_execute(
                    "INSERT INTO _coven_members(member,sealing,role,removed,access)
                     VALUES(?1,?2,'admin',0,?3) ON CONFLICT(member) DO NOTHING",
                    (
                        keys.signing.to_bytes(),
                        keys.sealing.as_bytes(),
                        serde_json::to_vec(&coven_format::MemberAccess::S3AccessKey {
                            access_key_id: "fixture-access-key".into(),
                            principal: coven_format::S3Principal::Console,
                        })
                        .unwrap(),
                    ),
                )?;
                db.internal_execute(
                    "INSERT INTO _coven_devices(device,member,name,state)
                     VALUES(?1,?2,'Local device','active') ON CONFLICT(device) DO NOTHING",
                    (device.0.to_be_bytes(), keys.signing.to_bytes()),
                )?;
                db.internal_execute(
                    "INSERT INTO _coven_circles(circle,name)
                     VALUES(?1,'Circle') ON CONFLICT(circle) DO NOTHING",
                    [circle.to_string()],
                )?;
                db.internal_execute(
                    "INSERT INTO _coven_circle_members(circle,member)
                     SELECT ?1,member FROM _coven_devices WHERE device=?2
                     ON CONFLICT(circle,member) DO NOTHING",
                    (circle.to_string(), device.0.to_be_bytes()),
                )?;
                Ok(())
            })
            .unwrap();
        });
    }

    pub(crate) fn assert_writer_locked(&self) {
        assert!(matches!(
            self.directory.lock_exclusive(),
            Err(coven_foundation::files::StoreLockError::AlreadyOpen(_))
        ));
    }

    pub(crate) fn assert_writer_unlocked(&self) {
        drop(self.directory.lock_exclusive().unwrap());
    }

    pub(crate) fn builder(
        &self,
        tables: Vec<SyncedTable>,
        migrations: Vec<Migration>,
    ) -> DatabaseBuilder {
        DatabaseBuilder::new(self.directory.clone())
            .synced_tables(tables)
            .migrations(migrations)
    }

    pub(crate) async fn schema(
        &self,
        tables: Vec<SyncedTable>,
        sql: &'static str,
    ) -> Result<crate::Database, CovenError> {
        self.builder(tables, vec![Migration::sql(1, "schema", sql)])
            .open()
            .await
    }
}

pub(crate) fn database_error(error: CovenError) -> DbError {
    match error {
        CovenError::Database(error) => error,
        CovenError::Migration(crate::MigrationError::Failed { source, .. }) => *source,
        error => panic!("expected database error, received {error:?}"),
    }
}

#[tokio::test]
async fn required_builder_choices_are_typed() {
    let store = TestStore::new();
    for (builder, expected) in [
        (
            DatabaseBuilder::new(store.directory.clone()),
            "synced_tables",
        ),
        (
            DatabaseBuilder::new(store.directory.clone()).synced_tables(vec![]),
            "migrations",
        ),
    ] {
        assert!(
            matches!(builder.open().await.err().unwrap(), CovenError::MissingConfiguration { field } if field == expected)
        );
    }
}

/// Deliver an authenticated remote edit through the database's write boundary.
/// The local queue supplies the row identity and reference metadata the peer read.
pub(crate) async fn remote_update(
    database: &crate::Database,
    table: &str,
    id: &str,
    changes: &[(&str, coven_format::value::Value)],
) {
    use coven_merge::Operation;
    let records = crate::write::tests::records(database);
    let key =
        coven_format::key::encode_key(&[coven_format::value::Value::Text(id.into())]).unwrap();
    let rows: Vec<_> = records
        .iter()
        .flat_map(|r| &r.parts)
        .flat_map(|p| &p.rows)
        .filter(|r| r.row.table == table && r.row.key == key)
        .collect();
    let latest = rows.last().expect("locally authored row");
    let mut row = (*latest).clone();
    row.change.generation += u64::from(matches!(row.change.operation, Operation::Insert(_)));
    let mut columns = std::collections::BTreeMap::new();
    row.old.clear();
    for (name, value) in changes {
        let previous = rows
            .iter()
            .rev()
            .find_map(|r| match &r.change.operation {
                Operation::Insert(columns) | Operation::Update(columns) => columns.get(*name),
                Operation::Delete => None,
            })
            .expect("authored column");
        row.old.insert((*name).into(), previous.value.clone());
        let mut column = previous.clone();
        column.value = value.clone();
        columns.insert((*name).into(), column);
    }
    row.change.operation = Operation::Update(columns);
    remote_write(database, row).await;
}

pub(crate) async fn remote_write(database: &crate::Database, row: coven_format::write::RowChange) {
    use coven_format::write::{WriteHeader, WritePart, WriteRecord};
    let row = database.inspect_writer_schema(|db, schema| {
        let app = crate::write_rows::AppView::after(db, schema);
        let store = crate::merge_store::MergeStore::new(db, &app);
        crate::write_sources::authored(db, &store, row.row, row.change, row.old).unwrap()
    });
    let state = database.sync_state(vec![]).await.unwrap();
    let previous = crate::write::tests::records(database)
        .pop()
        .expect("local seed write")
        .header;
    let device = coven_foundation::id_source::DeviceId(
        state
            .positions
            .0
            .iter()
            .map(|p| p.device.0)
            .max()
            .unwrap()
            .checked_add(1)
            .unwrap(),
    );
    let timestamp = coven_merge::Timestamp::new(
        previous.timestamp.milliseconds() + state.positions.0.len() as u64,
        0,
        device,
    )
    .unwrap();
    let record = WriteRecord {
        header: WriteHeader {
            position: coven_merge::WriteId { device, number: 1 },
            timestamp,
            had_read: state.positions,
            store_log_read: state.store_log,
            schema_version: state.schema_version,
            disposition: coven_format::write::WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: row.row.audience.clone(),
            rows: vec![row],
            dismissals: vec![],
        }],
    };
    assert_eq!(
        database.apply_downloaded(record.into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
}

/// All app and coven tables, for whole-transaction rollback assertions.
pub(crate) fn contents(
    database: &crate::Database,
) -> std::collections::BTreeMap<String, Vec<Vec<crate::types::Value>>> {
    database.inspect_writer(|db| {
        let tables = db
            .query(
                "SELECT name FROM main.sqlite_schema WHERE type='table' ORDER BY name",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        tables
            .into_iter()
            .map(|table| {
                let values = db
                    .query(
                        &format!(
                            "SELECT * FROM main.{} ORDER BY 1",
                            crate::sql::identifier(&table)
                        ),
                        [],
                        |r| (0..r.as_ref().column_count()).map(|i| r.get(i)).collect(),
                    )
                    .unwrap();
                (table, values)
            })
            .collect()
    })
}

/// Seed an independent authored log, with no reads of other devices.
pub(crate) fn applied(database: &crate::Database, stamp: coven_merge::Timestamp, number: u64) {
    use coven_format::{merge_fields, value::WritePositions};
    database.inspect_writer(|db| {
        db.internal_execute(
            "INSERT INTO _coven_writes(timestamp,number,had_read) VALUES(?1,?2,?3)",
            crate::params![
                merge_fields::encode_timestamp(&stamp).unwrap(),
                number.to_be_bytes().as_slice(),
                merge_fields::encode_write_positions(&WritePositions(vec![])).unwrap(),
            ],
        )
        .unwrap();
        db.internal_execute("INSERT INTO _coven_positions(device,number) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET number=excluded.number",crate::params![stamp.device().0.to_be_bytes().as_slice(),number.to_be_bytes().as_slice()]).unwrap();
    });
}

pub(crate) fn lean_model(runner: &std::ffi::OsStr, input: serde_json::Value) -> serde_json::Value {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

pub(crate) fn write_positions(numbers: [u64; 3]) -> coven_format::value::WritePositions {
    use coven_format::value::WritePositions;
    use coven_foundation::id_source::DeviceId;
    use coven_merge::WriteId;
    WritePositions(
        numbers
            .into_iter()
            .enumerate()
            .filter(|(_, number)| *number > 0)
            .map(|(device, number)| WriteId {
                device: DeviceId(device as u64 + 1),
                number,
            })
            .collect(),
    )
}

/// Replace consumed-log metadata with independent logs through the given positions.
pub(crate) fn independent_logs(
    database: &crate::Database,
    passed: &coven_format::value::WritePositions,
) {
    database.inspect_writer(|db| {
        db.internal_execute("DELETE FROM _coven_positions", [])
            .unwrap();
        db.internal_execute("DELETE FROM _coven_writes", [])
            .unwrap();
    });
    // Seed only consumed writes from three independent authored logs.
    for p in &passed.0 {
        for number in 1..=p.number {
            applied(
                database,
                coven_merge::Timestamp::new(number, 0, p.device).unwrap(),
                number,
            );
        }
    }
}
