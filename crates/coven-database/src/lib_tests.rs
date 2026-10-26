use crate::{CovenError, DatabaseBuilder, DbError, Migration, SyncedTable};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::{
    files::{StoreDir, StoreDirectories},
    id_source::{IdSource, SequentialIds, StoreId},
};

pub(crate) struct TestStore {
    directory: StoreDir,
    _temporary: tempfile::TempDir,
}

/// The store name chosen by fixture creation entries.
pub(crate) const STORE_NAME: &str = "database tests";

/// The id of [`TestStore::new`]'s store, which fixture creation entries name.
pub(crate) fn store_id() -> StoreId {
    StoreId(SequentialIds::new().new_id())
}

pub(crate) fn keys(seed: u8) -> coven_format::admin::MemberPublicKeys {
    coven_format::test_utils::keys_from_seed(seed).public_keys()
}
pub(crate) fn member(seed: u8) -> coven_foundation::id_source::MemberId {
    coven_foundation::id_source::MemberId(uuid::Uuid::from_bytes([seed; 16]))
}

impl TestStore {
    pub(crate) fn new() -> Self {
        Self::with_ids(&SequentialIds::new())
    }

    pub(crate) fn with_ids(ids: &dyn IdSource) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let layout = StoreDirectories::new(temporary.path().to_owned());
        // Every test store is a device of the one fixture store that fixture
        // creation entries name. Drawing an id first keeps device ids unchanged.
        ids.new_id();
        let directory = layout.create_store_dir(store_id(), ids).unwrap();
        Self {
            directory,
            _temporary: temporary,
        }
    }

    pub(crate) fn id(&self) -> StoreId {
        self.directory.id()
    }

    pub(crate) fn directory(&self) -> StoreDir {
        self.directory.clone()
    }

    pub(crate) fn database_path(&self) -> std::path::PathBuf {
        self.directory.database_path()
    }

    /// This store's device.
    pub(crate) fn device(&self) -> coven_foundation::id_source::DeviceId {
        self.directory.settings().unwrap().device_id
    }

    /// Record `device` as member `seed`'s, as an applied registration would,
    /// without replaying a log.
    pub(crate) fn register(
        database: &crate::Database,
        device: coven_foundation::id_source::DeviceId,
        seed: u8,
    ) {
        let keys = crate::tests::keys(seed);
        let member = crate::tests::member(seed);
        database.inspect_writer(|db| {
            db.transaction(|db| {
                db.internal_execute(
                    "INSERT INTO _coven_members(member,signing,sealing,role,removed,access)
                     VALUES(?1,?2,?3,'member',0,?4) ON CONFLICT(member) DO NOTHING",
                    (
                        member.0.as_bytes(),
                        keys.signing.as_bytes(),
                        keys.sealing.as_bytes(),
                        serde_json::to_vec(&coven_format::MemberAccess::S3AccessKey {
                            access_key_id: format!("fixture-key-{seed}"),
                        })
                        .unwrap(),
                    ),
                )?;
                db.internal_execute(
                    "INSERT INTO _coven_devices(device,member,name,state)
                     VALUES(?1,?2,'Device','active') ON CONFLICT(device) DO NOTHING",
                    (device.0.to_be_bytes(), member.0.as_bytes()),
                )?;
                Ok(())
            })
            .unwrap();
        });
    }

    /// Supply committed membership for tests of local writes, without replaying a log.
    pub(crate) fn join_circle(
        &self,
        database: &crate::Database,
        circle: coven_foundation::id_source::CircleId,
    ) {
        let device = self.directory.settings().unwrap().device_id;
        let keys = crate::tests::keys(1);
        let member = crate::tests::member(1);
        database.inspect_writer(|db| {
            db.transaction(|db| {
                db.internal_execute(
                    "INSERT INTO _coven_members(member,signing,sealing,role,removed,access)
                     VALUES(?1,?2,?3,'admin',0,?4) ON CONFLICT(member) DO NOTHING",
                    (
                        member.0.as_bytes(),
                        keys.signing.as_bytes(),
                        keys.sealing.as_bytes(),
                        serde_json::to_vec(&coven_format::MemberAccess::S3AccessKey {
                            access_key_id: "fixture-access-key".into(),
                        })
                        .unwrap(),
                    ),
                )?;
                db.internal_execute(
                    "INSERT INTO _coven_devices(device,member,name,state)
                     VALUES(?1,?2,'Local device','active') ON CONFLICT(device) DO NOTHING",
                    (device.0.to_be_bytes(), member.0.as_bytes()),
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
        self.builder(tables, vec![Migration::addition(1, "schema", sql)])
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
    assert_eq!(
        try_remote_update(database, table, id, changes)
            .await
            .unwrap(),
        crate::ApplyOutcome::Applied
    );
}

/// [`remote_update`], returning what applying it did.
pub(crate) async fn try_remote_update(
    database: &crate::Database,
    table: &str,
    id: &str,
    changes: &[(&str, coven_format::value::Value)],
) -> Result<crate::ApplyOutcome, DbError> {
    try_remote_update_at(database, table, id, changes, None).await
}

/// [`try_remote_update`] authored at `schema_version`, or at this database's
/// version when none is given.
pub(crate) async fn try_remote_update_at(
    database: &crate::Database,
    table: &str,
    id: &str,
    changes: &[(&str, coven_format::value::Value)],
    schema_version: Option<u32>,
) -> Result<crate::ApplyOutcome, DbError> {
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
    try_remote_write(database, row, schema_version).await
}

pub(crate) async fn remote_write(database: &crate::Database, row: coven_format::write::RowChange) {
    assert_eq!(
        try_remote_write(database, row, None).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
}

async fn try_remote_write(
    database: &crate::Database,
    row: coven_format::write::RowChange,
    schema_version: Option<u32>,
) -> Result<crate::ApplyOutcome, DbError> {
    use coven_format::write::{WriteHeader, WritePart, WriteRecord};
    let row = database.inspect_writer_schema(|db, schema| {
        let app = crate::write_rows::AppView::after(db, schema);
        let store = crate::merge_store::MergeStore::new(db, &app);
        crate::write_sources::authored(db, &store, row.row, row.change, row.old).unwrap()
    });
    let previous = crate::write::tests::records(database)
        .pop()
        .expect("local seed write")
        .header;
    for original in crate::write::tests::records(database) {
        database.receive(original.into()).await?;
    }
    let applied = database.applied_slot().await?;
    let device = coven_foundation::id_source::DeviceId(10_000 + applied);
    let timestamp =
        coven_merge::Timestamp::new(previous.timestamp.milliseconds() + applied + 1, 0, device)
            .unwrap();
    let record = WriteRecord {
        header: WriteHeader {
            position: coven_merge::WriteId { device, number: 1 },
            timestamp,
            read_through: applied,
            schema_version: schema_version.unwrap_or(database.schema_version().await?),
            disposition: coven_format::write::WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: row.row.audience.clone(),
            rows: vec![row],
            executions: vec![],
            dismissals: vec![],
        }],
    };
    database.receive(record.into()).await
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

/// Seed counted metadata for merge-unit fixtures.
pub(crate) fn applied(database: &crate::Database, stamp: coven_merge::Timestamp, number: u64) {
    database.inspect_writer(|db| {
        let slot = crate::sequence::applied(db).unwrap() + 1;
        db.internal_execute(
            "INSERT INTO _coven_writes(timestamp,number,read_through,slot) VALUES(?1,?2,?3,?4)",
            crate::params![
                coven_format::merge_fields::encode_timestamp(&stamp).unwrap(),
                number.to_be_bytes(),
                0u64.to_be_bytes(),
                slot.to_be_bytes()
            ],
        )
        .unwrap();
        crate::sequence::set_applied(db, slot).unwrap();
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

pub(crate) use crate::sequence::tests::*;
