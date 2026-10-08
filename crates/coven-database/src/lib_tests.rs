use crate::{CovenError, DatabaseBuilder, DbError, Migration, SyncedTable};
use coven_foundation::{
    files::{StoreDir, StoreLayout},
    id_source::{IdSource, SequentialIds, StoreId},
};

pub(crate) struct TestStore {
    directory: StoreDir,
    _temporary: tempfile::TempDir,
}

impl TestStore {
    pub(crate) fn new() -> Self {
        Self::with_ids(&SequentialIds::new())
    }

    pub(crate) fn with_ids(ids: &dyn IdSource) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(temporary.path().to_owned());
        let directory = layout
            .create_store_dir(StoreId(ids.new_id()), "database tests", ids)
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
