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
