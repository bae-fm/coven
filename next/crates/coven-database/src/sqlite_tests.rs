use super::*;
use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::TestStore,
};

impl DatabaseConnection {
    pub(crate) fn integrity_checks(&self) -> usize {
        self.authorization.integrity_checks()
    }

    pub(crate) fn internal_execute<P: Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<usize, DbError> {
        let _scope = self.authorization.internal();
        self.connection.execute(sql, params).map_err(Into::into)
    }
}

#[tokio::test]
async fn damaged_files_fail_before_migration_or_wal_changes() {
    let store = TestStore::new();
    let bytes = vec![0xaa; 8192];
    std::fs::write(store.database_path(), &bytes).unwrap();
    for read_only in [false, true] {
        let builder = store.builder(notes_tables(), notes_migrations());
        let result = if read_only {
            builder.open_read_only().await
        } else {
            builder.open().await
        };
        assert!(matches!(
            result.err().unwrap(),
            crate::CovenError::Database(DbError::DamagedDatabase)
        ));
        assert_eq!(std::fs::read(store.database_path()).unwrap(), bytes);
    }
}

#[tokio::test]
async fn integrity_check_notices_btree_damage_in_a_valid_database_file() {
    use std::io::{Seek, SeekFrom, Write};
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let (page, page_size) = db.inspect_writer(|sql| {
        (
            sql.query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name='notes'",
                [],
                |r| r.get::<_, u32>(0),
            )
            .unwrap(),
            sql.query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))
                .unwrap(),
        )
    });
    db.close().await.unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(store.database_path())
        .unwrap();
    file.seek(SeekFrom::Start(u64::from(page - 1) * u64::from(page_size)))
        .unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    let error = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        crate::CovenError::Database(DbError::DamagedDatabase)
    ));
}

#[tokio::test]
async fn internal_schema_migrations_obey_policy_and_roll_back_on_failure() {
    let store = TestStore::new();
    let error = store
        .builder(vec![], vec![])
        .coven_migration_policy(CovenMigrationPolicy::RefusePending)
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        crate::CovenError::CovenMigration(CovenMigrationError::Pending)
    ));
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch("CREATE TABLE coven_columns(sentinel TEXT)")
        .unwrap();
    drop(raw);
    let error = store.builder(vec![], vec![]).open().await.err().unwrap();
    assert!(matches!(
        error,
        crate::CovenError::CovenMigration(CovenMigrationError::Failed { .. })
    ));
    let raw = Connection::open(store.database_path()).unwrap();
    assert_eq!(
        raw.query_row("PRAGMA application_id", [], |r| r.get::<_, i32>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        raw.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = 'coven_writes'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn readonly_and_older_coven_refuse_an_internal_version_they_cannot_use() {
    let store = TestStore::new();
    let raw = Connection::open(store.database_path()).unwrap();
    drop(raw);
    assert!(matches!(
        store
            .builder(vec![], vec![])
            .open_read_only()
            .await
            .err()
            .unwrap(),
        crate::CovenError::CovenMigration(CovenMigrationError::Pending)
    ));
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.close().await.unwrap();
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch("PRAGMA application_id=2").unwrap();
    drop(raw);
    assert!(matches!(
        store.builder(vec![], vec![]).open().await.err().unwrap(),
        crate::CovenError::CovenMigration(CovenMigrationError::Pending)
    ));
}

#[tokio::test]
async fn bundled_sqlite_has_the_session_extension() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_writer(|sql| {
        assert!(sql.query_row("SELECT sqlite_compileoption_used('ENABLE_SESSION') AND sqlite_compileoption_used('ENABLE_PREUPDATE_HOOK')", [], |r| r.get::<_, bool>(0)).unwrap());
    });
    db.close().await.unwrap();
}

#[test]
fn wal_refusal_names_the_selected_journal_mode() {
    let sql =
        DatabaseConnection::open(Path::new(":memory:"), false, SqlAuthorization::new(&[])).unwrap();
    assert!(matches!(sql.enable_wal(), Err(DbError::WalUnavailable { mode }) if mode == "memory"));
}
