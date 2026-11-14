use crate::raise_capture::tests::connect;
use crate::tests::database_error;
use crate::tests::TestStore;
use crate::write::tests::sql;
use crate::{
    Database, DbError, FileColumn, Migration, Namespace, OldWrites, Provenance, RowIdentity,
    SyncedTable,
};
use coven_foundation::files::FileLease;
use coven_foundation::files::StoreDirAccess;
use coven_foundation::id_source::CircleId;
use coven_merge::Audience;

const SCHEMA: &str = "
    CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, note TEXT NOT NULL, tally INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE shared(id TEXT NOT NULL PRIMARY KEY, label TEXT);
    CREATE TABLE local_notes(id TEXT NOT NULL PRIMARY KEY)";

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("shared", RowIdentity::SharedKey),
    ]
}

fn gifts() -> CircleId {
    CircleId(uuid::Uuid::from_u128(10))
}

fn garden() -> CircleId {
    CircleId(uuid::Uuid::from_u128(11))
}

fn initial() -> Migration {
    Migration::addition(1, "pins", SCHEMA)
}

fn breaking(statements: &'static str) -> Migration {
    Migration::breaking(2, "change", statements, OldWrites::Drop, tables())
}

/// A store at version 1 holding a pin on `note` in each of `circles`, and a
/// shared row, a local row.
async fn library(store: &TestStore, circles: &[CircleId]) {
    library_from(store, circles, initial, tables()).await
}

async fn library_from(
    store: &TestStore,
    circles: &[CircleId],
    initial: fn() -> Migration,
    tables: Vec<SyncedTable>,
) {
    let db = store.builder(tables, vec![initial()]).open().await.unwrap();
    connect(&db).await;
    for circle in circles {
        store.join_circle(&db, *circle);
    }
    for (position, circle) in circles.iter().enumerate() {
        let statement = format!(
            "INSERT INTO pins VALUES('00000000-0000-4000-8000-00000000000{position}','{}','song',0)",
            Audience::Circle(*circle)
        );
        db.write(move |context| {
            context.execute_batch(&statement)?;
            Ok(())
        })
        .await
        .unwrap();
    }
    sql(&db, "INSERT INTO shared VALUES('s','shared')").await.unwrap();
    sql(&db, "INSERT INTO local_notes VALUES('l')").await.unwrap();
    db.close().await.unwrap();
}

async fn migrate(store: &TestStore, migration: Migration) -> Result<Database, DbError> {
    match store.builder(tables(), vec![initial(), migration]).open().await {
        Ok(db) => Ok(db),
        Err(error) => Err(database_error(error)),
    }
}

fn reads_across(error: DbError) -> (String, Audience) {
    match error {
        DbError::MigrationReadsAcross { table, audience } => (table, audience),
        other => panic!("expected MigrationReadsAcross, received {other:?}"),
    }
}

/// The scratch file is gone whether the run commits or not.
fn no_scratch_file(store: &TestStore) {
    let path = store.directory().audience_database_path();
    assert!(!path.exists());
}

const COUNT_SAME_NOTE: &str =
    "UPDATE pins SET tally=(SELECT count(*) FROM pins AS other WHERE other.note=pins.note)";

#[tokio::test]
async fn counting_another_circles_pins_fails_on_a_device_reading_both() {
    let store = TestStore::new();
    library(&store, &[gifts(), garden()]).await;
    let (table, audience) =
        reads_across(migrate(&store, breaking(COUNT_SAME_NOTE)).await.err().unwrap());
    assert_eq!(table, "pins");
    assert!(matches!(audience, Audience::Circle(_)));
    no_scratch_file(&store);
    // Nothing committed: the library is still at version 1.
    let db = store.builder(tables(), vec![initial()]).open().await.unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_same_migration_passes_on_a_device_reading_one_circle() {
    let store = TestStore::new();
    library(&store, &[gifts()]).await;
    let db = migrate(&store, breaking(COUNT_SAME_NOTE)).await.unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 2);
    db.close().await.unwrap();
    no_scratch_file(&store);
}

#[tokio::test]
async fn filling_a_circle_row_from_a_store_row_passes() {
    let store = TestStore::new();
    library(&store, &[gifts(), garden()]).await;
    let db =
        migrate(&store, breaking("UPDATE pins SET note=(SELECT label FROM shared WHERE id='s')"))
            .await
            .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn filling_a_store_row_from_a_circle_row_fails() {
    let store = TestStore::new();
    library(&store, &[gifts()]).await;
    let error =
        migrate(&store, breaking("UPDATE shared SET label=(SELECT note FROM pins LIMIT 1)"))
            .await
            .err()
            .unwrap();
    assert_eq!(reads_across(error), ("shared".into(), Audience::Store));
}

#[tokio::test]
async fn filling_a_synced_row_from_a_local_table_fails() {
    let store = TestStore::new();
    library(&store, &[gifts()]).await;
    let error =
        migrate(&store, breaking("UPDATE shared SET label=(SELECT count(*) FROM local_notes)"))
            .await
            .err()
            .unwrap();
    assert_eq!(reads_across(error).0, "shared");
}

#[tokio::test]
async fn sql_that_answers_differently_each_run_fails() {
    let store = TestStore::new();
    library(&store, &[gifts()]).await;
    let error = migrate(&store, breaking("UPDATE pins SET tally=random()")).await.err().unwrap();
    assert_eq!(reads_across(error).0, "pins");
}

#[tokio::test]
async fn an_addition_never_runs_the_check() {
    let store = TestStore::new();
    library(&store, &[gifts(), garden()]).await;
    let addition = Migration::addition(2, "tags", "ALTER TABLE pins ADD COLUMN tag TEXT");
    let db = migrate(&store, addition).await.unwrap();
    db.close().await.unwrap();
    no_scratch_file(&store);
}

#[tokio::test]
async fn a_library_with_no_rows_and_a_store_not_yet_made_pass() {
    let store = TestStore::new();
    let db = store.builder(tables(), vec![initial()]).open().await.unwrap();
    db.close().await.unwrap();
    let db = migrate(&store, breaking(COUNT_SAME_NOTE)).await.unwrap();
    db.close().await.unwrap();
    no_scratch_file(&store);
}

#[tokio::test]
async fn a_synced_row_with_no_merge_record_fails_naming_its_table() {
    let store = TestStore::new();
    library(&store, &[gifts()]).await;
    let db = store.builder(tables(), vec![initial()]).open().await.unwrap();
    db.inspect_writer(|writer| {
        crate::write_commit::tests::plant_unrecorded_rows(writer, "shared").unwrap();
    });
    db.close().await.unwrap();
    let error = migrate(&store, breaking(COUNT_SAME_NOTE)).await.err().unwrap();
    assert!(matches!(&error, DbError::UnrecordedRow { table } if table == "shared"), "{error:?}");
    no_scratch_file(&store);
}

#[tokio::test]
async fn a_migration_reading_file_records_sees_its_audiences() {
    let store = TestStore::new();
    let declare = || {
        vec![SyncedTable::new("pins", RowIdentity::IndependentUuid)
            .audience_column("audience")
            .file_column(FileColumn::new("file"))]
    };
    let namespaces = || vec![Namespace::new("files", Provenance::UserProvided, 1 << 30)];
    let schema = "CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, file TEXT, tally INTEGER NOT NULL DEFAULT 0)";
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"original").unwrap();
    let db = store
        .builder(declare(), vec![Migration::addition(1, "pins", schema)])
        .namespaces(namespaces())
        .open()
        .await
        .unwrap();
    connect(&db).await;
    store.join_circle(&db, gifts());
    let prepared = crate::prepare_user_file(original.path(), &FileLease::for_test().await, |_| {})
        .await
        .unwrap();
    let circle = Audience::Circle(gifts()).to_string();
    db.write_as::<_, _, DbError>(move |sql| {
        let file = sql.put_user_file("files", prepared, crate::Place::Cloud)?;
        sql.execute(
            "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000000',?1,?2,0)",
            [circle, file.to_string()],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    db.close().await.unwrap();
    // The pin's tally counts file records: one in its circle, none in the store alone.
    let counting = Migration::breaking(
        2,
        "count",
        "UPDATE pins SET tally=(SELECT count(*) FROM coven_files)",
        OldWrites::Drop,
        declare(),
    );
    let db = match store
        .builder(declare(), vec![Migration::addition(1, "pins", schema), counting])
        .namespaces(namespaces())
        .open()
        .await
    {
        Ok(db) => db,
        Err(error) => panic!("{:?}", database_error(error)),
    };
    assert_eq!(db.schema_version().await.unwrap(), 2);
    db.close().await.unwrap();
}

/// A shared trigger, which writes a synced table, that changes a synced row
/// when a pin is inserted.
fn with_trigger() -> Migration {
    Migration::addition(
        1,
        "pins",
        Box::leak(
            format!(
                "{SCHEMA}; CREATE TRIGGER pin_marks_shared AFTER INSERT ON pins WHEN NOT coven_applying() BEGIN UPDATE shared SET label = label || '!' WHERE id = 's'; END"
            )
            .into_boxed_str(),
        ),
    )
}

#[tokio::test]
async fn an_app_trigger_does_not_fire_on_a_copied_row() {
    let store = TestStore::new();
    let shared = || {
        vec![
            SyncedTable::new("pins", RowIdentity::IndependentUuid)
                .audience_column("audience")
                .shared_trigger("pin_marks_shared"),
            SyncedTable::new("shared", RowIdentity::SharedKey),
        ]
    };
    library_from(&store, &[gifts(), garden()], with_trigger, shared()).await;
    let migration =
        Migration::breaking(2, "change", "UPDATE pins SET tally=1", OldWrites::Drop, shared());
    let db = match store.builder(shared(), vec![with_trigger(), migration]).open().await {
        Ok(db) => db,
        Err(error) => panic!("{:?}", database_error(error)),
    };
    assert_eq!(db.schema_version().await.unwrap(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_table_local_until_the_run_holds_its_rows_in_every_run() {
    let store = TestStore::new();
    library(&store, &[gifts()]).await;
    let mut synced = tables();
    synced.push(SyncedTable::new("local_notes", RowIdentity::SharedKey));
    let counting = Migration::breaking(
        2,
        "count",
        "UPDATE pins SET tally=(SELECT count(*) FROM local_notes)",
        OldWrites::Drop,
        tables(),
    );
    let db = match store.builder(synced, vec![initial(), counting]).open().await {
        Ok(db) => db,
        Err(error) => panic!("{:?}", database_error(error)),
    };
    assert_eq!(db.schema_version().await.unwrap(), 2);
    db.close().await.unwrap();
}

fn with_search_index() -> Migration {
    Migration::addition(
        1,
        "pins",
        Box::leak(
            format!("{SCHEMA}; CREATE VIRTUAL TABLE pin_search USING fts5(note)").into_boxed_str(),
        ),
    )
}

#[tokio::test]
async fn a_breaking_migration_runs_over_a_schema_with_a_full_text_index() {
    let store = TestStore::new();
    library_from(&store, &[gifts()], with_search_index, tables()).await;
    let db = match store
        .builder(tables(), vec![with_search_index(), breaking("UPDATE pins SET tally=1")])
        .open()
        .await
    {
        Ok(db) => db,
        Err(error) => panic!("{:?}", database_error(error)),
    };
    assert_eq!(db.schema_version().await.unwrap(), 2);
    db.close().await.unwrap();
    no_scratch_file(&store);
}
