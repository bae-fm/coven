use super::*;
use coven_protocol::blob::BLOB_TOMBSTONE_GRACE;

fn open_image_database(path: &Path) -> Database {
    Database::open(
        path,
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "image-replacement".to_string(),
        Arc::new(coven_foundation::clock::SystemClock),
        CovenMigrationPolicy::ApplyPending,
        &[Migration::sql(
            1,
            "image-value",
            "CREATE TABLE image_value (value TEXT NOT NULL)",
        )],
    )
    .expect("open image test database")
}

#[tokio::test]
async fn replacing_a_database_image_preserves_the_backing_file() {
    let directory = tempfile::tempdir().expect("image replacement directory");
    let path = directory.path().join("database.sqlite3");
    let source = open_image_database(Path::new(":memory:"));
    source
        .execute_test_sql("INSERT INTO image_value VALUES ('replacement')")
        .await;
    let image = source
        .database_image_for_test()
        .await
        .expect("capture replacement image");
    let destination = open_image_database(&path);
    destination
        .execute_test_sql("INSERT INTO image_value VALUES ('original')")
        .await;

    destination
        .replace_with_database_image_for_test(image)
        .await
        .expect("replace database image");
    assert_eq!(
        destination
            .query_test_text("SELECT value FROM image_value")
            .await,
        "replacement"
    );
    destination
        .execute_test_sql("UPDATE image_value SET value = 'after replacement'")
        .await;
    destination
        .replace_with_database_image_for_test(b"invalid image".to_vec())
        .await
        .expect_err("reject invalid image");
    assert_eq!(
        destination
            .query_test_text("SELECT value FROM image_value")
            .await,
        "after replacement"
    );
    std::thread::spawn(move || drop(destination))
        .join()
        .expect("close replaced database");

    let reopened = open_image_database(&path);
    assert_eq!(
        reopened
            .query_test_text("SELECT value FROM image_value")
            .await,
        "after replacement"
    );
}

#[tokio::test]
async fn a_locked_image_destination_preserves_its_rows_and_clock() {
    let directory = tempfile::tempdir().expect("locked image directory");
    let path = directory.path().join("database.sqlite3");
    let destination = open_image_database(&path);
    destination
        .execute_test_sql("INSERT INTO image_value VALUES ('original')")
        .await;
    let clock_before = destination.store_hlc_high_water();
    let source = open_image_database(Path::new(":memory:"));
    source
        .execute_test_sql("INSERT INTO image_value VALUES ('replacement')")
        .await;
    let seed = format!(
        "{:013}-0000-image",
        destination.store_receive_wall_ms() + 10_000
    );
    source.execute_test_sql(&format!(
        "INSERT OR REPLACE INTO protocol_state (key, value) VALUES ('{HIGHWATER_STATE_KEY}', '{seed}')"
    )).await;
    let image = source
        .database_image_for_test()
        .await
        .expect("capture image with newer clock");
    let blocker = Connection::open(&path).expect("open competing connection");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold destination write lock");

    let error = destination
        .replace_with_database_image_for_test(image.clone())
        .await
        .expect_err("a locked destination cannot commit the image");
    assert!(error.to_string().contains("Busy"), "{error}");
    assert_eq!(destination.store_hlc_high_water(), clock_before);
    assert_eq!(
        destination
            .query_test_text("SELECT value FROM image_value")
            .await,
        "original"
    );
    blocker
        .execute_batch("ROLLBACK")
        .expect("release destination lock");
    destination
        .replace_with_database_image_for_test(image)
        .await
        .expect("retry the same image after unlocking");
    assert_eq!(
        destination
            .query_test_text("SELECT value FROM image_value")
            .await,
        "replacement"
    );
    let imported = Timestamp::parse(&destination.store_hlc_high_water()).expect("imported clock");
    let source_floor = Timestamp::parse(&seed).expect("source clock");
    let original = Timestamp::parse(&clock_before).expect("original clock");
    assert_eq!(imported.millis, source_floor.millis);
    assert_eq!(imported.counter, source_floor.counter);
    assert_eq!(imported.device_id, original.device_id);
}

const IMAGE_ROWS_SCHEMA: &str = "CREATE TABLE image_rows (
    id TEXT PRIMARY KEY, value TEXT, shared INTEGER NOT NULL, _updated_at TEXT NOT NULL
) STRICT";

fn open_synced_image_database(schema: &'static str) -> Database {
    Database::open(
        Path::new(":memory:"),
        vec![SyncedTable::new(
            "image_rows",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared")],
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "image-schema".to_string(),
        Arc::new(coven_foundation::clock::SystemClock),
        CovenMigrationPolicy::ApplyPending,
        &[Migration::sql(1, "image-rows", schema)],
    )
    .expect("open synced image database")
}

#[tokio::test]
async fn an_image_with_the_same_schema_in_different_formatting_can_be_imported() {
    let source = open_synced_image_database(
        "CREATE TABLE image_rows (id TEXT PRIMARY KEY, value TEXT, shared INTEGER NOT NULL, _updated_at TEXT NOT NULL) STRICT",
    );
    let destination = open_synced_image_database(IMAGE_ROWS_SCHEMA);
    source
        .execute_test_sql(
            "INSERT INTO image_rows VALUES ('row', 'replacement', 0, '0000000001000-0000-image')",
        )
        .await;
    let image = source
        .database_image_for_test()
        .await
        .expect("capture image");
    destination
        .replace_with_database_image_for_test(image)
        .await
        .expect("accept equivalent schema");
    assert_eq!(
        destination
            .query_test_text("SELECT value FROM image_rows")
            .await,
        "replacement"
    );
}

#[tokio::test]
async fn an_image_cannot_change_columns_behind_the_live_gate_schema() {
    let source = open_synced_image_database(IMAGE_ROWS_SCHEMA);
    let destination = open_synced_image_database(IMAGE_ROWS_SCHEMA);
    destination
        .execute_test_sql(
            "INSERT INTO image_rows VALUES ('row', 'original', 0, '0000000001000-0000-image')",
        )
        .await;
    let clock_before = destination.store_hlc_high_water();
    source
        .execute_test_sql("ALTER TABLE image_rows ADD COLUMN ordinary TEXT")
        .await;
    let image = source
        .database_image_for_test()
        .await
        .expect("capture image with an ordinary column");
    assert_eq!(source.sync_routing_hash(), destination.sync_routing_hash());
    assert_eq!(source.schema_version(), destination.schema_version());

    let error = destination
        .replace_with_database_image_for_test(image)
        .await
        .expect_err("the live gate cannot use a different table schema");
    assert!(
        error.to_string().contains("changes host table image_rows"),
        "{error}"
    );
    assert_eq!(
        destination
            .query_test_text("SELECT value FROM image_rows")
            .await,
        "original"
    );
    assert_eq!(destination.store_hlc_high_water(), clock_before);
}

/// A SQL closure that blocks for a while must not stall other tasks on the
/// same runtime, because jobs run on the dedicated connection thread rather
/// than the async executor.
#[tokio::test]
async fn slow_db_call_does_not_block_the_executor() {
    use std::time::{Duration, Instant};

    let db = Database::open(
        Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "liveness".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        CovenMigrationPolicy::ApplyPending,
        &[],
    )
    .expect("open database");

    let slow_db = db.clone();
    let slow = tokio::spawn(async move {
        slow_db
            .call_database(|session| session.select_one_after_delay(Duration::from_millis(500)))
            .await
    });

    let start = Instant::now();
    tokio::task::yield_now().await;
    let stalled = start.elapsed();

    assert!(
        stalled < Duration::from_millis(250),
        "unrelated task stalled {stalled:?} behind the slow DB call — the executor was blocked",
    );

    let value = slow
        .await
        .expect("slow DB task joins")
        .expect("slow DB call succeeds");
    assert_eq!(value, 1, "the slow DB call still returns its result");
}

/// Dropping the last handle from inside a runtime task must not block that
/// task on the connection thread's queue, and a job already dispatched must
/// still run to completion.
#[tokio::test]
async fn dropping_last_handle_in_async_context_does_not_stall_but_job_still_lands() {
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("db.sqlite");
    let marker = dir.path().join("marker");

    let db = Database::open(
        &db_path,
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "drop-async".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        CovenMigrationPolicy::ApplyPending,
        &[],
    )
    .expect("open");

    let job_db = db.clone();
    let job_marker = marker.clone();
    let task = tokio::spawn(async move {
        let _ = job_db
            .call_database(move |_session| {
                std::thread::sleep(Duration::from_millis(300));
                std::fs::write(&job_marker, b"landed").map_err(DbError::from)
            })
            .await;
    });
    tokio::task::yield_now().await;
    task.abort();
    let _ = task.await;

    let drop_start = Instant::now();
    drop(db);
    let drop_elapsed = drop_start.elapsed();
    assert!(
        drop_elapsed < Duration::from_millis(200),
        "dropping the last handle stalled {drop_elapsed:?} — it joined the connection thread \
         instead of detaching",
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while !marker.exists() {
        assert!(
            Instant::now() < deadline,
            "the dispatched job's effect never landed after the last handle dropped",
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
