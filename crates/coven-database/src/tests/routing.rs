use crate::*;
use coven_protocol::blob::BLOB_TOMBSTONE_GRACE;

#[tokio::test]
async fn fresh_open_requires_each_make_remote_intent_to_name_retain_pinned() {
    let db = Database::open(
        Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &[],
    )
    .expect("open database");

    let column = db
        .test_sql(|conn| {
            let rows = conn
                .query("PRAGMA table_info(blob_make_remote_intents)", [], |row| {
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(DbError::from)?;
            for (name, notnull, default_value) in rows {
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
