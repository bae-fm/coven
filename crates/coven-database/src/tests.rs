#[path = "tests/coven_migration.rs"]
mod coven_migration;
#[path = "tests/fixtures.rs"]
mod fixtures;
#[path = "tests/history.rs"]
mod history;
#[path = "tests/open.rs"]
mod open;
#[path = "tests/remote_objects.rs"]
mod remote_objects;
#[path = "tests/remote_ownership.rs"]
mod remote_ownership;
#[path = "tests/routing.rs"]
mod routing;
#[path = "tests/schema_initialization.rs"]
mod schema_initialization;
#[path = "tests/scoped_audience_capture.rs"]
mod scoped_audience_capture;

#[test]
fn mapped_queries_reuse_their_prepared_statement() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use rusqlite::hooks::{AuthAction, Authorization};

    let connection = rusqlite::Connection::open_in_memory().expect("open database");
    connection
        .execute_batch("CREATE TABLE facts(value TEXT); INSERT INTO facts VALUES ('kept');")
        .expect("create facts");
    let preparations = Arc::new(AtomicUsize::new(0));
    let observed = preparations.clone();
    connection
        .authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
            if matches!(context.action, AuthAction::Select) {
                observed.fetch_add(1, Ordering::SeqCst);
            }
            Authorization::Allow
        }))
        .expect("install preparation observer");

    for _ in 0..2 {
        let values = super::query_mapped_rows(
            &connection,
            "SELECT value FROM facts ORDER BY value",
            [],
            |row| row.get::<_, String>(0),
        )
        .expect("query facts");
        assert_eq!(values, ["kept"]);
    }

    assert_eq!(
        preparations.load(Ordering::SeqCst),
        1,
        "the second identical query must reuse the prepared statement"
    );
}
