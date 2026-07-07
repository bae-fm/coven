//! Shared helpers for the wasm test modules.

use coven_core::database::{Database, DbError};

/// Run `sql` (one or more statements) as a single journaled transaction — the
/// same path a host write takes — so the write records into the
/// pending-changeset journal as it commits and a cycle pushes it.
pub(crate) async fn journaled_exec(db: &Database, sql: &str) -> Result<(), DbError> {
    let tables = db.synced_tables().to_vec();
    let sql = sql.to_string();
    db.call(move |conn| {
        Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
            tx.execute_batch(&sql).map_err(DbError::from)
        })
    })
    .await
}
