//! The tables an app's local triggers may follow, for an app's test that
//! runs its own schema outside any store (api.md E4).

/// The SQL that makes each table the app's local triggers may follow, as
/// every store makes it, `coven_sync_status` and `coven_retry_effect` with
/// their one row. An app's test that runs its own schema on a connection it
/// opened itself, outside any store, runs these first, so its triggers on
/// them apply and fire as in a store. Nothing of coven's writes the tables
/// there: the test writes the rows coven would.
pub fn followed_tables_sql() -> impl Iterator<Item = &'static str> {
    crate::internal_schema::FOLLOWED.into_iter().map(|followed| followed.table)
}

#[cfg(test)]
#[path = "followed_tables_tests.rs"]
mod tests;
