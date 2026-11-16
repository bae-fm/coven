//! Coven's connection to a store's databases: sqlive's, judged by coven's
//! policy, and coven's checks of what a statement changed.

#[path = "app_triggers.rs"]
mod app_triggers;
#[path = "rowid_policy.rs"]
mod rowid_policy;
pub(crate) use app_triggers::check_followed_triggers;
pub(crate) use rowid_policy::{protect_rowids, rowid_guard};

use crate::app_policy::CovenPolicy;
use crate::DbError;

/// A connection to one of a store's databases, judged by coven's policy.
pub(crate) type StoreSql = sqlive::HostSql<CovenPolicy>;

/// Refuse a statement that should change exactly one row and didn't.
pub(crate) fn changed_once(changed: usize) -> Result<(), DbError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(sqlive::rusqlite::Error::StatementChangedRows(changed).into())
    }
}

/// `value` as an SQLite integer, which holds up to `i64::MAX`; a larger
/// `field` is `TooLarge`.
pub(crate) fn integer(field: &'static str, value: u64) -> Result<i64, DbError> {
    i64::try_from(value).map_err(|_past_i64| DbError::TooLarge {
        field,
        actual: value,
        maximum: i64::MAX as u64,
    })
}

/// `total` plus `addend`; a sum past `u64::MAX` is `TotalTooLarge`, naming
/// the running total and the addend that did not fit.
pub(crate) fn add_total(field: &'static str, total: u64, addend: u64) -> Result<u64, DbError> {
    total.checked_add(addend).ok_or(DbError::TotalTooLarge { field, total, addend })
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
