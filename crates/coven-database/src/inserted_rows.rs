//! The rows a local write inserts that removal need not judge (§9.3).
//!
//! SQLite judged each row the app's statements inserted: its CHECKs, its
//! unique values and its foreign keys, on the values the write's record then
//! holds. Only a row the app can't see could still take such a row out, so the
//! removal rules run for it only when one could. `unjudged` finds the rows
//! where none could; `proofs/merge.md` B9 (`fresh_rows_shown`) is why they show
//! and every other row keeps the removal it has without them.

use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::DbError;
use coven_format::value::Value;
use coven_merge::{Audience, RowId, RowUpdate};
use std::collections::{BTreeMap, BTreeSet};

/// The rows of `updates` that show whatever the removal rules would say, so
/// no rule is evaluated for them.
///
/// A row is one of these when the write inserts it, nothing the app can't see
/// competes with it, and nothing stored points at it:
///
/// - *present, passing its CHECKs, outside a deleted circle*: the write
///   inserts it, SQLite checked its values as the statement ran, and a write
///   into a deleted circle is refused when it is queued.
/// - *no rival for its key*: no merge row of its table and key exists in any
///   audience, so no row of any audience holds its key, taken out or not. A
///   row of the write with the same key is a stored row, which has one.
/// - *no rival for a unique value*: no taken-out row of its table claims a
///   unique value in its audience. SQLite's unique indexes include the
///   audience and leave no shown row with its values; a row of the write that
///   takes the same value is touched, and the recompute reaches this one
///   through the app's row.
/// - *nothing stored points at it*: a stored row naming it as a parent is a
///   row its arrival could bring back.
///
/// Its references are fresh without a check. The write's record names each
/// parent from the app's row, which the app shows. A parent the write changes
/// puts its children in the recompute's region, so this row is judged with
/// it. A parent the write doesn't change keeps the removal it had, under which
/// the app shows it.
pub(crate) fn unjudged(
    database: &DatabaseConnection,
    store: &MergeStore<'_>,
    updates: &BTreeMap<RowId, RowUpdate<Value>>,
) -> Result<BTreeSet<RowId>, DbError> {
    store.load_key_rows(updates.keys().map(|row| (row.table.as_str(), row.key.as_slice())))?;
    let mut claiming = BTreeMap::<(&str, &Audience), bool>::new();
    let mut rows = BTreeSet::new();
    for (id, update) in updates {
        if !update.state.present() || !store.key_rows(&id.table, &id.key)?.is_empty() {
            continue;
        }
        let claims = match claiming.get(&(id.table.as_str(), &id.audience)) {
            Some(claims) => *claims,
            None => {
                let claims = crate::row_queries::holds_claims(database, &id.table, &id.audience)?;
                claiming.insert((id.table.as_str(), &id.audience), claims);
                claims
            }
        };
        if !claims {
            rows.insert(id.clone());
        }
    }
    for pointed in crate::row_queries::pointed_at(database, &rows)? {
        rows.remove(&pointed);
    }
    Ok(rows)
}

#[cfg(test)]
#[path = "inserted_rows_tests.rs"]
mod tests;
