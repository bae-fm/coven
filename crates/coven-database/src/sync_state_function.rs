//! `_coven_sync_state`, the SQL function the `coven_sync_state` view calls
//! over the facts it reads (§19.1, api.md E4).
//!
//! The sync state is a SQL function so that an app's screen reads it in its
//! own live query, joined to its own rows, and the query runs again whenever
//! a fact the state reads changes. The computation stays sync's Rust code
//! (`Facts::state` in coven-sync), which is the one definition of the state:
//! the Lean model's differential test checks it, and each upload's phase
//! shares its logic. Sync gives it to the database as a `SyncStateFunction`,
//! and the database registers it on every connection with the functions
//! coven's views call (`coven_functions::register_views`).

use std::sync::Arc;

use sqlive::rusqlite::functions::{Context, FunctionFlags};
use sqlive::Setup;

use crate::sync_facts::SyncFacts;
use crate::DbError;

/// The function's name, which only `coven_sync_state` calls.
const NAME: &str = "_coven_sync_state";

/// The sync state as `coven_sync_state` shows it.
#[derive(Clone, Debug, PartialEq)]
pub struct SyncStateRow {
    /// The state's kind, in snake case: `synced`, `waiting_to_sync`, and the
    /// rest.
    pub state: &'static str,
    /// The pending record that names the state, by its position in
    /// `coven_pending`; none when the last pass names it, or nothing does.
    pub pending: Option<i64>,
    /// The whole state as sync encodes it: the activity, the wait, the
    /// cause and the rest.
    pub detail: serde_json::Value,
}

type Compute = dyn Fn(&Context<'_>) -> Result<SyncStateRow, DbError> + Send + Sync;

/// Sync's computation of the state from the facts (`SyncFacts`), which the
/// database registers as `_coven_sync_state`. Clones share it.
#[derive(Clone)]
pub struct SyncStateFunction {
    compute: Arc<Compute>,
}

impl SyncStateFunction {
    /// The function that decodes its arguments as the facts, with the
    /// pending records as `P`, and computes the state with `compute`. It must
    /// read only the facts and return at once: SQLite calls it inside a read,
    /// and coven runs a live query again only when a table the query reads
    /// changes.
    pub fn new<P: crate::PendingValue + Clone>(
        compute: fn(SyncFacts<P>) -> Result<SyncStateRow, DbError>,
    ) -> Self {
        Self { compute: Arc::new(move |context| compute(SyncFacts::decode(context)?)) }
    }

    /// Register the function on a connection to the store's database. Its
    /// result depends on its arguments alone, but SQLite isn't told so: it
    /// changes with coven's code, so no index, CHECK or generated column may
    /// keep one.
    pub(crate) fn register(
        &self,
        setup: &Setup<'_, crate::app_policy::CovenPolicy>,
    ) -> sqlive::rusqlite::Result<()> {
        let compute = Arc::clone(&self.compute);
        setup.scalar_function(
            NAME,
            crate::sync_facts::FACTS,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
            move |call| {
                let row = compute(call.context())
                    .map_err(|error| sqlive::rusqlite::Error::UserFunctionError(Box::new(error)))?;
                Ok(serde_json::json!({
                    "state": row.state,
                    "pending": row.pending,
                    "detail": row.detail,
                })
                .to_string())
            },
        )
    }
}
