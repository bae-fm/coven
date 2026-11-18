//! `_coven_sync_state` and `_coven_upload_phase`, the SQL functions the
//! `coven_sync_state` and `coven_upload_phases` views call over the facts
//! they read (§17.9, §19.1, api.md E4).
//!
//! The sync state and each upload's phase are SQL functions so that an app's
//! screen reads them in its own live query, joined to its own rows, and the
//! query runs again whenever a fact they read changes. The computations stay
//! sync's Rust code (`Facts::state` and `upload_phase` in coven-sync), which
//! is the one definition of each: the Lean model's differential tests check
//! them, and `uploads()` gives each upload the same phase. Sync gives them to
//! the database as `SyncFunctions`, and the database registers them on every
//! connection with the functions coven's views call
//! (`coven_functions::register_views`).

use std::sync::Arc;

use sqlive::rusqlite::functions::{Context, FunctionFlags};
use sqlive::Setup;

use crate::sync_facts::{SyncFacts, UploadPhaseFacts};
use crate::DbError;

/// The sync state function's name, which only `coven_sync_state` calls.
const STATE: &str = "_coven_sync_state";

/// The upload phase function's name, which only `coven_upload_phases` calls.
const UPLOAD_PHASE: &str = "_coven_upload_phase";

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

/// One upload's phase as `coven_upload_phases` shows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadPhaseRow {
    /// An attempt is sending it now.
    Uploading,
    /// Storage holds its bytes; it waits for the rest of its move.
    Stored,
    /// It waits, for the wait this kind names in snake case: `queued`,
    /// `paused` and the rest.
    Waiting(&'static str),
    /// It has failed, and the pending record at this position in
    /// `coven_pending` says why.
    Failed(i64),
}

impl UploadPhaseRow {
    /// The row's columns: the phase's kind, what it waits for while it
    /// waits, and the record a failure names.
    fn columns(self) -> (&'static str, Option<&'static str>, Option<i64>) {
        match self {
            Self::Uploading => ("uploading", None, None),
            Self::Stored => ("stored", None, None),
            Self::Waiting(wait) => ("waiting", Some(wait), None),
            Self::Failed(pending) => ("failed", None, Some(pending)),
        }
    }
}

type ComputeState = dyn Fn(&Context<'_>) -> Result<SyncStateRow, DbError> + Send + Sync;

type ComputePhase = dyn Fn(&Context<'_>) -> Result<UploadPhaseRow, DbError> + Send + Sync;

/// Sync's computations of the sync state from its facts (`SyncFacts`) and of
/// an upload's phase from its own (`UploadPhaseFacts`), which the database
/// registers as `_coven_sync_state` and `_coven_upload_phase`. Clones share
/// them.
#[derive(Clone)]
pub struct SyncFunctions {
    state: Arc<ComputeState>,
    upload_phase: Arc<ComputePhase>,
}

impl SyncFunctions {
    /// The functions that decode their arguments as the facts, with the
    /// pending records as `P`, and compute the state with `state` and an
    /// upload's phase with `upload_phase`. Each computation must read only
    /// its facts and return at once: SQLite calls it inside a read, and coven
    /// runs a live query again only when a table the query reads changes.
    pub fn new<P: crate::PendingValue + Clone>(
        state: fn(SyncFacts<P>) -> Result<SyncStateRow, DbError>,
        upload_phase: fn(UploadPhaseFacts<P>) -> Result<UploadPhaseRow, DbError>,
    ) -> Self {
        Self {
            state: Arc::new(move |context| state(SyncFacts::decode(context)?)),
            upload_phase: Arc::new(move |context| upload_phase(UploadPhaseFacts::decode(context)?)),
        }
    }

    /// Register the functions on a connection to the store's database. Each
    /// result depends on its arguments alone, but coven marks neither
    /// deterministic: the results change with coven's code, so no index,
    /// CHECK or generated column may store one.
    pub(crate) fn register(
        &self,
        setup: &Setup<'_, crate::app_policy::CovenPolicy>,
    ) -> sqlive::rusqlite::Result<()> {
        let state = Arc::clone(&self.state);
        setup.scalar_function(
            STATE,
            crate::sync_facts::FACTS,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
            move |call| {
                let row = state(call.context())
                    .map_err(|error| sqlive::rusqlite::Error::UserFunctionError(Box::new(error)))?;
                Ok(serde_json::json!({
                    "state": row.state,
                    "pending": row.pending,
                    "detail": row.detail,
                })
                .to_string())
            },
        )?;
        let upload_phase = Arc::clone(&self.upload_phase);
        setup.scalar_function(
            UPLOAD_PHASE,
            crate::sync_facts::UPLOAD_PHASE_FACTS,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
            move |call| {
                let (phase, wait, pending) = upload_phase(call.context())
                    .map_err(|error| sqlive::rusqlite::Error::UserFunctionError(Box::new(error)))?
                    .columns();
                Ok(serde_json::json!({
                    "phase": phase,
                    "wait": wait,
                    "pending": pending,
                })
                .to_string())
            },
        )
    }
}
