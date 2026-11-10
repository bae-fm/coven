//! What the person's retry would do if made now (§6.7, api.md E5), kept as a
//! table so the app's live queries and local triggers can follow it (§9.4),
//! as they follow `coven_sync_status`.
//!
//! Its one row holds what decides it: whether a started connection will
//! plan (`syncing`), whether a failure only new credentials, an update or a
//! reset can clear holds every pass (`held`), and when the provider cooldown
//! holding every pass ends (`cooling_until`, milliseconds since the Unix
//! epoch by the store's clock). `effect` is computed from them, so it never
//! disagrees with them. While sync is stopped no owner would write a
//! cooldown's end, so the row keeps none: the table refuses one. A held
//! failure stays across stopping and starting, as the connection's retry
//! state does. Opening writes the stopped row with nothing held, and each
//! change writes the row in the step that makes it, on the writer, where
//! the retry's own answer is decided from the same row
//! (`Database::change_retry_state`).
//!
//! Like the sync status, only the handle that holds the store's writer lock
//! reads it (`authorization.rs`): a read-only handle can't tell whether the
//! writer that last wrote it still runs.

use crate::stored_millis::{since_epoch, stored_time, Round};
use crate::DbError;
use std::time::SystemTime;

/// The retry effect table, which the app's triggers may follow.
pub(crate) const RETRY_EFFECT: &str = "coven_retry_effect";

/// The table and its one row, as every store holds them from its creation.
pub(super) const TABLE: &str = "CREATE TABLE coven_retry_effect (
        singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1),
        syncing INTEGER NOT NULL CHECK(syncing IN (0,1)),
        held INTEGER NOT NULL CHECK(held IN (0,1)),
        cooling_until INTEGER,
        effect TEXT NOT NULL GENERATED ALWAYS AS (CASE
            WHEN syncing=0 THEN 'not_syncing'
            WHEN held=1 THEN 'failed'
            WHEN cooling_until IS NOT NULL THEN 'cooling_down'
            ELSE 'pass' END) STORED,
        CHECK(syncing=1 OR cooling_until IS NULL)
    ) STRICT;
    INSERT INTO coven_retry_effect(singleton,syncing,held,cooling_until) VALUES(1,0,0,NULL);";

/// What decides the person's retry, as `coven_retry_effect` holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryState {
    /// Whether a started connection will plan, and the cooldown holding
    /// its passes.
    pub sync: RetrySync,
    /// A failure only new credentials, an update or a reset can clear
    /// holds every pass.
    pub held: bool,
}

/// Whether a started connection will plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetrySync {
    /// None will: a retry is answered at once that sync isn't running.
    Stopped,
    /// One will.
    Started {
        /// A provider cooldown holds every pass until then, by the store's
        /// clock, in whole milliseconds rounded up.
        cooling_until: Option<SystemTime>,
    },
}

impl RetryState {
    /// The state a store opens in: no connection plans yet, and its owner
    /// has found nothing holding a pass.
    pub(crate) const OPENED: Self = Self { sync: RetrySync::Stopped, held: false };
}

/// Milliseconds since the Unix epoch, rounded up so a stored end never comes
/// before the cooldown's own. A time before the epoch or past SQLite's
/// integers is an invariant failure.
fn millis(time: SystemTime) -> Result<i64, DbError> {
    since_epoch(time, Round::Up).ok_or(DbError::Invariant {
        what: "a cooldown ends between the Unix epoch and SQLite's last millisecond",
    })
}

/// The state as the row holds it.
pub(crate) fn read(db: &crate::sqlite::DatabaseConnection) -> Result<RetryState, DbError> {
    let (syncing, held, cooling_until) = db.query_row(
        "SELECT syncing,held,cooling_until FROM coven_retry_effect WHERE singleton=1",
        [],
        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?, row.get::<_, Option<i64>>(2)?)),
    )?;
    let sync = if syncing {
        RetrySync::Started { cooling_until: cooling_until.map(stored_time).transpose()? }
    } else {
        RetrySync::Stopped
    };
    Ok(RetryState { sync, held })
}

/// Write the state whole, leaving the row alone when it already says this.
/// The cooldown's end is stored rounded up to the millisecond.
pub(crate) fn save(
    db: &crate::sqlite::DatabaseConnection,
    state: RetryState,
) -> Result<(), DbError> {
    let held = state.held;
    let (syncing, cooling_until) = match state.sync {
        RetrySync::Stopped => (false, None),
        RetrySync::Started { cooling_until } => (true, cooling_until.map(millis).transpose()?),
    };
    db.internal_execute(
        "UPDATE coven_retry_effect SET syncing=?1,held=?2,cooling_until=?3
         WHERE singleton=1 AND (syncing IS NOT ?1 OR held IS NOT ?2 OR cooling_until IS NOT ?3)",
        rusqlite::params![syncing, held, cooling_until],
    )?;
    Ok(())
}

/// The state rounded as the row stores it, so a decision made from a state
/// before it is written matches one made from the row after.
pub(crate) fn stored(state: RetryState) -> Result<RetryState, DbError> {
    let sync = match state.sync {
        RetrySync::Stopped => RetrySync::Stopped,
        RetrySync::Started { cooling_until } => RetrySync::Started {
            cooling_until: cooling_until
                .map(|until| millis(until).and_then(stored_time))
                .transpose()?,
        },
    };
    Ok(RetryState { sync, ..state })
}

#[cfg(test)]
#[path = "internal_schema_retry_effect_tests.rs"]
mod tests;
