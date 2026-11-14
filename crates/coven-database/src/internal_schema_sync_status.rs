//! What this installation's sync state is computed from that the sync loop
//! knows (§19.1, api.md E4, E5), kept as a table so the app's live queries
//! and local triggers can follow it (§9.4), as they follow `coven_pins`.
//!
//! It has one row from the store's creation on, which says no storage is set
//! up until the first open writes over it, so a read always finds one. Each
//! fact is written in the step that changes it: opening writes whether a
//! location is recorded and this device holds its credentials (`storage`,
//! with the location's `provider`) and that no pass moves; setup, joining
//! and restoring write storage set up and their finish as the last synced
//! time; each start of sync adds one to `starts`, so even a start that ends
//! the way the last one did writes the row; a store-wide worker's panic
//! halts sync until the store reopens (`halted`), after which no pass writes
//! the row; the loop writes what its pass moved last, a slot it read or one it claimed (`moved`) and how the
//! pass ended (`last_pass`, which sync encodes as JSON and the app reads as
//! `LastPass`), with a synced pass's finish as `last_synced`, in milliseconds
//! since the Unix epoch by the store's clock. That time never goes backward:
//! a write keeps the later of it and the new one, so a clock set back moves
//! nothing.
//!
//! Only the handle that holds the store's writer lock reads it: a read-only
//! handle can't tell whether that writer still runs, so app SQL there may not
//! read the table (`authorization.rs`).

use crate::DbError;
use std::time::SystemTime;

/// The sync status table, which the app's triggers may follow.
pub(crate) const SYNC_STATUS: &str = "coven_sync_status";

/// The table and its one row, as every store holds them from its creation.
pub(super) const TABLE: &str = "CREATE TABLE coven_sync_status (
        singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1),
        storage TEXT NOT NULL CHECK(storage IN ('none','signed_out','connected')),
        provider TEXT,
        last_pass BLOB,
        moved TEXT CHECK(moved IN ('read','claimed')),
        last_synced INTEGER CHECK(last_synced>=0),
        starts INTEGER NOT NULL CHECK(starts>=0),
        halted INTEGER NOT NULL CHECK(halted IN (0,1)),
        CHECK((storage='none')=(provider IS NULL))
    ) STRICT;
    INSERT INTO coven_sync_status(singleton,storage,provider,last_pass,moved,last_synced,starts,halted)
        VALUES(1,'none',NULL,NULL,NULL,NULL,0,0);";

/// Whether a storage location is recorded on this device, and whether this
/// device holds credentials for it (§19.1), with the location's provider.
/// The database keeps the provider as the name sync gives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordedLocation<P> {
    /// No location is recorded.
    None,
    /// A location is recorded, but this device holds no credentials for it.
    SignedOut(P),
    /// A location is recorded, and this device holds its credentials.
    Connected(P),
}

impl<P> RecordedLocation<P> {
    /// A location is recorded and this device holds its credentials.
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected(_))
    }

    /// The same location, with its provider as `name` gives it.
    pub fn map<Q>(self, name: impl FnOnce(P) -> Q) -> RecordedLocation<Q> {
        match self {
            Self::None => RecordedLocation::None,
            Self::SignedOut(provider) => RecordedLocation::SignedOut(name(provider)),
            Self::Connected(provider) => RecordedLocation::Connected(name(provider)),
        }
    }
}

/// What a running pass moved: a slot it read that it hadn't, or one it claimed
/// for a write (§19.1). The sync state shows syncing, receiving or sending.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassMove {
    /// The pass read a slot it hadn't.
    Read,
    /// The pass claimed a slot.
    Claimed,
}

impl PassMove {
    fn column(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Claimed => "claimed",
        }
    }

    fn from_column(column: &str) -> Result<Self, DbError> {
        match column {
            "read" => Ok(Self::Read),
            "claimed" => Ok(Self::Claimed),
            _ => Err(DbError::DamagedDatabase),
        }
    }
}

/// `coven_sync_status`'s row as it is read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncStatusRow {
    /// Whether a location is recorded and its credentials held.
    pub location: RecordedLocation<String>,
    /// How the last pass ended, as sync encodes it; none until a pass ends
    /// on this device.
    pub last_pass: Option<Vec<u8>>,
    /// What the running pass moved last; none while it has moved nothing.
    pub moved: Option<PassMove>,
    /// When a pass last reached the end of the sequence, or setup, joining
    /// or restoring finished, by the store's clock.
    pub last_synced: Option<SystemTime>,
}

/// Milliseconds since the Unix epoch, rounded down so a stored finish never
/// comes after the pass's own.
fn millis(time: SystemTime) -> Result<i64, DbError> {
    crate::stored_millis::since_epoch(time, crate::stored_millis::Round::Down).ok_or(
        DbError::Invariant {
            what: "a sync finishes between the Unix epoch and SQLite's last millisecond",
        },
    )
}

/// The `storage` and `provider` columns of `location`.
fn columns(location: &RecordedLocation<String>) -> (&'static str, Option<&str>) {
    match location {
        RecordedLocation::None => ("none", None),
        RecordedLocation::SignedOut(provider) => ("signed_out", Some(provider)),
        RecordedLocation::Connected(provider) => ("connected", Some(provider)),
    }
}

/// The row as it stands.
pub(crate) fn read(db: &crate::sqlite::DatabaseConnection) -> Result<SyncStatusRow, DbError> {
    let (storage, provider, last_pass, moved, last_synced) = db.query_row(
        "SELECT storage,provider,last_pass,moved,last_synced FROM coven_sync_status
         WHERE singleton=1",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        },
    )?;
    let location = match (storage.as_str(), provider) {
        ("none", None) => RecordedLocation::None,
        ("signed_out", Some(provider)) => RecordedLocation::SignedOut(provider),
        ("connected", Some(provider)) => RecordedLocation::Connected(provider),
        _ => return Err(DbError::DamagedDatabase),
    };
    Ok(SyncStatusRow {
        location,
        last_pass,
        moved: moved.as_deref().map(PassMove::from_column).transpose()?,
        last_synced: last_synced.map(crate::stored_millis::stored_time).transpose()?,
    })
}

/// Write whether a location is recorded and its credentials held, and, when
/// `finished` names one, that sync finished then. Opening also writes that
/// no pass moves and that no halt holds. A row that already says this is
/// left alone.
pub(crate) fn save_location(
    db: &crate::sqlite::DatabaseConnection,
    location: &RecordedLocation<String>,
    finished: Option<SystemTime>,
    opening: bool,
) -> Result<(), DbError> {
    let (storage, provider) = columns(location);
    let finished = finished.map(millis).transpose()?;
    db.internal_execute(
        "UPDATE coven_sync_status SET storage=?1,provider=?2,
             moved=CASE WHEN ?4 THEN NULL ELSE moved END,
             halted=CASE WHEN ?4 THEN 0 ELSE halted END,
             last_synced=CASE WHEN ?3 IS NULL THEN last_synced
                 ELSE max(coalesce(last_synced,?3),?3) END
         WHERE singleton=1 AND (storage IS NOT ?1 OR provider IS NOT ?2
             OR (?4 AND (moved IS NOT NULL OR halted=1))
             OR (?3 IS NOT NULL AND (last_synced IS NULL OR last_synced<?3)))",
        rusqlite::params![storage, provider, finished, opening],
    )?;
    Ok(())
}

/// Write what the running pass moved last, when that changes it.
pub(crate) fn save_moved(
    db: &crate::sqlite::DatabaseConnection,
    moved: PassMove,
) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE coven_sync_status SET moved=?1 WHERE singleton=1 AND halted=0 AND moved IS NOT ?1",
        [moved.column()],
    )?;
    Ok(())
}

/// Write that sync started once more.
pub(crate) fn save_start(db: &crate::sqlite::DatabaseConnection) -> Result<(), DbError> {
    db.internal_execute("UPDATE coven_sync_status SET starts=starts+1 WHERE singleton=1", [])?;
    Ok(())
}

/// Write how the last pass ended, as sync encodes it, when sync failed
/// outside a pass's end, and that nothing moves: a loop that ends in a pass
/// that moved a slot shows its failure, never syncing. Once a halt holds,
/// it writes nothing: the halt's failure stands.
pub(crate) fn save_last_pass(
    db: &crate::sqlite::DatabaseConnection,
    last_pass: &[u8],
) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE coven_sync_status SET last_pass=?1,moved=NULL
         WHERE singleton=1 AND halted=0 AND (last_pass IS NOT ?1 OR moved IS NOT NULL)",
        [last_pass],
    )?;
    Ok(())
}

/// Write that a store-wide worker's panic halted sync until the store
/// reopens, with its failure as how the last pass ended, and that nothing
/// moves: from here no pass writes the row.
pub(crate) fn save_halt(
    db: &crate::sqlite::DatabaseConnection,
    last_pass: &[u8],
) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE coven_sync_status SET last_pass=?1,moved=NULL,halted=1 WHERE singleton=1",
        [last_pass],
    )?;
    Ok(())
}

/// Write how the running pass ended and that it moves no more, and, when
/// `finished` names one, that it reached the end of the sequence then. A
/// pass that ends after a halt writes nothing: the halt's failure stands.
pub(crate) fn save_pass_end(
    db: &crate::sqlite::DatabaseConnection,
    last_pass: &[u8],
    finished: Option<SystemTime>,
) -> Result<(), DbError> {
    let finished = finished.map(millis).transpose()?;
    db.internal_execute(
        "UPDATE coven_sync_status SET last_pass=?1,moved=NULL,
             last_synced=CASE WHEN ?2 IS NULL THEN last_synced
                 ELSE max(coalesce(last_synced,?2),?2) END
         WHERE singleton=1 AND halted=0 AND (last_pass IS NOT ?1 OR moved IS NOT NULL
             OR (?2 IS NOT NULL AND (last_synced IS NULL OR last_synced<?2)))",
        rusqlite::params![last_pass, finished],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "internal_schema_sync_status_tests.rs"]
mod tests;
