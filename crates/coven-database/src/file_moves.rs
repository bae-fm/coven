//! Moves of this device's files to the cloud (§17.3): the files one write
//! stages for the cloud, or the files the app moves. Each upload belongs to
//! one move. No file of a move switches to the cloud until every one of its
//! uploads is stored; then one write switches them all. A move that fails
//! or is cancelled switches none.

use crate::{sqlite::DatabaseConnection, DbError};

/// One move's identity on this device, never reused (§17.3, E3).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct MoveId(pub i64);

impl rusqlite::ToSql for MoveId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.0.into())
    }
}

/// Where a move stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MoveState {
    /// Its uploads are queued; none of its files has switched.
    Moving,
    /// A user's original changed before its upload: none of its files
    /// switched, and its record says which, until the app cancels it.
    Failed,
    /// The app cancelled it: none of its files switched.
    Cancelled,
    /// Its files switched to the cloud together.
    Finished,
}

impl MoveState {
    fn text(self) -> &'static str {
        match self {
            Self::Moving => "moving",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Finished => "finished",
        }
    }
}

/// Record a new move, moving.
pub(crate) fn create(db: &DatabaseConnection) -> Result<MoveId, DbError> {
    db.query_row(
        "INSERT INTO _coven_moves(state) VALUES('moving') RETURNING id",
        [],
        |r| r.get(0),
    )
    .map(MoveId)
}

/// The move's state, or `None` for a move this device never made.
pub(crate) fn state(db: &DatabaseConnection, id: MoveId) -> Result<Option<MoveState>, DbError> {
    db.query("SELECT state FROM _coven_moves WHERE id=?1", [id], |r| {
        r.get::<_, String>(0)
    })?
    .pop()
    .map(|state| match state.as_str() {
        "moving" => Ok(MoveState::Moving),
        "failed" => Ok(MoveState::Failed),
        "cancelled" => Ok(MoveState::Cancelled),
        "finished" => Ok(MoveState::Finished),
        _ => Err(DbError::DamagedDatabase),
    })
    .transpose()
}

/// The move an upload belongs to.
pub(crate) fn of_upload(db: &DatabaseConnection, upload: i64) -> Result<MoveId, DbError> {
    db.query_row(
        "SELECT move FROM _coven_file_uploads WHERE id=?1",
        [upload],
        |r| r.get(0),
    )
    .map(MoveId)
    .map_err(|error| match error {
        DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => DbError::Invariant {
            what: "a queued upload belongs to a move",
        },
        error => error,
    })
}

fn set(db: &DatabaseConnection, id: MoveId, state: MoveState) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_moves SET state=?2 WHERE id=?1",
        rusqlite::params![id, state.text()],
    )?;
    Ok(())
}

/// End a move switching none of its files: every file stays on this device
/// only, keeping its bytes here, and no upload of the move stores a file any
/// more, so the upload worker retires them, an attempt in flight once it
/// ends, and their stored bytes become deletion candidates (§17.9). The
/// move's pending record is replaced or cleared by the caller.
pub(crate) fn end_unswitched(
    db: &DatabaseConnection,
    id: MoveId,
    state: MoveState,
) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_upload_files
         WHERE upload IN (SELECT id FROM _coven_file_uploads WHERE move=?1)",
        [id],
    )?;
    set(db, id, state)
}

/// The moving moves none of whose uploads that still store a file waits to
/// be stored, each with those uploads: what one write switches now. A move
/// whose files all dropped out has none.
pub(crate) fn complete(db: &DatabaseConnection) -> Result<Vec<(MoveId, Vec<i64>)>, DbError> {
    let moves = db.query(
        "SELECT m.id FROM _coven_moves AS m WHERE m.state='moving'
         AND NOT EXISTS(SELECT 1 FROM _coven_file_uploads AS u
             WHERE u.move=m.id AND u.stored=0
             AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id))",
        [],
        |r| r.get(0).map(MoveId),
    )?;
    moves
        .into_iter()
        .map(|id| {
            let uploads = db.query(
                "SELECT u.id FROM _coven_file_uploads AS u WHERE u.move=?1
                 AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id)
                 ORDER BY u.id",
                [id],
                |r| r.get(0),
            )?;
            Ok((id, uploads))
        })
        .collect()
}

/// A move ended with its files switched: whatever its record said ends.
pub(crate) fn finish(db: &DatabaseConnection, id: MoveId) -> Result<(), DbError> {
    set(db, id, MoveState::Finished)?;
    clear_pending(db, id)
}

/// Mark an upload's bytes stored: its move switches it with the rest.
pub(crate) fn stored(db: &DatabaseConnection, upload: i64) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_file_uploads SET stored=1 WHERE id=?1",
        [upload],
    )?;
    Ok(())
}

/// Clear the move's pending record, whatever it says.
pub(crate) fn clear_pending(db: &DatabaseConnection, id: MoveId) -> Result<(), DbError> {
    db.pending_change(|| {
        db.internal_execute("DELETE FROM _coven_pending WHERE move=?1", [id])?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "file_moves_tests.rs"]
mod tests;
