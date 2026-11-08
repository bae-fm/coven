//! Moves of this device's files to the cloud (§17.3): the files one write
//! stages for the cloud, or the files the app moves. Each upload belongs to
//! one move. No file of a move switches to the cloud until every one of its
//! uploads is stored; then one write switches them all. A move that fails
//! or is cancelled switches none. Each moving or failed move lists its
//! files, for the app to see in `coven_moves`.

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::id_source::FileId;

/// One move's identity on this device, never reused (§17.3, E3).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct MoveId(pub i64);

/// A move id bound and read as an SQL integer, as `coven_uploads.move`
/// and `coven_pending.move` hold it for the app's `cancel_move` (E4).
impl rusqlite::ToSql for MoveId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.0.into())
    }
}

impl rusqlite::types::FromSql for MoveId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        i64::column_result(value).map(Self)
    }
}

/// Where a move stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MoveState {
    /// Its uploads are queued; none of its files has switched.
    Moving,
    /// A user's original changed before its upload: none of its files
    /// switched or waits for an upload, and its record says which original
    /// and its list which files, until the app cancels it.
    Failed,
    /// The app cancelled it: none of its files switched, and it lists none.
    Cancelled,
    /// Its files switched to the cloud together, and it lists none.
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
    db.query_row("INSERT INTO _coven_moves(state) VALUES('moving') RETURNING id", [], |r| r.get(0))
}

/// The move's state, or `None` for a move this device never made.
pub(crate) fn state(db: &DatabaseConnection, id: MoveId) -> Result<Option<MoveState>, DbError> {
    db.query("SELECT state FROM _coven_moves WHERE id=?1", [id], |r| r.get::<_, String>(0))?
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

/// Whether the move is a bound file's (§17.4): its upload holds a write
/// until the file is stored, so the app can't cancel it.
pub(crate) fn bound(db: &DatabaseConnection, id: MoveId) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_file_uploads WHERE move=?1 AND holds IS NOT NULL)",
        [id],
        |r| r.get(0),
    )
}

/// The move an upload belongs to.
pub(crate) fn of_upload(db: &DatabaseConnection, upload: i64) -> Result<MoveId, DbError> {
    db.query_row("SELECT move FROM _coven_file_uploads WHERE id=?1", [upload], |r| r.get(0))
        .map_err(|error| match error {
            DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => {
                DbError::Invariant { what: "a queued upload belongs to a move" }
            }
            error => error,
        })
}

/// The move that lists `file`, moving or failed, if any: a file is in at
/// most one move (§17.3).
pub(crate) fn of_file(db: &DatabaseConnection, file: FileId) -> Result<Option<MoveId>, DbError> {
    Ok(db.query("SELECT move FROM _coven_move_files WHERE file=?1", [file], |r| r.get(0))?.pop())
}

/// List each file as one of its move's, as the uploads of the moves come to
/// store them (§17.3), as one statement for each chunk.
pub(crate) fn list_all(db: &DatabaseConnection, files: &[(MoveId, FileId)]) -> Result<(), DbError> {
    let rows: Vec<_> = files
        .iter()
        .map(|(id, file)| {
            Ok(vec![crate::insert_rows::value(id)?, crate::insert_rows::value(file)?])
        })
        .collect::<Result<_, DbError>>()?;
    crate::insert_rows::insert_rows(
        db,
        2,
        &rows,
        |values| format!("INSERT INTO _coven_move_files(move,file) VALUES{values}"),
        |_| Ok(()),
    )?;
    Ok(())
}

/// The new file `to` over `from`'s bytes, now waiting for the upload `from`
/// waits for, joins that upload's move (§17.2, §17.3).
pub(crate) fn join(db: &DatabaseConnection, from: FileId, to: FileId) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_move_files(move,file)
         SELECT u.move,?2 FROM _coven_upload_files AS l
         JOIN _coven_file_uploads AS u ON u.id=l.upload WHERE l.file=?1",
        rusqlite::params![from, to],
    )?;
    Ok(())
}

/// `file`'s record is gone from this device: no move lists it.
pub(crate) fn forget(db: &DatabaseConnection, file: FileId) -> Result<(), DbError> {
    db.internal_execute("DELETE FROM _coven_move_files WHERE file=?1", [file])?;
    Ok(())
}

/// Move `id` now stands at `state`. A switch or a cancel ends its list with
/// it; a failure keeps the list, for the app to cancel (§17.3).
fn set(db: &DatabaseConnection, id: MoveId, state: MoveState) -> Result<(), DbError> {
    match state {
        MoveState::Cancelled | MoveState::Finished => {
            db.internal_execute("DELETE FROM _coven_move_files WHERE move=?1", [id])?;
        }
        MoveState::Moving | MoveState::Failed => {}
    }
    db.internal_execute(
        "UPDATE _coven_moves SET state=?2 WHERE id=?1",
        rusqlite::params![id, state.text()],
    )?;
    Ok(())
}

/// End a move switching none of its files: every file stays on this device
/// only, keeping its bytes here, and no upload of the move stores a file any
/// more, so the upload worker retires them, an attempt in flight once it
/// ends, and their stored bytes become deletion candidates (§17.10). A
/// failed move keeps its list; a cancelled one ends it. The move's pending
/// record is replaced or cleared by the caller.
pub(crate) fn end_unswitched(
    db: &DatabaseConnection,
    id: MoveId,
    state: MoveState,
) -> Result<(), DbError> {
    crate::file_queue::unswitch_move(db, id)?;
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
        |r| r.get(0),
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
    crate::pending::clear_move(db, id)
}

#[cfg(test)]
#[path = "file_moves_tests.rs"]
pub(crate) mod tests;
