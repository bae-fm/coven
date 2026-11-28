//! The upload queue as the app's SQL reads it from coven's views (E4), which
//! the files tests follow live and read once.

use crate::UploadProgress;

/// The upload queue as the app's SQL reads it: the pause from
/// `coven_upload_pause`, and each queued file, oldest first, from
/// `coven_uploads` (E4).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ViewedQueue {
    pub(super) paused: bool,
    pub(super) files: Vec<ViewedUpload>,
}
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ViewedUpload {
    pub(super) file: coven_foundation::id_source::FileId,
    /// `coven_uploads.sending`: this process sends it now.
    pub(super) sending: bool,
    pub(super) progress: Option<UploadProgress>,
    pub(super) retry_at: Option<i64>,
    pub(super) pending: bool,
}
/// The pause with each queued file, oldest first: one row with no file when
/// nothing is queued.
pub(super) const QUEUE: &str =
    "SELECT p.paused,u.file,u.sending,u.bytes_confirmed,u.bytes_total,u.retry_at,
                            u.pending IS NOT NULL
                     FROM coven_upload_pause AS p LEFT JOIN coven_uploads AS u ON 1
                     ORDER BY u.position";

/// A row of `QUEUE`: the pause, and the file it names.
pub(super) fn queue_row(
    r: &coven_database::Row<'_>,
) -> coven_database::SqliteResult<(bool, Option<ViewedUpload>)> {
    let Some(file) = r.get(1)? else {
        return Ok((r.get(0)?, None));
    };
    let progress = match (r.get::<_, Option<i64>>(3)?, r.get::<_, Option<i64>>(4)?) {
        (Some(confirmed), Some(total)) => {
            Some(UploadProgress { bytes_confirmed: confirmed as u64, bytes_total: total as u64 })
        }
        _ => None,
    };
    let upload = ViewedUpload {
        file,
        sending: r.get(2)?,
        progress,
        retry_at: r.get(5)?,
        pending: r.get(6)?,
    };
    Ok((r.get(0)?, Some(upload)))
}

/// The queue the rows of `QUEUE` show.
pub(super) fn queue_of(
    rows: Vec<(bool, Option<ViewedUpload>)>,
) -> Result<ViewedQueue, coven_database::DbError> {
    let paused = rows.first().ok_or(coven_database::SqliteError::QueryReturnedNoRows)?.0;
    Ok(ViewedQueue { paused, files: rows.into_iter().filter_map(|(_, file)| file).collect() })
}

pub(super) fn viewed_queue(
    sql: coven_database::SqlReadContext<'_>,
) -> Result<ViewedQueue, coven_database::DbError> {
    queue_of(sql.query(QUEUE, [], queue_row)?)
}
