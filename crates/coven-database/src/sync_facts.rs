//! What the sync state is computed from (§19.1, api.md E5), read in one
//! committed snapshot by one list of SQL expressions (`facts!`):
//! `coven_sync_status`'s row, whether sync runs and the cooldown
//! `coven_retry_effect` holds, the send queue, the bound uploads (the uploads
//! a write waits for, each with its retry wait and record), and the pending
//! records the state reads. The `coven_sync_state` view passes them to
//! `_coven_sync_state` as its arguments (`sync_state_function.rs`), and coven
//! reads them as a row where sync computes the state in Rust: a closed
//! store's state, and each upload's phase (`QueueFacts`). Both decode them
//! with `SyncFacts::decode`. Sync computes the state; this reads what it
//! needs.
//!
//! A trailing upload, a download and a pin coven keeps are not sync's: no
//! upload of theirs and no record about them is read here, and the state
//! never sees them (§17.4, §19.1). Which upload is bound comes from the
//! queue's own column (`holds`), and a record is left out when its row names
//! such an upload, a download or a pin, or a move none of whose uploads is
//! bound. Nor does the state read the pause, the network or a
//! self-replacement's write pause, which hold trailing uploads only: an
//! upload's phase reads them beside the facts (`TransferHolds`).
//!
//! A live query over these facts runs again whenever one of them changes:
//! the files in flight are read through `coven_sending`, the view their
//! starts and ends change (`transfers_in_flight.rs`). Each table is read
//! through a column, never only counted, so that SQLite names the view to
//! the authorizer for each read the view makes (`internal_schema_views.rs`).

use crate::sqlite::StoreSql;
use crate::DbError;
use sqlive::rusqlite::types::{FromSql, ValueRef};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The facts, as SQL expressions over `coven_sync_status AS s` and
/// `coven_retry_effect AS r`, in the order `SyncFacts::decode` reads them:
/// the status row's columns; whether sync runs and its cooldown; the send
/// queue's writes and the upload queue's uploads, counted; whether a bound
/// upload has bytes to send; the bound uploads the state can show, as a JSON
/// array; the pending records the state reads, as a JSON array; the sync
/// scope's backoff; and each operation's backoff, as a JSON array.
///
/// The bound uploads the state can show are each read from where its kind
/// is small: the uploads with a record from the pending records, the first
/// one sending from the files in flight, the first queued one with no record
/// and no retry by walking the bound queue in id order to the first match,
/// and the first with no record that waits out a retry from the retry rows.
/// The outer read then reads only those rows, and only bound ones. Each is
/// `[id, stored, sending, retry seconds, retry nanos, record position]`, the
/// retry time as `_coven_unix_seconds` and `_coven_unix_nanos` give it, since
/// JSON holds no bytes. A record is `[position, record, cause]`, in the order
/// the records arose, and an operation's backoff `[operation, seconds,
/// nanos]`.
macro_rules! facts {
    () => {
        concat!(
            "s.storage,s.provider,s.last_pass,s.moved,s.last_synced,r.syncing,r.cooling_until,",
            crate::file_queue::unsent_counts!(),
            ",EXISTS(SELECT 1 FROM _coven_file_uploads AS b
                 WHERE b.holds IS NOT NULL AND NOT b.stored
                   AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=b.id)),
             (SELECT json_group_array(json_array(u.id,u.stored,
                     u.id IN (SELECT c.position FROM coven_sending AS c),
                     iif(t.upload IS NULL,NULL,_coven_unix_seconds(t.not_before)),
                     iif(t.upload IS NULL,NULL,_coven_unix_nanos(t.not_before)),e.id)
                     ORDER BY u.id)
                 FROM _coven_file_uploads AS u
                 LEFT JOIN _coven_retries AS t ON t.upload=u.id
                 LEFT JOIN _coven_pending AS e
                     ON e.work='upload' AND e.device=u.device AND e.object=u.object
                 WHERE u.id IN (
                     SELECT f.id FROM _coven_pending AS p
                     JOIN _coven_file_uploads AS f ON f.device=p.device AND f.object=p.object
                     WHERE p.work='upload'
                     UNION
                     SELECT MIN(g.position) FROM coven_sending AS g
                     JOIN _coven_file_uploads AS b ON b.id=g.position
                     WHERE b.holds IS NOT NULL
                     UNION
                     SELECT * FROM (
                         SELECT f.id FROM _coven_file_uploads AS f
                         WHERE NOT f.stored AND f.holds IS NOT NULL
                           AND f.id NOT IN (SELECT position FROM coven_sending)
                           AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=f.id)
                           AND NOT EXISTS(SELECT 1 FROM _coven_pending AS p
                                          WHERE p.work='upload' AND p.device=f.device
                                            AND p.object=f.object)
                           AND NOT EXISTS(SELECT 1 FROM _coven_retries AS t WHERE t.upload=f.id)
                         ORDER BY f.id LIMIT 1)
                     UNION
                     SELECT MIN(f.id) FROM _coven_retries AS t
                     CROSS JOIN _coven_file_uploads AS f ON f.id=t.upload
                     WHERE t.upload IS NOT NULL
                       AND NOT f.stored AND f.holds IS NOT NULL
                       AND f.id NOT IN (SELECT position FROM coven_sending)
                       AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=f.id)
                       AND NOT EXISTS(SELECT 1 FROM _coven_pending AS p
                                      WHERE p.work='upload' AND p.device=f.device
                                        AND p.object=f.object))
                   AND u.holds IS NOT NULL
                   AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id)),
             (SELECT json_group_array(json_array(p.id,json(CAST(p.value AS TEXT)),p.cause)
                     ORDER BY p.id)
                 FROM _coven_pending AS p
                 WHERE (p.pin IS NOT NULL
                     OR p.work='download'
                     OR (p.work='upload' AND NOT EXISTS(
                         SELECT 1 FROM _coven_file_uploads AS f
                         WHERE f.device=p.device AND f.object=p.object AND f.holds IS NOT NULL))
                     OR (p.move IS NOT NULL AND NOT EXISTS(
                         SELECT 1 FROM _coven_file_uploads AS f
                         WHERE f.move=p.move AND f.holds IS NOT NULL))) IS NOT TRUE),
             (SELECT t.not_before FROM _coven_retries AS t WHERE t.scope='sync'),
             (SELECT json_group_array(json_array(t.operation,_coven_unix_seconds(t.not_before),
                     _coven_unix_nanos(t.not_before)))
                 FROM _coven_retries AS t WHERE t.operation>0)"
        )
    };
}
pub(crate) use facts;

/// How many expressions `facts!` lists: the arguments `_coven_sync_state`
/// takes.
pub(crate) const FACTS: i32 = 14;

/// The facts in one row.
const READ: &str =
    concat!("SELECT ", facts!(), " FROM coven_sync_status AS s JOIN coven_retry_effect AS r");

/// What the sync state is computed from, in one committed snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct SyncFacts<P> {
    /// What the sync loop knows: `coven_sync_status`'s row.
    pub row: crate::SyncStatusRow,
    /// Whether a started connection will plan, and the cooldown holding
    /// every request, as `coven_retry_effect` holds them.
    pub sync: crate::RetrySync,
    /// What the send queue and the upload queue hold (`UnsentWork`): a
    /// write waits when it counts one.
    pub unsent: crate::UnsentWork,
    /// The bound uploads the state can show, in queue order: the first one
    /// sending, the first queued one (neither sending nor stored) with no
    /// record and none waiting out a retry, the first with no record that
    /// does, and every one with a record. The state shows the first
    /// candidate of the highest rank, so no other upload changes it: later
    /// sending ones tie with the first, a later queued one ties with the
    /// first of its kind (syncing for its turn, waiting for its retry), and a
    /// stored one names none.
    pub uploads: Vec<UploadFacts<P>>,
    /// Whether the upload queue holds a bound upload with bytes to send,
    /// whether `uploads` lists it or not.
    pub queued: bool,
    /// The pending records the state reads, each with its position in
    /// `coven_pending`, in the order they arose: every one but those about a
    /// trailing upload, a download, a pin coven keeps or a move of trailing
    /// uploads alone.
    pub records: Vec<(i64, P)>,
    /// When the sync scope's backoff ends, if a failed pass armed one
    /// (§6.7): the next try of a pass or of a slot's application.
    pub pass_retry: Option<SystemTime>,
    /// When each journal operation's own backoff ends: the next try of
    /// coven's own operation a failed pass names.
    pub operation_retries: Vec<(crate::OperationId, SystemTime)>,
}

/// One bound upload in the queue, as the sync state reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct UploadFacts<P> {
    /// This process is sending it now.
    pub sending: bool,
    /// Storage holds its bytes; it waits for the rest of its move.
    pub stored: bool,
    /// Its last attempt failed, and it retries then (§6.7).
    pub retry_at: Option<SystemTime>,
    /// Its own pending record, if it has one.
    pub record: Option<P>,
}

/// What holds trailing uploads, and no bound upload or the sync state: an
/// upload's phase reads them beside the facts (§17.4, §17.9, §12.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransferHolds {
    /// Whether the app paused trailing uploads.
    pub paused: bool,
    /// Whether the app moves files only on unmetered networks.
    pub unmetered_only: bool,
    /// Whether the device's network is metered, as the store's watch last
    /// said.
    pub metered: bool,
    /// Whether a self-replacement of this member's keys pauses writes
    /// (§12.3).
    pub keys_changing: bool,
}

/// Where the facts are read from: a row of the facts' SELECT, or the
/// arguments of one call of `_coven_sync_state`.
pub(crate) trait FactValues {
    /// Value `index`, counted from zero, as SQLite holds it.
    fn value(&self, index: usize) -> sqlive::rusqlite::Result<ValueRef<'_>>;

    /// Value `index`, converted as a row's column is.
    fn get<T: FromSql>(&self, index: usize) -> Result<T, DbError> {
        let value = self.value(index)?;
        T::column_result(value).map_err(|error| {
            DbError::from(sqlive::rusqlite::Error::FromSqlConversionFailure(
                index,
                value.data_type(),
                Box::new(error),
            ))
        })
    }
}

impl FactValues for sqlive::rusqlite::Row<'_> {
    fn value(&self, index: usize) -> sqlive::rusqlite::Result<ValueRef<'_>> {
        self.get_ref(index)
    }
}

impl FactValues for sqlive::rusqlite::functions::Context<'_> {
    fn value(&self, index: usize) -> sqlive::rusqlite::Result<ValueRef<'_>> {
        Ok(self.get_raw(index))
    }
}

/// One listed upload as `facts!` gives it.
type ListedUpload = (i64, i64, i64, Option<i64>, Option<u32>, Option<i64>);

impl<P: crate::PendingValue + Clone> SyncFacts<P> {
    /// The facts from `facts!`'s values, as a row or a call's arguments hold
    /// them. A value that doesn't decode is `DamagedDatabase`, and an upload's
    /// record that isn't among the records an invariant failure.
    pub(crate) fn decode(values: &impl FactValues) -> Result<Self, DbError> {
        let row = crate::internal_schema::sync_status::decode(values, 0)?;
        let sync = crate::internal_schema::retry_effect::sync(values.get(5)?, values.get(6)?)?;
        let count = |index| {
            u64::try_from(values.get::<i64>(index)?).map_err(|_negative| DbError::DamagedDatabase)
        };
        let unsent = crate::UnsentWork { writes: count(7)?, files: count(8)? };
        let records: Vec<(i64, P, Option<String>)> = json(&values.get::<String>(11)?)?;
        let records: Vec<(i64, P)> = records
            .into_iter()
            .map(|(position, record, cause)| (position, record.with_cause(cause)))
            .collect();
        let by_position: BTreeMap<i64, &P> =
            records.iter().map(|(position, record)| (*position, record)).collect();
        let uploads = json::<Vec<ListedUpload>>(&values.get::<String>(10)?)?
            .into_iter()
            .map(|(_id, stored, sending, seconds, nanos, position)| {
                let record = position
                    .map(|position| {
                        by_position.get(&position).map(|record| (*record).clone()).ok_or(
                            DbError::Invariant {
                                what: "an upload's record is among the pending records",
                            },
                        )
                    })
                    .transpose()?;
                let retry_at = match (seconds, nanos) {
                    (Some(seconds), Some(nanos)) => Some(unix_time(seconds, nanos)?),
                    (None, None) => None,
                    _ => return Err(DbError::DamagedDatabase),
                };
                Ok(UploadFacts { sending: sending != 0, stored: stored != 0, retry_at, record })
            })
            .collect::<Result<_, DbError>>()?;
        let operation_retries = json::<Vec<(i64, i64, u32)>>(&values.get::<String>(13)?)?
            .into_iter()
            .map(|(operation, seconds, nanos)| {
                Ok((crate::OperationId(operation), unix_time(seconds, nanos)?))
            })
            .collect::<Result<_, DbError>>()?;
        Ok(Self {
            row,
            sync,
            unsent,
            uploads,
            queued: values.get(9)?,
            records,
            pass_retry: values
                .get::<Option<Vec<u8>>>(12)?
                .as_deref()
                .map(crate::user_file::decode_time)
                .transpose()?,
            operation_retries,
        })
    }
}

/// The value a JSON array of `facts!` holds.
fn json<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, DbError> {
    serde_json::from_str(text).map_err(|_damaged| DbError::DamagedDatabase)
}

/// The time `seconds` and `nanos` name, as `unix_seconds_and_nanos` splits
/// it: whole seconds since the Unix epoch, rounded down, and the nanoseconds
/// past that second.
fn unix_time(seconds: i64, nanos: u32) -> Result<SystemTime, DbError> {
    let whole = Duration::from_secs(seconds.unsigned_abs());
    let second =
        if seconds >= 0 { UNIX_EPOCH.checked_add(whole) } else { UNIX_EPOCH.checked_sub(whole) };
    second
        .and_then(|second| second.checked_add(Duration::from_nanos(u64::from(nanos))))
        .filter(|_| nanos < 1_000_000_000)
        .ok_or(DbError::DamagedDatabase)
}

/// Read the facts in the caller's snapshot.
pub(crate) fn read<P: crate::PendingValue + Clone>(db: &StoreSql) -> Result<SyncFacts<P>, DbError> {
    db.query_row(READ, [], |row| Ok(SyncFacts::decode(row)))?
}

/// What the phase of each upload in the queue is computed from (§17.9, api.md
/// E7): the facts the sync state reads, what holds trailing uploads, the
/// whole queue, and what is recorded about each upload, in one committed
/// snapshot.
#[derive(Clone, Debug)]
pub struct QueueFacts<P> {
    /// What the sync state is computed from.
    pub sync: SyncFacts<P>,
    /// What holds trailing uploads.
    pub holds: TransferHolds,
    /// The queue as the app observes it, every file in it.
    pub queue: crate::UploadQueueState,
    /// What is recorded about each upload in the queue, by queue id.
    pub recorded: Vec<UploadRecords<P>>,
}

/// What is recorded about one queued upload.
#[derive(Clone, Debug)]
pub struct UploadRecords<P> {
    /// The upload's queue id.
    pub upload: i64,
    /// The path of the user's original it uploads, if it uploads one.
    pub original: Option<std::path::PathBuf>,
    /// The record about the upload's stored bytes, if it has one.
    pub own: Option<P>,
    /// The record about the move the upload belongs to, if it has one: it
    /// names an original, which may be this upload's or a sibling's.
    pub moving: Option<P>,
}

/// Read the facts, what holds trailing uploads, and the queue with what is
/// recorded about each upload, in the caller's snapshot.
pub(crate) fn read_queue<P: crate::PendingValue + Clone>(
    db: &StoreSql,
) -> Result<QueueFacts<P>, DbError> {
    let records: BTreeMap<i64, P> = crate::pending::positioned::<P>(db)?.into_iter().collect();
    let recorded = db
        .query(
            "SELECT u.id,u.original,o.id,m.id FROM _coven_file_uploads AS u
             LEFT JOIN _coven_pending AS o
                 ON o.work='upload' AND o.device=u.device AND o.object=u.object
             LEFT JOIN _coven_pending AS m ON m.move=u.move
             ORDER BY u.id",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )?
        .into_iter()
        .map(|(upload, original, own, moving)| {
            let find = |position: Option<i64>| {
                position
                    .map(|position| {
                        records.get(&position).cloned().ok_or(DbError::Invariant {
                            what: "an upload's record is among the pending records",
                        })
                    })
                    .transpose()
            };
            Ok(UploadRecords {
                upload,
                original: original.map(crate::user_file::decode_path).transpose()?,
                own: find(own)?,
                moving: find(moving)?,
            })
        })
        .collect::<Result<_, DbError>>()?;
    let (metered, unmetered_only) =
        db.query_row("SELECT metered,unmetered_only FROM coven_network", [], |row| {
            Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?))
        })?;
    let holds = TransferHolds {
        paused: crate::file_queue::paused(db)?,
        unmetered_only,
        metered,
        keys_changing: crate::operation::writes_paused(db)?,
    };
    Ok(QueueFacts { sync: read(db)?, holds, queue: crate::file_queue::state(db)?, recorded })
}

#[cfg(test)]
#[path = "sync_facts_tests.rs"]
mod tests;
