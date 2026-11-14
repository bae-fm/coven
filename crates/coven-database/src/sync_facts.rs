//! What the sync state is computed from (§19.1, api.md E5), read in one
//! committed snapshot: `coven_sync_status`'s row, `coven_retry_effect`'s,
//! the send queue, the bound uploads (the uploads a write waits for, each
//! with its retry wait and record), and the pending records the state reads.
//! The pause, the network, the setting to move files only on unmetered
//! networks and a self-replacement's write pause come with them, since the
//! phase of each upload in the queue is computed from the same facts
//! (`QueueFacts`). Sync computes the state; this reads what it needs.
//!
//! A trailing upload, a download and a pin coven keeps are not sync's: no
//! upload of theirs and no record about them is read here, and the state
//! never sees them (§17.4, §19.1). Which upload is bound comes from the
//! queue's own column (`holds`), and a record is kept out when its row names
//! such an upload, a download or a pin, or a move none of whose uploads is
//! bound.
//!
//! A live query over this read runs again whenever one of them changes: the
//! files in flight are read through `coven_sending`, the view their starts
//! and ends change (`transfers_in_flight.rs`), and the network through
//! `coven_network`, which its changes change (`network_facts.rs`).

use crate::sqlite::DatabaseConnection;
use crate::DbError;
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

/// What the sync state is computed from, in one committed snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct SyncFacts<P> {
    /// What the sync loop knows: `coven_sync_status`'s row.
    pub row: crate::SyncStatusRow,
    /// What the person's retry would do: whether sync runs, and the
    /// cooldown holding every request.
    pub retry: crate::RetryState,
    /// Whether the send queue holds a write.
    pub unsent: bool,
    /// The bound uploads the state can show, in queue order: the first one
    /// sending, the first queued one (neither sending nor stored) with no
    /// record, and every one with a record. The state shows the first
    /// candidate of the highest rank, so no other upload changes it: later
    /// sending or waiting ones tie with the first, and a stored one names
    /// none.
    pub uploads: Vec<UploadFacts<P>>,
    /// Whether the upload queue holds a bound upload with bytes to send,
    /// whether `uploads` lists it or not.
    pub queued: bool,
    /// The pending records the state reads, in the order they arose: every
    /// one but those about a trailing upload, a download, a pin coven keeps
    /// or a move of trailing uploads alone.
    pub records: Vec<P>,
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

/// The bound uploads the sync state can show (`SyncFacts::uploads`), each read
/// from where its kind is small: the uploads with a record from the pending
/// records, the first one sending from the files in flight, and the first
/// queued one with no record by walking the bound queue in id order to the
/// first match. The outer query then reads only those rows, and only bound
/// ones.
pub(crate) const UPLOADS: &str = "SELECT u.id,u.stored,r.not_before,e.id
     FROM _coven_file_uploads AS u
     LEFT JOIN _coven_retries AS r ON r.upload=u.id
     LEFT JOIN _coven_pending AS e
         ON e.work='upload' AND e.device=u.device AND e.object=u.object
     WHERE u.id IN (
         SELECT f.id FROM _coven_pending AS p
         JOIN _coven_file_uploads AS f ON f.device=p.device AND f.object=p.object
         WHERE p.work='upload'
         UNION
         SELECT MIN(s.position) FROM coven_sending AS s
         JOIN _coven_file_uploads AS b ON b.id=s.position
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
             ORDER BY f.id LIMIT 1))
       AND u.holds IS NOT NULL
       AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id)
     ORDER BY u.id";

/// The pending records the state doesn't read: those about an upload no
/// write waits for, a download, a pin coven keeps, or a move none of whose
/// uploads a write waits for (§19.1). The pending row names its subject's
/// kind in its own columns, and the queue's `holds` column says which upload
/// is bound.
const TRANSFER_RECORDS: &str = "SELECT p.id FROM _coven_pending AS p
     WHERE p.pin IS NOT NULL
        OR p.work='download'
        OR (p.work='upload' AND NOT EXISTS(
            SELECT 1 FROM _coven_file_uploads AS u
            WHERE u.device=p.device AND u.object=p.object AND u.holds IS NOT NULL))
        OR (p.move IS NOT NULL AND NOT EXISTS(
            SELECT 1 FROM _coven_file_uploads AS u
            WHERE u.move=p.move AND u.holds IS NOT NULL))";

/// Read the facts in the caller's snapshot.
pub(crate) fn read<P: crate::PendingValue + Clone>(
    db: &DatabaseConnection,
) -> Result<SyncFacts<P>, DbError> {
    let transfers: BTreeSet<i64> =
        db.query(TRANSFER_RECORDS, [], |row| row.get::<_, i64>(0))?.into_iter().collect();
    let records: Vec<(i64, P)> = crate::pending::positioned::<P>(db)?
        .into_iter()
        .filter(|(position, _)| !transfers.contains(position))
        .collect();
    let by_position: BTreeMap<i64, &P> =
        records.iter().map(|(position, record)| (*position, record)).collect();
    let sending: BTreeSet<i64> =
        db.query("SELECT position FROM coven_sending", [], |row| row.get(0))?.into_iter().collect();
    let uploads = db
        .query(UPLOADS, [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })?
        .into_iter()
        .map(|(id, stored, retry_at, position)| {
            let record = position
                .map(|position| {
                    by_position.get(&position).map(|record| (*record).clone()).ok_or(
                        DbError::Invariant {
                            what: "an upload's record is among the pending records",
                        },
                    )
                })
                .transpose()?;
            Ok(UploadFacts {
                sending: sending.contains(&id),
                stored,
                retry_at: retry_at.as_deref().map(crate::user_file::decode_time).transpose()?,
                record,
            })
        })
        .collect::<Result<_, DbError>>()?;
    let (metered, unmetered_only) =
        db.query_row("SELECT metered,unmetered_only FROM coven_network", [], |row| {
            Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?))
        })?;
    Ok(SyncFacts {
        row: crate::internal_schema::sync_status::read(db)?,
        retry: crate::internal_schema::retry_effect::read(db)?,
        unsent: db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_uploads)", [], |r| r.get(0))?,
        uploads,
        queued: db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_file_uploads AS u
                 WHERE u.holds IS NOT NULL AND NOT u.stored
                   AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id))",
            [],
            |r| r.get(0),
        )?,
        records: records.into_iter().map(|(_, record)| record).collect(),
        paused: crate::file_queue::paused(db)?,
        unmetered_only,
        metered,
        keys_changing: crate::operation::writes_paused(db)?,
    })
}

/// What the phase of each upload in the queue is computed from (§17.9, api.md
/// E7): the facts the sync state reads, the whole queue, and what is recorded
/// about each upload, in one committed snapshot.
#[derive(Clone, Debug)]
pub struct QueueFacts<P> {
    /// What the sync state is computed from.
    pub sync: SyncFacts<P>,
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

/// Read the facts, and the queue with what is recorded about each upload,
/// in the caller's snapshot.
pub(crate) fn read_queue<P: crate::PendingValue + Clone>(
    db: &DatabaseConnection,
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
    Ok(QueueFacts { sync: read(db)?, queue: crate::file_queue::state(db)?, recorded })
}

#[cfg(test)]
#[path = "sync_facts_tests.rs"]
mod tests;
