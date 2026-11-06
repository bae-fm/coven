//! What the sync state is computed from (§19.1, api.md E5), read in one
//! committed snapshot: `coven_sync_status`'s row, `coven_retry_effect`'s,
//! the send queue, the upload queue with its pause and each upload's retry
//! wait and record, the network and the setting to move files only on
//! unmetered networks, the files being sent or downloaded, and the pending
//! records. Sync computes the state; this reads what it needs.
//!
//! A live query over this read runs again whenever one of them changes: the
//! files in flight are read through `coven_sending` and `coven_downloading`,
//! the views their starts and ends change (`transfers_in_flight.rs`), and
//! the network through `coven_network`, which its changes change
//! (`network_facts.rs`).

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
    /// Each upload a file waits for, in queue order.
    pub uploads: Vec<UploadFacts<P>>,
    /// Whether this process is downloading a file.
    pub downloading: bool,
    /// Every pending record, in the order the records arose.
    pub records: Vec<P>,
    /// Whether the app paused trailing uploads.
    pub paused: bool,
    /// Whether the app moves files only on unmetered networks.
    pub unmetered_only: bool,
    /// Whether the device's network is metered, as the store's watch last
    /// said.
    pub metered: bool,
}

/// One upload in the queue, as the sync state reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct UploadFacts<P> {
    /// This process is sending it now.
    pub sending: bool,
    /// Storage holds its bytes; it waits for the rest of its move.
    pub stored: bool,
    /// A bound reference's, which the pause doesn't hold (§17.4).
    pub bound: bool,
    /// Its last attempt failed, and it retries then (§6.7).
    pub retry_at: Option<SystemTime>,
    /// Its own pending record, if it has one.
    pub record: Option<P>,
}

/// Read the facts in the caller's snapshot.
pub(crate) fn read<P: crate::PendingValue + Clone>(
    db: &DatabaseConnection,
) -> Result<SyncFacts<P>, DbError> {
    let records = crate::pending::positioned::<P>(db)?;
    let by_position: BTreeMap<i64, &P> =
        records.iter().map(|(position, record)| (*position, record)).collect();
    let sending: BTreeSet<i64> =
        db.query("SELECT position FROM coven_sending", [], |row| row.get(0))?.into_iter().collect();
    let uploads = db
        .query(
            "SELECT u.id,u.stored,u.holds IS NOT NULL,r.not_before,e.id
             FROM _coven_file_uploads AS u
             LEFT JOIN _coven_retries AS r ON r.upload=u.id
             LEFT JOIN _coven_pending AS e
                 ON e.work='upload' AND e.device=u.device AND e.object=u.object
             WHERE EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id)
             ORDER BY u.id",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            },
        )?
        .into_iter()
        .map(|(id, stored, bound, retry_at, position)| {
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
                bound,
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
        downloading: db
            .query_row("SELECT EXISTS(SELECT 1 FROM coven_downloading)", [], |r| r.get(0))?,
        records: records.into_iter().map(|(_, record)| record).collect(),
        paused: crate::file_queue::paused(db)?,
        unmetered_only,
        metered,
    })
}
