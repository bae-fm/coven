use super::*;
use crate::MakeRemoteIntentState;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct OutboxFailure {
    pub message: String,
    pub kind: OutboxFailureKind,
}

impl OutboxFailure {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: OutboxFailureKind::Other,
        }
    }

    pub fn source_unavailable(path: std::path::PathBuf, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: OutboxFailureKind::SourceUnavailable { path },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OutboxFailureKind {
    Other,
    SourceUnavailable { path: std::path::PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEntry {
    pub id: i64,
    pub attempt_count: i64,
    pub last_attempt_at: Option<String>,
    pub upload: OutboxUpload,
}

/// The cloud work one queue row holds. Uploading a blob is the whole of it:
/// retirement is accepted reclaim's, and a transition that never reached
/// acceptance unwinds through this same queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxUpload {
    pub root_table: String,
    pub root_id: String,
    pub row: coven_protocol::blob::RowBlobRef,
    pub source_path: std::path::PathBuf,
    pub retain_pinned: bool,
    pub state: OutboxUploadState,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OutboxUploadState {
    Pending,
    Prepared {
        authority: coven_protocol::audience_package::PackageAudience,
        stored: coven_protocol::blob::locator::StoredBlobRef,
        spool_path: std::path::PathBuf,
    },
    Created {
        authority: coven_protocol::audience_package::PackageAudience,
        stored: coven_protocol::blob::locator::StoredBlobRef,
        spool_path: std::path::PathBuf,
    },
}

/// How far a gated root's make-remote transition has got, as its durable
/// intent records it. The write id a publication carries is bookkeeping the
/// transition owns, so it is not part of this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MakeRemoteProgress {
    /// Blobs are queued or uploading.
    Uploading,
    /// A cancellation is unwinding the transition.
    Cancelling,
    /// Every upload landed; the Store write that publishes them is pending.
    Publishing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuedUploadPhase {
    Pending,
    Prepared,
    Created,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedMakeRemote {
    pub root_table: String,
    pub root_id: String,
    /// What the host called this root when it queued the work, snapshotted here
    /// so the queue can name its own entries. The root row is exactly what a
    /// cancelled or deleted root no longer has, and an entry that cannot say
    /// what it is is an entry a host cannot render or a person cancel.
    pub root_label: String,
    pub retain_pinned: bool,
    pub progress: MakeRemoteProgress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudOutboxSnapshot {
    pub uploads: Vec<QueuedUpload>,
    pub make_remotes: Vec<QueuedMakeRemote>,
}

/// One upload the durable cloud queue is holding, as a host renders it.
///
/// This is a projection of the queue row, not the drain's working entry: it
/// carries what a person needs to see — which blob, which row it belongs to,
/// whether it is retried and why — and none of the transfer bookkeeping the
/// drain needs. `attempt_count` is 0 and `last_error` is `None` until a
/// transfer has actually been tried and failed, so a freshly queued upload is
/// distinguishable from a retrying one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedUpload {
    /// The exact blob-bearing row version held by the durable queue. This is the
    /// same identity upload lifecycle callbacks report.
    pub blob: coven_protocol::blob::RowBlobRef,
    /// The gated root whose make-remote enqueued this upload. Every upload for
    /// one root shares this pair, and the root is what a host groups by.
    pub root_table: String,
    pub root_id: String,
    /// What the host called this root when it queued the work, snapshotted here
    /// so the queue can name its own entries. The root row is exactly what a
    /// cancelled or deleted root no longer has, and an entry that cannot say
    /// what it is is an entry a host cannot render or a person cancel.
    pub root_label: String,
    /// Whether the transition asked for the plaintext to stay cached locally
    /// once the upload lands.
    pub retain_pinned: bool,
    /// The durable handoff this exact upload has reached. In-memory transfer
    /// activity refines this into Preparing or Uploading; after a restart this
    /// is the authoritative lower bound the host renders immediately.
    pub phase: QueuedUploadPhase,
    /// Exact bytes the cloud provider receives once preparation has produced
    /// the stored object. Pending preparation has no provider denominator yet.
    pub provider_bytes_total: Option<u64>,
    /// Failed transfer attempts so far; 0 for an upload never yet tried.
    pub attempt_count: u64,
    /// Why the last attempt failed, if one has.
    pub last_failure: Option<OutboxFailure>,
    /// When the upload was enqueued.
    pub created_at: String,
    /// When it was last attempted, if it has been.
    pub last_attempt_at: Option<String>,
}

#[derive(Clone)]
pub struct PublishedBlobDropIntent {
    pub seq: u64,
    pub drop: coven_protocol::blob::DeferredLocalBlobDrop,
}

impl StoreSession<'_> {
    fn queued_upload_rows(
        &mut self,
        root: Option<(String, String)>,
    ) -> Result<Vec<QueuedUpload>, DbError> {
        const COLUMNS: &str = "SELECT row_ref, root_table, root_id, root_label, retain_pinned,
                    upload_state, attempt_count, last_error, created_at, last_attempt_at
             FROM cloud_outbox";
        let (sql, parameters): (String, Vec<String>) = match root {
            Some((root_table, root_id)) => (
                format!("{COLUMNS} WHERE root_table = ?1 AND root_id = ?2 ORDER BY id"),
                vec![root_table, root_id],
            ),
            None => (format!("{COLUMNS} ORDER BY id"), Vec::new()),
        };
        let mut statement = self.conn.prepare(&sql).map_err(DbError::from)?;
        let uploads = statement
            .query_map(rusqlite::params_from_iter(parameters), row_to_queued_upload)
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        Ok(uploads)
    }

    fn queued_make_remotes(&mut self) -> Result<Vec<QueuedMakeRemote>, DbError> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT root_table, root_id, root_label, retain_pinned, state
                 FROM blob_make_remote_intents ORDER BY root_table, root_id",
            )
            .map_err(DbError::from)?;
        let make_remotes = statement
            .query_map([], |row| {
                let state: String = row.get(4)?;
                let progress = match state.as_str() {
                    "uploading" => MakeRemoteProgress::Uploading,
                    "cancelling" => MakeRemoteProgress::Cancelling,
                    "publishing" => MakeRemoteProgress::Publishing,
                    _ => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::other(format!(
                                "invalid make_remote state {state:?}"
                            ))),
                        ));
                    }
                };
                Ok(QueuedMakeRemote {
                    root_table: row.get(0)?,
                    root_id: row.get(1)?,
                    root_label: row.get(2)?,
                    retain_pinned: row.get(3)?,
                    progress,
                })
            })
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        Ok(make_remotes)
    }

    fn cloud_outbox_snapshot(&mut self) -> Result<CloudOutboxSnapshot, DbError> {
        Ok(CloudOutboxSnapshot {
            uploads: self.queued_upload_rows(None)?,
            make_remotes: self.queued_make_remotes()?,
        })
    }

    fn pending_blob_uploads(&mut self) -> Result<Vec<OutboxEntry>, DbError> {
        let mut statement = self
            .conn
            .prepare(&format!(
                "SELECT {} FROM cloud_outbox ORDER BY id",
                crate::cloud_outbox_records::OUTBOX_ENTRY_COLUMNS
            ))
            .map_err(DbError::from)?;
        let entries = statement
            .query_map([], crate::row_to_outbox_entry)
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        Ok(entries)
    }

    fn published_blob_drop_intents(
        &mut self,
        max_seq: u64,
    ) -> Result<Vec<PublishedBlobDropIntent>, DbError> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition
                 FROM published_blob_drop_intents
                 WHERE seq <= ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM store_write_blob_leases lease
                       WHERE lease.namespace = published_blob_drop_intents.namespace
                         AND lease.blob_id = published_blob_drop_intents.blob_id
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM retained_replay_blob_leases baseline
                       WHERE baseline.namespace = published_blob_drop_intents.namespace
                         AND baseline.blob_id = published_blob_drop_intents.blob_id
                   )
                 ORDER BY seq, namespace, blob_id, locator_hash",
            )
            .map_err(DbError::from)?;
        let intents = statement
            .query_map([max_seq as i64], row_to_published_blob_drop_intent)
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        Ok(intents)
    }

    fn clear_published_blob_drop_intent(
        &mut self,
        seq: u64,
        namespace: String,
        id: String,
        locator_hash: String,
    ) -> Result<(), DbError> {
        self.conn
            .execute(
                "DELETE FROM published_blob_drop_intents
                 WHERE seq = ?1 AND namespace = ?2 AND blob_id = ?3 AND locator_hash = ?4",
                rusqlite::params![seq as i64, namespace, id, locator_hash],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    fn record_outbox_failure(
        &mut self,
        entry: OutboxEntry,
        failure: OutboxFailure,
        attempted_at: String,
    ) -> Result<(), DbError> {
        let identity = crate::outbox_identity(&entry.upload);
        let encoded = serde_json::to_string(&failure)
            .map_err(|error| DbError::context("serialize outbox failure", error))?;
        let updated = self
            .conn
            .execute(
                "UPDATE cloud_outbox SET attempt_count = attempt_count + 1,
                 last_error = ?1, last_attempt_at = ?2
                 WHERE id = ?3 AND table_name = ?4
                   AND row_id = ?5 AND column_name = ?6 AND row_stamp = ?7",
                rusqlite::params![
                    encoded,
                    attempted_at,
                    entry.id,
                    identity.table,
                    identity.row_id,
                    identity.column,
                    identity.row_stamp
                ],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(
                "cloud outbox entry changed before failure recording".to_string(),
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn swap_blob_upload_state(
        &mut self,
        id: i64,
        table: String,
        row_id: String,
        column: String,
        row_stamp: String,
        from: String,
        to: String,
        context: &'static str,
    ) -> Result<(), DbError> {
        let updated = self
            .conn
            .execute(
                "UPDATE cloud_outbox SET upload_state = ?1, last_error = NULL
                 WHERE id = ?2 AND table_name = ?3
                   AND row_id = ?4 AND column_name = ?5 AND row_stamp = ?6
                   AND upload_state = ?7",
                rusqlite::params![to, id, table, row_id, column, row_stamp, from],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!(
                "upload outbox entry changed before {context}"
            )));
        }
        Ok(())
    }

    fn reset_outbox_backoff(&mut self) -> Result<(), DbError> {
        self.conn
            .execute(
                "UPDATE cloud_outbox SET last_attempt_at = NULL WHERE attempt_count > 0",
                [],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    fn make_remote_intent_state(
        &mut self,
        root_table: String,
        root_id: String,
    ) -> Result<Option<MakeRemoteIntentState>, DbError> {
        Database::make_remote_intent_state(self.conn, &root_table, &root_id)
    }

    fn finish_cancelled_blob_upload(&mut self, entry: OutboxEntry) -> Result<bool, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let finished =
            crate::CloudOutboxRecords::new(&transaction).finish_cancelled_upload(&entry)?;
        transaction.commit().map_err(DbError::from)?;
        Ok(finished)
    }
}

pub(super) fn take_published_blob_drop_intents_for_restoration_on(
    conn: &rusqlite::Connection,
    blobs: &[coven_protocol::blob::BlobRef],
    can_restore: impl Fn(&PublishedBlobDropIntent, bool) -> Result<bool, DbError>,
) -> Result<Vec<PublishedBlobDropIntent>, DbError> {
    let blobs = blobs
        .iter()
        .map(|blob| (blob.namespace.as_str(), blob.id.as_str()))
        .collect::<std::collections::BTreeSet<_>>();
    let mut taken = Vec::new();
    for (namespace, blob_id) in blobs {
        let intents = {
            let mut statement = conn
                .prepare(
                    "SELECT seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition,
                       (
                           EXISTS (
                               SELECT 1 FROM store_write_blob_leases
                               WHERE namespace = ?1 AND blob_id = ?2
                           ) OR EXISTS (
                               SELECT 1 FROM retained_replay_blob_leases
                               WHERE namespace = ?1 AND blob_id = ?2
                           )
                       )
                     FROM published_blob_drop_intents
                     WHERE namespace = ?1 AND blob_id = ?2
                     ORDER BY seq, locator_hash",
                )
                .map_err(DbError::from)?;
            let intents = statement
                .query_map((namespace, blob_id), |row| {
                    Ok((
                        row_to_published_blob_drop_intent(row)?,
                        row.get::<_, bool>(7)?,
                    ))
                })
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            intents
        };
        for (intent, leased) in intents {
            if !can_restore(&intent, leased)? {
                continue;
            }
            let removed = crate::with_coven_sql_authority(|| {
                conn.execute(
                    "DELETE FROM published_blob_drop_intents
                     WHERE seq = ?1 AND namespace = ?2 AND blob_id = ?3 AND locator_hash = ?4",
                    rusqlite::params![
                        i64::try_from(intent.seq).map_err(|_| DbError::Message(format!(
                            "published blob drop sequence {} exceeds SQLite integer range",
                            intent.seq
                        )))?,
                        intent.drop.namespace,
                        intent.drop.id,
                        intent.drop.locator_hash.to_string(),
                    ],
                )
                .map_err(DbError::from)
            })?;
            if removed != 1 {
                return Err(DbError::Message(format!(
                    "published blob drop intent changed while restoring {namespace}/{blob_id}"
                )));
            }
            taken.push(intent);
        }
    }
    Ok(taken)
}

pub(super) fn reinsert_published_blob_drop_intent_on(
    conn: &rusqlite::Connection,
    intent: &PublishedBlobDropIntent,
) -> Result<(), DbError> {
    let inserted = crate::with_coven_sql_authority(|| {
        conn.execute(
            "INSERT INTO published_blob_drop_intents
             (seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                i64::try_from(intent.seq).map_err(|_| DbError::Message(format!(
                    "published blob drop sequence {} exceeds SQLite integer range",
                    intent.seq
                )))?,
                intent.drop.namespace,
                intent.drop.id,
                i64::try_from(intent.drop.size).map_err(|_| DbError::Message(format!(
                    "published blob drop size {} exceeds SQLite integer range",
                    intent.drop.size
                )))?,
                intent.drop.plaintext_hash.to_string(),
                intent.drop.locator_hash.to_string(),
                intent.drop.disposition.as_db(),
            ],
        )
        .map_err(DbError::from)
    })?;
    if inserted != 1 {
        return Err(DbError::Message(
            "published blob drop intent was not restored".to_string(),
        ));
    }
    Ok(())
}

impl StoreDatabase {
    #[doc(hidden)]
    pub async fn cloud_outbox_snapshot(&self) -> Result<CloudOutboxSnapshot, DbError> {
        self.call_store(|session| session.cloud_outbox_snapshot())
            .await
    }

    #[doc(hidden)]
    pub async fn queued_uploads(&self) -> Result<Vec<QueuedUpload>, DbError> {
        self.queued_upload_rows(None).await
    }

    #[doc(hidden)]
    pub async fn queued_uploads_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<QueuedUpload>, DbError> {
        self.queued_upload_rows(Some((root_table.to_string(), root_id.to_string())))
            .await
    }

    async fn queued_upload_rows(
        &self,
        root: Option<(String, String)>,
    ) -> Result<Vec<QueuedUpload>, DbError> {
        self.call_store(move |session| session.queued_upload_rows(root))
            .await
    }

    pub async fn published_blob_drop_intents(
        &self,
        max_seq: u64,
    ) -> Result<Vec<PublishedBlobDropIntent>, DbError> {
        self.call_store(move |session| session.published_blob_drop_intents(max_seq))
            .await
    }

    pub async fn clear_published_blob_drop_intent(
        &self,
        intent: &PublishedBlobDropIntent,
    ) -> Result<(), DbError> {
        let seq = intent.seq;
        let namespace = intent.drop.namespace.clone();
        let id = intent.drop.id.clone();
        let locator_hash = intent.drop.locator_hash.to_string();
        self.call_store(move |session| {
            session.clear_published_blob_drop_intent(seq, namespace, id, locator_hash)
        })
        .await
    }

    pub async fn pending_blob_uploads(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.call_store(|session| session.pending_blob_uploads())
            .await
    }

    pub async fn mark_blob_upload_prepared(
        &self,
        entry: &OutboxEntry,
        authority: coven_protocol::audience_package::PackageAudience,
        stored: coven_protocol::blob::locator::StoredBlobRef,
        spool_path: std::path::PathBuf,
    ) -> Result<(), DbError> {
        let OutboxUpload { row, state, .. } = &entry.upload;
        if state != &OutboxUploadState::Pending {
            return Err(DbError::Message(
                "blob upload is already prepared".to_string(),
            ));
        }
        let locator = stored.locator();
        if !coven_protocol::blob::locator_describes_row(
            locator,
            row.blob(),
            row.plaintext_size(),
            row.plaintext_hash(),
        ) {
            return Err(DbError::Message(
                "prepared blob differs from its exact Local row version".to_string(),
            ));
        }
        if locator.audience() != authority.remote_audience() {
            return Err(DbError::Message(
                "prepared blob audience differs from its package authority".to_string(),
            ));
        }
        let prepared = OutboxUploadState::Prepared {
            authority,
            stored,
            spool_path,
        };
        let prepared_json = serde_json::to_string(&prepared)
            .map_err(|error| DbError::context("serialize prepared blob upload", error))?;
        let pending_json = serde_json::to_string(&OutboxUploadState::Pending)
            .map_err(|error| DbError::context("serialize pending blob upload", error))?;
        self.swap_blob_upload_state(
            entry.id,
            row,
            pending_json,
            prepared_json,
            "prepared-object handoff",
        )
        .await
    }

    pub async fn mark_blob_upload_created(&self, entry: &OutboxEntry) -> Result<(), DbError> {
        let OutboxUpload { row, state, .. } = &entry.upload;
        let OutboxUploadState::Prepared {
            authority,
            stored,
            spool_path,
        } = state
        else {
            return Err(DbError::Message(
                "cloud creation requires a prepared upload object".to_string(),
            ));
        };
        let created_json = serde_json::to_string(&OutboxUploadState::Created {
            authority: authority.clone(),
            stored: stored.clone(),
            spool_path: spool_path.clone(),
        })
        .map_err(|error| DbError::context("serialize created blob upload", error))?;
        let prepared_json = serde_json::to_string(state)
            .map_err(|error| DbError::context("serialize prepared blob upload identity", error))?;
        self.swap_blob_upload_state(
            entry.id,
            row,
            prepared_json,
            created_json,
            "cloud-created handoff",
        )
        .await
    }

    pub async fn record_outbox_failure(
        &self,
        entry: &OutboxEntry,
        failure: OutboxFailure,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        let entry = entry.clone();
        let attempted_at = attempted_at.to_string();
        self.call_store(move |session| session.record_outbox_failure(entry, failure, attempted_at))
            .await
    }

    async fn swap_blob_upload_state(
        &self,
        id: i64,
        row: &coven_protocol::blob::RowBlobRef,
        from: String,
        to: String,
        context: &'static str,
    ) -> Result<(), DbError> {
        let table = row.table().to_string();
        let row_id = row.row_id().to_string();
        let column = row.column().to_string();
        let row_stamp = row.row_stamp().to_string();
        self.call_store(move |session| {
            session.swap_blob_upload_state(id, table, row_id, column, row_stamp, from, to, context)
        })
        .await
    }

    pub async fn reset_outbox_backoff(&self) -> Result<(), DbError> {
        self.call_store(|session| session.reset_outbox_backoff())
            .await
    }

    pub async fn make_remote_intent_state(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<MakeRemoteIntentState>, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.call_store(move |session| session.make_remote_intent_state(root_table, root_id))
            .await
    }

    pub async fn make_remote_progress(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<crate::MakeRemoteProgress>, DbError> {
        Ok(self
            .make_remote_intent_state(root_table, root_id)
            .await?
            .map(|state| match state {
                MakeRemoteIntentState::Uploading => MakeRemoteProgress::Uploading,
                MakeRemoteIntentState::Cancelling => MakeRemoteProgress::Cancelling,
                MakeRemoteIntentState::Publishing(_) => MakeRemoteProgress::Publishing,
            }))
    }

    pub async fn finish_cancelled_blob_upload(&self, entry: &OutboxEntry) -> Result<bool, DbError> {
        let entry = entry.clone();
        self.call_store(move |session| session.finish_cancelled_blob_upload(entry))
            .await
    }
}

fn row_to_queued_upload(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueuedUpload> {
    let invalid = |index: usize, source: Box<dyn std::error::Error + Send + Sync>| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, source)
    };
    let encoded: String = row.get(0)?;
    let reference: coven_protocol::blob::RowBlobRef =
        serde_json::from_str(&encoded).map_err(|error| invalid(0, Box::new(error)))?;
    let state_json: String = row.get(5)?;
    let state: OutboxUploadState =
        serde_json::from_str(&state_json).map_err(|error| invalid(5, Box::new(error)))?;
    let (phase, provider_bytes_total) = match &state {
        OutboxUploadState::Pending => (QueuedUploadPhase::Pending, None),
        OutboxUploadState::Prepared { stored, .. } => (
            QueuedUploadPhase::Prepared,
            Some(stored.object().stored_size()),
        ),
        OutboxUploadState::Created { stored, .. } => (
            QueuedUploadPhase::Created,
            Some(stored.object().stored_size()),
        ),
    };
    let attempt_count: i64 = row.get(6)?;
    let last_failure = row
        .get::<_, Option<String>>(7)?
        .map(|encoded| serde_json::from_str(&encoded).map_err(|error| invalid(7, Box::new(error))))
        .transpose()?;
    Ok(QueuedUpload {
        blob: reference,
        root_table: row.get(1)?,
        root_id: row.get(2)?,
        root_label: row.get(3)?,
        retain_pinned: row.get(4)?,
        phase,
        provider_bytes_total,
        attempt_count: u64::try_from(attempt_count).map_err(|error| invalid(6, Box::new(error)))?,
        last_failure,
        created_at: row.get(8)?,
        last_attempt_at: row.get(9)?,
    })
}

fn row_to_published_blob_drop_intent(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<PublishedBlobDropIntent> {
    let size: Option<i64> = row.get(3)?;
    let size = size.ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "published blob drop intent is missing size",
            )),
        )
    })?;
    if size < 0 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("published blob drop intent has negative size {size}"),
            )),
        ));
    }
    let plaintext_hash = row.get::<_, String>(4)?.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let locator_hash = row.get::<_, String>(5)?.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let disposition_raw: String = row.get(6)?;
    let disposition = coven_protocol::blob::DeferredLocalBlobDisposition::from_db(&disposition_raw)
        .map_err(|message| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    message,
                )),
            )
        })?;
    Ok(PublishedBlobDropIntent {
        seq: row.get::<_, i64>(0)? as u64,
        drop: coven_protocol::blob::DeferredLocalBlobDrop {
            namespace: row.get(1)?,
            id: row.get(2)?,
            size: size as u64,
            plaintext_hash,
            locator_hash,
            disposition,
        },
    })
}
