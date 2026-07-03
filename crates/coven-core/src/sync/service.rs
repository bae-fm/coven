//! Full sync orchestrator: gate + push local changes, pull remote changes.
//!
//! Protocol within a cycle:
//! 1. The caller captured the outgoing changeset (resetting the recorded batch)
//!    and passes the bytes in. Capture stays ENABLED throughout.
//! 2. Gate the captured changeset (cut gated-false rows, re-emit on flip).
//! 3. Push our changeset's blobs, then build the signed envelope to push.
//! 4. Pull incoming changesets and apply them — the pull disables capture around
//!    only each apply (so applied rows aren't echoed) and re-enables it at once,
//!    so a host write landing during the network phases is still captured.
//! 5. The caller runs snapshot policy.
//!
//! All connection access goes through the owned [`Database`]; capture is never
//! suspended across the network steps — only the apply briefly disables it.

use std::collections::{HashMap, HashSet};

use rusqlite::OptionalExtension;
use tracing::{debug, error, info, warn};

use crate::blob::{BlobRef, CacheFill, Provenance};
use crate::database::Database;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::sync::session::SyncedTable;

use super::envelope;
use super::gate;
use super::membership::{self, MembershipCoord};
use super::pull::{self, PullResult};
use super::push::OutgoingChangeset;
use super::storage::SyncStorage;

/// Everything the caller needs after the gate + push-prep + pull steps.
pub struct SyncResult {
    /// The outgoing changeset bytes (if any local changes survived the gate).
    /// The caller is responsible for pushing this to the storage.
    pub outgoing: Option<OutgoingChangeset>,
    /// Pull results (how many incoming changesets were applied).
    pub pull: PullResult,
    /// Updated cursor map (caller should persist to sync_cursors table).
    pub updated_cursors: HashMap<String, u64>,
}

pub async fn complete_host_provided_make_remotes(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    timestamp: &str,
    library_dir: &LibraryDir,
) -> Result<bool, SyncCycleError> {
    let roots = ready_host_provided_make_remotes(db, tables).await?;
    let mut completed = false;
    for root in roots {
        let mut uploaded = Vec::new();
        for blob in &root.host_blobs {
            uploaded.push(
                upload_host_provided_blob(
                    db,
                    storage,
                    library_dir,
                    blob,
                    root.intent.retain_pinned,
                )
                .await?,
            );
        }

        finish_host_provided_make_remote(db, root.clone(), timestamp.to_string()).await?;

        // A user-provided blob is the user's own file, referenced in place.
        // make_remote uploads a copy to the cloud and drops the external ref
        // (`finish_host_provided_make_remote` above), leaving the blob
        // Remote/CacheLazy — but it NEVER deletes the user's original on disk.
        // Only host-provided local-store copies, whose bytes coven owns, are
        // dropped below.
        for uploaded in uploaded {
            uploaded.drop_local_store(library_dir).await?;
        }
        completed = true;
    }
    Ok(completed)
}

/// Gate the captured `outgoing` changeset, prepare its push envelope, and
/// pull remote changes.
///
/// `outgoing` is the changeset the caller captured via
/// `Database::take_changeset`; capture stays enabled, and the apply inside
/// `pull` disables it around only the apply, so the applied rows are not
/// re-recorded while host writes during the network steps are.
pub async fn sync(
    device_id: &str,
    db: &Database,
    tables: &[SyncedTable],
    outgoing: Vec<u8>,
    local_seq: u64,
    cursors: &HashMap<String, u64>,
    storage: &dyn SyncStorage,
    timestamp: &str,
    message: &str,
    keypair: &UserKeypair,
    library_dir: &LibraryDir,
) -> Result<SyncResult, SyncCycleError> {
    // Step 2: apply row-level sync gating. Cut gated-false rows (and their
    // FK-descendants) so they stay local; re-emit a root's full subtree when
    // its gate flips false→true. Runs on the owned connection; capture stays
    // enabled (gating reads current row state from the live tables, and the
    // pull disables capture only around its apply). Done before the blob scan
    // so blob upload sees the gated set, not the cut rows.
    let outgoing_cs: Option<Vec<u8>> = if outgoing.is_empty() {
        None
    } else {
        let gates = db.gates();
        let gated = db
            .call(move |conn| {
                gate::gate_outbound(conn, &outgoing, &gates)
                    .map_err(|e| crate::database::DbError(format!("gate outbound: {e}")))
            })
            .await
            .map_err(|e| SyncCycleError::Gate(e.0))?;
        if gated.is_empty() {
            None
        } else {
            Some(gated)
        }
    };

    // Step 3: upload the HOST-PROVIDED blobs the outgoing changeset references,
    // before the envelope, so pullers can fetch them as soon as they see the
    // change. coven owns a host-provided blob's bytes (in its local store while
    // Local, or its cache once moved), so it can upload one inline as its row
    // reaches a changeset — whether the row is ungated or just re-emitted by a
    // make_remote gate flip. The pull's blob-before-row invariant needs the blob
    // in the cloud first, and this path uploads in the same cycle. A
    // user-provided blob is the user's own file, uploaded only via the durable
    // outbox (make_remote, which reads the user's path), so it is intentionally
    // NOT uploaded here.
    //
    // The plaintext is read from the host-provided Local home — coven's local
    // store, where the host stored the blob when it wrote the row — falling back
    // to the cache (a prior cycle that uploaded but crashed before the move). A
    // blob absent from both means its row is not ready to publish — a missing
    // blob would make pullers 404 on it permanently (the seq advances; the row is
    // never a fresh INSERT again) — so the cycle aborts rather than skipping the
    // upload. After a successful upload the blob is Remote, so its local-store
    // copy moves into the cache (a cache copy is evictable + re-fetchable).
    if let Some(ref cs) = outgoing_cs {
        let changes = crate::changeset::walk(cs).map_err(SyncCycleError::AssetScan)?;
        let blob_decls = db.blob_decls();
        let host_blobs = crate::sync::pull::host_provided_blobs(&blob_decls, &changes);
        let make_remote_intents = make_remote_intents_for_blobs(db, &host_blobs).await?;
        let mut consumed_intents: HashSet<(String, String)> = HashSet::new();
        for blob in host_blobs {
            let intent = make_remote_intents.get(&(blob.namespace.clone(), blob.id.clone()));
            let retain_pinned = intent.is_some_and(|intent| intent.retain_pinned);
            let uploaded =
                upload_host_provided_blob(db, storage, library_dir, &blob, retain_pinned).await?;
            if let Some(intent) = intent {
                consumed_intents.insert((intent.root_table.clone(), intent.root_id.clone()));
            }

            // The blob is now Remote, so its local-store copy (its Local home)
            // must not stay there — a Remote blob's bytes in the local store
            // would read as Local. What happens to that copy is a CacheFill
            // policy, not a provenance one: `CacheEager` warms the cache so the
            // first read is a local hit (move the copy into the evictable cache);
            // `CacheLazy` drops it, since the cloud has the bytes and a later read
            // fetches them. Reached only on a successful upload, so the bytes are
            // durably in the cloud before we touch the local copy. A copy already
            // in the cache (crash-recovery fallback) needs neither.
            uploaded.drop_local_store(library_dir).await?;
        }
        if !consumed_intents.is_empty() {
            delete_make_remote_intents(db, consumed_intents).await?;
        }
    }

    // Bind the outgoing changeset to the membership entry that authorizes us
    // to write. A puller that has not yet seen that entry (membership entries
    // and changesets are separate, unordered object streams) fetches it by
    // this coordinate to resolve the gap, instead of judging us non-member and
    // skipping the changeset forever. Only needed when we actually publish.
    let membership_grant = match &outgoing_cs {
        Some(_) => resolve_write_grant(storage, keypair).await?,
        None => None,
    };

    let outgoing = outgoing_cs.map(|cs| {
        let next_seq = local_seq + 1;
        let packed = envelope::pack_signed(
            device_id,
            next_seq,
            db.schema_version(),
            message,
            timestamp,
            keypair,
            membership_grant,
            &cs,
        );
        OutgoingChangeset {
            packed,
            seq: next_seq,
        }
    });

    // Step 4 + 5: pull incoming changesets and apply them (the pull disables
    // capture around only each apply, so applied rows are not echoed).
    let (updated_cursors, pull_result) =
        pull::pull_changes(db, tables, storage, device_id, cursors, library_dir)
            .await
            .map_err(SyncCycleError::Pull)?;

    if pull_result.changesets_applied > 0 {
        info!(
            applied = pull_result.changesets_applied,
            devices = pull_result.devices_pulled,
            "pull complete"
        );
    }

    Ok(SyncResult {
        outgoing,
        pull: pull_result,
        updated_cursors,
    })
}

/// The storage coordinate of the membership entry that authorizes this device
/// to write, or `None` for a solo library (no membership chain). Embedded in
/// the outgoing changeset so a puller can resolve a membership-propagation gap.
///
/// A storage failure aborts the cycle rather than publishing a changeset with
/// no grant: a puller hitting the gap window would otherwise skip it as
/// non-member — the very loss this binding exists to prevent.
async fn resolve_write_grant(
    storage: &dyn SyncStorage,
    keypair: &UserKeypair,
) -> Result<Option<MembershipCoord>, SyncCycleError> {
    let entry_keys = storage
        .list_membership_entries()
        .await
        .map_err(|e| SyncCycleError::Membership(format!("list membership entries: {e}")))?;
    if entry_keys.is_empty() {
        return Ok(None);
    }
    let entries = super::membership_ops::download_entries(storage, &entry_keys)
        .await
        .map_err(SyncCycleError::Membership)?;
    let our_pubkey = hex::encode(keypair.public_key());
    Ok(membership::write_grant_coord(&entries, &our_pubkey))
}

#[derive(Clone)]
struct InlineMakeRemoteIntent {
    root_table: String,
    root_id: String,
    retain_pinned: bool,
}

#[derive(Clone)]
struct ReadyHostProvidedMakeRemote {
    intent: InlineMakeRemoteIntent,
    gate_column: String,
    user_blob_ids: Vec<String>,
    host_blobs: Vec<BlobRef>,
}

struct UploadedHostBlob {
    blob: BlobRef,
    was_in_local_store: bool,
}

impl UploadedHostBlob {
    async fn drop_local_store(self, library_dir: &LibraryDir) -> Result<(), SyncCycleError> {
        if self.was_in_local_store {
            crate::blob::local_files::drop_blob(library_dir, &self.blob.namespace, &self.blob.id)
                .await
                .map_err(|e| {
                    SyncCycleError::AssetUpload(format!(
                        "make_remote completed but dropping local-store blob {}/{} failed: {e}",
                        self.blob.namespace, self.blob.id
                    ))
                })?;
        }
        Ok(())
    }
}

async fn upload_host_provided_blob(
    db: &Database,
    storage: &dyn SyncStorage,
    library_dir: &LibraryDir,
    blob: &BlobRef,
    retain_pinned: bool,
) -> Result<UploadedHostBlob, SyncCycleError> {
    let local = match crate::blob::local_files::read(library_dir, &blob.namespace, &blob.id).await {
        Ok(bytes) => bytes,
        Err(e) => {
            return Err(SyncCycleError::AssetUpload(format!(
                "reading local-store blob for {}: {e}",
                blob.id
            )));
        }
    };
    let was_in_local_store = local.is_some();
    let bytes = match local {
        Some(bytes) => bytes,
        None => match crate::blob::cache::read_staged(library_dir, &blob.namespace, &blob.id).await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                error!(
                    id = %blob.id,
                    "host-provided blob is in neither the local store nor the cache; \
                     aborting push so the changeset is not published without its blob"
                );
                return Err(SyncCycleError::BlobMissing(format!(
                    "host-provided blob {} is in neither the local store nor the cache",
                    blob.id
                )));
            }
            Err(e) => {
                return Err(SyncCycleError::AssetUpload(format!(
                    "reading cached blob for {}: {e}",
                    blob.id
                )));
            }
        },
    };

    let resolved = db
        .resolve_blob_scope(blob.scope.clone())
        .await
        .map_err(|e| SyncCycleError::AssetUpload(e.0))?;
    storage
        .put_blob(
            &blob.namespace,
            &blob.id,
            resolved,
            blob.cloud_path.as_deref(),
            bytes.clone(),
        )
        .await
        .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
    info!(id = %blob.id, namespace = %blob.namespace, "uploaded blob");

    if retain_pinned {
        // The host-provided inline push already holds the plaintext in memory
        // (read from the local store / cache above), so pin it by writing those
        // bytes into the protected cache folder. The streaming outbox drain pins
        // from the source path instead (see `cache::populate_pinned`), never
        // holding the whole blob.
        let pinned = library_dir
            .pinned_blob_path(&blob.namespace, &blob.id)
            .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
        crate::local_blob::write_atomic(&pinned, &bytes)
            .await
            .map_err(SyncCycleError::AssetUpload)?;
    } else if was_in_local_store && blob.fill == CacheFill::CacheEager {
        crate::blob::cache::write_blob(db, library_dir, blob, &bytes)
            .await
            .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
    }

    Ok(UploadedHostBlob {
        blob: blob.clone(),
        was_in_local_store,
    })
}

async fn ready_host_provided_make_remotes(
    db: &Database,
    tables: &[SyncedTable],
) -> Result<Vec<ReadyHostProvidedMakeRemote>, SyncCycleError> {
    let gates = db.gates();
    let decls = db.blob_decls();
    let tables = tables.to_vec();
    db.call(move |conn| {
        let gate_columns: HashMap<String, String> = tables
            .iter()
            .filter_map(|table| {
                table
                    .gate_column()
                    .map(|column| (table.name().to_string(), column.to_string()))
            })
            .collect();
        let mut stmt = conn
            .prepare(
                "SELECT root_table, root_id, retain_pinned FROM blob_make_remote_intents \
                 ORDER BY root_table, root_id",
            )
            .map_err(crate::database::DbError::from)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            })
            .map_err(crate::database::DbError::from)?;
        let mut ready = Vec::new();
        for row in rows {
            let (root_table, root_id, retain_pinned) =
                row.map_err(crate::database::DbError::from)?;
            let refs = decls
                .refs_for_root(conn, &gates, &root_table, &root_id)
                .map_err(|e| crate::database::DbError(e.to_string()))?;
            let user_blob_ids: Vec<String> = refs
                .iter()
                .filter(|blob| blob.provenance == Provenance::UserProvided)
                .map(|blob| blob.id.clone())
                .collect();
            let host_blobs: Vec<BlobRef> = refs
                .into_iter()
                .filter(|blob| blob.provenance == Provenance::HostProvided)
                .collect();
            if host_blobs.is_empty() {
                warn!(
                    root_table = %root_table,
                    root_id = %root_id,
                    "make_remote intent has no host-provided blobs ready for inline completion"
                );
                continue;
            }
            if has_pending_upload(conn, &user_blob_ids)? {
                debug!(
                    root_table = %root_table,
                    root_id = %root_id,
                    user_blob_count = user_blob_ids.len(),
                    "make_remote intent is waiting for user-provided blob uploads"
                );
                continue;
            }
            let gate_column = gate_columns.get(&root_table).cloned().ok_or_else(|| {
                crate::database::DbError(format!(
                    "make_remote completion: gated root {root_table} has no gate column"
                ))
            })?;
            ready.push(ReadyHostProvidedMakeRemote {
                intent: InlineMakeRemoteIntent {
                    root_table,
                    root_id,
                    retain_pinned,
                },
                gate_column,
                user_blob_ids,
                host_blobs,
            });
        }
        Ok(ready)
    })
    .await
    .map_err(|e| SyncCycleError::AssetScan(e.0))
}

fn has_pending_upload(
    conn: &rusqlite::Connection,
    blob_ids: &[String],
) -> Result<bool, crate::database::DbError> {
    for id in blob_ids {
        let pending = conn
            .query_row(
                "SELECT 1 FROM cloud_outbox WHERE operation = 'upload' AND file_id = ?1 LIMIT 1",
                [id],
                |_| Ok(()),
            )
            .optional()
            .map_err(crate::database::DbError::from)?
            .is_some();
        if pending {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn finish_host_provided_make_remote(
    db: &Database,
    root: ReadyHostProvidedMakeRemote,
    stamp: String,
) -> Result<(), SyncCycleError> {
    db.call(move |conn| {
        let tx = conn.unchecked_transaction()?;
        crate::sync::gate::write_gate(
            &tx,
            &root.intent.root_table,
            &root.gate_column,
            true,
            &stamp,
            &root.intent.root_id,
        )
        .map_err(crate::database::DbError::from)?;
        for id in &root.user_blob_ids {
            Database::clear_external_blob_on(&tx, id)?;
        }
        Database::delete_make_remote_intent_on(&tx, &root.intent.root_table, &root.intent.root_id)?;
        tx.commit().map_err(crate::database::DbError::from)
    })
    .await
    .map_err(|e| SyncCycleError::AssetUpload(e.0))
}

async fn make_remote_intents_for_blobs(
    db: &Database,
    blobs: &[BlobRef],
) -> Result<HashMap<(String, String), InlineMakeRemoteIntent>, SyncCycleError> {
    if blobs.is_empty() {
        return Ok(HashMap::new());
    }
    let gates = db.gates();
    let decls = db.blob_decls();
    let blobs = blobs.to_vec();
    db.call(move |conn| {
        let mut out = HashMap::new();
        for blob in blobs {
            let Some((table, pk)) = decls
                .row_for_blob_in_namespace(conn, &blob.namespace, &blob.id)
                .map_err(|e| crate::database::DbError(e.to_string()))?
            else {
                continue;
            };
            let Some((root_table, root_id)) = gates
                .resolve_root_of(conn, &table, &pk)
                .map_err(|e| crate::database::DbError(e.to_string()))?
            else {
                continue;
            };
            let Some(retain_pinned) =
                Database::make_remote_intent_retain_pinned(conn, &root_table, &root_id)?
            else {
                continue;
            };
            out.insert(
                (blob.namespace, blob.id),
                InlineMakeRemoteIntent {
                    root_table,
                    root_id,
                    retain_pinned,
                },
            );
        }
        Ok(out)
    })
    .await
    .map_err(|e| SyncCycleError::AssetScan(e.0))
}

async fn delete_make_remote_intents(
    db: &Database,
    roots: HashSet<(String, String)>,
) -> Result<(), SyncCycleError> {
    db.call(move |conn| {
        let tx = conn.unchecked_transaction()?;
        for (root_table, root_id) in roots {
            Database::delete_make_remote_intent_on(&tx, &root_table, &root_id)?;
        }
        tx.commit().map_err(crate::database::DbError::from)
    })
    .await
    .map_err(|e| SyncCycleError::AssetUpload(e.0))
}

#[derive(Debug)]
pub enum SyncCycleError {
    Gate(String),
    Pull(pull::PullError),
    AssetScan(String),
    AssetUpload(String),
    /// An outgoing changeset references a blob whose local file is missing, so the
    /// changeset cannot be published without stranding pullers on a 404.
    BlobMissing(String),
    /// The membership chain could not be loaded to bind the outgoing changeset to
    /// the entry that authorizes this device — publishing without it risks pullers
    /// skipping the changeset as non-member.
    Membership(String),
}

impl std::fmt::Display for SyncCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncCycleError::Gate(e) => write!(f, "gate error: {e}"),
            SyncCycleError::Pull(e) => write!(f, "pull error: {e}"),
            SyncCycleError::AssetScan(e) => write!(f, "asset scan error: {e}"),
            SyncCycleError::AssetUpload(e) => write!(f, "asset upload error: {e}"),
            SyncCycleError::BlobMissing(e) => write!(f, "blob missing: {e}"),
            SyncCycleError::Membership(e) => write!(f, "membership error: {e}"),
        }
    }
}

impl std::error::Error for SyncCycleError {}
