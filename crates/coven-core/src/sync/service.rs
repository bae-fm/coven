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
use super::publish_blobs::{
    ensure_publishable_blobs, publish_blobs_from_changes, PublishBlobError,
};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeferredLocalBlobDisposition {
    Drop,
    Cache,
    Pin,
}

#[derive(Clone)]
pub struct DeferredLocalBlobDrop {
    pub namespace: String,
    pub id: String,
    pub size: u64,
    pub disposition: DeferredLocalBlobDisposition,
}

pub async fn complete_host_provided_make_remotes(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    timestamp: &str,
    library_dir: &LibraryDir,
    local_seq: u64,
) -> Result<bool, SyncCycleError> {
    let roots = ready_host_provided_make_remotes(db, tables).await?;
    let mut completed = false;
    for root in roots {
        // Upload each host-provided blob to the cloud before the gate flips, so the
        // blob is durable in the cloud the moment the row is shareable. The upload
        // is idempotent (it skips a blob already present), so the inline push
        // re-seeing this re-emitted blob does not re-upload it.
        //
        // The blob is now Remote, so its local-store copy (its Local home) must not
        // stay there — a Remote blob's bytes in the local store would read as Local.
        // What happens to that copy is a `CacheFill`/pin policy: pin keeps it in
        // `pinned/`, `CacheEager` moves it into the evictable cache, `CacheLazy`
        // drops it (the cloud has the bytes). That disposition is committed as a
        // durable intent inside the flip transaction below and applied by the
        // existing drain after the flip's changeset is published — so a crash
        // between the flip and the disposition replays it instead of stranding the
        // copy. A copy already in the cache (crash-recovery fallback) needs neither.
        let mut drops = Vec::new();
        for blob in &root.host_blobs {
            let uploaded = upload_host_provided_blob(db, storage, library_dir, blob).await?;
            if let Some(drop) = uploaded
                .deferred_local_blob_drop(local_blob_disposition(blob, root.intent.retain_pinned))
            {
                drops.push(drop);
            }
        }

        finish_host_provided_make_remote(db, root.clone(), local_seq, drops, timestamp.to_string())
            .await?;
        completed = true;
    }
    Ok(completed)
}

pub(super) async fn upload_snapshot_host_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    library_dir: &LibraryDir,
    blobs: &[BlobRef],
) -> Result<(), SyncCycleError> {
    for blob in blobs {
        let uploaded = upload_host_provided_blob(db, storage, library_dir, blob).await?;
        uploaded
            .apply_local_store_disposition(db, library_dir, local_blob_disposition(blob, false))
            .await?;
    }
    Ok(())
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
    // upload. After a successful upload the outgoing changeset carries the
    // local-store cleanup that runs only after the changeset is published.
    let mut deferred_local_blob_drops = Vec::new();
    if let Some(ref cs) = outgoing_cs {
        let changes = crate::changeset::walk(cs).map_err(SyncCycleError::AssetScan)?;
        let blob_decls = db.blob_decls();
        let publish_blobs = publish_blobs_from_changes(&blob_decls, &changes)?;
        let host_blobs = crate::sync::pull::host_provided_blobs(&blob_decls, &changes)
            .map_err(|e| SyncCycleError::AssetScan(e.to_string()))?;
        let make_remote_intents = make_remote_intents_for_blobs(db, &host_blobs).await?;
        let mut consumed_intents: HashSet<(String, String)> = HashSet::new();
        for blob in host_blobs {
            let intent = make_remote_intents.get(&(blob.namespace.clone(), blob.id.clone()));
            let retain_pinned = intent.is_some_and(|intent| intent.retain_pinned);
            let uploaded = upload_host_provided_blob(db, storage, library_dir, &blob).await?;
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
            if let Some(deferred) =
                uploaded.deferred_local_blob_drop(local_blob_disposition(&blob, retain_pinned))
            {
                deferred_local_blob_drops.push(deferred);
            }
        }
        if !consumed_intents.is_empty() {
            delete_make_remote_intents(db, consumed_intents).await?;
        }
        ensure_publishable_blobs(db, storage, &publish_blobs)
            .await
            .map_err(SyncCycleError::from)?;
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
            deferred_local_blob_drops,
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
    expected_size: u64,
    cleanup_local_store_after_publish: bool,
}

impl UploadedHostBlob {
    fn deferred_local_blob_drop(
        self,
        disposition: DeferredLocalBlobDisposition,
    ) -> Option<DeferredLocalBlobDrop> {
        self.cleanup_local_store_after_publish
            .then_some(DeferredLocalBlobDrop {
                namespace: self.blob.namespace,
                id: self.blob.id,
                size: self.expected_size,
                disposition,
            })
    }

    async fn apply_local_store_disposition(
        self,
        db: &Database,
        library_dir: &LibraryDir,
        disposition: DeferredLocalBlobDisposition,
    ) -> Result<(), SyncCycleError> {
        if self.cleanup_local_store_after_publish {
            apply_deferred_local_blob_drop(
                db,
                library_dir,
                &DeferredLocalBlobDrop {
                    namespace: self.blob.namespace,
                    id: self.blob.id,
                    size: self.expected_size,
                    disposition,
                },
            )
            .await?;
        }
        Ok(())
    }
}

async fn upload_host_provided_blob(
    db: &Database,
    storage: &dyn SyncStorage,
    library_dir: &LibraryDir,
    blob: &BlobRef,
) -> Result<UploadedHostBlob, SyncCycleError> {
    let expected_size = expected_blob_size(db, blob).await?;
    let exists = storage
        .blob_exists(&blob.namespace, &blob.id, blob.cloud_path.as_deref())
        .await
        .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
    if exists {
        let local = crate::blob::local_files::path_if_present(
            library_dir,
            &blob.namespace,
            &blob.id,
            expected_size,
        )
        .await
        .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
        return Ok(UploadedHostBlob {
            blob: blob.clone(),
            expected_size,
            cleanup_local_store_after_publish: local.is_some(),
        });
    }

    let local = match crate::blob::local_files::path_if_present(
        library_dir,
        &blob.namespace,
        &blob.id,
        expected_size,
    )
    .await
    {
        Ok(path) => path,
        Err(e) => {
            return Err(SyncCycleError::AssetUpload(format!(
                "reading local-store blob for {}: {e}",
                blob.id
            )));
        }
    };
    let was_in_local_store = local.is_some();
    let source_path = match local {
        Some(path) => path,
        None => match crate::blob::cache::staged_path(
            library_dir,
            &blob.namespace,
            &blob.id,
            expected_size,
        )
        .await
        {
            Ok(Some(path)) => path,
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
        .put_blob_from_file(
            &blob.namespace,
            &blob.id,
            resolved,
            blob.cloud_path.as_deref(),
            &source_path,
        )
        .await
        .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
    info!(id = %blob.id, namespace = %blob.namespace, "uploaded blob");

    Ok(UploadedHostBlob {
        blob: blob.clone(),
        expected_size,
        cleanup_local_store_after_publish: was_in_local_store,
    })
}

async fn expected_blob_size(db: &Database, blob: &BlobRef) -> Result<u64, SyncCycleError> {
    let decls = db.blob_decls();
    let namespace = blob.namespace.clone();
    let id = blob.id.clone();
    db.call(move |conn| {
        decls
            .size_for_blob_in_namespace(conn, &namespace, &id)
            .map_err(|e| crate::database::DbError(e.to_string()))
    })
    .await
    .map_err(|e| SyncCycleError::AssetScan(e.0))?
    .ok_or_else(|| {
        SyncCycleError::AssetScan(format!(
            "cannot read expected size for blob {}/{}: no carrying row",
            blob.namespace, blob.id
        ))
    })
}

pub async fn apply_deferred_local_blob_drop(
    db: &Database,
    library_dir: &LibraryDir,
    deferred: &DeferredLocalBlobDrop,
) -> Result<(), SyncCycleError> {
    let local = crate::blob::local_files::path_if_present(
        library_dir,
        &deferred.namespace,
        &deferred.id,
        deferred.size,
    )
    .await
    .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
    match (deferred.disposition, local) {
        (DeferredLocalBlobDisposition::Pin, Some(source)) => {
            let pinned = library_dir
                .pinned_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            crate::local_blob::copy_atomic(&source, &pinned)
                .await
                .map_err(SyncCycleError::AssetUpload)?;
        }
        (DeferredLocalBlobDisposition::Cache, Some(source)) => {
            crate::blob::cache::write_blob_from_file(
                db,
                library_dir,
                &deferred.namespace,
                &deferred.id,
                &source,
            )
            .await
            .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
        }
        (DeferredLocalBlobDisposition::Drop, _) => {}
        // The source is gone. This disposition (copy to a destination, then drop the
        // source) is applied in one step but its intent clears in a separate commit,
        // so a crash in that window leaves the blob correctly placed with the intent
        // still pending. Recognize that finished work by its destination — Ok clears
        // the intent — and fail loud only when the destination is ALSO empty.
        (DeferredLocalBlobDisposition::Pin, None) => {
            let pinned = library_dir
                .pinned_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            return recognize_applied_disposition_or_fail(&pinned, deferred).await;
        }
        (DeferredLocalBlobDisposition::Cache, None) => {
            let cached = library_dir
                .cache_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            return recognize_applied_disposition_or_fail(&cached, deferred).await;
        }
    }
    crate::blob::local_files::drop_blob(library_dir, &deferred.namespace, &deferred.id)
        .await
        .map(|_| ())
        .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))
}

/// A Pin/Cache disposition whose local-store source is gone is either already applied
/// — a prior drain copied the blob to `destination` and dropped the source, then
/// crashed before clearing its intent — or a genuine loss. Ok when the destination
/// holds the blob at its expected size (the work is done, so the caller clears the
/// intent); a loud Err when the destination is also empty (the bytes are gone, so the
/// intent stays pending and retries).
async fn recognize_applied_disposition_or_fail(
    destination: &std::path::Path,
    deferred: &DeferredLocalBlobDrop,
) -> Result<(), SyncCycleError> {
    let present = crate::local_blob::exists(destination)
        .await
        .map_err(SyncCycleError::AssetUpload)?
        && crate::local_blob::file_len(destination)
            .await
            .map_err(SyncCycleError::AssetUpload)?
            == deferred.size;
    if present {
        return Ok(());
    }
    Err(SyncCycleError::AssetUpload(format!(
        "published blob {}/{} is missing from both the local store and its {:?} destination",
        deferred.namespace, deferred.id, deferred.disposition
    )))
}

fn local_blob_disposition(blob: &BlobRef, retain_pinned: bool) -> DeferredLocalBlobDisposition {
    if retain_pinned {
        DeferredLocalBlobDisposition::Pin
    } else if blob.fill == CacheFill::CacheEager {
        DeferredLocalBlobDisposition::Cache
    } else {
        DeferredLocalBlobDisposition::Drop
    }
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
    local_seq: u64,
    drops: Vec<DeferredLocalBlobDrop>,
    stamp: String,
) -> Result<(), SyncCycleError> {
    let tables = db.synced_tables().to_vec();
    // The flip's changeset is published at the next sequence, so the local-store
    // disposition (keyed by that sequence) is applied only after the row that
    // shares the blob is durable — matching the inline-push path.
    let intent_seq = local_seq + 1;
    db.call_with_capture_reset(move |conn| {
        Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
            crate::sync::gate::write_gate(
                tx,
                &root.intent.root_table,
                &root.gate_column,
                true,
                &stamp,
                &root.intent.root_id,
            )
            .map_err(crate::database::DbError::from)?;
            for id in &root.user_blob_ids {
                Database::clear_external_blob_on(tx, id)?;
            }
            // Commit the local-store disposition as the authoritative record, in the
            // same transaction as the flip. The flip re-emits the subtree, so the
            // inline push (`sync`) also scans this host blob — but the make_remote
            // intent is gone by then (deleted below), so it can't recover
            // `retain_pinned` and would record the default disposition. The insert's
            // conflict clause (`DO NOTHING`) keeps this authoritative record, written
            // first, from being overwritten by that inline re-scan.
            for drop in &drops {
                super::cycle::insert_published_blob_drop_intent(tx, intent_seq, drop)?;
            }
            Database::delete_make_remote_intent_on(
                tx,
                &root.intent.root_table,
                &root.intent.root_id,
            )
        })
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

impl From<PublishBlobError> for SyncCycleError {
    fn from(e: PublishBlobError) -> Self {
        match e {
            PublishBlobError::LocalUserProvided { .. } | PublishBlobError::MissingRemote { .. } => {
                SyncCycleError::BlobMissing(e.to_string())
            }
            PublishBlobError::PackedChangeset(_) | PublishBlobError::ChangesetScan(_) => {
                SyncCycleError::AssetScan(e.to_string())
            }
            PublishBlobError::ExternalLookup { .. } | PublishBlobError::RemoteCheck { .. } => {
                SyncCycleError::AssetUpload(e.to_string())
            }
        }
    }
}
