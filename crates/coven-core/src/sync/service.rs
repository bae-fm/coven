//! Full sync orchestrator: gate + push local changes, pull remote changes.
//!
//! Protocol within a cycle:
//! 1. The caller drained the outgoing changeset from the pending-changeset journal
//!    and passes the bytes in.
//! 2. Gate the captured changeset (cut gated-false rows, re-emit on flip).
//! 3. Push our changeset's blobs, then build the signed envelope to push.
//! 4. Pull incoming changesets and apply them. An apply is a plain connection
//!    write, never a journaled one, so applied rows are never recorded as this
//!    device's own outgoing changes — while a host write during the network phases
//!    journals normally.
//! 5. The caller runs snapshot policy.
//!
//! All connection access goes through the owned [`Database`]; only a host write
//! wrapped in a journaled transaction is ever captured, so applies need no special
//! handling.

use std::collections::HashSet;

use tracing::{debug, error, info, warn};

use crate::blob::{BlobRef, CacheFill, Provenance};
use crate::database::{
    Database, StoreBatchCompletion, StoreBatchLocalCleanup, StoreBlobManifest,
    StoreConsumedMakeRemoteIntent, StoreWriteBlobFact, StoreWriteBlobFacts,
    StoreWriteHostBlobState, StoreWriteUserBlobState,
};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::store_dir::StoreDir;
use crate::sync::session::SyncedTable;

use super::membership::{MembershipChain, MembershipCoord};
use super::storage::{StorageError, SyncStorage};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeferredLocalBlobDisposition {
    Drop,
    Cache,
    Pin,
}

impl DeferredLocalBlobDisposition {
    pub(crate) fn as_db(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Cache => "cache",
            Self::Pin => "pin",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeferredLocalBlobDrop {
    pub namespace: String,
    pub id: String,
    pub size: u64,
    pub disposition: DeferredLocalBlobDisposition,
}

pub(crate) struct PreparedStorePayload {
    pub blob_manifest: StoreBlobManifest,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
    pub membership_grant: Option<MembershipCoord>,
}

/// Upload the blobs referenced by exact staged package bytes and persist every
/// fact needed to retry their publication without re-deriving it from later rows.
pub(crate) async fn prepare_store_payload(
    db: &Database,
    storage: &dyn SyncStorage,
    blob_facts: &StoreWriteBlobFacts,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership_chain: Option<&MembershipChain>,
    cancel: Option<&HostUploadCloud<'_>>,
) -> Result<PreparedStorePayload, SyncCycleError> {
    let mut drops = Vec::new();
    let mut consumed = HashSet::new();
    let mut manifest = Vec::with_capacity(blob_facts.blobs.len());
    for fact in &blob_facts.blobs {
        let blob = fact.blob();
        match fact {
            StoreWriteBlobFact::UserProvided { state, .. } => {
                if *state == StoreWriteUserBlobState::Local {
                    return Err(SyncCycleError::LocalUserBlob {
                        namespace: blob.namespace.clone(),
                        id: blob.id.clone(),
                    });
                }
                let exists = storage
                    .blob_exists(&blob.namespace, &blob.id, blob.cloud_path.as_deref())
                    .await
                    .map_err(|source| SyncCycleError::Storage {
                        operation: "check user-provided blob",
                        source,
                    })?;
                if !exists {
                    return Err(SyncCycleError::MissingBlob {
                        namespace: blob.namespace.clone(),
                        id: blob.id.clone(),
                    });
                }
            }
            StoreWriteBlobFact::HostProvided { size, state, .. } => {
                let retain_pinned = match state {
                    StoreWriteHostBlobState::Ordinary => false,
                    StoreWriteHostBlobState::MakeRemote {
                        root_table,
                        root_id,
                        retain_pinned,
                    } => {
                        consumed.insert((root_table.clone(), root_id.clone()));
                        *retain_pinned
                    }
                };
                let uploaded =
                    upload_host_provided_blob_exact(db, storage, store_dir, blob, *size, cancel)
                        .await?;
                if let Some(drop) =
                    uploaded.deferred_local_blob_drop(local_blob_disposition(blob, retain_pinned))
                {
                    drops.push(drop);
                }
            }
        }
        manifest.push(blob.clone());
    }
    manifest.sort_by(|left, right| (&left.namespace, &left.id).cmp(&(&right.namespace, &right.id)));
    let mut consumed: Vec<_> = consumed
        .into_iter()
        .map(|(root_table, root_id)| StoreConsumedMakeRemoteIntent {
            root_table,
            root_id,
        })
        .collect();
    consumed.sort_by(|left, right| {
        (&left.root_table, &left.root_id).cmp(&(&right.root_table, &right.root_id))
    });

    Ok(PreparedStorePayload {
        blob_manifest: StoreBlobManifest { blobs: manifest },
        local_cleanup: StoreBatchLocalCleanup { drops },
        completion: StoreBatchCompletion {
            consumed_make_remote_intents: consumed,
        },
        membership_grant: resolve_write_grant(membership_chain, keypair),
    })
}

pub async fn complete_host_provided_make_remotes(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    timestamp: &str,
    store_dir: &StoreDir,
    local_seq: u64,
    routing_encryption: Option<&EncryptionService>,
    cancel: Option<&HostUploadCloud<'_>>,
) -> Result<bool, SyncCycleError> {
    Database::validate_store_write_routing(
        db.gates().as_ref(),
        db.write_policy(),
        routing_encryption,
    )
    .map_err(|error| SyncCycleError::AssetUpload(error.into_message()))?;
    let roots = ready_host_provided_make_remotes(db, tables).await?;
    let mut completed = false;
    for root in roots {
        // Upload each host-provided blob to the cloud before the gate flips, so the
        // blob is durable in the cloud the moment the row is shareable. The upload
        // is idempotent (it skips a blob whose bytes already stand at its cloud key),
        // so the inline push re-seeing this re-emitted blob does not re-upload it.
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
            let uploaded = upload_host_provided_blob(db, storage, store_dir, blob, cancel).await?;
            if let Some(drop) = uploaded
                .deferred_local_blob_drop(local_blob_disposition(blob, root.intent.retain_pinned))
            {
                drops.push(drop);
            }
        }

        finish_host_provided_make_remote(
            db,
            root.clone(),
            local_seq,
            drops,
            timestamp.to_string(),
            routing_encryption.cloned(),
        )
        .await?;
        completed = true;
    }
    Ok(completed)
}

pub(super) async fn upload_snapshot_host_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    blobs: &[BlobRef],
    cancel: Option<&HostUploadCloud<'_>>,
) -> Result<(), SyncCycleError> {
    for blob in blobs {
        let uploaded = upload_host_provided_blob(db, storage, store_dir, blob, cancel).await?;
        uploaded
            .apply_local_store_disposition(db, store_dir, local_blob_disposition(blob, false))
            .await?;
    }
    Ok(())
}

/// The storage coordinate of the membership entry that authorizes this device
/// to write. `None` means a pre-initialization caller supplied no chain or the
/// current identity has no write grant; an initialized authorized writer has a
/// coordinate.
/// Embedded in the outgoing changeset so a puller can resolve a
/// membership-propagation gap. Read off the cycle's once-loaded chain, so it
/// judges the same membership state as the rest of the cycle rather than
/// re-listing (the very disagreement that once had a puller skip the write it was
/// meant to accept).
fn resolve_write_grant(
    membership_chain: Option<&MembershipChain>,
    keypair: &UserKeypair,
) -> Option<MembershipCoord> {
    let our_pubkey = hex::encode(keypair.public_key());
    membership_chain.and_then(|chain| chain.write_grant_coord(&our_pubkey))
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
        store_dir: &StoreDir,
        disposition: DeferredLocalBlobDisposition,
    ) -> Result<(), SyncCycleError> {
        if self.cleanup_local_store_after_publish {
            apply_deferred_local_blob_drop(
                db,
                store_dir,
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

/// The cloud handle, object-key suffix, and timestamp the inline host-provided upload
/// path needs to cancel a pending tombstone after a successful (re-)upload. Built once
/// per cycle when a cloud home is present; `None` on a cloud-less run, which has no
/// tombstones to cancel.
pub struct HostUploadCloud<'a> {
    pub cloud_home: &'a dyn crate::storage::cloud::CloudHome,
    pub suffix: &'a str,
    pub now_rfc: &'a str,
}

/// Cancel any tombstone standing for `blob`'s cloud key after this path re-uploaded
/// (or found already-present) the blob, mirroring what the outbox drain does on every
/// upload — so a re-shared host blob a prior make_local tombstoned is not reclaimed by
/// a GC that outraces the re-upload. [`upload_host_provided_blob`] calls this however it
/// left the blob's cloud object: the skip is the live case when the blob is still within
/// its deletion grace. The cloud key comes from the home itself ([`SyncStorage::blob_cloud_key`]),
/// so it names exactly where the (re-)upload wrote the blob and where any tombstone for
/// it sits — not a re-derivation that could disagree with the home's own keying.
async fn cancel_host_blob_tombstone(
    db: &Database,
    storage: &dyn SyncStorage,
    blob: &BlobRef,
    cancel: Option<&HostUploadCloud<'_>>,
) -> Result<(), SyncCycleError> {
    let Some(cancel) = cancel else {
        return Ok(());
    };
    let cloud_key = storage
        .blob_cloud_key(&blob.namespace, &blob.id, blob.cloud_path.as_deref())
        .map_err(|source| SyncCycleError::Storage {
            operation: "form host-provided blob key",
            source,
        })?;
    crate::blob::delete::cancel_tombstone_or_enqueue(
        db,
        cancel.cloud_home,
        cancel.suffix,
        &cloud_key,
        cancel.now_rfc,
    )
    .await
    .map_err(|e| SyncCycleError::AssetUpload(e.into_message()))
}

/// Where this device holds a host-provided blob's plaintext, if it holds it at all.
enum LocalHostBlob {
    /// coven's local store — the blob's Local home, where the host put it. Once the
    /// blob is Remote this copy must not stay there (it would read as Local), so the
    /// push carries a disposition for it.
    LocalStore(std::path::PathBuf),
    /// The cache: a prior cycle that uploaded but crashed before moving the copy, or a
    /// Remote blob whose bytes this device pulled. Nothing to dispose of — the cache is
    /// where a Remote blob's copy belongs.
    Cache(std::path::PathBuf),
}

impl LocalHostBlob {
    fn path(&self) -> &std::path::Path {
        match self {
            LocalHostBlob::LocalStore(path) | LocalHostBlob::Cache(path) => path,
        }
    }

    fn in_local_store(&self) -> bool {
        matches!(self, LocalHostBlob::LocalStore(_))
    }
}

/// Find the blob's local plaintext: its Local home first, then the cache.
async fn local_host_blob(
    store_dir: &StoreDir,
    blob: &BlobRef,
    expected_size: u64,
) -> Result<Option<LocalHostBlob>, SyncCycleError> {
    if let Some(path) = crate::blob::local_files::path_if_present(
        store_dir,
        &blob.namespace,
        &blob.id,
        expected_size,
    )
    .await
    .map_err(|e| {
        SyncCycleError::AssetUpload(format!("reading local-store blob for {}: {e}", blob.id))
    })? {
        return Ok(Some(LocalHostBlob::LocalStore(path)));
    }
    let cached =
        crate::blob::cache::staged_path(store_dir, &blob.namespace, &blob.id, expected_size)
            .await
            .map_err(|e| {
                SyncCycleError::AssetUpload(format!("reading cached blob for {}: {e}", blob.id))
            })?;
    Ok(cached.map(LocalHostBlob::Cache))
}

async fn upload_host_provided_blob(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    blob: &BlobRef,
    cancel: Option<&HostUploadCloud<'_>>,
) -> Result<UploadedHostBlob, SyncCycleError> {
    let expected_size = expected_blob_size(db, blob).await?;
    upload_host_provided_blob_exact(db, storage, store_dir, blob, expected_size, cancel).await
}

async fn upload_host_provided_blob_exact(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    blob: &BlobRef,
    expected_size: u64,
    cancel: Option<&HostUploadCloud<'_>>,
) -> Result<UploadedHostBlob, SyncCycleError> {
    let local = local_host_blob(store_dir, blob, expected_size).await?;
    // Every cloud key names the blob standing at it — the hashed scheme carries the id in
    // the key, and a browsable home's readable path is required to name it
    // ([`crate::sync::cloud_storage::CloudSyncStorage::blob_key`]). So no two blobs share
    // a key, no object is ever overwritten by a different blob, and an object standing at
    // THIS blob's key holds THIS blob's bytes: presence is proof of content, and there is
    // nothing to upload. A repointed row whose cloud path did not move fails to key at
    // all, so it never reaches this skip.
    let uploaded = storage
        .blob_exists(&blob.namespace, &blob.id, blob.cloud_path.as_deref())
        .await
        .map_err(|source| SyncCycleError::Storage {
            operation: "check host-provided blob",
            source,
        })?;

    // Upload iff the blob is not in the cloud AND coven holds the bytes to put there.
    // The two absences are different: a blob coven does not hold was never coven's to
    // publish — a host-provided blob's bytes live in coven's own local store or cache
    // while this device owns them, so a device with no copy did not author this content.
    // Its row came from a peer, and a peer uploads a blob before publishing the row that
    // names it, so the object standing at the key IS the row's content and there is
    // nothing to push. With no object either, the row would publish pointing at nothing,
    // which is the abort below.
    match (&local, uploaded) {
        (_, true) => {}
        (Some(local), false) => {
            storage
                .put_blob_from_file(
                    &blob.namespace,
                    &blob.id,
                    blob.scope.clone(),
                    blob.cloud_path.as_deref(),
                    local.path(),
                )
                .await
                .map_err(|source| SyncCycleError::Storage {
                    operation: "upload host-provided blob",
                    source,
                })?;
            info!(id = %blob.id, namespace = %blob.namespace, "uploaded blob");
        }
        (None, false) => {
            error!(
                id = %blob.id,
                "host-provided blob is in neither the local store nor the cache; \
                 aborting push so the changeset is not published without its blob"
            );
            return Err(SyncCycleError::MissingBlob {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
            });
        }
    }

    // A (re-)upload — and a blob already in the cloud — wins over any pending deletion
    // of this key: cancel a tombstone a prior make_local left, so a GC past the grace
    // won't reclaim what stands there now.
    cancel_host_blob_tombstone(db, storage, blob, cancel).await?;
    record_self_upload(db, storage, blob).await?;

    Ok(UploadedHostBlob {
        blob: blob.clone(),
        expected_size,
        cleanup_local_store_after_publish: local.is_some_and(|local| local.in_local_store()),
    })
}

/// Record in the local uploader index that this device uploaded `blob`, so a later
/// self-read after a cache eviction keys it under us without a listing scan. A
/// no-op on a browsable home (no uploader segment). The upload has already
/// succeeded when this runs, so a failure fails the push loudly and the whole
/// (idempotent) upload retries — the record then lands on the retry.
async fn record_self_upload(
    db: &Database,
    storage: &dyn SyncStorage,
    blob: &BlobRef,
) -> Result<(), SyncCycleError> {
    if let Some(uploader) = storage.own_uploader() {
        db.record_blob_uploader(&blob.namespace, &blob.id, &uploader)
            .await
            .map_err(|e| SyncCycleError::AssetUpload(e.into_message()))?;
    }
    Ok(())
}

async fn expected_blob_size(db: &Database, blob: &BlobRef) -> Result<u64, SyncCycleError> {
    let decls = db.blob_decls();
    let namespace = blob.namespace.clone();
    let id = blob.id.clone();
    db.call(move |conn| {
        decls
            .size_for_blob_in_namespace(conn, &namespace, &id)
            .map_err(|e| crate::database::DbError::Message(e.to_string()))
    })
    .await
    .map_err(|e| SyncCycleError::AssetScan(e.into_message()))?
    .ok_or_else(|| {
        SyncCycleError::AssetScan(format!(
            "cannot read expected size for blob {}/{}: no carrying row",
            blob.namespace, blob.id
        ))
    })
}

pub async fn apply_deferred_local_blob_drop(
    db: &Database,
    store_dir: &StoreDir,
    deferred: &DeferredLocalBlobDrop,
) -> Result<(), SyncCycleError> {
    let local = crate::blob::local_files::path_if_present(
        store_dir,
        &deferred.namespace,
        &deferred.id,
        deferred.size,
    )
    .await
    .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
    match (deferred.disposition, local) {
        (DeferredLocalBlobDisposition::Pin, Some(source)) => {
            let pinned = store_dir
                .pinned_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            crate::local_blob::copy_atomic(&source, &pinned)
                .await
                .map_err(SyncCycleError::AssetUpload)?;
        }
        (DeferredLocalBlobDisposition::Cache, Some(source)) => {
            crate::blob::cache::write_blob_from_file(
                db,
                store_dir,
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
            let pinned = store_dir
                .pinned_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            return recognize_applied_disposition_or_fail(&pinned, deferred).await;
        }
        (DeferredLocalBlobDisposition::Cache, None) => {
            let cached = store_dir
                .cache_blob_path(&deferred.namespace, &deferred.id)
                .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
            return recognize_applied_disposition_or_fail(&cached, deferred).await;
        }
    }
    crate::blob::local_files::drop_blob(store_dir, &deferred.namespace, &deferred.id)
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
        let gate_columns = crate::blob::transition::gate_columns(&tables);
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
                .map_err(|e| crate::database::DbError::Message(e.to_string()))?;
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
            if crate::blob::transition::pending_upload_exists(conn, &user_blob_ids, None)? {
                debug!(
                    root_table = %root_table,
                    root_id = %root_id,
                    user_blob_count = user_blob_ids.len(),
                    "make_remote intent is waiting for user-provided blob uploads"
                );
                continue;
            }
            let gate_column = gate_columns.get(&root_table).cloned().ok_or_else(|| {
                crate::database::DbError::Message(format!(
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
    .map_err(|e| SyncCycleError::AssetScan(e.into_message()))
}

async fn finish_host_provided_make_remote(
    db: &Database,
    root: ReadyHostProvidedMakeRemote,
    local_seq: u64,
    drops: Vec<DeferredLocalBlobDrop>,
    stamp: String,
    routing_encryption: Option<EncryptionService>,
) -> Result<(), SyncCycleError> {
    let tables = db.synced_tables().to_vec();
    // The flip's changeset is published at the next sequence, so the local-store
    // disposition (keyed by that sequence) is applied only after the row that
    // shares the blob is durable — matching the inline-push path.
    let intent_seq = local_seq + 1;
    let write_id = db.new_write_id();
    let write_policy = db.write_policy();
    db.call(move |conn| {
        crate::blob::transition::commit_make_remote_flip(
            conn,
            &tables,
            write_policy,
            routing_encryption.as_ref(),
            write_id,
            &root.intent.root_table,
            &root.gate_column,
            &root.intent.root_id,
            &stamp,
            &root.user_blob_ids,
            crate::blob::transition::MakeRemoteCompletion::Dispositions { intent_seq, drops },
        )
    })
    .await
    .map_err(|e| SyncCycleError::AssetUpload(e.into_message()))
}

#[derive(Debug)]
pub enum SyncCycleError {
    Gate(String),
    AssetScan(String),
    AssetUpload(String),
    Storage {
        operation: &'static str,
        source: StorageError,
    },
    /// An outgoing changeset still names a user-owned local file.
    LocalUserBlob {
        namespace: String,
        id: String,
    },
    /// An outgoing changeset references bytes that are absent from their required
    /// publication location.
    MissingBlob {
        namespace: String,
        id: String,
    },
}

impl std::fmt::Display for SyncCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncCycleError::Gate(e) => write!(f, "gate error: {e}"),
            SyncCycleError::AssetScan(e) => write!(f, "asset scan error: {e}"),
            SyncCycleError::AssetUpload(e) => write!(f, "asset upload error: {e}"),
            SyncCycleError::Storage { operation, source } => {
                write!(f, "{operation}: {source}")
            }
            SyncCycleError::LocalUserBlob { namespace, id } => {
                write!(
                    f,
                    "user-provided blob {namespace}/{id} still has a local external ref"
                )
            }
            SyncCycleError::MissingBlob { namespace, id } => {
                write!(
                    f,
                    "blob {namespace}/{id} is absent from its publication location"
                )
            }
        }
    }
}

impl std::error::Error for SyncCycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage { source, .. } => Some(source),
            Self::Gate(_)
            | Self::AssetScan(_)
            | Self::AssetUpload(_)
            | Self::LocalUserBlob { .. }
            | Self::MissingBlob { .. } => None,
        }
    }
}
