/// Membership and blob helpers shared by Store pull and bootstrap.
use tracing::{debug, warn};

use super::conflict::TableSchema;
use super::hlc::Timestamp;
use super::membership::MembershipChain;
use super::storage::SyncStorage;
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup::LocalBlobCleanupIntent;
use crate::blob::CacheFill;
use crate::changeset::RowChange;
use crate::database::Database;
use crate::store_dir::StoreDir;

/// The membership state one sync cycle judges every authorization against, loaded
/// and anchored once at the top of the cycle and threaded to the pull, the
/// write-grant binding, the snapshot-authorization decision, and the tombstone GC.
/// Loading it once (instead of each of those sites re-listing and re-downloading
/// the chain) both saves the round-trips and — more importantly — makes every
/// authorization decision in the cycle judge the *same* chain state, so two reads
/// can't disagree mid-cycle. Because [`load_anchored_chain`] writes the reader's
/// per-author-stream head watermark, loading once also writes each watermark once.
///
/// [`load_anchored_chain`]: super::membership_ops::load_anchored_chain
pub struct CycleMembership {
    /// The owner-anchored, committed chain. `None` is representable for
    /// pre-initialization bootstrap callers; initialized cycles always carry one.
    pub chain: Option<MembershipChain>,
    /// The store's pinned owner (issue #102). `None` only before initialization.
    /// An owner-pinned store that fails to produce a valid chain aborts the load,
    /// so `Some(owner)` here always travels with `Some(chain)`.
    pub pinned_owner: Option<String>,
    /// The raw membership listing this cycle read (one `list_membership_entries`).
    /// The key refresh reads the visible activation coordinates from it, which is
    /// the LIST view (an entry is visible as soon as it is listed), distinct from
    /// the committed chain (an entry is committed only once a head certifies it).
    pub listed_entries: Vec<super::membership::MembershipCoord>,
    pub discovery_proof: MembershipDiscoveryProof,
}

#[derive(Debug, Clone, Copy)]
pub struct MembershipDiscoveryProof;

/// Load and anchor the cycle's membership chain once. Every successful listing
/// is validated; a loader error aborts regardless of owner pin because an
/// unpinned database may already have accepted author floors. Only the loader's
/// explicit `Ok(None)` represents an unpinned pre-initialization read. LIST
/// transport errors retain their separate pinned/unpinned classification.
pub async fn load_cycle_membership(
    storage: &dyn SyncStorage,
    db: &Database,
) -> Result<CycleMembership, PullError> {
    let pinned_owner = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| PullError::Apply(format!("read pinned owner: {error}")))?;
    let root = db
        .local_store_root_ref()
        .await
        .map_err(|error| PullError::Apply(format!("read Store root reference: {error}")))?
        .ok_or_else(|| PullError::Apply("Store root reference is absent".to_string()))?;
    let root_value = super::store_objects::load_store_protocol_root(storage, &root)
        .await
        .map_err(PullError::MembershipObject)?
        .value;
    let owner = pinned_owner
        .clone()
        .unwrap_or_else(|| root_value.descriptor.founder_pubkey.clone());
    let chain = super::membership_ops::load_and_persist_owner_anchor(storage, &root, &owner, db)
        .await
        .map_err(|error| match error {
            super::membership_ops::AnchoredChainError::StorageUnavailable { .. } => {
                PullError::MembershipLoad(error)
            }
            _ => PullError::MembershipTampered(error.to_string()),
        })?;
    let listed_entries = chain.author_heads();
    Ok(CycleMembership {
        chain: Some(chain),
        pinned_owner: Some(owner),
        listed_entries,
        discovery_proof: MembershipDiscoveryProof,
    })
}

/// Advance `max` past the greatest `_updated_at` among `changes`, parsing each
/// as an HLC [`Timestamp`]. A row whose `_updated_at` fails to parse is logged
/// and skipped — it must not panic the pull or silently default the clock.
///
/// `max` becomes the value the caller advances the local HLC past, and that
/// advance is deliberately uncapped (it trusts a value already written to disk).
/// So the bound lives here, at the point a stamp is *collected*: a grossly-future
/// stamp — beyond `receiver_wall_ms` + [`super::hlc::MAX_FUTURE_SKEW_MS`] — is
/// logged and skipped, so it can never ratchet the clock. A conflicting row with
/// such a stamp was already refused by the apply, but a *non-conflicting* INSERT
/// (no local row to conflict with) reaches here as an applied row, so this is the
/// gate that stops it from dragging the clock forward.
pub(super) fn advance_max_updated_at(
    max: &mut Option<Timestamp>,
    changes: &[RowChange],
    schema: &TableSchema,
    receiver_wall_ms: u64,
) {
    for change in changes {
        let Some(idx) = schema.updated_at(&change.table) else {
            // Incoming apply rejects the entire changeset before mutation when any
            // operation names an undeclared table. Reaching this after a successful
            // apply means its walked rows and the apply schema disagree.
            debug!(
                table = %change.table,
                "applied changeset references a table absent from the synced set, not advancing HLC"
            );
            continue;
        };
        let Some(raw) = change.col(idx) else {
            // A DELETE carries no new-state columns, and an absent value at the
            // schema's `_updated_at` index means this row change has no stamp to
            // advance past — expected for deletes, but a genuinely wrong index
            // or a schema mismatch surfaces here as the same absence, so log it.
            debug!(
                table = %change.table,
                updated_at_idx = idx,
                "applied row change has no _updated_at value (DELETE or absent new-state column), not advancing HLC past it"
            );
            continue;
        };
        match Timestamp::parse(raw) {
            Some(ts) if !ts.is_within_future_bound(receiver_wall_ms) => warn!(
                table = %change.table,
                value = raw,
                receiver_wall_ms,
                "applied row's _updated_at is grossly beyond the offline-skew \
                 allowance, not advancing HLC past it"
            ),
            Some(ts) => {
                if max.as_ref().is_none_or(|cur| ts > *cur) {
                    *max = Some(ts);
                }
            }
            None => warn!(
                table = %change.table,
                value = raw,
                "applied row has an unparseable _updated_at, not advancing HLC past it"
            ),
        }
    }
}

pub(crate) struct BlobDownload {
    authority: crate::blob::RowBlobAuthority,
    stored: crate::blob::locator::StoredBlobRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobDownloadFailureCause {
    Invalid(String),
    Local(String),
    Metadata(String),
    Storage(super::storage::StorageError),
}

impl std::fmt::Display for BlobDownloadFailureCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(formatter, "invalid blob: {reason}"),
            Self::Local(reason) => write!(formatter, "local cache: {reason}"),
            Self::Metadata(reason) => write!(formatter, "blob metadata: {reason}"),
            Self::Storage(error) => write!(formatter, "provider: {error}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobDownloadFailure {
    pub namespace: String,
    pub id: String,
    pub cause: BlobDownloadFailureCause,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobDownloadFailures(Vec<BlobDownloadFailure>);

impl BlobDownloadFailures {
    pub fn failures(&self) -> &[BlobDownloadFailure] {
        &self.0
    }

    pub fn has_transport_failure(&self) -> bool {
        self.0.iter().any(|failure| {
            matches!(
                &failure.cause,
                BlobDownloadFailureCause::Storage(error) if error.is_transport()
            )
        })
    }
}

impl std::fmt::Display for BlobDownloadFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} blob download(s) failed", self.0.len())?;
        for failure in &self.0 {
            write!(
                formatter,
                "; {}/{}: {}",
                failure.namespace, failure.id, failure.cause
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for BlobDownloadFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.iter().find_map(|failure| match &failure.cause {
            BlobDownloadFailureCause::Storage(error) if error.is_transport() => {
                Some(error as &(dyn std::error::Error + 'static))
            }
            _ => None,
        })
    }
}

impl BlobDownload {
    pub(crate) fn from_row(reference: crate::blob::RowBlobRef) -> Result<Self, String> {
        let stored = reference
            .stored()
            .cloned()
            .ok_or_else(|| "remote eager blob row has no exact stored reference".to_string())?;
        Ok(Self {
            authority: reference.authority().clone(),
            stored,
        })
    }
}

/// The `CacheEager` blobs the `changes` reference, derived per row from the
/// declarations. The cache fill a pulling device fetches into its cache before
/// applying the rows — fill-based, regardless of provenance. The incoming row's
/// declared plaintext size rides with each blob because incremental pull downloads
/// before applying that row to the DB. When an UPDATE changes the blob id but not
/// its size, SQLite omits the unchanged size column, so the old blob ref is kept
/// as the pre-apply DB lookup key for that unchanged size.
pub(crate) fn cache_eager_blobs(
    blob_decls: &BlobDecls,
    changes: &[RowChange],
    package: &crate::sync::audience_package::AudiencePackage,
) -> Result<Vec<BlobDownload>, String> {
    let authority = crate::blob::RowBlobAuthority::Remote(package.audience().clone());
    let mut downloads = Vec::new();
    for change in changes {
        if change.op == crate::changeset::ChangeOp::Delete {
            continue;
        }
        let Some(blob) = blob_decls
            .ref_from_change(change)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        if blob.fill != CacheFill::CacheEager {
            continue;
        }
        let row_id = change.pk().ok_or_else(|| {
            format!(
                "blob-bearing incoming row {:?} has no primary key",
                change.table
            )
        })?;
        let matches = package
            .blob_bindings()
            .iter()
            .filter(|binding| {
                binding.table() == change.table
                    && binding.row_id() == row_id
                    && binding.blob().locator().namespace() == blob.namespace
                    && binding.blob().locator().blob_id() == blob.id
            })
            .collect::<Vec<_>>();
        let [binding] = matches.as_slice() else {
            return Err(format!(
                "incoming eager blob row {:?}/{row_id:?} has {} exact locator bindings",
                change.table,
                matches.len()
            ));
        };
        downloads.push(BlobDownload {
            authority: authority.clone(),
            stored: binding.blob().clone(),
        });
    }
    Ok(downloads)
}

/// Derive every local-blob cleanup obligation from a changeset before its rows
/// apply. The caller stores these intents in the same transaction as the rows and
/// position, so filesystem cleanup may happen afterward without leaving an
/// unrecorded obligation. A DELETE removes its old blob; an UPDATE does so only
/// when it repoints or clears the blob reference.
pub(super) fn local_blob_cleanup_intents(
    blob_decls: &BlobDecls,
    old_changes: &[RowChange],
    new_changes: &[RowChange],
) -> Result<Vec<LocalBlobCleanupIntent>, crate::blob::decl::BlobDeclError> {
    if old_changes.len() != new_changes.len() {
        return Err(crate::blob::decl::BlobDeclError::ChangesetWalkMismatch {
            old_count: old_changes.len(),
            new_count: new_changes.len(),
        });
    }
    let mut intents = Vec::new();
    for (old, new) in old_changes.iter().zip(new_changes) {
        let old_blob_to_drop = match old.op {
            crate::changeset::ChangeOp::Delete => blob_decls.ref_from_change(old)?,
            crate::changeset::ChangeOp::Update => {
                let Some(old_blob) = blob_decls.ref_from_change(old)? else {
                    continue;
                };
                let should_drop = match blob_decls.ref_from_change(new)? {
                    Some(new_blob) => {
                        old_blob.namespace != new_blob.namespace || old_blob.id != new_blob.id
                    }
                    None => true,
                };
                if should_drop {
                    Some(old_blob)
                } else {
                    None
                }
            }
            crate::changeset::ChangeOp::Insert => None,
        };
        if let Some(blob) = old_blob_to_drop {
            intents.push(LocalBlobCleanupIntent::new(blob.namespace, blob.id));
        }
    }
    Ok(intents)
}

/// Download each blob in `blobs` into the evictable cache
/// `storage/cache/<namespace>/<id>` under `store_dir`, decrypting via storage
/// (which returns plaintext) and writing the bytes atomically. Every blob is read and
/// verified from the exact remote object. An existing exact plaintext copy in either
/// cache folder (`pinned/` or `cache/`) prevents only the duplicate cache write.
///
/// Only `CacheEager` blobs reach here (callers filter). On a peer the release is
/// Remote, so a `CacheEager` blob's bytes are a cache copy — evictable +
/// re-fetchable, not pinned: it lands in `storage/cache/<namespace>/<id>`, where it
/// evicts against its own namespace's budget (a cover never wiped by audio pressure).
/// (A cover that later falls out of that budget shows a placeholder until the next
/// read re-fetches it; covers are not pinned.) The destination is coven-built from
/// the validated namespace + blob id.
///
/// Shared by the incremental pull (per applied changeset) and the snapshot
/// bootstrap backfill (per row in the freshly bootstrapped DB), so the
/// download/decrypt/write path lives in one place.
/// Download a set of blobs into the cache. `known_uploader` is the prefix each
/// blob lives under when the caller already knows it — the changeset author for an
/// incremental pull, since the author of a changeset uploaded the blobs it
/// introduces. `None` when the caller doesn't know (a snapshot backfill, whose
/// restored rows carry no per-blob uploader): each blob's uploader is then
/// resolved from the local index, falling back to a listing scan that records what
/// it finds. A browsable/plain home resolves to no uploader segment.
pub(crate) async fn download_blobs(
    db: &Database,
    blobs: Vec<BlobDownload>,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
) -> Result<(), BlobDownloadFailures> {
    let mut failures = Vec::new();
    for download in blobs {
        let BlobDownload { authority, stored } = download;
        let namespace = stored.locator().namespace();
        let id = stored.locator().blob_id();
        let protection = match crate::blob::cache::opening_protection_for_authority(
            db, storage, &authority, &stored,
        )
        .await
        {
            Ok(protection) => protection,
            Err(error) => {
                let message = error.to_string();
                warn!(id, namespace, error = %message, "cannot resolve exact blob opening authority");
                failures.push(BlobDownloadFailure {
                    namespace: namespace.to_string(),
                    id: id.to_string(),
                    cause: BlobDownloadFailureCause::Metadata(message),
                });
                continue;
            }
        };
        if let Err(cause) =
            verify_blob_plaintext(db, storage, store_dir, &stored, protection, true).await
        {
            warn!(id, namespace, error = %cause, "failed to verify pulled blob");
            failures.push(BlobDownloadFailure {
                namespace: namespace.to_string(),
                id: id.to_string(),
                cause,
            });
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(BlobDownloadFailures(failures))
    }
}

pub(crate) async fn verify_package_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    bindings: &[crate::sync::audience_package::RowBlobLocatorBinding],
    protection: super::storage::BlobSpoolProtection,
    eager: &[BlobDownload],
) -> Result<(), BlobDownloadFailures> {
    let mut verified = Vec::new();
    let mut failures = Vec::new();
    for binding in bindings {
        let stored = binding.blob();
        if verified.iter().any(|candidate| candidate == stored) {
            continue;
        }
        verified.push(stored.clone());
        let locator = stored.locator();
        let namespace = locator.namespace();
        let id = locator.blob_id();
        let retain = eager.iter().any(|download| download.stored == *stored);
        if let Err(cause) =
            verify_blob_plaintext(db, storage, store_dir, stored, protection.clone(), retain).await
        {
            failures.push(BlobDownloadFailure {
                namespace: namespace.to_string(),
                id: id.to_string(),
                cause,
            });
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(BlobDownloadFailures(failures))
    }
}

async fn verify_blob_plaintext(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    stored: &crate::blob::locator::StoredBlobRef,
    protection: super::storage::BlobSpoolProtection,
    retain: bool,
) -> Result<(), BlobDownloadFailureCause> {
    let namespace = stored.locator().namespace();
    let id = stored.locator().blob_id();
    crate::store_dir::validate_path_token(namespace)
        .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
    crate::store_dir::validate_path_token(id)
        .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
    let cache = store_dir
        .cache_blob_path(namespace, id)
        .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
    let pinned = store_dir
        .pinned_blob_path(namespace, id)
        .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
    let staged = storage
        .stage_verified_blob_plaintext(stored, protection, &cache)
        .await
        .map_err(BlobDownloadFailureCause::Storage)?;
    if !retain {
        return Ok(());
    }
    if cached_exact_in_either_folder(
        &cache,
        &pinned,
        stored.locator().plaintext_size(),
        stored.locator().plaintext_hash(),
    )
    .await
    .map_err(BlobDownloadFailureCause::Local)?
    {
        return Ok(());
    }
    staged
        .commit()
        .await
        .map_err(BlobDownloadFailureCause::Local)?;
    crate::blob::cache::evict_to_budget(db, store_dir, namespace, Some(&cache))
        .await
        .map_err(|error| BlobDownloadFailureCause::Local(error.to_string()))
}

/// Whether a pulled blob already has an exact plaintext copy in either cache folder
/// (`cache/` or `pinned/`). The remote object has already been read and verified when
/// this runs; a match avoids committing its staged plaintext twice. An
/// existence-check failure is surfaced rather than collapsed into "absent".
async fn cached_exact_in_either_folder(
    cache: &std::path::Path,
    pinned: &std::path::Path,
    expected_size: u64,
    expected_hash: super::store_commit::ObjectHash,
) -> Result<bool, String> {
    for path in [cache, pinned] {
        if crate::local_blob::exists(path).await? {
            let (size, hash) = crate::local_blob::exact_file_facts(path).await?;
            if size == expected_size && hash == expected_hash {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[derive(Debug)]
pub enum PullError {
    Storage(super::storage::StorageError),
    MembershipObject(super::store_objects::StoreObjectError),
    MembershipLoad(super::membership_ops::AnchoredChainError),
    Apply(String),
    /// The sync storage requires a schema version newer than ours.
    /// The client must upgrade before syncing.
    SchemaVersionTooOld {
        local_version: u32,
        min_version: u32,
    },
    /// The membership chain is not anchored to the store's pinned owner — it was
    /// wiped and/or refounded under a different key (an owner-takeover attempt,
    /// issue #95). The cycle is refused rather than trusting the tampered chain.
    MembershipTampered(String),
}

impl std::fmt::Display for PullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PullError::Storage(e) => write!(f, "storage error: {e}"),
            PullError::MembershipObject(e) => {
                write!(f, "membership storage failed: {e}")
            }
            PullError::MembershipLoad(e) => write!(f, "membership chain failed: {e}"),
            PullError::Apply(e) => write!(f, "changeset apply failed: {e}"),
            PullError::SchemaVersionTooOld {
                local_version,
                min_version,
            } => write!(
                f,
                "Update the app to keep syncing — this store was upgraded by a newer device (schema v{min_version}; you have v{local_version})."
            ),
            PullError::MembershipTampered(e) => write!(f, "membership chain tampered: {e}"),
        }
    }
}

impl std::error::Error for PullError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::MembershipObject(error) => Some(error),
            Self::MembershipLoad(error) => Some(error),
            Self::Apply(_) | Self::SchemaVersionTooOld { .. } | Self::MembershipTampered(_) => None,
        }
    }
}
