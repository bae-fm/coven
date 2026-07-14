/// Membership and blob helpers shared by Store pull and bootstrap.
use tracing::{debug, error, warn};

use super::conflict::TableSchema;
use super::hlc::Timestamp;
use super::membership::MembershipChain;
use super::storage::SyncStorage;
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup::LocalBlobCleanupIntent;
use crate::blob::{CacheFill, Provenance};
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
/// per-author head watermark, loading once also writes that watermark once.
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
    pub listing_proof: MembershipListingProof,
}

#[derive(Debug, Clone, Copy)]
pub struct MembershipListingProof {
    entry_coverage: crate::storage::cloud::ListingCoverage,
    head_coverage: crate::storage::cloud::ListingCoverage,
}

impl MembershipListingProof {
    pub fn is_complete(self) -> bool {
        self.entry_coverage == crate::storage::cloud::ListingCoverage::CompleteAtScan
            && self.head_coverage == crate::storage::cloud::ListingCoverage::CompleteAtScan
    }

    #[cfg(test)]
    pub(crate) fn complete_for_test() -> Self {
        Self {
            entry_coverage: crate::storage::cloud::ListingCoverage::CompleteAtScan,
            head_coverage: crate::storage::cloud::ListingCoverage::CompleteAtScan,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        entry_coverage: crate::storage::cloud::ListingCoverage,
        head_coverage: crate::storage::cloud::ListingCoverage,
    ) -> Self {
        Self {
            entry_coverage,
            head_coverage,
        }
    }
}

/// Load and anchor the cycle's membership chain once. Every successful listing
/// is validated; a loader error aborts regardless of owner pin because an
/// unpinned database may already have accepted author floors. Only the loader's
/// explicit `Ok(None)` represents an unpinned pre-initialization read. LIST
/// transport errors retain their separate pinned/unpinned classification.
pub async fn load_cycle_membership(
    storage: &dyn SyncStorage,
    db: &Database,
) -> Result<CycleMembership, PullError> {
    // The store's established owner, pinned at create/join/restore (issue #102).
    // An initialized plaintext or encrypted store has `Some`; `None` is reserved
    // for bootstrap callers that run before owner establishment.
    let pinned_owner = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|e| PullError::Apply(format!("read pinned owner: {e}")))?;

    let membership_listing =
        match super::store_objects::list_membership_entry_objects(storage).await {
            Ok(entries) => entries,
            Err(e) => {
                // Can't even list membership. For an owner-pinned store we cannot
                // verify authorship, so fail closed (abort, retry next cycle) rather
                // than apply changesets unvalidated. Only an unpinned
                // pre-initialization caller can proceed without a chain.
                if pinned_owner.is_some() {
                    return Err(PullError::MembershipTampered(e.to_string()));
                }
                warn!("failed to list membership entries for validation: {e}");
                return Ok(CycleMembership {
                    chain: None,
                    pinned_owner,
                    listed_entries: Vec::new(),
                    listing_proof: MembershipListingProof {
                        entry_coverage: crate::storage::cloud::ListingCoverage::BestEffort,
                        head_coverage: crate::storage::cloud::ListingCoverage::BestEffort,
                    },
                });
            }
        };
    let entry_coverage = membership_listing.coverage;
    let listed_entries: Vec<super::membership::MembershipCoord> = membership_listing
        .entries
        .into_iter()
        .map(|(coord, _)| coord)
        .collect();

    // Load + validate the chain and anchor it to the pinned owner. Every
    // successful LIST result, including an empty one, reaches the same optional
    // loader: persisted author floors can recover heads and entries omitted by
    // the listing. For an owner-pinned store a chain that won't validate, or one
    // founded by a different key, is tamper. This also fails loud without an
    // owner pin: an unpinned database may already hold accepted author floors,
    // whose missing or regressed state must not turn authorization off. The
    // database makes this load monotonic per author.
    let loaded = match super::membership_ops::load_anchored_chain_if_known_with_proof(
        storage,
        &listed_entries,
        pinned_owner.as_deref(),
        Some(db),
    )
    .await
    {
        Ok(loaded) => loaded,
        Err(error) => return Err(PullError::MembershipTampered(error.to_string())),
    };
    let chain = loaded.chain;
    let listing_proof = MembershipListingProof {
        entry_coverage,
        head_coverage: loaded.head_coverage,
    };

    Ok(CycleMembership {
        chain,
        pinned_owner,
        listed_entries,
        listing_proof,
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
    blob: crate::blob::BlobRef,
    size: BlobDownloadSize,
    hash: BlobDownloadHash,
    cloud_path: BlobDownloadCloudPath,
}

/// The row a change is about, named the way every change names it: its table and primary
/// key. A changeset UPDATE reports only the columns whose values CHANGED, so a column it
/// omits has to be read back from the row itself — and this is the only handle on that row
/// that a change always carries, and that a device always resolves.
///
/// Naming it by the change's *old blob id* instead — "the row that carries blob X" — asks a
/// question a device whose row has already moved on cannot answer: a concurrent repointing
/// of the same row leaves no row carrying X at all, and the lookup fails on a row that is
/// sitting right there under its primary key.
///
/// What the row can answer is the value it currently holds, which is the author's omitted
/// value exactly when this device agrees with the author's pre-image on that column. It
/// does not when a *concurrent* change moved that same column: then the author's value is
/// in no local state and in no part of the changeset, and the download fails its hash
/// check. Recovering an omitted column from local state cannot cover that; only reading the
/// blob's length from its own cloud object would, and the object is reachable precisely
/// because its key names the blob.
#[derive(Clone)]
struct PreApplyRow {
    table: String,
    pk: String,
}

/// Where the download reads a blob's declared plaintext size and content hash
/// from. Both ride with the changeset row when the row change carries them
/// (`Declared`); an update that changed the blob id but not size/hash omits those
/// columns — from its old values as well as its new ones — so they are read back from the
/// [`PreApplyRow`] (`ExistingRow`); a snapshot backfill reads both from the freshly
/// bootstrapped DB row (`InstalledRow`).
enum BlobDownloadSize {
    Declared(u64),
    ExistingRow(PreApplyRow),
    InstalledRow,
    Missing,
}

enum BlobDownloadHash {
    Declared(String),
    ExistingRow(PreApplyRow),
    InstalledRow,
    Missing,
}

/// Where the download reads a blob's readable cloud path from — the key a browsable
/// home stores it at. The same three sources the size and hash have, with one
/// difference: a blob legitimately has no cloud path at all (`Absent`) on an opaque
/// home, which keys by id, so its absence is a value rather than an error. An update
/// that repointed a row at a new blob left its cloud path alone, so the column is
/// missing from the change and the pre-apply row holds the (unchanged) value.
enum BlobDownloadCloudPath {
    Declared(String),
    ExistingRow(crate::blob::BlobRef),
    Absent,
}

impl BlobDownload {
    fn from_change(
        blob: crate::blob::BlobRef,
        source_size: Option<u64>,
        source_hash: Option<String>,
        lookup_blob: Option<crate::blob::BlobRef>,
        pre_apply: Option<PreApplyRow>,
    ) -> Self {
        let size = match (source_size, pre_apply.clone()) {
            (Some(size), _) => BlobDownloadSize::Declared(size),
            (None, Some(row)) => BlobDownloadSize::ExistingRow(row),
            (None, None) => BlobDownloadSize::Missing,
        };
        let hash = match (source_hash, pre_apply) {
            (Some(hash), _) => BlobDownloadHash::Declared(hash),
            (None, Some(row)) => BlobDownloadHash::ExistingRow(row),
            (None, None) => BlobDownloadHash::Missing,
        };
        let cloud_path = match (blob.cloud_path.clone(), lookup_blob) {
            (Some(path), _) => BlobDownloadCloudPath::Declared(path),
            (None, Some(blob)) => BlobDownloadCloudPath::ExistingRow(blob),
            (None, None) => BlobDownloadCloudPath::Absent,
        };
        Self {
            blob,
            size,
            hash,
            cloud_path,
        }
    }

    pub(crate) fn from_installed_db(blob: crate::blob::BlobRef) -> Self {
        // A row read whole out of the bootstrapped DB carries its cloud path already.
        let cloud_path = match blob.cloud_path.clone() {
            Some(path) => BlobDownloadCloudPath::Declared(path),
            None => BlobDownloadCloudPath::Absent,
        };
        Self {
            blob,
            size: BlobDownloadSize::InstalledRow,
            hash: BlobDownloadHash::InstalledRow,
            cloud_path,
        }
    }

    async fn resolve_source_size(
        db: &Database,
        blob: &crate::blob::BlobRef,
        size: BlobDownloadSize,
    ) -> Result<u64, String> {
        match size {
            BlobDownloadSize::Declared(size) => Ok(size),
            BlobDownloadSize::ExistingRow(row) => {
                crate::blob::cache::row_blob_size(db, &row.table, &row.pk)
                    .await
                    .map_err(|e| e.to_string())
            }
            BlobDownloadSize::InstalledRow => crate::blob::cache::expected_blob_size(db, blob)
                .await
                .map_err(|e| e.to_string()),
            BlobDownloadSize::Missing => Err(format!(
                "incoming blob {}/{} has no declared size",
                blob.namespace, blob.id
            )),
        }
    }

    async fn resolve_source_hash(
        db: &Database,
        blob: &crate::blob::BlobRef,
        hash: BlobDownloadHash,
    ) -> Result<String, String> {
        match hash {
            BlobDownloadHash::Declared(hash) => Ok(hash),
            BlobDownloadHash::ExistingRow(row) => {
                crate::blob::cache::row_blob_hash(db, &row.table, &row.pk)
                    .await
                    .map_err(|e| e.to_string())
            }
            BlobDownloadHash::InstalledRow => crate::blob::cache::expected_blob_hash(db, blob)
                .await
                .map_err(|e| e.to_string()),
            BlobDownloadHash::Missing => Err(format!(
                "incoming blob {}/{} has no declared content hash",
                blob.namespace, blob.id
            )),
        }
    }

    async fn resolve_cloud_path(
        db: &Database,
        cloud_path: BlobDownloadCloudPath,
    ) -> Result<Option<String>, String> {
        match cloud_path {
            BlobDownloadCloudPath::Declared(path) => Ok(Some(path)),
            BlobDownloadCloudPath::ExistingRow(lookup) => {
                crate::blob::cache::row_cloud_path(db, &lookup)
                    .await
                    .map_err(|e| e.to_string())
            }
            BlobDownloadCloudPath::Absent => Ok(None),
        }
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
    old_changes: &[RowChange],
    changes: &[RowChange],
) -> Result<Vec<BlobDownload>, crate::blob::decl::BlobDeclError> {
    if old_changes.len() != changes.len() {
        return Err(crate::blob::decl::BlobDeclError::ChangesetWalkMismatch {
            old_count: old_changes.len(),
            new_count: changes.len(),
        });
    }
    old_changes
        .iter()
        .zip(changes)
        .filter_map(
            |(old, change)| match blob_decls.ref_size_hash_from_change(change) {
                Ok(Some((blob, size, hash))) if blob.fill == CacheFill::CacheEager => {
                    // The row this change is about: every change carries its primary key,
                    // in its old values as well as its new ones.
                    let pre_apply = change.pk().map(|pk| PreApplyRow {
                        table: change.table.clone(),
                        pk: pk.to_string(),
                    });
                    match blob_decls.ref_from_change(old) {
                        Ok(old_blob) => Some(Ok(BlobDownload::from_change(
                            blob, size, hash, old_blob, pre_apply,
                        ))),
                        Err(e) => Some(Err(e)),
                    }
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            },
        )
        .collect()
}

/// The blobs a changeset *introduces* — a row whose new blob ref the pre-image
/// lacked, or differs from — paired with the `author` that uploaded them (the
/// author of a changeset uploads the blobs its rows introduce, into its own cloud
/// prefix). A row updated without changing its blob re-references an existing
/// object and introduces nothing, so it is not recorded here. Empty when the
/// author is unknown, which is possible only before membership initialization.
/// Returns `(namespace, blob_id, uploader)` for the local uploader index.
pub(super) fn introduced_blob_uploads(
    blob_decls: &BlobDecls,
    old_changes: &[RowChange],
    changes: &[RowChange],
    author: Option<&str>,
) -> Result<Vec<(String, String, String)>, crate::blob::decl::BlobDeclError> {
    let Some(author) = author else {
        return Ok(Vec::new());
    };
    if old_changes.len() != changes.len() {
        return Err(crate::blob::decl::BlobDeclError::ChangesetWalkMismatch {
            old_count: old_changes.len(),
            new_count: changes.len(),
        });
    }
    let mut out = Vec::new();
    for (old, new) in old_changes.iter().zip(changes) {
        let Some(new_blob) = blob_decls.ref_from_change(new)? else {
            continue;
        };
        // An insert always introduces its blob; an update introduces one only when
        // it moves the row to a different blob (a same-blob update re-references an
        // existing object and uploads nothing). A delete carries no new blob and is
        // already skipped above.
        let introduced = match new.op {
            crate::changeset::ChangeOp::Insert => true,
            crate::changeset::ChangeOp::Update => match blob_decls.ref_from_change(old)? {
                Some(old_blob) => {
                    old_blob.namespace != new_blob.namespace || old_blob.id != new_blob.id
                }
                None => true,
            },
            crate::changeset::ChangeOp::Delete => false,
        };
        if introduced {
            out.push((new_blob.namespace, new_blob.id, author.to_string()));
        }
    }
    Ok(out)
}

/// The **host-provided** blobs the `changes` reference, derived per row from the
/// declarations. The inline push uploads these before publishing the changeset:
/// coven owns a host-provided blob's bytes (in its local store or cache), so it can
/// upload it inline as its row reaches a changeset — provenance-based, regardless of
/// cache fill. A user-provided blob is the user's own file, uploaded only via
/// `make_remote` (which reads the user's path), so it is never on this inline path.
pub(crate) fn host_provided_blobs(
    blob_decls: &BlobDecls,
    changes: &[RowChange],
) -> Result<Vec<crate::blob::BlobRef>, crate::blob::decl::BlobDeclError> {
    changes
        .iter()
        .filter(|change| {
            matches!(
                change.op,
                crate::changeset::ChangeOp::Insert | crate::changeset::ChangeOp::Update
            )
        })
        .filter_map(|change| match blob_decls.ref_from_change(change) {
            Ok(Some(blob)) if blob.provenance == Provenance::HostProvided => Some(Ok(blob)),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
        .collect()
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
/// (which returns plaintext) and writing the bytes atomically. Returns true if every
/// blob succeeded. Skips blobs already present in either cache folder (`pinned/` or
/// `cache/`).
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
    known_uploader: Option<&str>,
) -> bool {
    let mut all_ok = true;
    for download in blobs {
        let BlobDownload {
            blob,
            size,
            hash,
            cloud_path,
        } = download;
        // The blob's `id`/`namespace`/`cloud_path` come from a row in an incoming
        // changeset authored by any write-capable member. An id or namespace that is
        // not a single safe path token, or a cloud_path that escapes its prefix,
        // would write attacker-chosen bytes outside the store or under a forged
        // cloud key — refuse the blob as bad data before resolving or writing it, the
        // same posture as any other failed blob in this loop. The id check makes
        // local traversal unrepresentable: the destination path below is built from
        // the validated id by coven, so there is no host-supplied local path left to
        // independently re-check. The cloud path is checked below, once resolved,
        // because that resolved value is the one the read keys with.
        if let Err(e) = crate::store_dir::validate_path_token(&blob.namespace) {
            error!(id = %blob.id, namespace = %blob.namespace, "blob namespace is not a safe path token ({e}); refusing");
            all_ok = false;
            continue;
        }
        if let Err(e) = crate::store_dir::validate_path_token(&blob.id) {
            error!(id = %blob.id, namespace = %blob.namespace, "blob id is not a safe path token ({e}); refusing");
            all_ok = false;
            continue;
        }

        // The coven-built destination: the evictable cache
        // `storage/cache/<namespace>/<id>`. Building it validates the namespace and id
        // again (and that the id can form the `{ab}/{cd}` shard); a failure is the
        // same bad-data refusal as the token check above. A pinned copy (in
        // `pinned/`) is checked for presence below but never written here — a pull
        // populates the evictable cache, never the kept folder.
        let dest = match store_dir.cache_blob_path(&blob.namespace, &blob.id) {
            Ok(p) => p,
            Err(e) => {
                error!(id = %blob.id, "cannot build cache blob path ({e}); refusing");
                all_ok = false;
                continue;
            }
        };
        let pinned = match store_dir.pinned_blob_path(&blob.namespace, &blob.id) {
            Ok(p) => p,
            Err(e) => {
                error!(id = %blob.id, "cannot build pinned blob path ({e}); refusing");
                all_ok = false;
                continue;
            }
        };

        // Already on disk in either cache folder — don't re-download. A failed
        // existence check is a local-disk fault, not a missing blob — and the
        // download's own write would hit the same fault. Hold the position and retry
        // next cycle rather than treat the error as absence.
        match cached_in_either_folder(&dest, &pinned).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => {
                error!(id = %blob.id, error = %e, "cannot check for local blob; holding");
                all_ok = false;
                continue;
            }
        }

        let source_size = match BlobDownload::resolve_source_size(db, &blob, size).await {
            Ok(size) => size,
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot read blob size, skipping download");
                all_ok = false;
                continue;
            }
        };

        // The author-signed content hash the downloaded plaintext must match. A
        // blob whose row does not carry one is refused rather than downloaded
        // unverified — the hash is the authority that pins the bytes to the row's
        // author, so a missing one is bad data, not a case to skip past.
        let expected_hash = match BlobDownload::resolve_source_hash(db, &blob, hash).await {
            Ok(hash) => hash,
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot read blob content hash, skipping download");
                all_ok = false;
                continue;
            }
        };

        // The readable key a browsable home stores the blob at. A row repointed at a
        // new blob leaves its cloud path alone, so the change omits the column and the
        // path comes from the pre-apply row instead — which is the same value, that
        // being what "the change did not touch it" means.
        let cloud_path = match BlobDownload::resolve_cloud_path(db, cloud_path).await {
            Ok(path) => path,
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot read blob cloud path, skipping download");
                all_ok = false;
                continue;
            }
        };
        if let Some(path) = cloud_path.as_deref() {
            if let Err(e) = crate::store_dir::validate_cloud_path(path) {
                error!(id = %blob.id, cloud_path = %path, "blob cloud_path escapes its prefix ({e}); refusing");
                all_ok = false;
                continue;
            }
        }

        // The prefix the blob lives under: the known author for an incremental
        // pull; otherwise resolved from the index, then a listing scan.
        let uploader = match known_uploader {
            Some(uploader) => Some(uploader.to_string()),
            None => match crate::blob::cache::resolve_blob_uploader(db, storage, &blob).await {
                Ok(uploader) => uploader,
                Err(e) => {
                    warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "cannot resolve blob uploader, skipping download");
                    all_ok = false;
                    continue;
                }
            },
        };

        match storage
            .read_blob_to_file(
                &blob.namespace,
                uploader.as_deref(),
                &blob.id,
                blob.scope.clone(),
                cloud_path.as_deref(),
                source_size,
                &expected_hash,
                &dest,
            )
            .await
        {
            Ok(()) => {}
            Err(e) => {
                warn!(id = %blob.id, namespace = %blob.namespace, error = %e, "failed to download blob");
                all_ok = false;
            }
        }
    }
    all_ok
}

/// Whether a pulled blob is already on disk in either cache folder (`cache/` or
/// `pinned/`), so a pull doesn't re-download a blob a read already cached or the
/// user pinned. An existence-check failure (a broken filesystem) is surfaced, never
/// collapsed into "absent": re-downloading over a present file would mask the fault.
async fn cached_in_either_folder(
    cache: &std::path::Path,
    pinned: &std::path::Path,
) -> Result<bool, String> {
    for path in [cache, pinned] {
        if crate::local_blob::exists(path).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Debug)]
pub enum PullError {
    Storage(super::storage::StorageError),
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

impl std::error::Error for PullError {}
