/// Membership and blob helpers shared by Store pull and bootstrap.
use tracing::{debug, warn};

use crate::blob::cache::BlobDownloadFailureCause;
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup::LocalBlobCleanupIntent;
use crate::blob::CacheFill;
use crate::changeset::RowChange;
use crate::database::Database;
use crate::store_dir::StoreDir;
use crate::sync::conflict::TableSchema;
use crate::sync::hlc::Timestamp;
use crate::sync::storage::SyncStorage;
use crate::sync::store::owner::BlobDownload;

/// Advance `max` past the greatest `_updated_at` among `changes`, parsing each
/// as an HLC [`Timestamp`]. A row whose `_updated_at` fails to parse is logged
/// and skipped — it must not panic the pull or silently default the clock.
///
/// `max` becomes the value the caller advances the local HLC past, and that
/// advance is deliberately uncapped (it trusts a value already written to disk).
/// So the bound lives here, at the point a stamp is *collected*: a grossly-future
/// stamp — beyond `receiver_wall_ms` + [`crate::sync::hlc::MAX_FUTURE_SKEW_MS`] — is
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

/// The `CacheEager` blobs the `changes` reference, derived per row from the
/// declarations. The cache fill a pulling device fetches into its cache before
/// applying the rows — fill-based, regardless of provenance. The incoming row's
/// declared plaintext size rides with each blob because incremental pull downloads
/// before applying that row to the DB. When an UPDATE changes the blob id but not
/// its size, SQLite omits the unchanged size column, so the old blob ref is kept
/// as the pre-apply DB lookup key for that unchanged size.
pub(super) fn cache_eager_blobs(
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
            let row_id = old.pk().ok_or_else(|| {
                crate::blob::decl::BlobDeclError::MissingPublicationPrimaryKey {
                    table: old.table.clone(),
                }
            })?;
            intents.push(LocalBlobCleanupIntent::for_row(
                blob.namespace,
                blob.id,
                old.table.clone(),
                row_id,
            ));
        }
    }
    Ok(intents)
}

pub(super) async fn verify_package_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    bindings: &[crate::sync::audience_package::RowBlobLocatorBinding],
    protection: crate::sync::storage::BlobSpoolProtection,
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
        if let Err(cause) = crate::blob::cache::verify_blob_plaintext(
            db,
            storage,
            store_dir,
            stored,
            protection.clone(),
            retain,
        )
        .await
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

#[derive(Debug)]
pub enum PullError {
    Storage(crate::sync::storage::StorageError),
    MembershipObject(crate::sync::store_objects::StoreObjectError),
    MembershipLoad(crate::sync::store::membership::AnchoredChainError),
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
