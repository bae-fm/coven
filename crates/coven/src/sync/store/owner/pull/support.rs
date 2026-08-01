/// Membership and blob helpers shared by Store pull and bootstrap.
use tracing::{debug, warn};

use crate::changeset::RowChange;
use crate::database::TableSchema;
use crate::sync::hlc::Timestamp;
use crate::sync::store::blob::BlobDownloadFailureCause;

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
pub(crate) struct BlobDownloadFailure {
    pub(crate) namespace: String,
    pub(crate) id: String,
    pub(crate) cause: BlobDownloadFailureCause,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobDownloadFailures(Vec<BlobDownloadFailure>);

impl BlobDownloadFailures {
    pub(crate) fn new(failures: Vec<BlobDownloadFailure>) -> Self {
        Self(failures)
    }

    pub(crate) fn has_transport_failure(&self) -> bool {
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

#[derive(Debug)]
pub enum PullError {
    Storage(crate::storage::StorageError),
    MembershipObject(crate::storage::StoreObjectError),
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
