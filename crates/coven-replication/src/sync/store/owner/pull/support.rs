use crate::sync::store::blob::BlobDownloadFailureCause;

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
    Storage(coven_protocol::objects::StorageError),
    MembershipObject(coven_protocol::objects::StoreObjectError),
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
