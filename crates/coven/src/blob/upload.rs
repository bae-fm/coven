/// What one upload-queue drain pass did.
///
/// A pass that uploads nothing does so for one of four unlike reasons, and the
/// variant names which: the queue was empty, every entry was still inside its
/// retry backoff, the host had uploads paused, or entries were attempted and
/// none produced a new cloud object. Only [`Drained`](Self::Drained) carries a
/// count, so "nothing happened" can never be read as "the work is done".
#[derive(Debug)]
pub enum DrainOutcome {
    /// At least one queued entry was attempted.
    Drained {
        /// Cloud objects this pass created.
        ///
        /// Zero is a real answer here: an entry another pass had already created
        /// but not finished — a drain that died between the cloud write and its
        /// durable finalization — is finished by this pass and leaves the queue
        /// without a new object being written, so it is not counted. The count
        /// reports objects newly written to the cloud, not entries retired.
        uploaded: usize,
        /// The drain stopped early because an upload just *completed a make_remote*: the
        /// last of a gated root's user-provided blobs landed, so coven flipped the gate true
        /// and broke the drain so this cycle publishes the now-shareable subtree (and
        /// the loop runs the next cycle promptly to drain any other root's blobs).
        /// `false` when the queue drained in one pass, so the loop waits its
        /// normal interval.
        yielded_for_publish: bool,
        /// Exact failed queue entries. Provider failures remain typed so the sync
        /// loop can report Offline; local/semantic failures stay per-entry warnings.
        failures: UploadFailures,
    },
    /// The queue held no entries at all.
    QueueEmpty,
    /// The queue held entries and every one of them is still inside its retry
    /// backoff window, so none was attempted.
    AllInBackoff,
    /// The host's observer has uploads paused, so nothing was admitted. Entries
    /// eligible to run are still queued and the next pass after a resume takes
    /// them.
    Paused,
}

/// Readers for a test that planted work and expects the pass to have attempted
/// it. Each panics on any other disposition, so a drain that found an empty
/// queue — the shape a lost race produces — fails the test where it happened
/// instead of quietly reading as a zero count.
#[cfg(test)]
impl DrainOutcome {
    #[track_caller]
    fn drained(&self) -> (usize, bool, &UploadFailures) {
        match self {
            Self::Drained {
                uploaded,
                yielded_for_publish,
                failures,
            } => (*uploaded, *yielded_for_publish, failures),
            other => panic!("expected a drain that attempted queued entries, got {other:?}"),
        }
    }

    #[track_caller]
    pub(crate) fn uploaded(&self) -> usize {
        self.drained().0
    }

    #[track_caller]
    pub(crate) fn yielded_for_publish(&self) -> bool {
        self.drained().1
    }

    #[track_caller]
    pub(crate) fn failures(&self) -> &UploadFailures {
        self.drained().2
    }

    #[track_caller]
    pub(crate) fn into_failures(self) -> UploadFailures {
        match self {
            Self::Drained { failures, .. } => failures,
            other => panic!("expected a drain that attempted queued entries, got {other:?}"),
        }
    }
}

#[derive(Debug)]
pub enum UploadFailureCause {
    Local(String),
    Storage(crate::storage::StorageError),
}

impl std::fmt::Display for UploadFailureCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(reason) => write!(formatter, "local upload source: {reason}"),
            Self::Storage(error) => write!(formatter, "blob storage: {error}"),
        }
    }
}

#[derive(Debug)]
pub struct UploadFailure {
    pub entry_id: i64,
    pub object_key: String,
    pub cause: UploadFailureCause,
}

#[derive(Debug)]
pub struct UploadFailures(Vec<UploadFailure>);

impl UploadFailures {
    pub(crate) fn new(failures: Vec<UploadFailure>) -> Self {
        Self(failures)
    }

    pub fn failures(&self) -> &[UploadFailure] {
        &self.0
    }

    pub fn has_transport_failure(&self) -> bool {
        self.0.iter().any(|failure| {
            matches!(&failure.cause, UploadFailureCause::Storage(error) if error.is_transport())
        })
    }
}

impl std::fmt::Display for UploadFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} blob upload(s) failed", self.0.len())?;
        for failure in &self.0 {
            write!(
                formatter,
                "; entry {} {}: {}",
                failure.entry_id, failure.object_key, failure.cause
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for UploadFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.iter().find_map(|failure| match &failure.cause {
            UploadFailureCause::Storage(error) if error.is_transport() => {
                Some(error as &(dyn std::error::Error + 'static))
            }
            _ => None,
        })
    }
}
