#[derive(Debug)]
pub enum PullError {
    Storage(coven_protocol::objects::StorageError),
    MembershipObject(coven_protocol::objects::StoreObjectError),
    MembershipLoad(crate::sync::store::membership::AnchoredChainError),
    Apply(String),
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
            Self::Apply(_) | Self::MembershipTampered(_) => None,
        }
    }
}
