//! An upload a device left in storage, as another device finds it in that
//! device's upload places to end it (§17.10), or as a later detection or
//! connection finds what a detection run left (§20.7).

use crate::RequestKind;
use coven_crypto::SecretText;

/// One upload a device left: an unfinished provider session, a staged
/// temporary, or a detection probe. Only the provider that listed it can
/// end it.
pub struct LeftUpload(pub(crate) Left);

pub(crate) enum Left {
    /// An unfinished S3 multipart upload: its key and native id.
    S3Multipart { key: String, id: SecretText },
    /// An object, or on OneDrive the whole folder, staged in the device's
    /// reserved folder (D10): an S3 key, or a path under the configured
    /// OneDrive folder.
    Staged(String),
    /// An S3 detection probe under its run's `.coven-check` prefix (§20.7).
    Probe(String),
    /// A CloudKit upload root that isn't published; deleting it deletes the
    /// parts it owns.
    #[cfg(target_vendor = "apple")]
    CloudKitRoot(String),
    /// A session of the deterministic provider.
    #[cfg(any(test, feature = "test-utils"))]
    Memory(u64),
}

impl LeftUpload {
    /// The kind of request that ends it: an object's deletion, or a step of
    /// a session.
    pub(crate) fn request(&self) -> RequestKind {
        match self.0 {
            Left::Staged(_) | Left::Probe(_) => RequestKind::Delete,
            _ => RequestKind::Upload,
        }
    }
}
