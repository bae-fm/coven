//! Sessions named before their native multipart upload exists (Appendix
//! E.10): the first part starts the upload, and a recording without its
//! native id may name one whose start's reply was lost or never recorded.
use super::*;

impl S3Storage {
    /// Whether every native upload at the session's name is this session's:
    /// a copy profile's private staging name, or a destination in the
    /// device's own file or snapshot folders, which no other upload targets.
    /// A shared slot or floor path proves nothing (Appendix E.10).
    pub(super) fn owns_session_key(&self, session: &UploadSession) -> bool {
        self.settings.copy_publication.multipart().is_some()
            || session.path().device() == Some(session.device())
    }

    /// Abort every native upload at a planned session's name: one its
    /// initiation started before the reply was lost, or before the device
    /// stopped without recording it. Only a name the session owns proves
    /// they are its own (Appendix E.10).
    pub(super) async fn abort_initiated(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        if !self.owns_session_key(session) {
            return Err(StorageFailure::SessionIdentityUnknown.into());
        }
        let key = self.session_key(session)?;
        self.abort_private_uploads(&key, Some(publication)).await
    }
}
