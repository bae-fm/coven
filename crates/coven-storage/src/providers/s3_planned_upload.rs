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
    /// they are its own, so only such a name is asked (Appendix E.10).
    pub(super) async fn abort_initiated(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        debug_assert!(self.owns_session_key(session));
        let key = self.session_key(session)?;
        self.abort_private_uploads(&key, Some(publication)).await
    }
}

impl S3Storage {
    /// A marked plan at a shared name: read the destination instead of
    /// asking about uploads. Its own marker there means its bytes are
    /// published. Another object leaves the name taken, which the caller
    /// settles. An empty destination leaves one unknown, an upload the lost
    /// start may have begun, which can never publish: no record names it, and
    /// once anything is published at the name create-once refuses every other
    /// completion. The plan starts fresh at the same name without waiting on
    /// it, and whoever applies the name ends it (Appendix E.10).
    pub(super) async fn restart_shared_plan(
        &self,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        match self.completed(session).await {
            Ok(()) => Ok(()),
            Err(error) if error.failure() == StorageFailure::SessionExpired => {
                if let SessionState::S3Planned { started, .. } = &mut session.state {
                    *started = PlanStart::Unstarted;
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}
