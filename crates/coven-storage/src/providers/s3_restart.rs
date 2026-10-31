use super::*;

impl S3Storage {
    pub(super) async fn require_restart(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        cause: StorageError,
    ) -> Result<(), StorageError> {
        if let SessionState::S3 { id, token, .. } = &session.state {
            session.state = SessionState::S3Restart { id: id.clone(), token: token.clone() };
            session.confirmed = 0;
        }
        // Retain the native identifiers until abort and temporary deletion both
        // succeed. No completion or copy may reuse this rejected publication.
        crate::transfer::with_cleanup(Err(cause), self.abort_upload(publication, session).await)
    }
}
