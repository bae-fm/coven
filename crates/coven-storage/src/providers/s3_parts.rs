//! One part of a started multipart upload, sent with its number: S3 keeps
//! each part by its number, whatever order they arrive in, and a part sent
//! again replaces the one with its number (§17.9, Appendix E.10).
use super::*;
use crate::session::Confirmation;

impl S3Storage {
    /// Send part `offset..` of a started upload, returning S3's confirmation
    /// of it with its number and tag; the session is left as it is.
    pub(super) async fn send_part(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
        offset: u64,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<crate::ConfirmedPart, StorageError> {
        if !matches!(session.state, SessionState::S3 { .. }) {
            return Err(StorageFailure::SessionMismatch.into());
        }
        let id = self.upload_id(session)?;
        let number = i32::try_from(offset / session.part_size as u64 + 1)
            .map_err(|error| StorageFailure::InvalidPart.with_source(error))?;
        let response = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(self.session_key(session)?)
            .upload_id(id)
            .part_number(number)
            .body(ByteStream::from(bytes.to_vec()))
            .customize()
            .interceptor(publication.clone())
            .mutate_request({
                let sent = sent.clone();
                move |request| request.add_extension(sent.clone())
            })
            .send()
            .await
            .map_err(s3_error)?;
        let etag = metadata::required(response.e_tag(), "S3 omitted uploaded ETag")?.to_owned();
        Ok(crate::ConfirmedPart(Confirmation::S3(S3Part {
            number,
            size: bytes.len() as u64,
            etag,
        })))
    }
}
