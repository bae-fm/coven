#![cfg(test)]

//! Concurrent certification uses native publication replies: adapter recovery
//! must not hide two accepted writes. Preparation finishes before release.
use super::*;
use aws_sdk_s3::operation::complete_multipart_upload::builders::CompleteMultipartUploadFluentBuilder;

#[derive(Clone, Copy, Debug)]
pub(super) enum Race {
    Put,
    Multipart,
    Mixed,
    Replace,
}

pub(super) enum Write {
    Put { header: &'static str, value: String },
    Complete(UploadSession),
    Copy(UploadSession),
}

pub(super) struct Reply {
    pub(super) writer: usize,
    pub(super) start: Duration,
    pub(super) end: Duration,
    pub(super) result: Result<(), StorageError>,
}

fn multipart_request(
    s: &S3Storage,
    session: &UploadSession,
) -> Result<CompleteMultipartUploadFluentBuilder, StorageError> {
    let SessionState::S3 { id, parts, .. } = &session.state else {
        return Err(StorageFailure::SessionMismatch.into());
    };
    Ok(s.client
        .complete_multipart_upload()
        .bucket(&s.bucket)
        .key(s.session_key(session)?)
        .upload_id(id.as_str())
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(
                    parts
                        .iter()
                        .map(|part| {
                            CompletedPart::builder()
                                .part_number(part.number)
                                .e_tag(&part.etag)
                                .build()
                        })
                        .collect(),
                ))
                .build(),
        ))
}

pub(super) async fn prepare(
    s: &StorageConnection<S3Storage>,
    target: &ObjectPath,
    bytes: &[u8],
) -> Result<Write, StorageError> {
    let mut session = s.begin_upload(target, bytes.len() as u64).await?;
    s.upload_part(&mut session, bytes).await?;
    if s.provider.settings.multipart.uses_copy() {
        // The profile's publication request is the copy, not temporary assembly.
        multipart_request(&s.provider, &session)?
            .send()
            .await
            .map_err(s3_error)?;
        Ok(Write::Copy(session))
    } else {
        Ok(Write::Complete(session))
    }
}

pub(super) async fn fire(
    s: &S3Storage,
    target: &ObjectPath,
    bytes: Vec<u8>,
    write: Write,
) -> Result<(), StorageError> {
    match write {
        Write::Put { header, value } => {
            let mut request = s
                .client
                .put_object()
                .bucket(&s.bucket)
                .key(target.under(&s.prefix))
                .body(ByteStream::from(bytes));
            // Keep the adapter's replacement metadata so the verifying read
            // checks the current object through the real metadata parser too.
            if target.is_replaceable() && s.settings.revision == S3Revision::ETag {
                request = request.metadata("coven-revision", s.ids.new_id().to_string());
            }
            request
                .customize()
                .mutate_request(move |request| {
                    request.headers_mut().insert(header, value.clone());
                })
                .send()
                .await
                .map_err(s3_error)?;
        }
        Write::Complete(session) => {
            multipart_request(s, &session)?
                .if_none_match("*")
                .send()
                .await
                .map_err(s3_error)?;
        }
        Write::Copy(session) => {
            let S3Multipart::Copy(condition) = s.settings.multipart else {
                unreachable!()
            };
            let (header, value) = condition.header();
            s.client
                .copy_object()
                .bucket(&s.bucket)
                .key(target.under(&s.prefix))
                .copy_source(format!("/{}/{}", s.bucket, s.session_key(&session)?))
                .customize()
                .mutate_request(move |request| {
                    request.headers_mut().insert(header, value);
                })
                .send()
                .await
                .map_err(s3_error)?;
        }
    }
    Ok(())
}

/// All intervals must share an instant; a barrier alone does not establish it.
pub(super) fn judge(replies: &[Reply], payloads: &[Vec<u8>], stored: &[u8]) -> bool {
    let latest_start = replies.iter().map(|reply| reply.start).max();
    let earliest_end = replies.iter().map(|reply| reply.end).min();
    let winners: Vec<_> = replies
        .iter()
        .filter(|reply| reply.result.is_ok())
        .collect();
    latest_start
        .zip(earliest_end)
        .is_some_and(|(start, end)| start < end)
        && winners.len() == 1
        && stored == payloads[winners[0].writer]
        && replies.iter().all(|reply| match &reply.result {
            Ok(()) => true,
            Err(error) => matches!(
                error.failure(),
                StorageFailure::AlreadyExists | StorageFailure::Conflict
            ),
        })
}

#[cfg(test)]
#[path = "s3_race_tests.rs"]
mod tests;
