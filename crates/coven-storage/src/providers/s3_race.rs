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
    Put {
        header: &'static str,
        value: String,
    },
    Complete(UploadSession),
    /// A staged or assembled temporary, published by the profile's copy.
    Copy {
        temporary: String,
        condition: S3CopyCondition,
    },
}

pub(super) use crate::providers::tests::sequence_live::{judge, Reply};

/// The unconditional completion that assembles a session's upload.
pub(super) fn multipart_request(
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
    if let Some(condition) = s.provider.settings.copy_publication.multipart() {
        // The profile's publication request is the copy, not temporary assembly.
        multipart_request(&s.provider, &session)?.send().await.map_err(s3_error)?;
        Ok(Write::Copy { temporary: s.provider.session_key(&session)?, condition })
    } else {
        Ok(Write::Complete(session))
    }
}

/// A single-request create as the profile publishes it: a conditional PUT,
/// or on a profile publishing every object by copy, a staged temporary whose
/// copy is the publication request.
pub(super) async fn single(
    s: &S3Storage,
    target: &ObjectPath,
    bytes: &[u8],
) -> Result<Write, StorageError> {
    let Some(condition) = s.settings.copy_publication.single_request() else {
        let (header, value) = s.settings.single_put.header();
        return Ok(Write::Put { header, value: value.to_owned() });
    };
    let token = SecretText::new(s.ids.new_id().to_string());
    let temporary = s.create_temporary_key(target, bytes)?;
    s.client
        .put_object()
        .bucket(&s.bucket)
        .key(&temporary)
        .body(ByteStream::from(bytes.to_vec()))
        .metadata("coven-upload", token.as_str())
        .send()
        .await
        .map_err(s3_error)?;
    Ok(Write::Copy { temporary, condition })
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
                .key(s.prefix.key(target))
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
            multipart_request(s, &session)?.if_none_match("*").send().await.map_err(s3_error)?;
        }
        Write::Copy { temporary, condition } => {
            let (header, value) = condition.header();
            s.client
                .copy_object()
                .bucket(&s.bucket)
                .key(s.prefix.key(target))
                .copy_source(format!("/{}/{temporary}", s.bucket))
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

#[cfg(test)]
#[path = "s3_race_tests.rs"]
mod tests;
