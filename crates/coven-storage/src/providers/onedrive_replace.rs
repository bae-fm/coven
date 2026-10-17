//! Replace the floor through an upload session bound to the observed revision.
use super::*;
use crate::session::ONEDRIVE_PART_SIZE;
use crate::transfer::with_cleanup;

impl OneDriveStorage {
    /// A direct `PUT /content` with `If-Match` lets two writers holding the
    /// same eTag both succeed. Graph compares a session's `If-Match` when it
    /// creates the session and drops the session if the item changes before
    /// its final request. Microsoft documents only the first part; live
    /// two-account races are the evidence for the second.
    pub(super) async fn replace_content(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let created = http::json(
            PROVIDER,
            self.session
                .publish(
                    publication,
                    Method::POST,
                    &self.item(&previous.object_id, &["createUploadSession"])?,
                    &[("If-Match", previous.replace_token.clone())],
                    Body::Json(json!({
                        "item": {"@microsoft.graph.conflictBehavior": "replace"},
                        // A zero-length fragment is invalid, so an empty body
                        // needs a session that waits for explicit completion.
                        "deferCommit": bytes.is_empty()
                    })),
                    true,
                )
                .await?,
        )
        .await;
        let created = self.checked_folder_operation(created).await?;
        let url = http::string(&created, "uploadUrl")?;
        validate_download_url(&self.api, url)?;
        let result = self
            .send_replacement(publication, previous, url, bytes)
            .await;
        if result.is_ok() {
            return result;
        }
        // Cancelling cannot publish bytes, so it runs even when publication
        // authority was withdrawn after the session was created.
        let cancellation = async {
            let response = self
                .session
                .send(Method::DELETE, url, &[], Body::Empty, false)
                .await?;
            if response.status().as_u16() == 404 {
                return Ok(());
            }
            match http::checked(PROVIDER, response).await {
                // The session's own revision condition refuses its deletion
                // once another write changed the item: it can never publish.
                Err(error) if upload::native_code(&error, "resourceModified") => Ok(()),
                result => result.map(drop),
            }
        }
        .await;
        with_cleanup(result, cancellation)
    }

    async fn send_replacement(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        url: &str,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        if bytes.is_empty() {
            // The deferred session's completion keeps the original condition.
            let response = self
                .session
                .publish(
                    publication,
                    Method::POST,
                    url,
                    &[
                        ("If-Match", previous.replace_token.clone()),
                        ("Content-Length", "0".into()),
                    ],
                    Body::Bytes(Vec::new()),
                    false,
                )
                .await?;
            return acknowledged(response, previous, 0, 0).await;
        }
        let mut start = 0;
        for part in bytes.chunks(ONEDRIVE_PART_SIZE) {
            let end = start + part.len();
            let body = Body::Bytes(part.to_vec());
            let mut headers = body_headers(&body);
            headers.push((
                "Content-Range",
                format!("bytes {start}-{}/{}", end - 1, bytes.len()),
            ));
            let response = self
                .session
                .publish(publication, Method::PUT, url, &headers, body, false)
                .await?;
            acknowledged(response, previous, end as u64, bytes.len() as u64).await?;
            start = end;
        }
        Ok(())
    }
}

/// Accept only what the session protocol promises: before the last byte, a 202
/// asking for exactly the next byte; after it, the observed item at its new size.
async fn acknowledged(
    response: reqwest::Response,
    previous: &StoredObject,
    end: u64,
    total: u64,
) -> Result<(), StorageError> {
    let (value, receipt) = http::json_response(PROVIDER, response)
        .await
        .map_err(|mut error| {
            if let StorageError::Provider {
                failure: failure @ (StorageFailure::NotFound | StorageFailure::AlreadyExists),
                ..
            } = &mut error
            {
                // Another write changing the item drops the session (404) or
                // refuses its request (409 `resourceModified`). Either is a
                // refused replacement, never a reason to write the floor
                // another way. Mapped here so a failed cancellation cannot
                // carry it out as a create conflict.
                *failure = StorageFailure::Conflict;
            }
            error
        })?;
    let invalid = || {
        http::invalid_response(
            PROVIDER,
            receipt.status(),
            receipt.headers().clone(),
            receipt.body().to_vec(),
        )
    };
    let accepted = if end < total {
        receipt.status() == 202
            && upload::next_offset(&value["nextExpectedRanges"], end, total, invalid)? == end
    } else {
        matches!(receipt.status(), 200 | 201)
            && value["id"].as_str() == Some(previous.object_id.as_str())
            && value["name"].as_str() == Some(previous.path.file_name())
            && value["file"].is_object()
            && value["size"].as_u64() == Some(total)
    };
    if !accepted {
        return Err(invalid());
    }
    Ok(())
}
