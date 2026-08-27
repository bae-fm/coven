use super::*;

pub(crate) fn parse_drive_file_identities(
    page: &serde_json::Value,
) -> Result<Vec<DriveFileIdentity>, CloudHomeError> {
    let files = page["files"].as_array().ok_or_else(|| {
        CloudHomeError::Transport("Drive file identity response omitted files".to_string())
    })?;
    files
        .iter()
        .map(|file| {
            let id = file["id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    CloudHomeError::Transport(
                        "Drive file identity response omitted a file id".to_string(),
                    )
                })?
                .to_string();
            let create_token = file["appProperties"][CREATE_TOKEN_PROPERTY]
                .as_str()
                .filter(|token| !token.is_empty())
                .ok_or_else(|| {
                    CloudHomeError::Transport(format!(
                        "Drive file {id} omitted its Coven create token"
                    ))
                })?
                .to_string();
            Ok(DriveFileIdentity { id, create_token })
        })
        .collect()
}

pub(crate) fn select_drive_file(files: &[DriveFileIdentity]) -> Option<&DriveFileIdentity> {
    files.iter().min_by(|left, right| {
        left.create_token
            .cmp(&right.create_token)
            .then_with(|| left.id.cmp(&right.id))
    })
}

pub(crate) fn parse_create_file_id(body: &str, key: &str) -> Result<String, CloudHomeError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| CloudHomeError::transport(format!("create {key}: parse response"), e))?;
    match json.get("id").and_then(|id| id.as_str()) {
        Some(id) if !id.is_empty() => Ok(id.to_string()),
        _ => Err(CloudHomeError::Transport(format!(
            "create {key}: response missing id"
        ))),
    }
}

pub(crate) fn parse_generated_file_id(
    response: &serde_json::Value,
    key: &str,
) -> Result<String, CloudHomeError> {
    match response["ids"].as_array() {
        Some(ids) if ids.len() == 1 => match ids[0].as_str() {
            Some(id) if !id.is_empty() => Ok(id.to_string()),
            _ => Err(CloudHomeError::Transport(format!(
                "generate append id {key}: generated id is empty"
            ))),
        },
        _ => Err(CloudHomeError::Transport(format!(
            "generate append id {key}: expected exactly one generated id"
        ))),
    }
}

pub(crate) fn create_file_metadata_body(
    encoded_name: &str,
    folder_id: &str,
    create_token: &str,
) -> String {
    let mut app_properties = serde_json::Map::new();
    app_properties.insert(
        CREATE_TOKEN_PROPERTY.to_string(),
        serde_json::Value::String(create_token.to_string()),
    );
    serde_json::json!({
        "name": encoded_name,
        "parents": [folder_id],
        "appProperties": app_properties,
    })
    .to_string()
}

/// A Drive resumable sink. New objects use a resumable-create session, which
/// keeps the file absent until the final part commits; `finish` then resolves
/// concurrent same-name creates by the create token. Existing objects use a
/// resumable-update session and require no post-commit reconciliation.
pub(crate) struct DriveMultipartSink<'a> {
    home: &'a GoogleDriveCloudHome,
    inner: RangePutSink,
    key: String,
    encoded: String,
    created: Option<DriveFileIdentity>,
}

#[async_trait]
impl crate::cloud::PartSink for DriveMultipartSink<'_> {
    fn part_size(&self) -> usize {
        self.inner.part_size()
    }

    async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
        control: &crate::cloud::UploadControl,
    ) -> Result<(), CloudHomeError> {
        self.inner.send_part(part, offset, is_last, control).await
    }

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        self.inner.abort().await
    }

    async fn finish(mut self: Box<Self>) -> Result<(), CloudHomeError> {
        Box::new(self.inner).finish().await?;
        if let Some(created) = self.created.take() {
            self.home
                .reconcile_created_file(&self.key, &self.encoded, created)
                .await?;
        }
        Ok(())
    }
}

/// First `error.errors[].reason` in a Google API error body (the shape Drive,
/// Sheets, and other googleapis.com endpoints share), or `None` if the body isn't
/// that JSON.
pub(crate) fn parse_google_api_error_reason(body: &str) -> Option<String> {
    http::error_reason(body, |v| {
        v.get("error")?
            .get("errors")?
            .as_array()?
            .first()?
            .get("reason")?
            .as_str()
            .map(String::from)
    })
}

/// Map a Drive write failure to a `CloudHomeError`. The `storageQuotaExceeded`
/// reason gets a message naming the provider and the recovery step; everything
/// else keeps the raw HTTP status + body so transient failures stay debuggable.
pub(crate) fn classify_write_error(
    status: reqwest::StatusCode,
    body: &str,
    key: &str,
    op: &str,
) -> CloudHomeError {
    if status == reqwest::StatusCode::FORBIDDEN
        && parse_google_api_error_reason(body).as_deref() == Some("storageQuotaExceeded")
    {
        return CloudHomeError::Transport(
            "Your Google Drive storage is full. Free up space at drive.google.com to keep syncing."
                .to_string(),
        );
    }
    CloudHomeError::Transport(format!("{op} {key} (HTTP {status}): {body}"))
}

/// Files at or below this size go up through simple Drive requests; larger files
/// use a resumable session. Drive accepts a simple upload up to 5 MB.
pub(crate) const GDRIVE_SIMPLE_UPLOAD_MAX: usize = 4 * 1024 * 1024;

/// Resumable-session part size. Drive requires every part except the last to be a
/// multiple of 256 KiB; 8 MiB (32 × 256 KiB) keeps the request count low.
pub(crate) const GDRIVE_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl OAuthRestHome for GoogleDriveCloudHome {
    fn not_found(&self) -> NotFound {
        NotFound::Status
    }

    async fn send_read(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let file_id = self
            .find_file_id(&encode_key(key))
            .await?
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        let range = range.map(|(start, end)| crate::cloud::range_header(start, end));
        self.session
            .api_call(|oauth| {
                let mut req =
                    supports_all_drives(oauth.get(format!("{}/files/{}", self.drive_api, file_id)))
                        .query(&[("alt", "media")]);
                if let Some(ref range) = range {
                    req = req.header("Range", range);
                }
                req
            })
            .await
    }

    async fn send_delete(&self, key: &str) -> Result<reqwest::Response, CloudHomeError> {
        // No file id ⇒ already absent; surface as not-found so `rest_delete` treats
        // it as success.
        let file_id = self
            .find_file_id(&encode_key(key))
            .await?
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        self.session
            .api_call(|oauth| {
                supports_all_drives(oauth.delete(format!("{}/files/{}", self.drive_api, file_id)))
            })
            .await
    }

    async fn send_list_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let query = list_file_query(&self.folder_id, prefix);
        let page = cursor.map(str::to_string);
        self.session
            .api_call(|oauth| {
                let mut req = supports_all_drives(oauth.get(format!("{}/files", self.drive_api)))
                    .query(&[
                        ("q", query.as_str()),
                        ("fields", "nextPageToken,files(id,name)"),
                        ("pageSize", "1000"),
                        ("includeItemsFromAllDrives", "true"),
                    ]);
                if let Some(ref pt) = page {
                    req = req.query(&[("pageToken", pt.as_str())]);
                }
                req
            })
            .await
    }

    fn parse_list_page(&self, body: &str, prefix: &str) -> Result<ListPage, CloudHomeError> {
        let json: serde_json::Value = serde_json::from_str(body)
            .map_err(|e| CloudHomeError::transport("parse list".to_string(), e))?;
        let mut slots = Vec::new();
        if let Some(files) = json["files"].as_array() {
            for file in files {
                let (Some(name), Some(id)) = (file["name"].as_str(), file["id"].as_str()) else {
                    continue;
                };
                let Some(decoded) = decode_listed_key("Google Drive", name) else {
                    continue;
                };
                // The `contains` query may match mid-string, so filter to the
                // actual prefix.
                if decoded.starts_with(prefix) {
                    slots.push(ObjectSlot::opaque(decoded, id.to_string())?);
                }
            }
        }
        Ok(ListPage {
            slots,
            next: json["nextPageToken"].as_str().map(String::from),
        })
    }
}

#[async_trait]
impl CloudHome for GoogleDriveCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let media_body = Bytes::from(data);
        let encoded = encode_key(key);
        if let Some(file_id) = self.find_file_id(&encoded).await? {
            self.upload_file_media(key, &file_id, media_body, "update")
                .await?;
        } else {
            self.create_file_with_media(key, &encoded, media_body)
                .await?;
        }
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        let encoded = encode_key(key);
        let (session_url, created, op) = match self.find_file_id(&encoded).await? {
            Some(file_id) => (
                self.open_resumable_update_session(key, &file_id).await?,
                None,
                "update",
            ),
            None => {
                let attempt = self.new_append_attempt(key).await?;
                let session_url = match self.open_resumable_create_session(key, &attempt).await {
                    Ok(session_url) => session_url,
                    Err(operation) => {
                        return match self
                            .resolve_failed_append(key, &attempt, operation, false)
                            .await
                        {
                            Err(error) => Err(error),
                            Ok(file_id) => Err(CloudHomeError::Transport(format!(
                            "open mutable Drive upload {key}: uncommitted session resolved as {}",
                            file_id
                        ))),
                        }
                    }
                };
                (
                    session_url,
                    Some(DriveFileIdentity {
                        id: attempt.file_id,
                        create_token: attempt.create_token,
                    }),
                    "create",
                )
            }
        };
        let key_owned = key.to_string();
        let classify =
            Box::new(move |status, body: &str| classify_write_error(status, body, &key_owned, op));
        // Drive returns 308 Resume Incomplete for every non-final part.
        let inner = self.session.range_put_sink(
            session_url,
            308,
            total_len,
            GDRIVE_CHUNK_SIZE,
            key.to_string(),
            classify,
            drive_upload_cancellation_succeeded,
        );
        Ok(Box::new(DriveMultipartSink {
            home: self,
            inner,
            key: key.to_string(),
            encoded,
            created,
        }))
    }

    fn multipart_threshold(&self) -> u64 {
        GDRIVE_SIMPLE_UPLOAD_MAX as u64
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        rest_read(self, key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        rest_read_range(self, key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        rest_list(self, prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        rest_delete(self, key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        // A name query confirms existence in one request; the generic 2xx/404 rule
        // doesn't fit (the query returns 200 with an empty array for an absent key).
        Ok(self.find_file_id(&encode_key(key)).await?.is_some())
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        let email = desired.require_provider_email("Google Drive")?;
        let list_url = format!(
            "{}/files/{}/permissions?fields=permissions(id,emailAddress,role),nextPageToken&supportsAllDrives=true",
            self.drive_api, self.folder_id
        );
        let access = sharing::SharedFolderAccess::new(
            &self.session,
            list_url.clone(),
            "permissions",
            |permission: &serde_json::Value| permission["emailAddress"].as_str().map(String::from),
            |page: &serde_json::Value| drive_permissions_next_page_url(&list_url, page),
            |permission_id: &str| {
                format!(
                    "{}/files/{}/permissions/{}?supportsAllDrives=true",
                    self.drive_api, self.folder_id, permission_id
                )
            },
            |permission: &serde_json::Value| permission["role"].as_str() == Some("writer"),
            "writer",
            format!(
                "{}/files/{}/permissions?supportsAllDrives=true",
                self.drive_api, self.folder_id
            ),
            serde_json::json!({
                "type": "user",
                "role": "writer",
                "emailAddress": email,
            }),
        );
        match desired {
            CloudAccessState::Present { .. } => {
                access.ensure_present(email).await?;
                Ok(CloudAccessOutcome::Present(
                    CloudHomeJoinInfo::GoogleDrive {
                        folder_id: self.folder_id.clone(),
                    },
                ))
            }
            CloudAccessState::Absent { .. } => {
                access.ensure_absent(email).await?;
                Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
            }
        }
    }
}

#[async_trait]
impl ExactSlotStorage for GoogleDriveCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        use coven_protocol::objects::{
            GoogleDriveCorpus, ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
            StoreProviderBinding,
        };

        if self.folder_id.is_empty() {
            return Err(CloudHomeError::Configuration(
                "Google Drive provider binding has an empty folder id".to_string(),
            ));
        }
        let folder_response = self
            .session
            .api_call(|oauth| {
                supports_all_drives(
                    oauth.get(format!("{}/files/{}", self.drive_api, self.folder_id)),
                )
                .query(&[("fields", "id,driveId")])
            })
            .await?;
        let folder_response = ensure_ok(
            folder_response,
            "resolve Google Drive corpus",
            NotFound::Status,
        )
        .await?;
        let folder: serde_json::Value =
            ok_json(folder_response, "parse Google Drive corpus").await?;
        if folder["id"].as_str() != Some(self.folder_id.as_str()) {
            return Err(CloudHomeError::Transport(
                "Google Drive folder lookup returned a different folder id".to_string(),
            ));
        }
        let corpus = match folder.get("driveId") {
            None | Some(serde_json::Value::Null) => GoogleDriveCorpus::MyDrive {
                folder_id: self.folder_id.clone(),
            },
            Some(value) => GoogleDriveCorpus::SharedDrive {
                drive_id: value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CloudHomeError::Transport(
                            "Google Drive folder returned a malformed drive id".to_string(),
                        )
                    })?
                    .to_string(),
                folder_id: self.folder_id.clone(),
            },
        };

        let about_response = self
            .session
            .api_call(|oauth| {
                oauth
                    .get(format!("{}/about", self.drive_api))
                    .query(&[("fields", "user(permissionId)")])
            })
            .await?;
        let about_response = ensure_ok(
            about_response,
            "resolve Google Drive principal",
            NotFound::Status,
        )
        .await?;
        let about: serde_json::Value =
            ok_json(about_response, "parse Google Drive principal").await?;
        let permission_id = about["user"]["permissionId"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(
                    "Google Drive about response omitted the stable permission id".to_string(),
                )
            })?
            .to_string();

        Ok(ResolvedProviderBinding {
            store: StoreProviderBinding::GoogleDrive { corpus },
            device: ProviderDeviceBinding {
                principal: ProviderPrincipalId::GoogleDrive { permission_id },
            },
        })
    }

    async fn allocate_slot(&self, logical_key: &str) -> Result<ObjectSlot, CloudHomeError> {
        ObjectSlot::opaque(
            logical_key.to_string(),
            self.generate_file_id(logical_key).await?,
        )
        .map_err(CloudHomeError::from)
    }

    async fn create_at(
        &self,
        upload: &crate::cloud::ExactUpload<'_>,
        control: &crate::cloud::UploadControl,
    ) -> Result<crate::cloud::ExactCreateOutcome, CloudHomeError> {
        if matches!(
            self.exact_upload_verification,
            coven_foundation::config::ExactUploadVerification::UploadChecksum
        ) {
            return Err(CloudHomeError::Configuration(
                "Google Drive does not accept a caller-supplied upload checksum".to_string(),
            ));
        }
        let operation = GoogleDriveCloudHome::create_at_slot(
            self,
            upload.object().slot(),
            upload.body().await?,
            control,
        )
        .await;
        settle_exact_create(operation, |observed| {
            self.verify_exact_upload(upload, observed)
        })
        .await
    }
    /// Drive mints its own file ids, so the listing reports the id it saw
    /// beside the name rather than deriving a locator from the key.
    async fn list_slots(&self, prefix: &str) -> Result<Vec<ObjectSlot>, CloudHomeError> {
        crate::cloud::oauth_rest::rest_list_slots(self, prefix).await
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        GoogleDriveCloudHome::read_at_slot(self, slot).await
    }
    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        let range = crate::cloud::range_header(start, end);
        let response = self.send_exact_read(slot, Some(&range)).await?;
        validated_range_bytes(response, "read exact Drive range", start, end).await
    }
    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
        progress: crate::cloud::DownloadProgress,
    ) -> Result<(), crate::cloud::CloudFileReadError> {
        GoogleDriveCloudHome::read_at_slot_to_file(self, slot, destination, progress).await
    }
    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        GoogleDriveCloudHome::delete_at_slot(self, slot).await
    }
}

pub(crate) fn drive_permissions_next_page_url(
    list_url: &str,
    page: &serde_json::Value,
) -> Result<Option<String>, CloudHomeError> {
    let Some(token) = page["nextPageToken"].as_str() else {
        return Ok(None);
    };
    let query = serde_urlencoded::to_string([("pageToken", token)])
        .map_err(|e| CloudHomeError::transport("encode Drive page token".to_string(), e))?;
    let separator = if list_url.contains('?') { '&' } else { '?' };
    Ok(Some(format!("{list_url}{separator}{query}")))
}
