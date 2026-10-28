use super::access::PermissionAccess;
use super::http::{self, Body, OAuthSession};
use super::object_metadata;
use super::onedrive_access::AccountPermissions;
use super::pagination::Pagination;
use crate::session::{OneDriveUpload, SessionState};
use crate::*;
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use coven_crypto::SecretText;
use coven_foundation::id_source::IdSourceRef;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeMap;
const PROVIDER: CloudProvider = CloudProvider::OneDrive;

#[path = "onedrive_folder.rs"]
mod folder;
#[path = "onedrive_replace.rs"]
mod replace;
#[path = "onedrive_upload.rs"]
mod upload;

/// OneDrive objects under the store's shared folder, reached with each member's account.
pub(crate) struct OneDriveStorage {
    config: StorageConfig,
    drive: String,
    folder: String,
    session: OAuthSession,
    ids: IdSourceRef,
    api: String,
    single_request_limit: u64,
    folder_ids: tokio::sync::Mutex<BTreeMap<String, String>>,
}
impl OneDriveStorage {
    /// Construct at a composition root with the device's own provider sign-in.
    pub(crate) fn new(
        config: StorageConfig,
        session: OAuthSession,
        ids: IdSourceRef,
    ) -> Result<Self, StorageError> {
        let StorageConfig::OneDrive {
            drive_id,
            folder_id,
            ..
        } = &config
        else {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("expected OneDrive location")
            );
        };
        if session.provider() != PROVIDER {
            return Err(StorageFailure::InvalidConfiguration.with_source("wrong OAuth provider"));
        }
        Ok(Self {
            drive: drive_id.clone(),
            folder: folder_id.clone(),
            config,
            session,
            ids,
            api: "https://graph.microsoft.com/v1.0".into(),
            single_request_limit: 250_000_000,
            folder_ids: tokio::sync::Mutex::new(BTreeMap::new()),
        })
    }
    fn item(&self, id: &str, suffix: &[&str]) -> Result<String, StorageError> {
        let mut parts = vec!["drives", &self.drive, "items", id];
        parts.extend_from_slice(suffix);
        http::endpoint(&self.api, &parts, &[])
    }
    fn by_path(&self, path: &ObjectPath, suffix: &str) -> Result<String, StorageError> {
        let root = self.item(&self.folder, &[])?;
        Ok(format!("{root}:{}:{suffix}", path.absolute()))
    }
    async fn send(
        &self,
        method: Method,
        url: &str,
        body: Body,
    ) -> Result<reqwest::Response, StorageError> {
        let headers = body_headers(&body);
        self.session.send(method, url, &headers, body, true).await
    }
    async fn publish(
        &self,
        publication: &PublicationRequest,
        method: Method,
        url: &str,
        body: Body,
    ) -> Result<reqwest::Response, StorageError> {
        let headers = body_headers(&body);
        self.session
            .publish(publication, method, url, &headers, body, true)
            .await
    }
    async fn check_folder(&self) -> Result<(), StorageError> {
        let value = http::json(
            PROVIDER,
            self.send(Method::GET, &self.item(&self.folder, &[])?, Body::Empty)
                .await?,
        )
        .await
        .map_err(http::container_error)?;
        if http::string(&value, "id")? != self.folder {
            return Err(StorageFailure::Protocol.with_source("OneDrive returned another folder"));
        }
        if !value["folder"].is_object() {
            // The configured id now names a file or package, not the folder.
            return Err(StorageFailure::ContainerNotFound
                .with_source("OneDrive's configured item is not a folder"));
        }
        Ok(())
    }
    async fn metadata(&self, path: &ObjectPath) -> Result<Value, StorageError> {
        let result = http::json(
            PROVIDER,
            self.send(Method::GET, &self.by_path(path, "")?, Body::Empty)
                .await?,
        )
        .await;
        if result
            .as_ref()
            .is_err_and(|error| error.failure() == StorageFailure::NotFound)
        {
            self.check_folder().await?;
        }
        result
    }
    async fn parents(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
    ) -> Result<String, StorageError> {
        let parts = path.components();
        self.directory(publication, &parts[..parts.len() - 1]).await
    }
    async fn get(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectStream<'_>, StorageError> {
        let value = self.metadata(path).await?;
        let url = http::string(&value, "@microsoft.graph.downloadUrl")?;
        validate_download_url(&self.api, url)?;
        let headers = range
            .map(|range| vec![("Range", range.header())])
            .unwrap_or_default();
        let metadata = object_metadata::onedrive(path, &value)?;
        let stream = super::http_body::stream(
            PROVIDER,
            self.session
                .send(Method::GET, url, &headers, Body::Empty, false)
                .await?,
            metadata.clone(),
            range,
        )
        .await?;
        if path.is_replaceable() {
            let path = path.clone();
            return Ok(stream.after(async move {
                let after = object_metadata::onedrive(&path, &self.metadata(&path).await?)?;
                if after != metadata {
                    return Err(
                        StorageFailure::Protocol.with_source("OneDrive object changed during read")
                    );
                }
                Ok(())
            }));
        }
        Ok(stream)
    }
    async fn require_owner(&self) -> Result<(), StorageError> {
        let url = http::endpoint(&self.api, &["me", "drive"], &[("$select", "id")])?;
        let value = http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await?;
        if http::string(&value, "id")? != self.drive {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        Ok(())
    }
    /// Once [`Self::require_owner`] passed, the signed-in account holds the
    /// store; inviting either of its addresses is refused (§13.2).
    async fn refuse_owner(&self, account: &str) -> Result<(), StorageError> {
        let url = http::endpoint(&self.api, &["me"], &[("$select", "mail,userPrincipalName")])?;
        let value = http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await?;
        if ["mail", "userPrincipalName"].into_iter().any(|field| {
            value[field]
                .as_str()
                .is_some_and(|address| address.eq_ignore_ascii_case(account))
        }) {
            return Err(StorageFailure::InviteeIsStoreOwner.into());
        }
        Ok(())
    }
    async fn permissions(&self) -> Result<Vec<Value>, StorageError> {
        let mut url = self.item(&self.folder, &["permissions"])?;
        let mut seen = Pagination::new();
        let mut result = Vec::new();
        loop {
            seen.check(&url)?;
            http::same_origin(&self.api, &url)?;
            let value =
                http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await?;
            result.extend(http::array(&value, "value")?.iter().cloned());
            match value.get("@odata.nextLink") {
                None => break,
                Some(next) => {
                    url = next
                        .as_str()
                        .ok_or(StorageFailure::Protocol.with_source("invalid OneDrive next link"))?
                        .into()
                }
            }
        }
        Ok(result)
    }
    async fn invitation(&self, email: &str) -> Result<Option<StorageInvitation>, StorageError> {
        let permissions = self.permissions().await?;
        let account = AccountPermissions::new(email, &permissions)?;
        for permission in &permissions {
            if account.matches(permission)?
                && writable(permission)
                && permission["link"]["scope"].as_str() != Some("existingAccess")
            {
                // A permission's shareId can name the permission rather than a
                // usable shares key. Its sharing URL has Graph's defined u!
                // encoding; use that representation whenever it is supplied.
                let acceptance = match (permission["link"].get("webUrl"), permission.get("shareId"))
                {
                    (Some(_), _) => crate::invitation::InvitationAcceptance::OneDriveShare {
                        token: SecretText::new(format!(
                            "u!{}",
                            URL_SAFE_NO_PAD.encode(http::string(&permission["link"], "webUrl")?)
                        )),
                    },
                    (None, Some(_)) => crate::invitation::InvitationAcceptance::OneDriveShare {
                        token: SecretText::new(http::string(permission, "shareId")?.into()),
                    },
                    (None, None)
                        if permission
                            .get("invitation")
                            .is_some_and(|value| !value.is_null())
                            || permission.get("link").is_some_and(|value| !value.is_null()) =>
                    {
                        return Err(StorageFailure::Protocol
                            .with_source("OneDrive omitted acceptance token"))
                    }
                    (None, None) => crate::invitation::InvitationAcceptance::Granted,
                };
                return Ok(Some(StorageInvitation::new(self.config(), acceptance)?));
            }
        }
        Ok(None)
    }
}
fn validate_download_url(api: &str, target: &str) -> Result<(), StorageError> {
    let url =
        url::Url::parse(target).map_err(|error| StorageFailure::Protocol.with_source(error))?;
    if crate::web_url::is_web_url(&url) && url.scheme() == "https" {
        return Ok(());
    }
    // Loopback endpoints permit HTTP for provider conformance tests.
    http::same_origin(api, target)
}
#[async_trait]
impl ProviderOps for OneDriveStorage {
    async fn account(&self) -> Result<String, StorageError> {
        self.session.account().await
    }
    async fn set_oauth_tokens(&self, tokens: OAuthTokens) -> Result<(), StorageError> {
        self.session.set_tokens(tokens).await;
        Ok(())
    }

    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    fn single_request_limit(&self) -> u64 {
        self.single_request_limit
    }
    async fn create(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<Option<StoredObject>, StorageError> {
        self.parents(publication, path).await?;
        let url = format!(
            "{}?@microsoft.graph.conflictBehavior=fail",
            self.by_path(path, "/content")?
        );
        let result = http::json(
            PROVIDER,
            self.session
                .publish(
                    publication,
                    Method::PUT,
                    &url,
                    &[
                        ("If-None-Match", "*".into()),
                        ("Content-Type", "application/octet-stream".into()),
                        ("Content-Length", bytes.len().to_string()),
                    ],
                    Body::Bytes(bytes.to_vec(), sent.clone()),
                    true,
                )
                .await?,
        )
        .await;
        // The reply is the created item.
        let item = self.checked_folder_operation(result).await?;
        object_metadata::onedrive(path, &item).map(Some)
    }

    async fn replace(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.replace_content(publication, previous, bytes).await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        match self.metadata(path).await {
            Ok(item) => Ok(Some(object_metadata::onedrive(path, &item)?)),
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
        // The response body yields bytes as they arrive; the connection
        // hears its reads.
        _received: &ByteProgress,
    ) -> Result<ObjectStream<'_>, StorageError> {
        self.get(path, range).await
    }
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError> {
        self.list_folders(listing).await
    }

    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        let response = self
            .send(Method::DELETE, &self.by_path(path, "")?, Body::Empty)
            .await?;
        if response.status().as_u16() == 404 {
            self.check_folder().await?;
        } else {
            http::checked(PROVIDER, response).await?;
        }
        Ok(())
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.require_owner().await?;
        self.refuse_owner(account).await?;
        if let Some(invitation) = self.invitation(account).await? {
            return Ok(AccessGrant::Granted { invitation });
        }
        http::checked(PROVIDER,self.send(Method::POST,&self.item(&self.folder,&["invite"])?,Body::Json(json!({"recipients":[{"email":account}],"roles":["write"],"requireSignIn":true,"sendInvitation":true}))).await?).await?;
        let invitation = self
            .invitation(account)
            .await?
            .ok_or(StorageFailure::Protocol.with_source("OneDrive did not grant write access"))?;
        Ok(AccessGrant::Granted { invitation })
    }

    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        if let crate::invitation::InvitationAcceptance::OneDriveShare { token } =
            &invitation.acceptance
        {
            let url = http::endpoint(&self.api, &["shares", token.as_str(), "driveItem"], &[])?;
            for prefer in ["redeemSharingLinkIfNecessary", "redeemSharingLink"] {
                let value = http::json(
                    PROVIDER,
                    self.session
                        .send(
                            Method::GET,
                            &url,
                            &[("Prefer", prefer.into())],
                            Body::Empty,
                            true,
                        )
                        .await?,
                )
                .await?;
                if http::string(&value, "id")? != self.folder
                    || http::string(&value["parentReference"], "driveId")? != self.drive
                    || !value["folder"].is_object()
                {
                    return Err(StorageFailure::InvitationMismatch.into());
                }
            }
        }
        Ok(())
    }
    async fn revoke_access(&self, email: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        self.require_owner().await?;
        let permissions = self.permissions().await?;
        let account = AccountPermissions::new(email, &permissions)?;
        let mut exclusive = Vec::new();
        for permission in &permissions {
            if matches!(account.classify(permission)?, PermissionAccess::Exclusive) {
                exclusive.push((
                    account.is_named(permission)?,
                    http::string(permission, "id")?.to_owned(),
                ));
            }
        }
        // Keep email-bearing permissions until id-only grants are gone. If a
        // deletion reply is lost, a retry can still resolve the native account.
        exclusive.sort();
        for (_, id) in exclusive {
            let response = self
                .send(
                    Method::DELETE,
                    &self.item(&self.folder, &["permissions", &id])?,
                    Body::Empty,
                )
                .await?;
            if response.status().as_u16() != 404 {
                http::checked(PROVIDER, response).await?;
            }
        }
        let mut shares = Vec::new();
        for permission in self.permissions().await? {
            match account.classify(&permission)? {
                PermissionAccess::Unrelated => {}
                PermissionAccess::Exclusive => {
                    return Err(StorageFailure::Protocol
                        .with_source("OneDrive access remains after revocation"))
                }
                PermissionAccess::Retained(reason) => shares.push(RetainedAccess {
                    provider_id: http::string(&permission, "id")?.into(),
                    reason,
                }),
            }
        }
        Ok(shares)
    }

    async fn begin_upload(
        &self,
        publication: &PublicationRequest,
        target: &UploadTarget,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        publication.check()?;
        match target {
            UploadTarget::Create(path) => Ok(self.plan_temporary(path, total)),
            UploadTarget::Replace(previous) => {
                self.begin_replacement(publication, previous, total).await
            }
        }
    }
    async fn resume_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        match session.target() {
            UploadTarget::Create(_) => self.resume_temporary(publication, session).await,
            UploadTarget::Replace(_) => self.resume_replacement(session).await,
        }
    }
    async fn upload_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
        sent: &ByteProgress,
    ) -> Result<(), StorageError> {
        match session.target() {
            UploadTarget::Create(_) => self.send_part(publication, session, bytes, end, sent).await,
            UploadTarget::Replace(_) => {
                self.replacement_part(publication, session, bytes, end)
                    .await
            }
        }
    }
    async fn finish_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        match session.target() {
            UploadTarget::Create(_) => self.publish_temporary(publication, session).await,
            // The last fragment publishes; the connection accepts Complete before this call.
            UploadTarget::Replace(_) => Err(StorageFailure::InvalidPart.into()),
        }
    }
    async fn abort_upload(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        match session.target() {
            UploadTarget::Create(_) => self.abort_temporary(publication, session).await,
            UploadTarget::Replace(_) => self.cancel_replacement(replace::url(session)?).await,
        }
    }
}
fn body_headers(body: &Body) -> Vec<(&'static str, String)> {
    match body {
        Body::Bytes(bytes, _) => vec![
            ("Content-Type", "application/octet-stream".into()),
            ("Content-Length", bytes.len().to_string()),
        ],
        _ => Vec::new(),
    }
}

fn writable(value: &Value) -> bool {
    value["roles"].as_array().is_some_and(|roles| {
        roles
            .iter()
            .any(|r| matches!(r.as_str(), Some("write" | "owner")))
    })
}

#[cfg(test)]
#[path = "onedrive_tests.rs"]
mod tests;
