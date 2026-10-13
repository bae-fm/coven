use super::access::PermissionAccess;
use super::http::{self, Body, OAuthSession};
use super::object_metadata;
use super::onedrive_access::AccountPermissions;
use super::pagination::Pagination;
use crate::session::{OneDriveUpload, SessionState};
use crate::*;
use async_trait::async_trait;
use coven_crypto::SecretText;
use coven_foundation::id_source::IdSourceRef;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeSet;
const PROVIDER: CloudProvider = CloudProvider::OneDrive;

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
        let headers = if let Body::Bytes(bytes) = &body {
            vec![
                ("Content-Type", "application/octet-stream".into()),
                ("Content-Length", bytes.len().to_string()),
            ]
        } else {
            Vec::new()
        };
        self.session.send(method, url, &headers, body, true).await
    }
    async fn check_folder(&self) -> Result<(), StorageError> {
        let value = http::json(
            PROVIDER,
            self.send(Method::GET, &self.item(&self.folder, &[])?, Body::Empty)
                .await?,
        )
        .await
        .map_err(http::container_error)?;
        if http::string(&value, "id")? != self.folder || !value["folder"].is_object() {
            return Err(StorageFailure::InvalidPath.into());
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
    async fn parents(&self, path: &ObjectPath) -> Result<String, StorageError> {
        let parts = path.components();
        self.directory(&parts[..parts.len() - 1]).await
    }
    async fn directory(&self, parts: &[&str]) -> Result<String, StorageError> {
        self.check_folder().await?;
        let mut id = self.folder.clone();
        for name in parts {
            let value = http::json(
                PROVIDER,
                self.send(
                    Method::POST,
                    &self.item(&id, &["children"])?,
                    Body::Json(
                        json!({"name":name,"folder":{},"@microsoft.graph.conflictBehavior":"fail"}),
                    ),
                )
                .await?,
            )
            .await;
            let value = match value {
                Ok(value) => value,
                Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                    let base = self.item(&id, &[])?;
                    http::json(
                        PROVIDER,
                        self.send(Method::GET, &format!("{base}:/{name}:"), Body::Empty)
                            .await?,
                    )
                    .await?
                }
                Err(error) => return Err(error),
            };
            if !value["folder"].is_object() {
                return Err(StorageFailure::AlreadyExists.into());
            }
            id = http::string(&value, "id")?.into();
        }
        Ok(id)
    }
    async fn get(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectRead, StorageError> {
        let value = self.metadata(path).await?;
        let url = http::string(&value, "@microsoft.graph.downloadUrl")?;
        validate_download_url(&self.api, url)?;
        let headers = range
            .map(|range| vec![("Range", range.header())])
            .unwrap_or_default();
        let metadata = object_metadata::onedrive(path, &value)?;
        let bytes = http::bytes(
            PROVIDER,
            self.session
                .send(Method::GET, url, &headers, Body::Empty, false)
                .await?,
            range,
        )
        .await?;
        if path.is_replaceable() {
            let after = object_metadata::onedrive(path, &self.metadata(path).await?)?;
            if after != metadata {
                return Err(
                    StorageFailure::Protocol.with_source("OneDrive object changed during read")
                );
            }
        }
        Ok(ObjectRead { metadata, bytes })
    }
    async fn require_owner(&self) -> Result<(), StorageError> {
        let url = http::endpoint(&self.api, &["me", "drive"], &[("$select", "id")])?;
        let value = http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await?;
        if http::string(&value, "id")? != self.drive {
            return Err(StorageFailure::NotStoreOwner.into());
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
                let acceptance = match permission.get("shareId") {
                    Some(_) => crate::invitation::InvitationAcceptance::OneDriveShare {
                        token: SecretText::new(http::string(permission, "shareId")?.into()),
                    },
                    None if permission
                        .get("invitation")
                        .is_some_and(|value| !value.is_null())
                        || permission.get("link").is_some_and(|value| !value.is_null()) =>
                    {
                        return Err(StorageFailure::Protocol
                            .with_source("OneDrive omitted acceptance token"))
                    }
                    None => crate::invitation::InvitationAcceptance::Granted,
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
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.parents(path).await?;
        let url = format!(
            "{}?@microsoft.graph.conflictBehavior=fail",
            self.by_path(path, "/content")?
        );
        http::checked(
            PROVIDER,
            self.session
                .send(
                    Method::PUT,
                    &url,
                    &[
                        ("If-None-Match", "*".into()),
                        ("Content-Type", "application/octet-stream".into()),
                        ("Content-Length", bytes.len().to_string()),
                    ],
                    Body::Bytes(bytes.to_vec()),
                    true,
                )
                .await?,
        )
        .await?;
        Ok(())
    }

    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.parents(path).await?;
        http::checked(
            PROVIDER,
            self.send(
                Method::PUT,
                &self.by_path(path, "/content")?,
                Body::Bytes(bytes.to_vec()),
            )
            .await?,
        )
        .await?;
        Ok(())
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        match self.metadata(path).await {
            Ok(item) => Ok(Some(object_metadata::onedrive(path, &item)?)),
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
    async fn read_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectRead, StorageError> {
        self.get(path, range).await
    }
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError> {
        let prefix = listing.prefix().clone();
        self.check_folder().await?;
        let store = prefix.store().to_string();
        let url = format!("{}:/{store}:", self.item(&self.folder, &[])?);
        let folder =
            match http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await {
                Ok(folder) => folder,
                Err(error) if error.failure() == StorageFailure::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
        if !folder["folder"].is_object() || folder["file"].is_object() {
            return Err(StorageFailure::InvalidPath.into());
        }
        let mut folders = vec![(http::string(&folder, "id")?.to_owned(), vec![store])];
        let mut visited = BTreeSet::new();
        while let Some((folder, names)) = folders.pop() {
            if !visited.insert(folder.clone()) {
                return Err(StorageFailure::Protocol.with_source("OneDrive folder cycle"));
            }
            let mut url = self.item(&folder, &["children"])?;
            let mut pages = Pagination::new();
            loop {
                pages.check(&url)?;
                http::same_origin(&self.api, &url)?;
                let value =
                    http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await?;
                for item in http::array(&value, "value")? {
                    let name = http::string(item, "name")?;
                    if ["folder", "file"].iter().any(|facet| {
                        item.get(*facet)
                            .is_some_and(|value| !value.is_null() && !value.is_object())
                    }) || (item["folder"].is_object() && item["file"].is_object())
                    {
                        return Err(
                            StorageFailure::Protocol.with_source("invalid OneDrive item kind")
                        );
                    }
                    if names.len() == 1 && name == upload::TEMPORARIES {
                        if !item["folder"].is_object() {
                            return Err(StorageFailure::Protocol
                                .with_source("OneDrive upload namespace is not a folder"));
                        }
                        tracing::debug!("OneDrive upload temporaries retained for session cleanup");
                        continue;
                    }
                    if item["folder"].is_object() {
                        let mut parts = names.clone();
                        parts.push(name.to_owned());
                        crate::path::validate_directory(&parts.join("/"))?;
                        folders.push((http::string(item, "id")?.into(), parts));
                    } else if item["file"].is_object() {
                        let path = ObjectPath::from_components(&names, name)?;
                        if prefix.contains(&path) {
                            let object = object_metadata::onedrive(&path, item)?;
                            listing.insert(object)?;
                        }
                    } else {
                        return Err(StorageFailure::InvalidPath.into());
                    }
                }
                match value.get("@odata.nextLink") {
                    None => break,
                    Some(next) => {
                        url = next
                            .as_str()
                            .ok_or(
                                StorageFailure::Protocol
                                    .with_source("invalid OneDrive listing link"),
                            )?
                            .into()
                    }
                }
            }
        }
        Ok(())
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
    async fn revoke_access(
        &self,
        member: &MemberAccess,
    ) -> Result<ProviderRevocation, StorageError> {
        let MemberAccess::ProviderAccount(email) = member else {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("OneDrive requires an account")
            );
        };
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
        Ok(ProviderRevocation::Remaining(shares))
    }

    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.begin_temporary(path, total).await
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.resume_temporary(session).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
    ) -> Result<(), StorageError> {
        self.send_part(session, bytes, end).await
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.publish_temporary(session).await
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.abort_temporary(session).await
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
