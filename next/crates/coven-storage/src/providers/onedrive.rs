use super::access::PermissionAccess;
use super::http::{self, Body, OAuthSession};
use super::onedrive_access::AccountPermissions;
use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use coven_crypto::SecretText;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
const PROVIDER: CloudProvider = CloudProvider::OneDrive;

/// OneDrive objects under the store's shared folder, reached with each member's account.
pub struct OneDriveStorage {
    config: StorageConfig,
    drive: String,
    folder: String,
    session: OAuthSession,
    api: String,
}
impl OneDriveStorage {
    /// Construct at a composition root with the device's own provider sign-in.
    pub fn new(config: StorageConfig, session: OAuthSession) -> Result<Self, StorageError> {
        config.validate()?;
        let StorageConfig::OneDrive {
            drive_id,
            folder_id,
        } = &config
        else {
            return Err(StorageError::InvalidConfiguration(
                "expected OneDrive location",
            ));
        };
        if session.provider() != PROVIDER {
            return Err(StorageError::InvalidConfiguration("wrong OAuth provider"));
        }
        Ok(Self {
            drive: drive_id.clone(),
            folder: folder_id.clone(),
            config,
            session,
            api: "https://graph.microsoft.com/v1.0".into(),
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
        let headers = if matches!(body, Body::Bytes(_)) {
            vec![("Content-Type", "application/octet-stream".into())]
        } else {
            Vec::new()
        };
        self.session.send(method, url, &headers, body, true).await
    }
    async fn metadata(&self, path: &ObjectPath) -> Result<Value, StorageError> {
        http::json(
            PROVIDER,
            self.send(Method::GET, &self.by_path(path, "")?, Body::Empty)
                .await?,
        )
        .await
    }
    async fn parents(&self, path: &ObjectPath) -> Result<(), StorageError> {
        let mut id = self.folder.clone();
        let parts = path.components();
        for name in &parts[..parts.len() - 1] {
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
                return Err(StorageError::AlreadyExists);
            }
            id = http::string(&value, "id")?.into();
        }
        Ok(())
    }
    async fn get(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, StorageError> {
        let value = self.metadata(path).await?;
        let url = http::string(&value, "@microsoft.graph.downloadUrl")?;
        validate_download_url(&self.api, url)?;
        let headers = range
            .map(|range| vec![("Range", range.header())])
            .unwrap_or_default();
        http::bytes(
            PROVIDER,
            self.session
                .send(Method::GET, url, &headers, Body::Empty, false)
                .await?,
            range,
        )
        .await
    }
    fn upload_url<'a>(&self, session: &'a UploadSession) -> Result<&'a str, StorageError> {
        session.check(&self.config)?;
        let SessionState::OneDrive { url } = &session.state else {
            return Err(StorageError::SessionMismatch);
        };
        validate_download_url(&self.api, url.as_str())?;
        Ok(url.as_str())
    }
    async fn progress(
        &self,
        session: &mut UploadSession,
        response: reqwest::Response,
    ) -> Result<(), StorageError> {
        if response.status().as_u16() == 404 {
            return Err(StorageError::SessionExpired);
        }
        let response = http::checked(PROVIDER, response).await?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(|e| http::transport(PROVIDER, e))?;
        let invalid = || http::invalid_response(PROVIDER, status, headers.clone(), body.to_vec());
        let value: Value = serde_json::from_slice(&body).map_err(|_| invalid())?;
        if let Some(ranges) = value.get("nextExpectedRanges") {
            let ranges = ranges.as_array().ok_or_else(invalid)?;
            let mut first: Option<u64> = None;
            for range in ranges {
                let (start, end) = range
                    .as_str()
                    .and_then(|s| s.split_once('-'))
                    .ok_or_else(invalid)?;
                let start = start.parse::<u64>().map_err(|_| invalid())?;
                if start < session.confirmed || start >= session.total {
                    return Err(invalid());
                }
                if !end.is_empty() {
                    let end = end.parse::<u64>().map_err(|_| invalid())?;
                    if end < start || end >= session.total {
                        return Err(invalid());
                    }
                }
                first = Some(match first {
                    Some(previous) => previous.min(start),
                    None => start,
                });
            }
            session.confirmed = first.ok_or_else(invalid)?;
        } else {
            if value["size"].as_u64() != Some(session.total)
                || http::string(&value, "name")? != session.path.file_name()
            {
                return Err(StorageError::Protocol("OneDrive completed another file"));
            }
            session.confirmed = session.total;
            session.state = SessionState::Complete;
        }
        Ok(())
    }
    async fn require_owner(&self) -> Result<(), StorageError> {
        let url = http::endpoint(&self.api, &["me", "drive"], &[("$select", "id")])?;
        let value = http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await?;
        if http::string(&value, "id")? != self.drive {
            return Err(StorageError::NotStoreOwner);
        }
        Ok(())
    }
    async fn permissions(&self) -> Result<Vec<Value>, StorageError> {
        let mut url = self.item(&self.folder, &["permissions"])?;
        let mut seen = BTreeSet::new();
        let mut result = Vec::new();
        loop {
            if !seen.insert(url.clone()) {
                return Err(StorageError::Protocol("repeated OneDrive permission page"));
            }
            http::same_origin(&self.api, &url)?;
            let value =
                http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await?;
            result.extend(http::array(&value, "value")?.iter().cloned());
            match value.get("@odata.nextLink") {
                None => break,
                Some(next) => {
                    url = next
                        .as_str()
                        .ok_or(StorageError::Protocol("invalid OneDrive next link"))?
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
                        return Err(StorageError::Protocol("OneDrive omitted acceptance token"))
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
    let url = url::Url::parse(target)
        .map_err(|_| StorageError::Protocol("invalid OneDrive transfer URL"))?;
    if url.scheme() == "https"
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
    {
        return Ok(());
    }
    // Loopback endpoints permit HTTP for provider conformance tests.
    http::same_origin(api, target)
}
#[async_trait]
impl Storage for OneDriveStorage {
    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    async fn set_oauth_tokens(&self, tokens: OAuthTokens) -> Result<(), StorageError> {
        self.session.set_tokens(tokens).await;
        Ok(())
    }
    fn single_request_limit(&self) -> u64 {
        250 * 1024 * 1024
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        crate::transfer::upload_bytes(self, path, bytes, async {
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
                        ],
                        Body::Bytes(bytes.to_vec()),
                        true,
                    )
                    .await?,
            )
            .await?;
            Ok(())
        })
        .await
    }

    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if !path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        crate::transfer::check_single_request(bytes.len() as u64, self.single_request_limit())?;
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
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        self.get(path, None).await
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        self.get(path, Some(range)).await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        let mut folders = vec![(self.folder.clone(), Vec::new())];
        let mut visited = BTreeSet::new();
        let mut paths = BTreeMap::new();
        while let Some((folder, names)) = folders.pop() {
            if !visited.insert(folder.clone()) {
                return Err(StorageError::Protocol("OneDrive folder cycle"));
            }
            let mut url = self.item(&folder, &["children"])?;
            let mut pages = BTreeSet::new();
            loop {
                if !pages.insert(url.clone()) {
                    return Err(StorageError::Protocol("repeated OneDrive listing page"));
                }
                http::same_origin(&self.api, &url)?;
                let value =
                    http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?).await?;
                for item in http::array(&value, "value")? {
                    let name = http::string(item, "name")?;
                    if item["folder"].is_object() {
                        let mut parts = names.clone();
                        parts.push(name.to_owned());
                        crate::path::validate_directory(&parts.join("/"))?;
                        folders.push((http::string(item, "id")?.into(), parts));
                    } else if item["file"].is_object() {
                        let path = ObjectPath::from_components(&names, name)?;
                        if prefix.contains(&path) {
                            let object = StoredObject {
                                path: path.clone(),
                                size: item["size"].as_u64().ok_or(StorageError::Protocol(
                                    "OneDrive omitted object size",
                                ))?,
                                stored_at: http::timestamp(item, "createdDateTime")?,
                            };
                            if let Some(previous) = paths.insert(path, object.clone()) {
                                if previous != object {
                                    return Err(StorageError::Protocol(
                                        "OneDrive listed conflicting objects",
                                    ));
                                }
                            }
                        }
                    } else {
                        return Err(StorageError::Protocol("unexpected OneDrive item kind"));
                    }
                }
                match value.get("@odata.nextLink") {
                    None => break,
                    Some(next) => {
                        url = next
                            .as_str()
                            .ok_or(StorageError::Protocol("invalid OneDrive listing link"))?
                            .into()
                    }
                }
            }
        }
        Ok(paths.into_values().collect())
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        let response = self
            .send(Method::DELETE, &self.by_path(path, "")?, Body::Empty)
            .await?;
        if response.status().as_u16() != 404 {
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
            .ok_or(StorageError::Protocol(
                "OneDrive did not grant write access",
            ))?;
        Ok(AccessGrant::Granted { invitation })
    }

    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        invitation.check(&self.config)?;
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
                    return Err(StorageError::InvitationMismatch);
                }
            }
        }
        self.list(&ObjectPrefix::all()).await?;
        Ok(())
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        let MemberAccess::ProviderAccount(email) = member else {
            return Err(StorageError::InvalidConfiguration(
                "OneDrive requires an account",
            ));
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
                    return Err(StorageError::Protocol(
                        "OneDrive access remains after revocation",
                    ))
                }
                PermissionAccess::Retained(reason) => shares.push(RetainedAccess {
                    provider_id: http::string(&permission, "id")?.into(),
                    reason,
                }),
            }
        }
        if shares.is_empty() {
            Ok(MemberRemoval::Revoked)
        } else {
            Ok(MemberRemoval::AccessRemains { shares })
        }
    }

    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        if path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        if total == 0 {
            return Err(StorageError::InvalidPart);
        }
        self.parents(path).await?;
        let value=http::json(PROVIDER,self.send(Method::POST,&self.by_path(path,"/createUploadSession")?,Body::Json(json!({"item":{"@microsoft.graph.conflictBehavior":"fail","name":path.file_name()}}))).await?).await?;
        let url = http::string(&value, "uploadUrl")?;
        validate_download_url(&self.api, url)?;
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: crate::session::ONEDRIVE_PART_SIZE,
            state: SessionState::OneDrive {
                url: SecretText::new(url.into()),
            },
        })
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() || matches!(session.state, SessionState::VerifyPublished) {
            return Ok(());
        }
        let response = self
            .session
            .send(
                Method::GET,
                self.upload_url(session)?,
                &[],
                Body::Empty,
                false,
            )
            .await?;
        if response.status().as_u16() == 404 {
            let value = match self.metadata(&session.path).await {
                Ok(value) => value,
                Err(error) if error.failure() == StorageFailure::NotFound => {
                    return Err(StorageError::SessionExpired)
                }
                Err(error) => return Err(error),
            };
            if value["size"].as_u64() != Some(session.total)
                || http::string(&value, "name")? != session.path.file_name()
            {
                return Err(StorageError::AlreadyExists);
            }
            session.confirmed = 0;
            session.state = SessionState::VerifyPublished;
            return Ok(());
        }
        self.progress(session, response).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if matches!(session.state, SessionState::VerifyPublished) {
            return http::verify_published_part(self, session, bytes).await;
        }
        let end = session.end_of_part(bytes.len())?;
        let response = self
            .session
            .send(
                Method::PUT,
                self.upload_url(session)?,
                &[
                    (
                        "Content-Range",
                        format!("bytes {}-{}/{}", session.confirmed, end - 1, session.total),
                    ),
                    ("Content-Type", "application/octet-stream".into()),
                ],
                Body::Bytes(bytes.to_vec()),
                false,
            )
            .await?;
        self.progress(session, response).await?;
        if session.confirmed < end {
            return Err(StorageError::Protocol(
                "OneDrive did not store the entire part",
            ));
        }
        Ok(())
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.resume_upload(session).await?;
        if !session.is_complete() {
            return Err(StorageError::InvalidPart);
        }
        Ok(())
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() || matches!(session.state, SessionState::VerifyPublished) {
            return Ok(());
        }
        let response = self
            .session
            .send(
                Method::DELETE,
                self.upload_url(session)?,
                &[],
                Body::Empty,
                false,
            )
            .await?;
        if response.status().as_u16() != 404 {
            http::checked(PROVIDER, response).await?;
        }
        Ok(())
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
