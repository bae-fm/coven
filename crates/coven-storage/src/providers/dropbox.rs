use super::dropbox_access::{remaining_parent_access, FolderMembers, MemberId};
use super::http::{self, Body, OAuthSession};
use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use coven_crypto::SecretText;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
const PROVIDER: CloudProvider = CloudProvider::Dropbox;

/// Dropbox objects reached through the shared namespace, independent of mount names.
pub struct DropboxStorage {
    config: StorageConfig,
    namespace: String,
    session: OAuthSession,
    api: String,
    content: String,
}
impl DropboxStorage {
    /// Construct with this device's own Dropbox sign-in.
    pub fn new(config: StorageConfig, session: OAuthSession) -> Result<Self, StorageError> {
        config.validate()?;
        let StorageConfig::Dropbox { namespace_id } = &config else {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("expected Dropbox namespace")
            );
        };
        if session.provider() != PROVIDER {
            return Err(StorageFailure::InvalidConfiguration.with_source("wrong OAuth provider"));
        }
        Ok(Self {
            namespace: namespace_id.clone(),
            config,
            session,
            api: "https://api.dropboxapi.com/2".into(),
            content: "https://content.dropboxapi.com/2".into(),
        })
    }
    fn root(&self) -> String {
        json!({".tag":"namespace_id","namespace_id":self.namespace}).to_string()
    }
    async fn rpc(&self, method: &str, value: Value) -> Result<Value, StorageError> {
        http::json(PROVIDER, self.rpc_response(method, value).await?).await
    }
    async fn rpc_response(
        &self,
        method: &str,
        value: Value,
    ) -> Result<reqwest::Response, StorageError> {
        let url = http::endpoint(&self.api, &method.split('/').collect::<Vec<_>>(), &[])?;
        let headers = if method.starts_with("files/") {
            vec![("Dropbox-API-Path-Root", self.root())]
        } else {
            Vec::new()
        };
        self.session
            .send(Method::POST, &url, &headers, Body::Json(value), true)
            .await
    }

    async fn content(
        &self,
        method: &str,
        arg: Value,
        data: Vec<u8>,
        range: Option<ByteRange>,
    ) -> Result<reqwest::Response, StorageError> {
        let mut headers = vec![
            ("Dropbox-API-Path-Root", self.root()),
            ("Dropbox-API-Arg", ascii_json(&arg)),
            ("Content-Type", "application/octet-stream".into()),
        ];
        if let Some(range) = range {
            headers.push(("Range", range.header()));
        }
        let url = http::endpoint(&self.content, &method.split('/').collect::<Vec<_>>(), &[])?;
        self.session
            .send(Method::POST, &url, &headers, Body::Bytes(data), true)
            .await
    }
    async fn write(&self, path: &ObjectPath, bytes: &[u8], mode: &str) -> Result<(), StorageError> {
        let response = self.content("files/upload",json!({"path":path.absolute(),"mode":mode,"autorename":false,"strict_conflict":true,"mute":true}),bytes.to_vec(),None).await?;
        let value = http::json(PROVIDER, response).await?;
        check_file(&value, path, bytes.len() as u64)
    }
    fn id<'a>(&self, session: &'a UploadSession) -> Result<&'a str, StorageError> {
        session.check(&self.config)?;
        match &session.state {
            SessionState::Dropbox { id } => Ok(id.as_str()),
            _ => Err(StorageFailure::SessionMismatch.into()),
        }
    }
    async fn require_owner(&self) -> Result<(), StorageError> {
        let value = self
            .rpc(
                "sharing/get_folder_metadata",
                json!({"shared_folder_id":self.namespace}),
            )
            .await?;
        if http::string(&value["access_type"], ".tag")? != "owner" {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        Ok(())
    }
    async fn members(&self, email: &str, inherited: bool) -> Result<FolderMembers, StorageError> {
        let mut method = "sharing/list_folder_members";
        let mut request = json!({"shared_folder_id":self.namespace});
        if inherited {
            request["path"] = json!(format!("ns:{}", self.namespace));
        }
        let mut seen = BTreeSet::new();
        let mut members = FolderMembers::default();
        loop {
            let value = self.rpc(method, request).await?;
            members.append(&value, email, inherited)?;
            let Some(_) = value.get("cursor") else {
                return Ok(members);
            };
            let cursor = http::string(&value, "cursor")?;
            if !seen.insert(cursor.to_owned()) {
                return Err(StorageFailure::Protocol.with_source("repeated Dropbox member cursor"));
            }
            method = "sharing/list_folder_members/continue";
            request = json!({"cursor":cursor});
        }
    }
    async fn remove_member(&self, member: &MemberId) -> Result<Vec<RetainedAccess>, StorageError> {
        let value = self.rpc("sharing/remove_folder_member",json!({"shared_folder_id":self.namespace,"member":member.selector(),"leave_a_copy":false})).await?;
        if http::string(&value, ".tag")? != "async_job_id" {
            return Err(StorageFailure::Protocol.with_source("invalid Dropbox remove launch"));
        }
        let job = http::string(&value, "async_job_id")?;
        for _ in 0..60 {
            let (value, original) = http::json_response(
                PROVIDER,
                self.rpc_response(
                    "sharing/check_remove_member_job_status",
                    json!({"async_job_id":job}),
                )
                .await?,
            )
            .await?;
            match http::string(&value, ".tag")? {
                "complete" => return remaining_parent_access(&value["complete"], member),
                "in_progress" => self.session.sleep(std::time::Duration::from_secs(1)).await,
                "failed" => return Err(original.into_error(PROVIDER)),
                _ => {
                    return Err(
                        StorageFailure::Protocol.with_source("invalid Dropbox remove status")
                    )
                }
            }
        }
        Err(StorageError::Provider {
            provider: PROVIDER,
            failure: StorageFailure::Network,
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Dropbox member removal timed out",
            )),
        })
    }
}
enum UploadLookup {
    Offset { confirmed: u64, cause: StorageError },
    Gone,
}
fn upload_lookup(
    error: StorageError,
    confirmed: u64,
    total: u64,
) -> Result<UploadLookup, StorageError> {
    let StorageError::Provider { source, .. } = &error else {
        return Err(error);
    };
    let Some(response) = source.downcast_ref::<http::ProviderResponse>() else {
        return Err(error);
    };
    let value: Value = match serde_json::from_slice(response.body()) {
        Ok(value) => value,
        // Preserve the provider failure, including a non-JSON body, for the caller.
        Err(_) => return Err(error),
    };
    match value["error"][".tag"].as_str() {
        Some("not_found" | "closed") => Ok(UploadLookup::Gone),
        Some("incorrect_offset") => {
            let offset = value["error"]["correct_offset"].as_u64();
            match offset {
                Some(offset) if offset > confirmed && offset <= total => Ok(UploadLookup::Offset {
                    confirmed: offset,
                    cause: error,
                }),
                _ => Err(http::invalid_response(
                    PROVIDER,
                    response.status(),
                    response.headers().clone(),
                    response.body().to_vec(),
                )),
            }
        }
        _ => Err(error),
    }
}

fn ascii_json(value: &Value) -> String {
    let mut result = String::new();
    for character in value.to_string().chars() {
        if character.is_ascii() && character != '\u{7f}' {
            result.push(character);
        } else {
            for unit in character.encode_utf16(&mut [0; 2]) {
                result.push_str(&format!("\\u{unit:04x}"));
            }
        }
    }
    result
}
fn check_file(value: &Value, path: &ObjectPath, size: u64) -> Result<(), StorageError> {
    if http::string(value, "path_lower")? != path.absolute() || value["size"].as_u64() != Some(size)
    {
        return Err(StorageFailure::Protocol.with_source("Dropbox returned another file"));
    }
    Ok(())
}
#[async_trait]
impl Storage for DropboxStorage {
    async fn account(&self) -> Result<String, StorageError> {
        self.session.account().await
    }

    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    async fn set_oauth_tokens(&self, tokens: OAuthTokens) -> Result<(), StorageError> {
        self.session.set_tokens(tokens).await;
        Ok(())
    }
    fn single_request_limit(&self) -> u64 {
        150 * 1024 * 1024
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        crate::transfer::upload_bytes(self, path, bytes, self.write(path, bytes, "add")).await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if !path.is_replaceable() {
            return Err(StorageFailure::InvalidPath.into());
        }
        crate::transfer::check_single_request(bytes.len() as u64, self.single_request_limit())?;
        self.write(path, bytes, "overwrite").await
    }
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        http::bytes(
            PROVIDER,
            self.content(
                "files/download",
                json!({"path":path.absolute()}),
                Vec::new(),
                None,
            )
            .await?,
            None,
        )
        .await
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        http::bytes(
            PROVIDER,
            self.content(
                "files/download",
                json!({"path":path.absolute()}),
                Vec::new(),
                Some(range),
            )
            .await?,
            Some(range),
        )
        .await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        let mut method = "files/list_folder";
        let mut request = json!({"path":"","recursive":true,"include_deleted":false,"limit":2000});
        let mut seen = BTreeSet::new();
        let mut paths = BTreeMap::new();
        loop {
            let value = self.rpc(method, request).await?;
            for entry in http::array(&value, "entries")? {
                match http::string(entry, ".tag")? {
                    "folder" => {
                        crate::path::validate_directory(
                            http::string(entry, "path_lower")?
                                .strip_prefix('/')
                                .ok_or(StorageFailure::InvalidPath)?,
                        )?;
                        continue;
                    }
                    "file" => {}
                    _ => {
                        return Err(StorageFailure::Protocol
                            .with_source("unexpected Dropbox listing entry"))
                    }
                }
                let path = ObjectPath::parse(
                    http::string(entry, "path_lower")?.strip_prefix('/').ok_or(
                        StorageFailure::Protocol.with_source("Dropbox path outside namespace"),
                    )?,
                )?;
                if prefix.contains(&path) {
                    let object = StoredObject {
                        path: path.clone(),
                        size: entry["size"].as_u64().ok_or(
                            StorageFailure::Protocol.with_source("Dropbox omitted object size"),
                        )?,
                        stored_at: http::timestamp(entry, "server_modified")?,
                    };
                    if let Some(previous) = paths.insert(path, object.clone()) {
                        if previous != object {
                            return Err(StorageFailure::Protocol
                                .with_source("Dropbox listed conflicting objects"));
                        }
                    }
                }
            }
            match value["has_more"].as_bool() {
                Some(false) => break,
                Some(true) => {}
                None => {
                    return Err(
                        StorageFailure::Protocol.with_source("Dropbox omitted pagination state")
                    )
                }
            }
            let cursor = http::string(&value, "cursor")?;
            if !seen.insert(cursor.to_owned()) {
                return Err(StorageFailure::Protocol.with_source("repeated Dropbox cursor"));
            }
            method = "files/list_folder/continue";
            request = json!({"cursor":cursor});
        }
        Ok(paths.into_values().collect())
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        match self
            .rpc("files/delete_v2", json!({"path":path.absolute()}))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.failure() == StorageFailure::NotFound => {
                tracing::debug!("Dropbox object already absent");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.require_owner().await?;
        let members = self.members(account, false).await?;
        if members.direct.iter().any(|member| member.role == "editor")
            || members
                .retained
                .iter()
                .any(|share| share.reason == RetainedAccessReason::StoreOwner)
        {
            return Ok(AccessGrant::Granted {
                invitation: StorageInvitation::for_account(self.config())?,
            });
        }
        if let Some(member) = members
            .direct
            .iter()
            .find(|member| matches!(member.id, MemberId::Account(_)))
        {
            self.rpc("sharing/update_folder_member", json!({"shared_folder_id":self.namespace,"member":member.id.selector(),"access_level":{".tag":"editor"}})).await?;
        } else if !members.direct.is_empty() {
            return Err(StorageFailure::AccountIdUnavailable.into());
        } else {
            self.rpc("sharing/add_folder_member", json!({"shared_folder_id":self.namespace,"members":[{"member":{".tag":"email","email":account},"access_level":{".tag":"editor"}}],"quiet":false})).await?;
        }
        let members = self.members(account, false).await?;
        if !members.direct.iter().any(|member| member.role == "editor")
            && !members
                .retained
                .iter()
                .any(|share| share.reason == RetainedAccessReason::StoreOwner)
        {
            return Err(StorageFailure::Protocol.with_source("Dropbox did not grant editor access"));
        }
        Ok(AccessGrant::Granted {
            invitation: StorageInvitation::for_account(self.config())?,
        })
    }
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        invitation.check(&self.config)?;
        let response = self
            .rpc_response(
                "sharing/mount_folder",
                json!({"shared_folder_id":self.namespace}),
            )
            .await?;
        if response.status().is_success() {
            let value = http::json(PROVIDER, response).await?;
            if http::string(&value, "shared_folder_id")? != self.namespace {
                return Err(StorageFailure::InvitationMismatch.into());
            }
        } else {
            let error = http::response_error(PROVIDER, response).await;
            let mounted = match &error {
                StorageError::Provider { source, .. } => {
                    match source.downcast_ref::<http::ProviderResponse>() {
                        Some(response) => match serde_json::from_slice::<Value>(response.body()) {
                            Ok(value) => value["error"][".tag"].as_str() == Some("already_mounted"),
                            Err(_) => false,
                        },
                        None => false,
                    }
                }
                _ => false,
            };
            if !mounted {
                return Err(error);
            }
        }
        self.list(&ObjectPrefix::all()).await?;
        Ok(())
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        let MemberAccess::ProviderAccount(email) = member else {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("Dropbox requires an account")
            );
        };
        self.require_owner().await?;
        let members = self.members(email, false).await?;
        let mut completed = Vec::new();
        for member in members.direct {
            completed.extend(self.remove_member(&member.id).await?);
        }
        let mut remaining = self.members(email, false).await?;
        if !remaining.direct.is_empty() {
            return Err(StorageFailure::Protocol.with_source("Dropbox direct access remains"));
        }
        remaining
            .retained
            .extend(self.members(email, true).await?.retained);
        remaining.retained.extend(completed);
        let mut shares = Vec::new();
        for share in remaining.retained {
            if !shares.contains(&share) {
                shares.push(share);
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
            return Err(StorageFailure::InvalidPath.into());
        }
        if total == 0 {
            return Err(StorageFailure::InvalidPart.into());
        }
        let value = http::json(
            PROVIDER,
            self.content(
                "files/upload_session/start",
                json!({"close":false}),
                Vec::new(),
                None,
            )
            .await?,
        )
        .await?;
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: crate::session::DROPBOX_PART_SIZE,
            state: SessionState::Dropbox {
                id: SecretText::new(http::string(&value, "session_id")?.into()),
            },
        })
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() || matches!(session.state, SessionState::VerifyPublished) {
            return Ok(());
        }
        // Empty append is Dropbox's offset query. An incorrect-offset response
        // is authoritative progress, including a part whose reply was lost.
        let response = self.content("files/upload_session/append_v2",json!({"cursor":{"session_id":self.id(session)?,"offset":session.confirmed},"close":false}),Vec::new(),None).await?;
        if response.status().is_success() {
            return Ok(());
        }
        let error = http::response_error(PROVIDER, response).await;
        match upload_lookup(error, session.confirmed, session.total)? {
            UploadLookup::Offset { confirmed, .. } => {
                session.confirmed = confirmed;
                Ok(())
            }
            UploadLookup::Gone => {
                let value = match self
                    .rpc(
                        "files/get_metadata",
                        json!({"path":session.path.absolute()}),
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(error) if error.failure() == StorageFailure::NotFound => {
                        return Err(StorageFailure::SessionExpired.with_source(error))
                    }
                    Err(error) => return Err(error),
                };
                check_file(&value, &session.path, session.total)?;
                session.confirmed = 0;
                session.state = SessionState::VerifyPublished;
                Ok(())
            }
        }
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
        let response = self.content("files/upload_session/append_v2",json!({"cursor":{"session_id":self.id(session)?,"offset":session.confirmed},"close":false}),bytes.to_vec(),None).await?;
        http::checked(PROVIDER, response).await?;
        session.confirmed = end;
        Ok(())
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        if session.confirmed != session.total {
            return Err(StorageFailure::InvalidPart.into());
        }
        let response = self.content("files/upload_session/finish",json!({"cursor":{"session_id":self.id(session)?,"offset":session.confirmed},"commit":{"path":session.path.absolute(),"mode":"add","autorename":false,"strict_conflict":true,"mute":true}}),Vec::new(),None).await?;
        let value = http::json(PROVIDER, response).await?;
        check_file(&value, &session.path, session.total)?;
        session.state = SessionState::Complete;
        Ok(())
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() || matches!(session.state, SessionState::VerifyPublished) {
            return Ok(());
        }
        let mut offset = session.confirmed;
        let mut retried = false;
        loop {
            let response = self
                .content(
                    "files/upload_session/append_v2",
                    json!({"cursor":{"session_id":self.id(session)?,"offset":offset},"close":true}),
                    Vec::new(),
                    None,
                )
                .await?;
            if response.status().is_success() {
                return Ok(());
            }
            let error = http::response_error(PROVIDER, response).await;
            match upload_lookup(error, offset, session.total)? {
                UploadLookup::Gone => return Ok(()),
                UploadLookup::Offset { confirmed, cause } => {
                    if retried {
                        return Err(cause);
                    }
                    offset = confirmed;
                    retried = true;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "dropbox_tests.rs"]
mod tests;
