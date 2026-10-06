use super::http::{self, Body, OAuthSession};
use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use coven_crypto::SecretText;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeSet;
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
            return Err(StorageError::InvalidConfiguration(
                "expected Dropbox namespace",
            ));
        };
        if session.provider() != PROVIDER {
            return Err(StorageError::InvalidConfiguration("wrong OAuth provider"));
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
        let url = http::endpoint(&self.api, &method.split('/').collect::<Vec<_>>(), &[])?;
        http::json(
            PROVIDER,
            self.session
                .send(
                    Method::POST,
                    &url,
                    &[("Dropbox-API-Path-Root", self.root())],
                    Body::Json(value),
                    true,
                )
                .await?,
        )
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
            _ => Err(StorageError::SessionMismatch),
        }
    }
    async fn members(&self, email: &str) -> Result<Option<String>, StorageError> {
        let mut method = "sharing/list_folder_members";
        let mut request = json!({"shared_folder_id":self.namespace,"include_inherited":false});
        let mut seen = BTreeSet::new();
        loop {
            let value = self.rpc(method, request).await?;
            for (array, field) in [("users", "user"), ("invitees", "invitee")] {
                for member in http::array(&value, array)? {
                    if member[field]["email"]
                        .as_str()
                        .is_some_and(|e| e.eq_ignore_ascii_case(email))
                    {
                        return Ok(Some(http::string(&member["access_type"], ".tag")?.into()));
                    }
                }
            }
            let Some(cursor) = value.get("cursor") else {
                return Ok(None);
            };
            let cursor = cursor
                .as_str()
                .ok_or(StorageError::Protocol("invalid Dropbox member cursor"))?;
            if !seen.insert(cursor.to_owned()) {
                return Err(StorageError::Protocol("repeated Dropbox member cursor"));
            }
            method = "sharing/list_folder_members/continue";
            request = json!({"cursor":cursor});
        }
    }
    async fn remove_member(&self, email: &str) -> Result<(), StorageError> {
        if self.members(email).await?.is_none() {
            return Ok(());
        }
        let mut value = self.rpc("sharing/remove_folder_member",json!({"shared_folder_id":self.namespace,"member":{".tag":"email","email":email},"leave_a_copy":false})).await?;
        let job = match http::string(&value, ".tag")? {
            "complete" => None,
            "async_job_id" => Some(http::string(&value, "async_job_id")?.to_owned()),
            _ => return Err(StorageError::Protocol("invalid Dropbox remove launch")),
        };
        if let Some(job) = job {
            for _ in 0..60 {
                value = self
                    .rpc(
                        "sharing/check_remove_member_job_status",
                        json!({"async_job_id":job}),
                    )
                    .await?;
                match http::string(&value, ".tag")? {
                    "complete" => break,
                    "in_progress" => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
                    "failed" => {
                        return Err(StorageError::Protocol("Dropbox member removal failed"))
                    }
                    _ => return Err(StorageError::Protocol("invalid Dropbox remove status")),
                }
            }
            if value[".tag"].as_str() != Some("complete") {
                return Err(StorageError::Provider {
                    provider: PROVIDER,
                    failure: StorageFailure::Network,
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Dropbox member removal timed out",
                    )),
                });
            }
        }
        if self.members(email).await?.is_some() {
            return Err(StorageError::Protocol("Dropbox member access remains"));
        }
        Ok(())
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
        return Err(StorageError::Protocol("Dropbox returned another file"));
    }
    Ok(())
}
#[async_trait]
impl Storage for DropboxStorage {
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
            return Err(StorageError::InvalidPath);
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
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectPath>, StorageError> {
        let mut method = "files/list_folder";
        let mut request = json!({"path":"","recursive":true,"include_deleted":false,"limit":2000});
        let mut seen = BTreeSet::new();
        let mut paths = BTreeSet::new();
        loop {
            let value = self.rpc(method, request).await?;
            for entry in http::array(&value, "entries")? {
                match http::string(entry, ".tag")? {
                    "folder" => continue,
                    "file" => {}
                    _ => return Err(StorageError::Protocol("unexpected Dropbox listing entry")),
                }
                let path = ObjectPath::parse(
                    http::string(entry, "path_lower")?
                        .strip_prefix('/')
                        .ok_or(StorageError::Protocol("Dropbox path outside namespace"))?,
                )?;
                if prefix.contains(&path) {
                    paths.insert(path);
                }
            }
            match value["has_more"].as_bool() {
                Some(false) => break,
                Some(true) => {}
                None => return Err(StorageError::Protocol("Dropbox omitted pagination state")),
            }
            let cursor = http::string(&value, "cursor")?;
            if !seen.insert(cursor.to_owned()) {
                return Err(StorageError::Protocol("repeated Dropbox cursor"));
            }
            method = "files/list_folder/continue";
            request = json!({"cursor":cursor});
        }
        Ok(paths.into_iter().collect())
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
        if self.members(account).await?.as_deref() != Some("editor") {
            if self.members(account).await?.is_some() {
                self.remove_member(account).await?;
            }
            self.rpc("sharing/add_folder_member",json!({"shared_folder_id":self.namespace,"members":[{"member":{".tag":"email","email":account},"access_level":{".tag":"editor"}}],"quiet":false})).await?;
        }
        if self.members(account).await?.as_deref() != Some("editor") {
            return Err(StorageError::Protocol(
                "Dropbox did not grant editor access",
            ));
        }
        Ok(AccessGrant::Granted)
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        let MemberAccess::ProviderAccount(email) = member else {
            return Err(StorageError::InvalidConfiguration(
                "Dropbox requires an account",
            ));
        };
        self.remove_member(email).await?;
        Ok(MemberRemoval::Revoked)
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
                        return Err(StorageError::SessionExpired)
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
            return Err(StorageError::InvalidPart);
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
