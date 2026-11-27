//! Choosing provider folders through coven-held sign-in, before storage setup.

use super::{
    dropbox_job::JobEnd,
    http::{self, Body},
    onedrive_item::{item_kind, Kind},
    pagination::Pagination,
    OAuthSession,
};
use crate::{
    Cooldown, DropboxAccess, DropboxLocation, OAuthAccess, StorageConfig, StorageError,
    StorageFailure,
};
use reqwest::Method;
use serde_json::{json, Value};
use std::sync::Arc;

/// A provider folder's display name and the exact location accepted by setup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageFolder {
    /// The provider's current folder name; never a token or credential.
    pub name: String,
    /// Pass unchanged to storage setup, or as a parent when browsing OneDrive.
    pub storage: StorageConfig,
}

/// What stands at a name at the top of the signed-in Dropbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DropboxName {
    /// Nothing has the name.
    Free,
    /// A file or folder has it.
    Taken {
        /// The shared folder's id, when what has the name is a shared folder.
        shared_folder_id: Option<String>,
    },
}

/// What Dropbox answered a request to make a shared folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DropboxShare {
    /// The folder is made and shared.
    Shared(StorageFolder),
    /// Dropbox runs the request as the job with this id.
    Job(String),
    /// A shared folder already has the name.
    AlreadyShared,
}

/// How following a sharing job ended.
#[derive(Debug)]
pub enum DropboxShareJob {
    /// It made and shared the folder.
    Shared(StorageFolder),
    /// It failed, with the failure Dropbox names.
    Failed(StorageError),
    /// Dropbox no longer knows the job, so it can't say how it ended.
    Forgotten,
}

impl StorageFolder {
    /// The Dropbox shared folder `shared_folder_id`, named `name`, as a setup
    /// location. An id no location can have is the provider's protocol
    /// failure.
    pub fn dropbox_shared(name: &str, shared_folder_id: &str) -> Result<Self, StorageError> {
        let storage = StorageConfig::Dropbox {
            account_label: None,
            folder_label: None,
            location: DropboxLocation::SharedFolder { namespace_id: shared_folder_id.into() },
        };
        storage.validate().map_err(|error| StorageFailure::Protocol.with_source(error))?;
        Ok(Self { name: name.into(), storage })
    }
}

/// Provider metadata calls using the same session and cooldown as store setup.
pub(super) struct OAuthFolders {
    session: OAuthSession,
    access: OAuthAccess,
    api: String,
    cooldown: Arc<dyn Cooldown>,
}

impl OAuthFolders {
    pub(super) fn new(
        session: OAuthSession,
        access: OAuthAccess,
        api: String,
        cooldown: Arc<dyn Cooldown>,
    ) -> Self {
        Self { session, access, api, cooldown }
    }

    pub(super) async fn account_label(&self) -> Result<String, StorageError> {
        crate::cooldown::cooling(self.cooldown.as_ref())?;
        let result = self.session.account_at(&self.api).await;
        crate::cooldown::observed(self.cooldown.as_ref(), result).await
    }

    pub(super) async fn account_identity(&self) -> Result<crate::ProviderAccount, StorageError> {
        crate::cooldown::cooling(self.cooldown.as_ref())?;
        let result = self.session.identity_at(&self.api).await;
        crate::cooldown::observed(self.cooldown.as_ref(), result).await
    }

    fn require(&self, access: OAuthAccess) -> Result<(), StorageError> {
        if self.access != access {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("folder selection requires the matching sign-in access"));
        }
        Ok(())
    }

    async fn request(&self, method: Method, url: &str, body: Body) -> Result<Value, StorageError> {
        Ok(self.request_response(method, url, body).await?.0)
    }

    /// `request`'s answer with the response it came in, for a status whose
    /// failure the response describes.
    async fn request_response(
        &self,
        method: Method,
        url: &str,
        body: Body,
    ) -> Result<(Value, http::ProviderResponse), StorageError> {
        crate::cooldown::cooling(self.cooldown.as_ref())?;
        http::same_origin(&self.api, url)?;
        let result = async {
            http::json_response(
                self.access.provider(),
                self.session.send(method, url, &[], body, true).await?,
            )
            .await
        }
        .await;
        crate::cooldown::observed(self.cooldown.as_ref(), result).await
    }

    /// The status of the sharing job `job`, read at `url`. A check that
    /// fails as a network failure or is rate limited says nothing of the
    /// job, so it is sent again, after the backoff of §6.7 or the longer
    /// wait Dropbox asks for.
    async fn share_job_status(
        &self,
        url: &str,
        job: &str,
    ) -> Result<(Value, http::ProviderResponse), StorageError> {
        let mut resends = 0u32;
        loop {
            let body = Body::Json(json!({"async_job_id":job}));
            match self.request_response(Method::POST, url, body).await {
                Err(error) if error.retryable() => {
                    let wait = crate::cooldown::throttled_wait(&error, resends);
                    tracing::warn!(
                        ?error,
                        ?wait,
                        "a Dropbox sharing job's status check will be sent again"
                    );
                    self.session.wait_out(wait).await;
                    resends = resends.saturating_add(1);
                }
                answered => return answered,
            }
        }
    }

    /// The signed-in person's default OneDrive root, including its actual item id.
    pub(super) async fn onedrive_root(&self) -> Result<StorageFolder, StorageError> {
        self.require(OAuthAccess::OneDrive)?;
        let url = http::endpoint(&self.api, &["me", "drive", "root"], &[])?;
        let value = self.request(Method::GET, &url, Body::Empty).await?;
        let drive = http::string(&value["parentReference"], "driveId")?;
        if item_kind(&value)? != Kind::Folder {
            return Err(StorageFailure::Protocol.with_source("OneDrive did not return a folder"));
        }
        onedrive_folder(&value, drive, http::string(&value, "name")?)
    }

    /// The immediate child folders; every page is read, without reading file bytes.
    pub(super) async fn onedrive_folders(
        &self,
        parent: &StorageConfig,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        self.require(OAuthAccess::OneDrive)?;
        let (drive, folder) = onedrive_parent(parent)?;
        let mut url =
            http::endpoint(&self.api, &["drives", drive, "items", folder, "children"], &[])?;
        let mut pages = Pagination::new();
        let mut folders = Vec::new();
        loop {
            pages.check(&url)?;
            let value = self.request(Method::GET, &url, Body::Empty).await?;
            for item in http::array(&value, "value")? {
                if let Some(folder) = onedrive_child(item, drive)? {
                    folders.push(folder);
                }
            }
            match http::next_link(&value)? {
                None => return Ok(folders),
                Some(next) => url = next.into(),
            }
        }
    }

    /// Create exactly the requested name. Occupied names remain provider errors.
    pub(super) async fn create_onedrive_folder(
        &self,
        parent: &StorageConfig,
        name: &str,
    ) -> Result<StorageFolder, StorageError> {
        self.require(OAuthAccess::OneDrive)?;
        let (drive, folder) = onedrive_parent(parent)?;
        let url = http::endpoint(&self.api, &["drives", drive, "items", folder, "children"], &[])?;
        let value = self
            .request(
                Method::POST,
                &url,
                Body::Json(crate::providers::onedrive_item::new_folder(name)),
            )
            .await?;
        if item_kind(&value)? != Kind::Folder {
            return Err(StorageFailure::Protocol.with_source("OneDrive did not return a folder"));
        }
        onedrive_folder(&value, drive, http::string(&value, "name")?)
    }

    /// Shared folders the signed-in Dropbox account can access, including all pages.
    pub(super) async fn dropbox_shared_folders(&self) -> Result<Vec<StorageFolder>, StorageError> {
        if self.access == OAuthAccess::Dropbox(DropboxAccess::AppFolder) {
            return Err(StorageFailure::DropboxAppFolderNotShareable.into());
        }
        self.require(OAuthAccess::Dropbox(DropboxAccess::FullDropbox))?;
        let mut url = http::endpoint(&self.api, &["sharing", "list_folders"], &[])?;
        let mut body = json!({});
        let mut pages = Pagination::new();
        let mut folders = Vec::new();
        loop {
            let value = self.request(Method::POST, &url, Body::Json(body)).await?;
            for item in http::array(&value, "entries")? {
                folders.push(dropbox_folder(item)?);
            }
            match http::page_cursor(&value)? {
                None => return Ok(folders),
                Some(cursor) => {
                    pages.check(cursor)?;
                    url = http::endpoint(&self.api, &["sharing", "list_folders", "continue"], &[])?;
                    body = json!({"cursor": cursor});
                }
            }
        }
    }

    /// What stands at `name` at the top of the signed-in Dropbox.
    pub(super) async fn dropbox_name(&self, name: &str) -> Result<DropboxName, StorageError> {
        let path = self.dropbox_new_folder(name)?;
        let lookup = http::endpoint(&self.api, &["files", "get_metadata"], &[])?;
        match self.request(Method::POST, &lookup, Body::Json(json!({ "path": path }))).await {
            Ok(item) => {
                let shared = match item.get("sharing_info") {
                    Some(sharing) if http::string(&item, ".tag")? == "folder" => {
                        http::optional_string(sharing, "shared_folder_id")?
                    }
                    _ => None,
                };
                Ok(DropboxName::Taken { shared_folder_id: shared.map(str::to_owned) })
            }
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(DropboxName::Free),
            Err(error) => Err(error),
        }
    }

    /// Ask Dropbox, in one `share_folder` request, to make a folder named
    /// `name` at the top of the signed-in Dropbox and share it. The request
    /// shares a folder that already has the name, so the caller looks the
    /// name up first.
    pub(super) async fn share_dropbox_folder(
        &self,
        name: &str,
    ) -> Result<DropboxShare, StorageError> {
        let path = self.dropbox_new_folder(name)?;
        let share = http::endpoint(&self.api, &["sharing", "share_folder"], &[])?;
        let body = Body::Json(json!({"path":path,"force_async":false}));
        let launched = match self.request(Method::POST, &share, body).await {
            Ok(launched) => launched,
            Err(error)
                if http::provider_json(&error).is_some_and(|reply| {
                    reply["error"]["bad_path"][".tag"] == "already_shared"
                }) =>
            {
                return Ok(DropboxShare::AlreadyShared)
            }
            Err(error) => return Err(error),
        };
        match http::string(&launched, ".tag")? {
            "complete" => Ok(DropboxShare::Shared(dropbox_folder(&launched)?)),
            "async_job_id" => {
                Ok(DropboxShare::Job(http::string(&launched, "async_job_id")?.into()))
            }
            _ => Err(StorageFailure::Protocol.with_source("invalid Dropbox share launch")),
        }
    }

    /// Follow the sharing job `job` until Dropbox reports its end. An error
    /// is a status check's, which leaves the job's end unknown.
    pub(super) async fn follow_dropbox_share(
        &self,
        job: &str,
    ) -> Result<DropboxShareJob, StorageError> {
        self.require(OAuthAccess::Dropbox(DropboxAccess::FullDropbox))?;
        let status = http::endpoint(&self.api, &["sharing", "check_share_job_status"], &[])?;
        let followed = super::dropbox_job::ended(&self.session, "folder sharing", || {
            self.share_job_status(&status, job)
        })
        .await;
        match followed {
            Ok(JobEnd::Complete(shared)) => Ok(DropboxShareJob::Shared(dropbox_folder(&shared)?)),
            Ok(JobEnd::Failed(failure)) => Ok(DropboxShareJob::Failed(failure)),
            Err(error)
                if http::provider_json(&error)
                    .is_some_and(|reply| reply["error"][".tag"] == "invalid_async_job_id") =>
            {
                Ok(DropboxShareJob::Forgotten)
            }
            Err(error) => Err(error),
        }
    }

    /// The shared folder `shared_folder_id`, named `name` at the top of the
    /// signed-in Dropbox, when it is as a request to share a new folder
    /// leaves it: this account owns it, and it holds nothing.
    pub(super) async fn untouched_dropbox_folder(
        &self,
        name: &str,
        shared_folder_id: &str,
    ) -> Result<Option<StorageFolder>, StorageError> {
        let path = self.dropbox_new_folder(name)?;
        let metadata = http::endpoint(&self.api, &["sharing", "get_folder_metadata"], &[])?;
        let body = Body::Json(json!({ "shared_folder_id": shared_folder_id }));
        let shared = self.request(Method::POST, &metadata, body).await?;
        if http::string(&shared["access_type"], ".tag")? != "owner" {
            return Ok(None);
        }
        let list = http::endpoint(&self.api, &["files", "list_folder"], &[])?;
        let page = self.request(Method::POST, &list, Body::Json(json!({ "path": path }))).await?;
        let more = page["has_more"]
            .as_bool()
            .ok_or(StorageFailure::Protocol.with_source("invalid Dropbox folder listing"))?;
        if more || !http::array(&page, "entries")?.is_empty() {
            return Ok(None);
        }
        Ok(Some(dropbox_folder(&shared)?))
    }

    /// The path of a new shared folder named `name`, which only a Full
    /// Dropbox sign-in can make.
    fn dropbox_new_folder(&self, name: &str) -> Result<String, StorageError> {
        if self.access == OAuthAccess::Dropbox(DropboxAccess::AppFolder) {
            return Err(StorageFailure::DropboxAppFolderNotShareable.into());
        }
        self.require(OAuthAccess::Dropbox(DropboxAccess::FullDropbox))?;
        if name.is_empty() || name.contains('/') {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("a Dropbox folder name is one non-empty path segment"));
        }
        Ok(format!("/{name}"))
    }
}

/// The shared folder Dropbox's metadata `item` describes, as a setup
/// location.
fn dropbox_folder(item: &Value) -> Result<StorageFolder, StorageError> {
    let id = http::string(item, "shared_folder_id")?;
    StorageFolder::dropbox_shared(http::string(item, "name")?, id)
}

fn onedrive_parent(parent: &StorageConfig) -> Result<(&str, &str), StorageError> {
    parent.validate()?;
    match parent {
        StorageConfig::OneDrive { drive_id, folder_id, .. } => Ok((drive_id, folder_id)),
        _ => {
            Err(StorageFailure::InvalidConfiguration.with_source("parent is not a OneDrive folder"))
        }
    }
}

fn onedrive_child(item: &Value, parent_drive: &str) -> Result<Option<StorageFolder>, StorageError> {
    let remote = match item.get("remoteItem") {
        None | Some(Value::Null) => None,
        Some(value) if value.is_object() => Some(value),
        Some(_) => return Err(StorageFailure::Protocol.with_source("invalid OneDrive remote item")),
    };
    let target = remote.unwrap_or(item);
    if item_kind(target)? != Kind::Folder {
        tracing::debug!("OneDrive folder picker skipped a non-folder item");
        return Ok(None);
    }
    let drive = match remote {
        Some(_) => http::string(&target["parentReference"], "driveId")?,
        None => parent_drive,
    };
    Ok(Some(onedrive_folder(target, drive, http::string(item, "name")?)?))
}

fn onedrive_folder(value: &Value, drive: &str, name: &str) -> Result<StorageFolder, StorageError> {
    if let Some(reference) = value.get("parentReference") {
        if http::string(reference, "driveId")? != drive {
            return Err(
                StorageFailure::Protocol.with_source("OneDrive folder belongs to another drive")
            );
        }
    }
    let storage = StorageConfig::OneDrive {
        account_label: None,
        folder_label: None,
        drive_id: drive.into(),
        folder_id: http::string(value, "id")?.into(),
    };
    storage.validate().map_err(|error| StorageFailure::Protocol.with_source(error))?;
    Ok(StorageFolder { name: name.into(), storage })
}

#[cfg(test)]
#[path = "oauth_folders_tests.rs"]
mod tests;
