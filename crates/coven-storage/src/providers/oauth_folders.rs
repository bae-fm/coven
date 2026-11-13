//! Choosing provider folders through coven-held sign-in, before storage setup.

use super::{
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

    /// Make a folder named `name` at the top of the signed-in Dropbox and
    /// share it. The name is looked up first, so a name already taken fails
    /// with `AlreadyExists` and nothing of the person's is shared; then one
    /// `share_folder` request makes the folder and shares it, and its job, if
    /// Dropbox runs one, is checked until it completes.
    pub(super) async fn create_dropbox_shared_folder(
        &self,
        name: &str,
    ) -> Result<StorageFolder, StorageError> {
        if self.access == OAuthAccess::Dropbox(DropboxAccess::AppFolder) {
            return Err(StorageFailure::DropboxAppFolderNotShareable.into());
        }
        self.require(OAuthAccess::Dropbox(DropboxAccess::FullDropbox))?;
        if name.is_empty() || name.contains('/') {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("a Dropbox folder name is one non-empty path segment"));
        }
        let path = format!("/{name}");
        let lookup = http::endpoint(&self.api, &["files", "get_metadata"], &[])?;
        match self.request(Method::POST, &lookup, Body::Json(json!({ "path": path }))).await {
            Ok(_) => {
                return Err(StorageFailure::AlreadyExists
                    .with_source("the Dropbox folder name is already taken"))
            }
            Err(error) if error.failure() == StorageFailure::NotFound => {}
            Err(error) => return Err(error),
        }
        let share = http::endpoint(&self.api, &["sharing", "share_folder"], &[])?;
        let launched = self
            .request(Method::POST, &share, Body::Json(json!({"path":path,"force_async":false})))
            .await?;
        let shared = match http::string(&launched, ".tag")? {
            "complete" => launched,
            "async_job_id" => {
                let job = http::string(&launched, "async_job_id")?;
                let status =
                    http::endpoint(&self.api, &["sharing", "check_share_job_status"], &[])?;
                super::dropbox_job::completed(&self.session, "folder sharing", || {
                    self.request_response(
                        Method::POST,
                        &status,
                        Body::Json(json!({"async_job_id":job})),
                    )
                })
                .await?
            }
            _ => return Err(StorageFailure::Protocol.with_source("invalid Dropbox share launch")),
        };
        dropbox_folder(&shared)
    }
}

/// The shared folder Dropbox's metadata `item` describes, as a setup
/// location.
fn dropbox_folder(item: &Value) -> Result<StorageFolder, StorageError> {
    let storage = StorageConfig::Dropbox {
        account_label: None,
        folder_label: None,
        location: DropboxLocation::SharedFolder {
            namespace_id: http::string(item, "shared_folder_id")?.into(),
        },
    };
    storage.validate().map_err(|error| StorageFailure::Protocol.with_source(error))?;
    Ok(StorageFolder { name: http::string(item, "name")?.into(), storage })
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
