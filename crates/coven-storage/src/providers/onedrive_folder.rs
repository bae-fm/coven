//! Connection-held folder IDs and traversal of the requested OneDrive directory.
use super::*;
use std::collections::BTreeSet;

impl OneDriveStorage {
    pub(super) async fn writer_folders(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<coven_foundation::id_source::DeviceId>, StorageError> {
        let directory = format!("{store}/store-log");
        let known_id = self.folder_ids.lock().await.get(&directory).cloned();
        let mut url = match &known_id {
            Some(id) => self.item(id, &["children"])?,
            None => format!("{}:/{directory}:/children", self.item(&self.folder, &[])?),
        };
        let mut pages = Pagination::new();
        let mut writers = BTreeSet::new();
        let mut folders = BTreeMap::new();
        let mut first = true;
        loop {
            pages.check(&url)?;
            http::same_origin(&self.api, &url)?;
            let value = match http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?)
                .await
            {
                Ok(value) => value,
                Err(error) if first && error.failure() == StorageFailure::NotFound => {
                    self.check_folder().await?;
                    self.folder_ids.lock().await.clear();
                    if known_id.is_some() {
                        return Err(error);
                    }
                    return Ok(Vec::new());
                }
                Err(error) => return Err(error),
            };
            for item in http::array(&value, "value")? {
                if !item["folder"].is_object() || item["file"].is_object() {
                    return Err(StorageFailure::Protocol
                        .with_source("OneDrive writer listing returned a file"));
                }
                let name = http::string(item, "name")?;
                let path = ObjectPath::parse(&format!("{directory}/{name}/1"))?;
                writers.insert(path.device().expect("store-log writer"));
                folders.insert(
                    format!("{directory}/{name}"),
                    http::string(item, "id")?.to_owned(),
                );
            }
            let Some(next) = value.get("@odata.nextLink") else {
                break;
            };
            url = next
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or(StorageFailure::Protocol)?
                .to_owned();
            first = false;
        }
        self.folder_ids.lock().await.extend(folders);
        Ok(writers.into_iter().collect())
    }

    pub(super) async fn checked_folder_operation<T>(
        &self,
        result: Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        if result
            .as_ref()
            .is_err_and(|error| error.failure() == StorageFailure::NotFound)
        {
            // A missing ancestor invalidates every descendant ID. Fail this
            // operation; its caller can retry after resolving the folders again.
            self.folder_ids.lock().await.clear();
            self.check_folder().await?;
        }
        result
    }
    pub(super) async fn directory(
        &self,
        publication: &PublicationRequest,
        parts: &[&str],
    ) -> Result<String, StorageError> {
        // Concurrent creates share the same resolution, including newly made
        // parents. The empty path records validation of the configured folder.
        let mut known = self.folder_ids.lock().await;
        let result = async {
            if known.is_empty() {
                self.check_folder().await?;
                known.insert(String::new(), self.folder.clone());
            }
            let (start, mut id) = (0..=parts.len())
                .rev()
                .find_map(|count| {
                    known.get(&parts[..count].join("/"))
                        .map(|id| (count, id.clone()))
                })
                .expect("validated configured folder");
            for (index, name) in parts.iter().enumerate().skip(start) {
                let value = http::json(
                    PROVIDER,
                    self.publish(publication,
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
                            self.publish(publication, Method::GET, &format!("{base}:/{name}:"), Body::Empty)
                                .await?,
                        )
                        .await?
                    }
                    Err(error) => return Err(error),
                };
                if !value["folder"].is_object() || value["file"].is_object() {
                    return Err(StorageFailure::AlreadyExists.into());
                }
                id = http::string(&value, "id")?.into();
                known.insert(parts[..=index].join("/"), id.clone());
            }
            Ok(id)
        }.await;
        drop(known);
        self.checked_folder_operation(result).await
    }
    pub(super) async fn list_folders(
        &self,
        listing: &mut ObjectListing,
    ) -> Result<(), StorageError> {
        let prefix = listing.prefix().clone();
        let directory = prefix.as_str().trim_end_matches('/');
        let mut known = self.folder_ids.lock().await;
        let result = async {
            if known.is_empty() {
                self.check_folder().await?;
                known.insert(String::new(), self.folder.clone());
            }
            let id = match known.get(directory) {
                Some(id) => id.clone(),
                None => {
                    let url = format!("{}:/{directory}:", self.item(&self.folder, &[])?);
                    let value = match http::json(
                        PROVIDER,
                        self.send(Method::GET, &url, Body::Empty).await?,
                    )
                    .await
                    {
                        Ok(value) => value,
                        Err(error) if error.failure() == StorageFailure::NotFound => {
                            self.check_folder().await?;
                            tracing::debug!(directory, "OneDrive listing folder is absent");
                            return Ok(());
                        }
                        Err(error) => return Err(error),
                    };
                    if !value["folder"].is_object() || value["file"].is_object() {
                        return Err(StorageFailure::InvalidPath.into());
                    }
                    http::string(&value, "id")?.to_owned()
                }
            };
            let mut discovered = BTreeMap::from([(directory.to_owned(), id.clone())]);
            let mut folders = vec![(
                id,
                directory.split('/').map(str::to_owned).collect::<Vec<_>>(),
            )];
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
                        http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?)
                            .await?;
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
                            tracing::debug!(
                                "OneDrive upload temporaries retained for session cleanup"
                            );
                            continue;
                        }
                        if item["folder"].is_object() {
                            let mut parts = names.clone();
                            parts.push(name.to_owned());
                            crate::path::validate_directory(&parts.join("/"))?;
                            let id = http::string(item, "id")?.to_owned();
                            discovered.insert(parts.join("/"), id.clone());
                            folders.push((id, parts));
                        } else if item["file"].is_object() {
                            let path = ObjectPath::from_components(&names, name)?;
                            let object = object_metadata::onedrive(&path, item)?;
                            listing.insert(object)?;
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
            // Do not retain a partial discovery if any page or descendant failed.
            known.extend(discovered);
            Ok(())
        }
        .await;
        drop(known);
        self.checked_folder_operation(result).await
    }
}

#[cfg(test)]
#[path = "onedrive_folder_tests.rs"]
mod tests;
