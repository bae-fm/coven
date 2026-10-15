use super::DropboxStorage;
use crate::providers::{http, pagination::Pagination};
use crate::{ObjectListing, StorageError, StorageFailure};
use serde_json::json;

impl DropboxStorage {
    pub(super) async fn list_objects(
        &self,
        listing: &mut ObjectListing,
    ) -> Result<(), StorageError> {
        let prefix = listing.prefix().clone();
        self.folder_entries(&prefix, true, |entry| {
            let name = http::string(entry, "path_lower")?
                .strip_prefix('/')
                .ok_or(StorageFailure::Protocol.with_source("Dropbox listed a relative path"))?;
            match http::string(entry, ".tag")? {
                // A recursive listing already includes every descendant.
                "folder" => {
                    listing.object_folder(name)?;
                }
                "file" => {
                    if let Some(path) = listing.object_path(name)? {
                        listing
                            .insert(crate::providers::object_metadata::dropbox(&path, entry)?)?;
                    }
                }
                _ => {
                    return Err(
                        StorageFailure::Protocol.with_source("unexpected Dropbox listing entry")
                    )
                }
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn writer_folders(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<coven_foundation::id_source::DeviceId>, StorageError> {
        let prefix = crate::ObjectPrefix::store_logs(store);
        let mut writers = std::collections::BTreeSet::new();
        self.folder_entries(&prefix, false, |entry| {
            let name = http::string(entry, "path_lower")?
                .strip_prefix('/')
                .and_then(|path| path.strip_prefix(prefix.as_str()))
                .ok_or(StorageFailure::Protocol.with_source("Dropbox listed outside store-log"))?;
            // Files directly under `store-log/` are no writer folders (D10).
            match http::string(entry, ".tag")? {
                "folder" => writers.extend(crate::object_listing::writer_folder(store, name)),
                "file" => {}
                _ => {
                    return Err(
                        StorageFailure::Protocol.with_source("unexpected Dropbox listing entry")
                    )
                }
            }
            Ok(())
        })
        .await?;
        Ok(writers.into_iter().collect())
    }

    async fn folder_entries(
        &self,
        prefix: &crate::ObjectPrefix,
        recursive: bool,
        mut visit: impl FnMut(&serde_json::Value) -> Result<(), StorageError> + Send,
    ) -> Result<(), StorageError> {
        let mut method = "files/list_folder";
        let mut request = json!({"path":format!("/{}", prefix.as_str().trim_end_matches('/')),"recursive":recursive,"include_deleted":false,"limit":2000});
        let mut seen = Pagination::new();
        loop {
            let value = match self.rpc(method, request).await {
                Ok(value) => value,
                Err(error)
                    if method == "files/list_folder"
                        && error.failure() == StorageFailure::NotFound =>
                {
                    self.check_root().await?;
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            for entry in http::array(&value, "entries")? {
                visit(entry)?;
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
            seen.check(cursor)?;
            method = "files/list_folder/continue";
            request = json!({"cursor":cursor});
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "dropbox_listing_tests.rs"]
mod tests;
