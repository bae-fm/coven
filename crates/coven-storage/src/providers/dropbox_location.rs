//! The location marker at a shared folder's root, and first setup's check
//! that the folder holds nothing but the store (§20.3, Appendix E.2).
use super::{http, DropboxStorage, PROVIDER};
use crate::location_claim::{check_marker_size, occupied, LOCATION_MARKER, LOCATION_MARKER_SIZE};
use crate::{ByteProgress, StorageError, StorageFailure};
use coven_foundation::id_source::StoreId;
use serde_json::json;

fn marker_path() -> String {
    format!("/{LOCATION_MARKER}")
}

impl DropboxStorage {
    /// Upload the marker in mode `add`, which never renames and refuses a
    /// taken name with 409 `path/conflict`.
    pub(super) async fn create_marker(&self, store: StoreId) -> Result<(), StorageError> {
        let bytes = store.to_string().into_bytes();
        let size = bytes.len() as u64;
        let arg = json!({"path":marker_path(),"mode":"add","autorename":false,"strict_conflict":true,"mute":true});
        let response =
            self.content("files/upload", arg, bytes, ByteProgress::unread(), None, None).await?;
        let value = http::json(PROVIDER, response).await?;
        if value["path_lower"].as_str() != Some(marker_path().as_str())
            || value["size"].as_u64() != Some(size)
        {
            return Err(StorageFailure::Protocol.with_source("Dropbox returned another file"));
        }
        Ok(())
    }

    /// Download the marker, refusing any size but a store id's before
    /// reading it. A folder at its name is not a marker. A marker that isn't
    /// there is absent only once the shared folder is confirmed.
    pub(super) async fn marker_bytes(&self) -> Result<Vec<u8>, StorageError> {
        let response = self
            .content(
                "files/download",
                json!({"path":marker_path()}),
                Vec::new(),
                ByteProgress::unread(),
                None,
                None,
            )
            .await?;
        let response = match http::checked(PROVIDER, response).await {
            Ok(response) => response,
            Err(error) if error.failure() == StorageFailure::NotFound => {
                self.check_root().await?;
                return Err(error);
            }
            Err(error) if not_file(&error) => return Err(occupied()),
            Err(error) => return Err(error),
        };
        let size = super::download_metadata(&response)?["size"]
            .as_u64()
            .ok_or(StorageFailure::Protocol.with_source("Dropbox omitted the marker's size"))?;
        check_marker_size(size)?;
        crate::providers::http_body::bounded(PROVIDER, response, LOCATION_MARKER_SIZE + 1).await
    }

    /// Whether the shared folder's root holds only `store`'s folder and the
    /// marker. Dropbox matches names without regard to case, and its
    /// lowercase paths compare that way. An entry without a path, such as a
    /// folder shared with this account but not mounted, is someone else's.
    pub(crate) async fn holds_only(&self, store: StoreId) -> Result<bool, StorageError> {
        let folder = format!("/{store}");
        let mut admitted = true;
        self.folder_entries(String::new(), false, |entry| {
            let path = entry["path_lower"].as_str();
            admitted &= match http::string(entry, ".tag")? {
                "folder" => path == Some(folder.as_str()),
                "file" => path == Some(marker_path().as_str()),
                _ => false,
            };
            Ok(())
        })
        .await?;
        Ok(admitted)
    }
}

/// Whether Dropbox refused a download because a folder holds the name.
fn not_file(error: &StorageError) -> bool {
    http::provider_json(error)
        .is_some_and(|value| value["error"]["path"][".tag"].as_str() == Some("not_file"))
}
