//! The location marker at the folder's root, and first setup's check that
//! the folder holds nothing but the store (§20.3, Appendix E.2).
use super::*;
use crate::location_claim::{check_marker_size, occupied, LOCATION_MARKER, LOCATION_MARKER_SIZE};
use crate::providers::onedrive_item::{item_kind, Kind};
use coven_foundation::id_source::StoreId;

impl OneDriveStorage {
    fn marker(&self) -> Result<String, StorageError> {
        self.under(&self.folder, LOCATION_MARKER)
    }

    /// Put the marker's bytes in one request, as for any small object, with
    /// `If-None-Match: *` and `conflictBehavior=fail`, refused with 409
    /// `nameAlreadyExists` when the name is taken.
    pub(super) async fn create_marker(&self, store: StoreId) -> Result<(), StorageError> {
        let bytes = store.to_string().into_bytes();
        let size = bytes.len() as u64;
        let url = format!("{}/content?@microsoft.graph.conflictBehavior=fail", self.marker()?);
        let headers = [
            ("If-None-Match", "*".into()),
            ("Content-Type", "application/octet-stream".into()),
            ("Content-Length", size.to_string()),
        ];
        let body = Body::Bytes(bytes, ByteProgress::unread());
        let sent = self.session.send(Method::PUT, &url, &headers, body, true).await?;
        let item = self.checked_folder_operation(http::json(PROVIDER, sent).await).await?;
        if item["name"].as_str() != Some(LOCATION_MARKER) || item["size"].as_u64() != Some(size) {
            return Err(StorageFailure::Protocol.with_source("OneDrive created another item"));
        }
        Ok(())
    }

    /// Read the marker's metadata, refusing anything but a file of a store
    /// id's size, then its bytes. A marker that isn't there is absent only
    /// once the folder is confirmed.
    pub(super) async fn marker_bytes(&self) -> Result<Vec<u8>, StorageError> {
        let found =
            http::json(PROVIDER, self.send(Method::GET, &self.marker()?, Body::Empty).await?).await;
        if found.as_ref().is_err_and(|error| error.failure() == StorageFailure::NotFound) {
            self.check_folder().await?;
        }
        let item = found?;
        if item_kind(&item)? != Kind::File {
            return Err(occupied());
        }
        check_marker_size(
            item["size"].as_u64().ok_or(
                StorageFailure::Protocol.with_source("OneDrive omitted the marker's size"),
            )?,
        )?;
        let url = http::string(&item, "@microsoft.graph.downloadUrl")?;
        validate_download_url(&self.api, url)?;
        let response = self.session.send(Method::GET, url, &[], Body::Empty, false).await?;
        crate::providers::http_body::bounded(PROVIDER, response, LOCATION_MARKER_SIZE + 1).await
    }

    /// Whether the folder holds only `store`'s folder and the marker, by
    /// every page of its children. OneDrive matches names without regard to
    /// case, and so does this. A package or another item that is neither a
    /// file nor a folder is someone else's.
    pub(crate) async fn holds_only(&self, store: StoreId) -> Result<bool, StorageError> {
        let folder = store.to_string();
        let mut url = self.item(&self.folder, &["children"])?;
        let mut pages = Pagination::new();
        loop {
            pages.check(&url)?;
            http::same_origin(&self.api, &url)?;
            let value = http::json(PROVIDER, self.send(Method::GET, &url, Body::Empty).await?)
                .await
                .map_err(http::container_error)?;
            for item in http::array(&value, "value")? {
                let name = item["name"].as_str();
                let admitted = match (item_kind(item)?, name) {
                    (Kind::Folder, Some(name)) => name.eq_ignore_ascii_case(&folder),
                    (Kind::File, Some(name)) => name.eq_ignore_ascii_case(LOCATION_MARKER),
                    _ => false,
                };
                if !admitted {
                    return Ok(false);
                }
            }
            match http::next_link(&value)? {
                None => return Ok(true),
                Some(next) => url = next.into(),
            }
        }
    }
}
