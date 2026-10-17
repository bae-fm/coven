//! Test-only OneDrive folders and upload capabilities used by the live evidence.
#![cfg(test)]

use super::*;
use crate::providers::http::Body;
use coven_format::value::StorageTime;
use coven_foundation::{
    clock::ClockRef,
    id_source::{DeviceId, StoreId},
};
use std::sync::Arc;

const GRAPH: &str = "https://graph.microsoft.com/v1.0";
type Connection = Arc<StorageConnection<OneDriveStorage>>;

struct Folders {
    session: OAuthSession,
    drive: String,
    owned: Vec<String>,
}
impl Folders {
    async fn json(&self, method: Method, url: &str, body: Body) -> Value {
        http::json(
            PROVIDER,
            self.session
                .send(method, url, &[], body, true)
                .await
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn create(&mut self, parent: &str, name: &str) -> Result<String, StorageError> {
        let value = http::json(
            PROVIDER,
            self.session
                .send(
                    Method::POST,
                    &format!("{parent}/children"),
                    &[],
                    Body::Json(
                        json!({"name":name,"folder":{},"@microsoft.graph.conflictBehavior":"fail"}),
                    ),
                    true,
                )
                .await?,
        )
        .await?;
        let id = http::string(&value, "id")?.to_owned();
        // Record ownership before checking the rest of the response or making another request.
        self.owned.push(http::endpoint(
            GRAPH,
            &["drives", &self.drive, "items", &id],
            &[],
        )?);
        assert!(value["folder"].is_object());
        Ok(id)
    }

    async fn prepare(&mut self, store: StoreId) -> String {
        let root = http::endpoint(GRAPH, &["drives", &self.drive, "root"], &[]).unwrap();
        let parent = match self.create(&root, "coven-live").await {
            Ok(id) => id,
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                let value = self
                    .json(Method::GET, &format!("{root}:/coven-live:"), Body::Empty)
                    .await;
                assert!(value["folder"].is_object(), "coven-live must be a folder");
                http::string(&value, "id").unwrap().to_owned()
            }
            Err(error) => panic!("create coven-live parent: {error}"),
        };
        let parent =
            http::endpoint(GRAPH, &["drives", &self.drive, "items", &parent], &[]).unwrap();
        self.create(&parent, &format!("onedrive-{store}"))
            .await
            .unwrap()
    }

    async fn cleanup(&self) -> Vec<StorageError> {
        let mut failures = Vec::new();
        for url in self.owned.iter().rev() {
            let result = async {
                let response = self
                    .session
                    .send(Method::DELETE, url, &[], Body::Empty, true)
                    .await?;
                http::checked(PROVIDER, response).await?;
                let response = self
                    .session
                    .send(Method::GET, url, &[], Body::Empty, true)
                    .await?;
                if response.status().as_u16() != 404 {
                    http::checked(PROVIDER, response).await?;
                    return Err(StorageFailure::Protocol
                        .with_source("deleted live folder remains readable"));
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                failures.push(error);
            }
        }
        failures
    }
}

struct Live<'a> {
    storage: Connection,
    store: StoreId,
    clock: ClockRef,
    uploads: &'a mut Vec<(Connection, UploadSession)>,
    native_uploads: &'a mut Vec<SecretText>,
}
impl Live<'_> {
    fn path(&self, number: u64) -> ObjectPath {
        ObjectPath::slot(self.store, number.try_into().unwrap())
    }
    async fn observed(&self, path: &ObjectPath) -> StoredObject {
        self.storage
            .status(path)
            .await
            .unwrap()
            .expect("published object status")
    }
    async fn marker(&self, number: u64) -> (String, StorageTime) {
        let path = self.path(number);
        self.storage
            .create(&path, b"publication marker")
            .await
            .unwrap();
        let native = self.storage.provider.metadata(&path).await.unwrap();
        let raw = http::string(&native, "lastModifiedDateTime")
            .unwrap()
            .to_owned();
        let time = http::timestamp(&native, "lastModifiedDateTime").unwrap();
        assert_eq!(self.observed(&path).await.stored_at, time);
        (raw, time)
    }
    async fn begin(&mut self, path: &ObjectPath) -> UploadSession {
        let upload = self
            .storage
            .begin_upload(path, crate::session::ONEDRIVE_PART_SIZE as u64 + 1)
            .await
            .unwrap();
        // Retain the original capability even if a part succeeds but its reply is lost.
        self.uploads.push((self.storage.clone(), upload.clone()));
        upload
    }
    async fn assert_temporaries_deleted(&self) {
        let provider = &self.storage.provider;
        let root = provider.item(&provider.folder, &[]).unwrap();
        assert!(!self.uploads.is_empty());
        for (_, recorded) in self.uploads.iter() {
            let SessionState::OneDrive { temporary, .. } = &recorded.state else {
                panic!("expected the original OneDrive recording")
            };
            let response = provider
                .send(
                    Method::GET,
                    &format!("{root}:/{}/.coven-uploads/{temporary}:", self.store),
                    Body::Empty,
                )
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 404);
        }
    }
}

async fn native(provider: &OneDriveStorage, method: Method, url: &str, body: Body) -> Value {
    http::json(PROVIDER, provider.send(method, url, body).await.unwrap())
        .await
        .unwrap()
}

async fn children(provider: &OneDriveStorage, parent: &str) -> Vec<Value> {
    let mut url = provider.item(parent, &["children"]).unwrap();
    let mut pages = Pagination::new();
    let mut items = Vec::new();
    loop {
        pages.check(&url).unwrap();
        http::same_origin(&provider.api, &url).unwrap();
        let value = native(provider, Method::GET, &url, Body::Empty).await;
        items.extend(http::array(&value, "value").unwrap().iter().cloned());
        match value.get("@odata.nextLink") {
            Some(_) => url = http::string(&value, "@odata.nextLink").unwrap().into(),
            None => return items,
        }
    }
}

async fn copy_response(
    provider: &OneDriveStorage,
    source: &str,
    parent: &str,
    name: &str,
) -> reqwest::Response {
    let url = format!(
        "{}?@microsoft.graph.conflictBehavior=fail",
        provider.item(source, &["copy"]).unwrap()
    );
    provider
        .send(
            Method::POST,
            &url,
            Body::Json(
                json!({"parentReference":{"driveId":provider.drive,"id":parent},"name":name}),
            ),
        )
        .await
        .unwrap()
}

fn pattern(offset: usize, length: usize) -> Vec<u8> {
    (offset..offset + length)
        .map(|i| ((i * 7) % 251) as u8)
        .collect()
}

async fn exact_bytes(provider: &OneDriveStorage, item: &Value, total: usize) {
    let url = http::string(item, "@microsoft.graph.downloadUrl").unwrap();
    validate_download_url(&provider.api, url).unwrap();
    let mut response = http::checked(
        PROVIDER,
        provider
            .session
            .send(Method::GET, url, &[], Body::Empty, false)
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let mut offset = 0;
    while let Some(bytes) = response
        .chunk()
        .await
        .map_err(|error| error.without_url())
        .unwrap()
    {
        assert!(
            offset + bytes.len() <= total,
            "copied body exceeds promised size"
        );
        assert!(
            bytes.as_ref() == pattern(offset, bytes.len()),
            "copied bytes differ at offset {offset}"
        );
        offset += bytes.len();
    }
    assert_eq!(offset, total, "copied body was truncated");
}

fn report_item(label: &str, item: &Value) {
    println!(
        "onedrive_copy {label}: size={} createdDateTime={} lastModifiedDateTime={} fileSystemInfo.lastModifiedDateTime={} eTag={}",
        item["size"], item["createdDateTime"], item["lastModifiedDateTime"], item["fileSystemInfo"]["lastModifiedDateTime"], item["eTag"]
    );
}

#[cfg(test)]
#[path = "onedrive_live_tests.rs"]
mod tests;
