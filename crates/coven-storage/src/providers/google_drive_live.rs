#![cfg(test)]

use super::*;
use crate::providers::http::Body as RequestBody;
use crate::providers::tests::live_drive_tokens;
use coven_foundation::{
    clock::{ClockRef, SystemClock},
    id_source::{IdSource, StoreId, UuidIds},
};
use std::{future::Future, time::Duration};

struct Drive {
    storage: Arc<StorageConnection<GoogleDriveStorage>>,
    store: StoreId,
    parent: String,
    clock: ClockRef,
    uploads: Mutex<Vec<UploadSession>>,
    reserved: Mutex<Vec<String>>,
}
impl Drive {
    async fn json(&self, method: Method, parts: &[&str], fields: &str, body: RequestBody) -> Value {
        let provider = &self.storage.provider;
        let url = provider.url(parts, &[("fields", fields)]).unwrap();
        http::json(PROVIDER, provider.send(method, &url, body).await.unwrap())
            .await
            .unwrap()
    }
    fn path(&self, number: u64) -> ObjectPath {
        ObjectPath::device_log(
            self.store,
            self.storage.provider.device,
            number.try_into().unwrap(),
        )
    }
    async fn listed(&self, path: &ObjectPath) -> StoredObject {
        self.storage
            .list(&ObjectPrefix::all(self.store))
            .await
            .unwrap()
            .into_iter()
            .find(|item| &item.path == path)
            .expect("published object listed")
    }
    async fn metadata(&self, path: &ObjectPath) -> Value {
        let found = self
            .storage
            .provider
            .find(path)
            .await
            .unwrap()
            .expect("published file id");
        self.json(
            Method::GET,
            &["files", http::string(&found, "id").unwrap()],
            "id,size,createdTime,modifiedTime,version,headRevisionId",
            RequestBody::Empty,
        )
        .await
    }
    async fn marker(&self, number: u64) -> Value {
        let path = self.path(number);
        self.storage
            .create(&path, b"publication marker")
            .await
            .unwrap();
        self.metadata(&path).await
    }
    async fn begin(&self, path: &ObjectPath) -> UploadSession {
        let upload = self
            .storage
            .begin_upload(path, crate::session::GOOGLE_DRIVE_PART_SIZE as u64 + 1)
            .await
            .unwrap();
        self.uploads.lock().unwrap().push(upload.clone());
        upload
    }
    async fn children(&self) -> Result<Vec<String>, StorageError> {
        let provider = &self.storage.provider;
        let query = format!(
            "'{}' in parents and trashed = false",
            escape(&provider.folder)
        );
        let mut objects = Vec::new();
        let mut token = None::<String>;
        let mut seen = Pagination::new();
        loop {
            let mut params = vec![
                ("q", query.as_str()),
                ("fields", "nextPageToken,incompleteSearch,files(id)"),
                ("pageSize", "1000"),
            ];
            if let Some(token) = &token {
                params.push(("pageToken", token));
            }
            let url = provider.url(&["files"], &params)?;
            let value = http::json(
                PROVIDER,
                provider.send(Method::GET, &url, RequestBody::Empty).await?,
            )
            .await?;
            if value["incompleteSearch"].as_bool() != Some(false) {
                return Err(StorageFailure::Protocol.with_source("incomplete cleanup listing"));
            }
            for item in http::array(&value, "files")? {
                objects.push(http::string(item, "id")?.to_owned());
            }
            let Some(next) = value.get("nextPageToken") else {
                break;
            };
            let next = next
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or(StorageFailure::Protocol)?;
            seen.check(next)?;
            token = Some(next.to_owned());
        }
        Ok(objects)
    }
    async fn cleanup(&self) {
        let uploads = std::mem::take(&mut *self.uploads.lock().unwrap());
        let mut failures = Vec::new();
        for upload in uploads {
            if let Err(error) = self.storage.abort_upload(&upload).await {
                failures.push(error);
            }
        }
        let provider = &self.storage.provider;
        let mut objects: Vec<_> = std::mem::take(&mut *self.reserved.lock().unwrap())
            .into_iter()
            .rev()
            .collect();
        objects.extend(match self.children().await {
            Ok(objects) => objects,
            Err(error) => {
                failures.push(error);
                Vec::new()
            }
        });
        objects.extend([provider.folder.clone(), self.parent.clone()]);
        // Delete children directly: parent deletion can return before they disappear.
        for id in &objects {
            let url = provider.url(&["files", id], &[]).unwrap();
            let deleted = match provider
                .send(Method::DELETE, &url, RequestBody::Empty)
                .await
            {
                Ok(response) if response.status().as_u16() == 404 => Ok(()),
                Ok(response) => http::checked(PROVIDER, response).await.map(|_| ()),
                Err(error) => Err(error),
            };
            if let Err(error) = deleted {
                failures.push(error);
            }
        }
        for id in &objects {
            let url = provider.url(&["files", id], &[]).unwrap();
            match provider.send(Method::GET, &url, RequestBody::Empty).await {
                Ok(response) if response.status().as_u16() == 404 => {}
                Ok(response) if response.status().is_success() => failures.push(
                    StorageFailure::Protocol
                        .with_source(format!("test object {id} survived deletion")),
                ),
                Ok(response) => failures.push(http::response_error(PROVIDER, response).await),
                Err(error) => failures.push(error),
            }
        }
        assert!(failures.is_empty(), "Drive cleanup failed: {failures:?}");
        println!(
            "Drive cleanup store={}: upload sessions aborted; objects, store and parent folders absent",
            self.store
        );
    }
}
#[cfg(test)]
#[path = "google_drive_live_tests.rs"]
mod tests;
