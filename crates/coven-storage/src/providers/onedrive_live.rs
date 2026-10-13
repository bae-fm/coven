//! Test-only OneDrive folders and upload capabilities used by the live evidence.
#![cfg(test)]

use super::*;
use crate::providers::http::Body;
use coven_foundation::{
    clock::ClockRef,
    id_source::{DeviceId, StoreId},
};
use std::{sync::Arc, time::SystemTime};

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
}
impl Live<'_> {
    fn path(&self, number: u64) -> ObjectPath {
        ObjectPath::device_log(self.store, DeviceId(31), number.try_into().unwrap())
    }
    async fn observed(&self, path: &ObjectPath) -> StoredObject {
        self.storage
            .status(path)
            .await
            .unwrap()
            .expect("published object status")
    }
    async fn marker(&self, number: u64) -> (String, SystemTime) {
        let path = self.path(number);
        self.storage
            .create(&path, b"publication marker")
            .await
            .unwrap();
        let native = self.storage.provider.metadata(&path).await.unwrap();
        let raw = http::string(&native, "createdDateTime").unwrap().to_owned();
        let time = http::timestamp(&native, "createdDateTime").unwrap();
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
}

#[cfg(test)]
#[path = "onedrive_live_tests.rs"]
mod tests;
