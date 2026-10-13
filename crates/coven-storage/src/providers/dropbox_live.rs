#![cfg(test)]

//! Live checks: `COVEN_LIVE_DROPBOX_ENV=<main-file>
//! COVEN_LIVE_DROPBOX_B_ENV=<second-file> cargo test -p coven-storage
//! --all-features --lib dropbox_live -- --ignored --nocapture`.
//! The file contains a Full Dropbox app's DROPBOX_APP_KEY and DROPBOX_REFRESH_TOKEN,
//! with no app secret.
//! Sharing also requires COVEN_LIVE_DROPBOX_B_ENV with the second account's
//! credentials in the same format.
//! Every test owns a fresh shared folder under /coven-live and removes it, even
//! after an assertion panics. A pre-existing parent is never removed.

use super::{DropboxStorage, PROVIDER};
use crate::providers::http::{self, Body, OAuthSession};
use crate::providers::tests::live_settings;
use crate::providers::{OAuthClients, OAuthError, OAuthFlow, OAuthPresenter};
use crate::*;
use async_trait::async_trait;
use coven_crypto::SecretText;
use coven_foundation::{
    clock::{ClockRef, SystemClock},
    id_source::{DeviceId, IdSource, StoreId, UuidIds},
};
use reqwest::Method;
use serde_json::{json, Value};
use std::{
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

// Serialize ownership of the optional parent folder, including its cleanup.
static LIVE_RUN: Mutex<()> = Mutex::new(());

struct NoPresentation;

#[async_trait]
impl OAuthPresenter for NoPresentation {
    fn redirect_uri(&self, _: CloudProvider) -> &str {
        panic!("refresh must not request an authorization redirect")
    }

    async fn present(&self, _: &str) -> Result<SecretText, OAuthError> {
        panic!("refresh must not open an authorization presenter")
    }
}

struct Live {
    store: StoreId,
    clock: ClockRef,
    tokens: OAuthTokens,
    account: OAuthSession,
    folders: Vec<String>,
    uploads: Vec<(Arc<StorageConnection<DropboxStorage>>, UploadSession)>,
    members: Vec<(Arc<StorageConnection<DropboxStorage>>, MemberAccess)>,
}

async fn tokens(variable: &str, clock: ClockRef) -> OAuthTokens {
    let mut settings = live_settings(variable);
    let clients = OAuthClients::new(
        None,
        Some(
            settings
                .remove("DROPBOX_APP_KEY")
                .expect("DROPBOX_APP_KEY missing")
                .as_str()
                .into(),
        ),
        None,
        clock.clone(),
    );
    OAuthFlow::new(clients, Arc::new(NoPresentation))
        .refresh(
            PROVIDER,
            &OAuthTokens {
                // A refresh exchange needs only the refresh token and app key.
                access_token: SecretText::new(String::new()),
                refresh_token: Some(
                    settings
                        .remove("DROPBOX_REFRESH_TOKEN")
                        .expect("DROPBOX_REFRESH_TOKEN missing"),
                ),
                expires_at: None,
            },
        )
        .await
        .expect("Dropbox OAuth refresh")
}

#[cfg(test)]
impl Live {
    async fn authenticate(store: StoreId, clock: ClockRef) -> Self {
        let tokens = tokens("COVEN_LIVE_DROPBOX_ENV", clock.clone()).await;
        let account = OAuthSession::new(PROVIDER, tokens.clone(), clock.clone()).unwrap();
        Self {
            store,
            clock,
            tokens,
            account,
            folders: Vec::new(),
            uploads: Vec::new(),
            members: Vec::new(),
        }
    }

    async fn rpc(&self, method: &str, value: Value) -> Result<Value, StorageError> {
        let response = self
            .account
            .send(
                Method::POST,
                &format!("https://api.dropboxapi.com/2/{method}"),
                &[],
                Body::Json(value),
                true,
            )
            .await?;
        http::json(PROVIDER, response).await
    }

    async fn connect(&mut self) -> Arc<StorageConnection<DropboxStorage>> {
        match self
            .rpc(
                "files/create_folder_v2",
                json!({"path":"/coven-live","autorename":false}),
            )
            .await
        {
            Ok(value) => self
                .folders
                .push(http::string(&value["metadata"], "id").unwrap().into()),
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                let value = self
                    .rpc("files/get_metadata", json!({"path":"/coven-live"}))
                    .await
                    .unwrap();
                assert_eq!(http::string(&value, ".tag").unwrap(), "folder");
            }
            Err(error) => panic!("create live parent: {error}"),
        }
        let folder = format!("/coven-live/{}", self.store);
        // Reserve the unique path before sending, including a lost create reply.
        self.folders.push(folder.clone());
        self.rpc(
            "files/create_folder_v2",
            json!({"path":folder,"autorename":false}),
        )
        .await
        .unwrap();
        let mut shared = self
            .rpc(
                "sharing/share_folder",
                json!({"path":folder,"force_async":false}),
            )
            .await
            .expect("sharing the live folder requires a Full Dropbox app");
        if http::string(&shared, ".tag").unwrap() == "async_job_id" {
            let job = http::string(&shared, "async_job_id").unwrap().to_owned();
            shared = tokio::time::timeout(Duration::from_secs(60), async {
                loop {
                    let status = self
                        .rpc(
                            "sharing/check_share_job_status",
                            json!({"async_job_id":job}),
                        )
                        .await
                        .unwrap();
                    match http::string(&status, ".tag").unwrap() {
                        "in_progress" => self.clock.sleep(Duration::from_secs(1)).await,
                        "complete" => break status,
                        _ => panic!("Dropbox folder sharing failed (details withheld)"),
                    }
                }
            })
            .await
            .expect("Dropbox folder sharing timed out");
        }
        assert_eq!(http::string(&shared, ".tag").unwrap(), "complete");
        let namespace_id = http::string(&shared, "shared_folder_id").unwrap().into();
        let session = OAuthSession::new(PROVIDER, self.tokens.clone(), self.clock.clone()).unwrap();
        Arc::new(StorageConnection::from_provider(Arc::new(
            DropboxStorage::new(StorageConfig::Dropbox { namespace_id }, session).unwrap(),
        )))
    }

    async fn begin(
        &mut self,
        storage: &Arc<StorageConnection<DropboxStorage>>,
        path: &ObjectPath,
    ) -> UploadSession {
        let upload = storage
            .begin_upload(path, crate::session::DROPBOX_PART_SIZE as u64 + 1)
            .await
            .unwrap();
        // Retain the native capability before any assertion or part request can
        // fail. abort_upload queries the remote offset when this copy is stale.
        self.uploads.push((storage.clone(), upload.clone()));
        upload
    }

    async fn cleanup(&self) {
        let mut failures = Vec::new();
        for (storage, upload) in &self.uploads {
            if let Err(error) = storage.abort_upload(upload).await {
                failures.push(format!("abort: {:?}", error.failure()));
            }
        }
        for (storage, member) in &self.members {
            match storage.revoke_access(member).await {
                Ok(MemberRemoval::Revoked) => {}
                Ok(_) => failures.push("recipient access remains".into()),
                Err(error) => failures.push(format!("revoke: {:?}", error.failure())),
            }
        }
        for id in self.folders.iter().rev() {
            match self.rpc("files/delete_v2", json!({"path":id})).await {
                Ok(_) => {}
                Err(error) if error.failure() == StorageFailure::NotFound => {}
                Err(error) => failures.push(format!("delete folder: {:?}", error.failure())),
            }
            match self.rpc("files/get_metadata", json!({"path":id})).await {
                Err(error) if error.failure() == StorageFailure::NotFound => {}
                Err(error) => failures.push(format!("verify deletion: {:?}", error.failure())),
                Ok(_) => failures.push("deleted folder still readable".into()),
            }
        }
        assert!(failures.is_empty(), "Dropbox cleanup failed: {failures:?}");
        println!(
            "Dropbox cleanup: store={} folders={} sessions={} members={} removed/closed",
            self.store,
            self.folders.len(),
            self.uploads.len(),
            self.members.len()
        );
    }

    fn path(&self, number: u64) -> ObjectPath {
        ObjectPath::device_log(self.store, DeviceId(31), number.try_into().unwrap())
    }
}

#[cfg(test)]
fn run(test: impl AsyncFnOnce(&mut Live, Arc<StorageConnection<DropboxStorage>>)) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut live = runtime.block_on(Live::authenticate(
        StoreId(UuidIds.new_id()),
        Arc::new(SystemClock),
    ));
    let serial = LIVE_RUN.lock().expect("live test lock poisoned");
    let result = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            let storage = live.connect().await;
            test(&mut live, storage).await;
        })
    }));
    // Cleanup is outside the caught future, so assertion and setup failures take
    // the same awaited cleanup path as successful tests.
    let cleanup = catch_unwind(AssertUnwindSafe(|| runtime.block_on(live.cleanup())));
    drop(serial);
    if let Err(panic) = result {
        resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        resume_unwind(panic);
    }
}

async fn listed(storage: &StorageConnection<DropboxStorage>, path: &ObjectPath) -> StoredObject {
    storage
        .list(&ObjectPrefix::all(path.store()))
        .await
        .unwrap()
        .into_iter()
        .find(|object| &object.path == path)
        .expect("object absent from listing")
}

async fn metadata(storage: &StorageConnection<DropboxStorage>, path: &ObjectPath) -> Value {
    storage
        .provider
        .rpc("files/get_metadata", json!({"path":path.absolute()}))
        .await
        .unwrap()
}

async fn marker(storage: &StorageConnection<DropboxStorage>, path: &ObjectPath) -> SystemTime {
    storage.create(path, b"publication marker").await.unwrap();
    storage.status(path).await.unwrap().unwrap().stored_at
}

fn seconds(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
#[path = "dropbox_live_tests.rs"]
mod tests;
