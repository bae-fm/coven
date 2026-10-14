//! Opt-in probes use production authorization and adapters; credentials never enter output.

use super::{
    http::{self, Body},
    DesktopOAuthPresenter, DropboxClients, GoogleDriveClient, OAuthClients, OAuthFlow,
    OAuthSession, ProviderConnector, StorageConnector,
};
use crate::*;
use coven_crypto::SecretText;
use coven_foundation::{
    clock::{ClockRef, SystemClock},
    id_source::{DeviceId, IdSource, StoreId, UuidIds},
};
use reqwest::Method;
use serde_json::json;
use std::sync::Arc;

const FILE: OAuthAccess = OAuthAccess::GoogleDrive(GoogleDriveAccess::File);
const FULL: OAuthAccess = OAuthAccess::GoogleDrive(GoogleDriveAccess::Full);

#[tokio::test]
#[ignore = "requires COVEN_LIVE_GOOGLE_CLIENT_JSON and two browser sign-ins; creates and deletes its test folder"]
async fn provider_access_live_google_upgrade() {
    #[derive(serde::Deserialize)]
    struct Registration {
        installed: Installed,
    }
    #[derive(serde::Deserialize)]
    struct Installed {
        client_id: String,
        client_secret: SecretText,
    }
    let path = std::env::var("COVEN_LIVE_GOOGLE_CLIENT_JSON").expect("Google client JSON path");
    let contents = SecretText::new(std::fs::read_to_string(path).expect("read Google client JSON"));
    let registration: Registration =
        serde_json::from_str(contents.as_str()).expect("Google desktop client JSON");
    let clock: ClockRef = Arc::new(SystemClock);
    let clients = OAuthClients::new(
        Some(GoogleDriveClient::new(
            registration.installed.client_id,
            GoogleDriveAccess::Full,
        )),
        None,
        None,
        clock.clone(),
    )
    .with_google_client_secret(registration.installed.client_secret)
    .unwrap();
    let flow = OAuthFlow::new(clients, Arc::new(DesktopOAuthPresenter));
    let narrow = flow.authenticate(FILE).await.unwrap();
    assert_eq!(narrow.access, FILE);
    let cleanup_session = OAuthSession::new(
        CloudProvider::GoogleDrive,
        narrow.clone(),
        Arc::new(crate::test_utils::RefusedRefresh),
        clock.clone(),
    )
    .unwrap();
    let store = StoreId(UuidIds.new_id());
    let folder = http::json(CloudProvider::GoogleDrive, cleanup_session.send(Method::POST, "https://www.googleapis.com/drive/v3/files?fields=id", &[], Body::Json(json!({"name":format!("coven-access-test-{store}"), "mimeType":"application/vnd.google-apps.folder"})), true).await.unwrap()).await.unwrap();
    let folder_id = http::string(&folder, "id").unwrap().to_owned();
    let location = StorageConfig::GoogleDrive {
        folder_id: folder_id.clone(),
    };
    let connector = ProviderConnector::new(clock.clone(), Arc::new(UuidIds));
    let storage = connector
        .connect(
            location.clone(),
            crate::ProviderAuth::OAuth {
                tokens: narrow.clone(),
                custody: Arc::new(crate::test_utils::RefusedRefresh),
            },
            DeviceId(31),
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(crate::NoCooldown),
        )
        .await
        .unwrap();
    let object = ObjectPath::device_log(store, DeviceId(31), std::num::NonZeroU64::MIN);
    let result = tokio::spawn({
        let storage = storage.clone();
        let object = object.clone();
        async move {
            storage
                .create(&object, b"created with drive.file")
                .await
                .unwrap();
            let before = storage.read_object(&object, None).await.unwrap();
            let account = storage.account().await.unwrap();
            let full = flow.authenticate(FULL).await.unwrap();
            assert_eq!(full.access, FULL);
            assert!(full.client_id == narrow.client_id, "upgrade changed client");
            let upgraded = connector
                .connect(
                    location,
                    crate::ProviderAuth::OAuth {
                        tokens: full,
                        custody: Arc::new(crate::test_utils::RefusedRefresh),
                    },
                    DeviceId(32),
                    Arc::new(crate::test_utils::UnrestrictedPublications),
                    Arc::new(crate::NoCooldown),
                )
                .await
                .unwrap();
            assert!(
                upgraded.account().await.unwrap() == account,
                "upgrade changed account"
            );
            let after = upgraded.read_object(&object, None).await.unwrap();
            assert_eq!(after.bytes, before.bytes);
            assert_eq!(after.metadata, before.metadata);
            wait_for_listing(upgraded.as_ref(), &before.metadata, &clock).await;
        }
    })
    .await;
    let delete_object = storage.delete(&object).await;
    let delete_folder = http::checked(
        CloudProvider::GoogleDrive,
        cleanup_session
            .send(
                Method::DELETE,
                &format!("https://www.googleapis.com/drive/v3/files/{folder_id}"),
                &[],
                Body::Empty,
                true,
            )
            .await
            .unwrap(),
    )
    .await;
    delete_object.unwrap();
    delete_folder.unwrap();
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV for an App-folder app; creates and deletes its test store"]
async fn provider_access_live_dropbox_app_folder() {
    let mut settings = super::live_settings("COVEN_LIVE_DROPBOX_ENV");
    let client_id = settings
        .remove("DROPBOX_APP_KEY")
        .expect("Dropbox app key")
        .as_str()
        .to_owned();
    let refresh_token = settings
        .remove("DROPBOX_REFRESH_TOKEN")
        .expect("Dropbox refresh token");
    let clock: ClockRef = Arc::new(SystemClock);
    let tokens = OAuthClients::new(
        None,
        Some(DropboxClients::AppFolder {
            app_key: client_id.clone(),
        }),
        None,
        clock.clone(),
    )
    .refresh(&crate::providers::tests::refresh_only(
        &client_id,
        OAuthAccess::Dropbox(DropboxAccess::AppFolder),
        refresh_token,
    ))
    .await
    .unwrap();
    let session = OAuthSession::new(
        CloudProvider::Dropbox,
        tokens.clone(),
        Arc::new(crate::test_utils::RefusedRefresh),
        clock.clone(),
    )
    .unwrap();
    let connector = ProviderConnector::new(clock.clone(), Arc::new(UuidIds));
    let location = StorageConfig::Dropbox {
        location: DropboxLocation::AppFolder,
    };
    let storage = connector
        .connect(
            location.clone(),
            crate::ProviderAuth::OAuth {
                tokens: tokens.clone(),
                custody: Arc::new(crate::test_utils::RefusedRefresh),
            },
            DeviceId(31),
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(crate::NoCooldown),
        )
        .await
        .unwrap();
    let store = StoreId(UuidIds.new_id());
    let result = tokio::spawn(async move {
        let path = ObjectPath::device_log(store, DeviceId(31), std::num::NonZeroU64::MIN);
        assert!(storage.status(&path).await.unwrap().is_none());
        assert!(storage.list(&ObjectPrefix::all(store)).await.unwrap().is_empty());
        storage.create(&path, b"app folder bytes").await.unwrap();
        let before = storage.read_object(&path, None).await.unwrap();
        assert_eq!(before.bytes, b"app folder bytes");
        let restored = connector
            .connect(
                StorageConfig::decode(location.encode().unwrap().as_bytes()).unwrap(),
                crate::ProviderAuth::OAuth { tokens, custody: Arc::new(crate::test_utils::RefusedRefresh) },
                DeviceId(32), Arc::new(crate::test_utils::UnrestrictedPublications), Arc::new(crate::NoCooldown))
            .await
            .unwrap();
        let after = restored.read_object(&path, None).await.unwrap();
        assert_eq!(after.bytes, before.bytes);
        assert_eq!(after.metadata, before.metadata);
        wait_for_listing(restored.as_ref(), &before.metadata, &clock).await;
        assert!(
            matches!(restored.grant_access("member@example.test").await, Err(error) if error.failure() == StorageFailure::DropboxAppFolderNotShareable)
        );
    })
    .await;
    let deleted = http::checked(
        CloudProvider::Dropbox,
        session
            .send(
                Method::POST,
                "https://api.dropboxapi.com/2/files/delete_v2",
                &[],
                Body::Json(json!({"path":format!("/{store}")})),
                true,
            )
            .await
            .unwrap(),
    )
    .await;
    deleted.unwrap();
    result.unwrap();
}

// Live provider listings lag successful writes; this probe observes visibility
// without changing the object or treating an initial listing miss as absence.
async fn wait_for_listing(storage: &dyn Storage, object: &StoredObject, clock: &ClockRef) {
    let deadline = clock.now() + std::time::Duration::from_secs(30);
    loop {
        if storage
            .list(&ObjectPrefix::all(object.path.store()))
            .await
            .unwrap()
            .contains(object)
        {
            return;
        }
        assert!(
            clock.now() < deadline,
            "test object did not become visible in provider listing"
        );
        clock.sleep(std::time::Duration::from_millis(200)).await;
    }
}
