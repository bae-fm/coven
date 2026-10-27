//! Opt-in probes use production authorization and adapters; credentials never enter output.

use super::{
    http::{self, Body},
    DropboxClients, OAuthClients, OAuthSession, ProviderConnector, StorageConnector,
};
use crate::providers::OAuthTokenRefresh;
use crate::*;
use coven_foundation::{
    clock::{ClockRef, SystemClock},
    id_source::{IdSource, StoreId, UuidIds},
};
use reqwest::Method;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV for an App-folder app; creates and deletes its test store"]
async fn provider_access_live_dropbox_app_folder() {
    let (client_id, refresh_token) =
        super::read_live_settings("COVEN_LIVE_DROPBOX_ENV", |settings| {
            (
                settings.take("DROPBOX_APP_KEY").as_str().to_owned(),
                settings.take("DROPBOX_REFRESH_TOKEN"),
            )
        });
    let clock: ClockRef = Arc::new(SystemClock);
    let tokens = OAuthClients::new(
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
        crate::providers::tests::http_client(),
    )
    .unwrap();
    let connector = ProviderConnector::new(clock.clone(), Arc::new(UuidIds));
    let location = StorageConfig::Dropbox {
        account_label: None,
        location: DropboxLocation::AppFolder,
    };
    let storage = connector
        .connect(
            location.clone(),
            crate::ProviderAuth::OAuth {
                tokens: tokens.clone(),
                custody: Arc::new(crate::test_utils::RefusedRefresh),
            },
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(crate::NoCooldown),
            Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
        )
        .await
        .unwrap();
    let store = StoreId(UuidIds.new_id());
    let result = tokio::spawn(async move {
        let path = ObjectPath::slot(store, std::num::NonZeroU64::MIN);
        assert!(storage.status(&path).await.unwrap().is_none());
        assert!(storage.list(&ObjectPrefix::all(store)).await.unwrap().is_empty());
        storage.create(&path, b"app folder bytes").await.unwrap();
        let before = storage.read_object(&path, None).await.unwrap();
        assert_eq!(before.bytes, b"app folder bytes");
        let restored = connector
            .connect(
                crate::RestoreStorage::decode(
                    crate::RestoreStorage { location: location.clone(), s3_key: None }
                        .encode()
                        .unwrap()
                        .as_bytes(),
                )
                .unwrap()
                .location,
                crate::ProviderAuth::OAuth { tokens, custody: Arc::new(crate::test_utils::RefusedRefresh) }, Arc::new(crate::test_utils::UnrestrictedPublications), Arc::new(crate::NoCooldown),
Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
)
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
