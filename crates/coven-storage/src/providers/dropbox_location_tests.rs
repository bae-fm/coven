//! A Dropbox shared folder holds one store, claimed by the location marker
//! at its root; the App folder, which nobody else reaches, holds several
//! stores and no marker (§20.3).
use super::*;
use coven_foundation::id_source::StoreId;

const OTHER: StoreId = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
const MARKER: &str = "/coven-location";

fn occupied(error: StorageError) {
    assert_eq!(error.failure(), StorageFailure::OwnLocationRequired, "{error:?}");
}

#[tokio::test]
async fn a_shared_folder_admits_one_store_and_its_retry() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.claim_location(STORE_ID).await.unwrap();
    assert_eq!(state.lock().unwrap().objects.get(MARKER), Some(&STORE_ID.to_string().into_bytes()));
    occupied(storage.claim_location(OTHER).await.unwrap_err());
    occupied(storage.check_location_claim(OTHER).await.unwrap_err());
    storage.claim_location(STORE_ID).await.unwrap();
    storage.check_location_claim(STORE_ID).await.unwrap();
    let remote = state.lock().unwrap();
    assert_eq!(
        remote.objects,
        BTreeMap::from([(MARKER.to_owned(), STORE_ID.to_string().into_bytes())])
    );
}

#[tokio::test]
async fn reconnect_refuses_a_missing_malformed_or_other_marker_without_writing() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    for bytes in [
        None,
        Some(Vec::new()),
        Some(vec![1; 4096]),
        Some(format!("{STORE_ID}\n").into_bytes()),
        Some(OTHER.to_string().into_bytes()),
    ] {
        let planted: BTreeMap<_, _> =
            bytes.into_iter().map(|bytes| (MARKER.to_owned(), bytes)).collect();
        state.lock().unwrap().objects = planted.clone();
        occupied(storage.check_location_claim(STORE_ID).await.unwrap_err());
        assert_eq!(state.lock().unwrap().objects, planted);
    }
    // A folder at the marker's name is no marker.
    state.lock().unwrap().folders.insert(MARKER.to_owned());
    occupied(storage.check_location_claim(STORE_ID).await.unwrap_err());
}

#[tokio::test]
async fn a_lost_marker_reply_is_settled_by_reading_the_marker() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    state.lock().unwrap().fail_reply = true;
    assert_eq!(
        storage.claim_location(STORE_ID).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    storage.claim_location(STORE_ID).await.unwrap();
    storage.check_location_claim(STORE_ID).await.unwrap();
}

/// A shared folder this account no longer reaches is not a missing marker:
/// the read keeps the provider's cause, as an object's status does.
#[tokio::test]
async fn an_unreachable_shared_folder_keeps_its_cause() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    state.lock().unwrap().missing_namespace = true;
    let expected = storage.status(&ObjectPath::floor(STORE_ID)).await.unwrap_err().failure();
    assert_ne!(expected, StorageFailure::OwnLocationRequired);
    assert_eq!(storage.check_location_claim(STORE_ID).await.unwrap_err().failure(), expected);
}

#[tokio::test]
async fn the_app_folder_holds_several_stores_without_a_marker() {
    let state = Arc::new(Mutex::new(Remote { app_folder: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut provider = DropboxStorage::new(
        StorageConfig::Dropbox { account_label: None, location: crate::DropboxLocation::AppFolder },
        session(PROVIDER),
    )
    .unwrap();
    provider.api = format!("{}/2", server.url);
    provider.content = provider.api.clone();
    let storage = StorageConnection::from_provider(Arc::new(provider));
    for store in [STORE_ID, OTHER] {
        storage.claim_location(store).await.unwrap();
        storage.check_location_claim(store).await.unwrap();
    }
    assert_eq!(state.lock().unwrap().requests, 0);
}

/// First setup admits a shared folder holding nothing, or only what an
/// earlier attempt at this store's creation wrote, and refuses anything
/// else at its root, whatever the case of its name.
#[tokio::test]
async fn first_setup_admits_only_the_store_folder_and_the_marker() {
    let own = format!("/{STORE_ID}/floor");
    for (objects, folders, admitted) in [
        (vec![], vec![], true),
        (vec![own.as_str(), MARKER], vec![], true),
        (vec![MARKER], vec![format!("/{STORE_ID}")], true),
        (vec!["/notes.txt"], vec![], false),
        (vec![own.as_str()], vec!["/music".to_owned()], false),
        (vec![], vec![format!("/{OTHER}")], false),
        (vec!["/coven-location/inside"], vec![], false),
    ] {
        let state = Arc::new(Mutex::new(Remote::default()));
        {
            let mut remote = state.lock().unwrap();
            remote.objects = objects.iter().map(|path| (path.to_string(), b"x".to_vec())).collect();
            remote.folders = folders.iter().cloned().collect();
        }
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        assert_eq!(
            storage.provider().holds_only(STORE_ID).await.unwrap(),
            admitted,
            "{objects:?} {folders:?}"
        );
        assert!(!state.lock().unwrap().list_recursive);
    }
}

#[tokio::test]
#[ignore = "requires COVEN_CREATE_LEAN"]
async fn location_claims_match_lean() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    crate::providers::tests::compare_location_model(&storage, || {
        state.lock().unwrap().objects.get(MARKER).cloned()
    })
    .await;
}
