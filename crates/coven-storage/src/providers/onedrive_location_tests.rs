//! A OneDrive folder holds one store, claimed by the location marker at its
//! root (§20.3).
use super::*;
use coven_foundation::id_source::StoreId;

const OTHER: StoreId = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
const MARKER: &str = "coven-location";

fn occupied(error: StorageError) {
    assert_eq!(error.failure(), StorageFailure::OwnLocationRequired, "{error:?}");
}

#[tokio::test]
async fn a_folder_admits_one_store_and_its_retry() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.claim_location(STORE_ID).await.unwrap();
    assert_eq!(state.lock().unwrap().files.get(MARKER), Some(&STORE_ID.to_string().into_bytes()));
    occupied(storage.claim_location(OTHER).await.unwrap_err());
    occupied(storage.check_location_claim(OTHER).await.unwrap_err());
    storage.claim_location(STORE_ID).await.unwrap();
    storage.check_location_claim(STORE_ID).await.unwrap();
    let remote = state.lock().unwrap();
    assert_eq!(
        remote.files,
        BTreeMap::from([(MARKER.to_owned(), STORE_ID.to_string().into_bytes())])
    );
}

#[tokio::test]
async fn reconnect_refuses_a_missing_malformed_or_other_marker_without_writing() {
    let state = remote();
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
        {
            let mut remote = state.lock().unwrap();
            remote.revisions = planted.keys().map(|path| (path.clone(), 1)).collect();
            remote.files = planted.clone();
            remote.requests.clear();
        }
        occupied(storage.check_location_claim(STORE_ID).await.unwrap_err());
        let remote = state.lock().unwrap();
        assert_eq!(remote.files, planted);
        assert!(remote.requests.iter().all(|(method, _)| *method == Method::GET));
    }
    // A folder at the marker's name is no marker.
    state.lock().unwrap().dirs.insert("marker-folder".into(), MARKER.into());
    occupied(storage.check_location_claim(STORE_ID).await.unwrap_err());
}

#[tokio::test]
async fn a_lost_marker_reply_is_settled_by_reading_the_marker() {
    let state = remote();
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

/// A folder that is gone is not a missing marker: the read keeps the
/// provider's cause.
#[tokio::test]
async fn a_missing_folder_keeps_its_cause() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    state.lock().unwrap().dirs.clear();
    assert_eq!(
        storage.check_location_claim(STORE_ID).await.unwrap_err().failure(),
        StorageFailure::ContainerNotFound
    );
}

/// First setup admits a folder holding nothing, or only what an earlier
/// attempt at this store's creation wrote, and refuses anything else among
/// its children, whatever the case of its name; every page is read.
#[tokio::test]
async fn first_setup_admits_only_the_store_folder_and_the_marker() {
    let own = STORE_ID.to_string();
    let upper = own.to_uppercase();
    for (files, dirs, admitted) in [
        (vec![], vec![], true),
        (vec![MARKER], vec![own.as_str()], true),
        (vec!["COVEN-LOCATION"], vec![upper.as_str()], true),
        (vec![MARKER, "notes.txt"], vec![own.as_str()], false),
        (vec![], vec![own.as_str(), "music"], false),
        (vec![], vec![MARKER], false),
    ] {
        let state = remote();
        {
            let mut remote = state.lock().unwrap();
            for file in &files {
                remote.files.insert(file.to_string(), b"x".to_vec());
                remote.revisions.insert(file.to_string(), 1);
            }
            for (index, dir) in dirs.iter().enumerate() {
                remote.dirs.insert(format!("dir-{index}"), dir.to_string());
            }
        }
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        assert_eq!(
            storage.provider().holds_only(STORE_ID).await.unwrap(),
            admitted,
            "{files:?} {dirs:?}"
        );
    }
}

#[tokio::test]
#[ignore = "requires COVEN_CREATE_LEAN"]
async fn location_claims_match_lean() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    crate::providers::tests::compare_location_model(&storage, || {
        state.lock().unwrap().files.get(MARKER).cloned()
    })
    .await;
}
