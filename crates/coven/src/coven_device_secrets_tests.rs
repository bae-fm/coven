use crate::*;
use std::sync::Arc;

/// An app keeps one secret for the whole device: each store on it sees the
/// same one, deleting a store leaves it, another device has none, and the
/// app's reset deletes it with every other device secret and no store entry.
#[tokio::test]
async fn a_device_secret_is_one_per_device_whatever_its_stores() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let first = app.create_closed_store("Household", ids.clone()).await.unwrap();
    let second = app.create_closed_store("Work", ids).await.unwrap();
    assert_eq!(app.device_secret("speaker").unwrap(), None);
    app.set_device_secret("speaker", "pairing keys").unwrap();
    app.set_device_secret("another speaker", "other keys").unwrap();
    app.discard_store(&first).await.unwrap();
    assert_eq!(app.device_secret("speaker").unwrap().as_deref(), Some("pairing keys"));
    let elsewhere = tempfile::tempdir().unwrap();
    let other = app.another_device(elsewhere.path().to_owned());
    assert_eq!(other.device_secret("speaker").unwrap(), None);
    app.fail_next_keychain_operation();
    assert!(app.delete_device_secrets().is_err());
    app.delete_device_secrets().unwrap();
    assert_eq!(app.device_secret("speaker").unwrap(), None);
    assert_eq!(app.device_secret("another speaker").unwrap(), None);
    assert!(app.store_keychain(second.id()).device_id().unwrap().is_some());
}
