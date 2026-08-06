use super::*;
use crate::sync::test_helpers::{open_test_db, temp_store_dir, TestStore};

#[tokio::test]
async fn loaded_store_authorization_retains_its_verified_root() {
    let db = open_test_db();
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let fixture = TestStore::create(&db, "retained-root-authority", signer.clone(), home.clone())
        .await
        .expect("create Store");
    let (_store_dir_temp, store_dir) = temp_store_dir();
    let store = Store::load(
        coven_database::StoreDatabase::new(&db),
        fixture.storage(),
        store_dir,
        signer,
    )
    .await
    .expect("load Store");

    home.remove_exact_object(fixture.root.object.slot());

    store
        .authorize()
        .await
        .expect("authorize from the root verified while loading");
}
