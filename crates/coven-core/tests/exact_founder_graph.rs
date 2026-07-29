use coven_core::keys::UserKeypair;
use coven_core::sync::test_helpers::{
    host_exec, open_test_db, pull_into, query_text, temp_store_dir, TestStore,
};

#[tokio::test]
async fn created_merge_store_immediately_has_its_exact_founder_chain() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(&db, "exact-founder-graph", founder.clone())
        .await
        .expect("create Store with founder graph");
    store
        .open_into(&db)
        .await
        .expect("created Store founder chain is immediately readable");
}

#[tokio::test]
async fn opened_store_persists_the_exact_root_needed_by_membership_pull() {
    let creator_db = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(&creator_db, "opened-exact-root", founder.clone())
        .await
        .expect("create source Store");
    let opened_db = open_test_db();
    store
        .open_into(&opened_db)
        .await
        .expect("opened Store membership uses its durable exact root");
}

#[tokio::test]
async fn opened_store_cannot_mint_a_second_founder_registration() {
    let creator_db = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(&creator_db, "one-founder-registration", founder.clone())
        .await
        .expect("create source Store");
    let opened_db = open_test_db();
    store
        .open_into(&opened_db)
        .await
        .expect("open existing Store without registering a device");

    let creates_before = store.home.exact_create_count();
    let local_registrations_before = table_count(
        &opened_db,
        "SELECT COUNT(*) FROM local_store_device_registration",
    )
    .await;
    let activations_before = table_count(
        &opened_db,
        "SELECT COUNT(*) FROM store_device_registration_activations",
    )
    .await;

    let error = match store.bind_device(&opened_db, &founder).await {
        Ok(_) => panic!("an opened Store without a registered device authorized a writer"),
        Err(error) => error,
    };
    assert!(
        error.contains(coven_core::database::LOCAL_DEVICE_ID_STATE_KEY),
        "{error}"
    );
    assert_eq!(store.home.exact_create_count(), creates_before);
    assert_eq!(
        table_count(
            &opened_db,
            "SELECT COUNT(*) FROM local_store_device_registration",
        )
        .await,
        local_registrations_before,
    );
    assert_eq!(
        table_count(
            &opened_db,
            "SELECT COUNT(*) FROM store_device_registration_activations",
        )
        .await,
        activations_before,
    );
}

async fn table_count(db: &coven_core::database::Database, sql: &'static str) -> i64 {
    db.call(move |conn| {
        conn.query_row(sql, [], |row| row.get(0))
            .map_err(coven_core::database::DbError::from)
    })
    .await
    .expect("count lifecycle rows")
}

#[tokio::test]
async fn opened_store_pulls_a_production_commit_through_exact_refs() {
    let source = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(&source, "opened-exact-pull", founder)
        .await
        .expect("create source Store");
    host_exec(
        &source,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('note-1', 'exact pull', 1, '0000000001000-0000-source', '2026-07-16')",
    )
    .await;
    let (_source_temp, source_dir) = temp_store_dir();
    assert!(store
        .publish_pending(&source, &source_dir)
        .await
        .expect("publish production Store commit"));

    let destination = open_test_db();
    let (_destination_temp, destination_dir) = temp_store_dir();
    let (_, result) = pull_into(&destination, &store, &destination_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(
        query_text(&destination, "SELECT title FROM notes WHERE id = 'note-1'").await,
        "exact pull"
    );

    host_exec(
        &source,
        "UPDATE notes SET title = 'exact successor', \
         _updated_at = '0000000002000-0000-source' WHERE id = 'note-1'",
    )
    .await;
    assert!(store
        .publish_pending(&source, &source_dir)
        .await
        .expect("publish exact successor commit"));
    let (_, successor) = pull_into(&destination, &store, &destination_dir).await;
    assert_eq!(successor.changesets_applied, 1);
    assert_eq!(
        query_text(&destination, "SELECT title FROM notes WHERE id = 'note-1'").await,
        "exact successor"
    );
}
