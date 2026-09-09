use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    store_current_publication_semantic_prefix, store_publication_entry_semantic_prefix,
    StoreCurrentPublicationRecord, StorePublicationEntry, StorePublicationRef,
};
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn a_new_publication_cannot_reuse_an_already_accepted_author_sequence() {
    assert_accepted_author_sequence_cannot_be_reused(false).await;
}

#[tokio::test]
async fn interval_installation_cannot_reuse_an_already_accepted_author_sequence() {
    assert_accepted_author_sequence_cannot_be_reused(true).await;
}

async fn assert_accepted_author_sequence_cannot_be_reused(install_directly: bool) {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "duplicate-accepted-publication",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('published-row', 'Accepted once', 1, \
             '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
    let database = StoreDatabase::new(&source);
    let mut writer = owner.authorize_writer().await.expect("authorize writer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare row"));
    let pending = database
        .oldest_prepared_store_write()
        .await
        .expect("load pending row")
        .expect("row is prepared");
    assert_eq!(writer.drain_store_writes().await.expect("publish row"), 1);
    drop(writer);
    let before = database
        .store_current_publication()
        .await
        .expect("read accepted boundary");
    let commit = &pending.commit.value;
    let device_signer = commit
        .author()
        .device_signer(&signer)
        .expect("derive the publishing device key");
    // Send a new, validly signed publication envelope for the same commit.
    let entry = StorePublicationEntry::signed_commit(before.record(), commit, &device_signer)
        .expect("sign the duplicate publication envelope");
    let prefix = store_publication_entry_semantic_prefix(&entry);
    let context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StorePublicationEntry,
    );
    let object = store
        .create_exact_protocol_object(&context, &prefix, ".json", &entry.to_bytes())
        .await
        .expect("upload the duplicate publication envelope");
    let reference =
        StorePublicationRef::from_entry(&entry, object).expect("reference duplicate publication");
    let replacement = StoreCurrentPublicationRecord::advance_commit(
        before.record(),
        &entry,
        reference.clone(),
        commit,
        &device_signer,
    )
    .expect("sign a successor record");
    let context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreCurrentPublication,
    );
    let result = storage
        .replace_protocol_record_if_version(
            &context,
            &owner
                .protocol_root_for_test()
                .descriptor
                .current_publication_slot,
            store_current_publication_semantic_prefix(),
            before
                .require_observed()
                .expect("accepted provider observation")
                .version(),
            replacement.to_bytes(),
        )
        .await
        .expect("the provider accepts a matching revision");
    let coven_storage::cloud::ConditionalWriteOutcome::Replaced(version) = result else {
        panic!("the current record changed unexpectedly");
    };
    let error = if install_directly {
        let author = coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
            commit.author_registration.clone(),
            commit.author().clone(),
        )
        .expect("reference author");
        let interval = coven_protocol::store_commit::VerifiedStorePublicationInterval::verified(
            before.record().clone(),
            replacement,
            vec![
                coven_protocol::store_commit::StorePublicationIntervalEntry::new(
                    entry, reference, author,
                ),
            ],
        )
        .expect("verify contiguous signed publication interval");
        let installed = database
            .apply_received_store_publication_interval(
                Vec::new(),
                coven_database::AcceptedStorePublicationInterval::from_verified(
                    interval.clone(),
                    Some(version),
                ),
                interval,
                Vec::new(),
                coven_protocol::membership::LocalStoreMembership::Current,
                None,
                None,
                database.receive_wall_ms(),
            )
            .await;
        match installed {
            Ok(_) => panic!("installation accepted a repeated author sequence"),
            Err(error) => error.to_string(),
        }
    } else {
        owner
            .pull_store()
            .await
            .expect_err("reject an already accepted author sequence")
            .to_string()
    };
    assert!(error.contains("author sequence"), "{error}");
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("read preserved boundary"),
        before,
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'published-row'")
            .await,
        "Accepted once",
    );
}
