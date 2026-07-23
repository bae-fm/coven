use std::sync::Arc;

use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::membership::MemberRole;
use crate::sync::storage::SyncStorage;
use crate::sync::store_commit::ObjectHash;
use crate::sync::wrapped_store_key::{prepare_wrapped_store_key, WrappedStoreKey};

use super::{
    finish_membership_transition, prepare_membership_transition, select_mutation_author_stream,
    unwrap_store_keyring_for_refs, validate_prepared_publication, validate_prepared_transition,
};

#[tokio::test]
async fn wrapped_ref_generation_must_match_its_decrypted_keyring() {
    let owner = UserKeypair::generate();
    let recipient = UserKeypair::generate();
    let recipient_pubkey = keys::public_key_hex(&recipient);
    let storage = CloudSyncStorage::new(
        Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Encrypted(EncryptionService::from_key([3; 32])),
        BlobPathScheme::Hashed,
        "wrapped-generation-test",
        owner.clone(),
    )
    .expect("build exact test storage");
    let keyring = EncryptionService::from_key([7; 32]);
    let sealed = keys::seal_box_encrypt(
        &keyring.to_keyring_payload().expect("serialize keyring"),
        &recipient.to_x25519_public_key(),
    );
    let prepared = prepare_wrapped_store_key(
        &storage,
        ObjectHash::digest(b"wrapped generation root"),
        &recipient_pubkey,
        WrappedStoreKey::signed(
            "wrapped-generation-store",
            &recipient_pubkey,
            2,
            sealed,
            &owner,
        ),
    )
    .await
    .expect("prepare mismatched generation wrap");
    storage
        .create_protocol_object(&prepared.object)
        .await
        .expect("create mismatched generation wrap");

    assert!(unwrap_store_keyring_for_refs(
        &storage,
        ObjectHash::digest(b"wrapped generation root"),
        &recipient,
        "wrapped-generation-store",
        &[prepared.reference],
    )
    .await
    .is_err());
}

#[tokio::test]
async fn prepared_membership_transition_rejects_substituted_slots_and_bytes() {
    let db = crate::sync::test_helpers::open_test_db();
    let owner = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "prepared-membership-binding",
        owner.clone(),
    )
    .await
    .expect("create Merge Store");
    let database = crate::sync::store::StoreDatabase::new(&db);
    let chain = crate::sync::store::membership::load_current_exact_chain(
        &store.storage,
        &store.root,
        Some(&keys::public_key_hex(&owner)),
        Some(&db),
    )
    .await
    .expect("load exact membership chain");
    let stream_id = select_mutation_author_stream(&database, &chain, &owner)
        .await
        .expect("select membership stream");
    let entry = chain
        .signed_set_member_in_stream(
            &owner,
            stream_id,
            keys::public_key_hex(&UserKeypair::generate()),
            None,
            MemberRole::Member,
            "2026-07-21T00:00:00Z".to_string(),
        )
        .expect("sign membership entry");
    let prepared = prepare_membership_transition(
        &store.storage,
        &database,
        store.root.store_root_hash,
        &chain,
        entry,
        &owner,
    )
    .await
    .expect("prepare membership transition");
    validate_prepared_transition(&prepared).expect("validate prepared transition");

    let mut redirected_head = prepared.clone();
    redirected_head.transition.head_slot = crate::storage::cloud::ObjectSlot::logical(
        "store-v1/tests/redirected-membership-head.json".to_string(),
    )
    .expect("valid redirected head slot");
    assert!(validate_prepared_transition(&redirected_head).is_err());

    let mut redirected_successor = prepared.clone();
    redirected_successor.transition.body.successor.next_slot =
        crate::storage::cloud::ObjectSlot::logical(
            "store-v1/tests/redirected-membership-successor.json".to_string(),
        )
        .expect("valid redirected successor slot");
    assert!(validate_prepared_transition(&redirected_successor).is_err());

    let mut substituted_entry = prepared.clone();
    let substituted_bytes = b"substituted exact membership entry".to_vec();
    let substituted_ref = crate::sync::storage::ExactObjectRef::new(
        substituted_entry.entry_object.reference().slot().clone(),
        substituted_bytes.len() as u64,
        ObjectHash::digest(&substituted_bytes),
    );
    substituted_entry.entry_object =
        crate::sync::storage::PreparedExactObject::new(substituted_ref.clone(), substituted_bytes)
            .expect("prepare substituted membership entry object");
    substituted_entry.entry_ref.object = substituted_ref.clone();
    substituted_entry.transition.body.entry.object = substituted_ref;
    assert!(validate_prepared_transition(&substituted_entry).is_err());

    let publication = finish_membership_transition(
        &store.storage,
        &database,
        store.root.store_root_hash,
        prepared,
        crate::sync::membership::MembershipHeadActivation::Direct,
        &owner,
    )
    .await
    .expect("finish membership transition");
    let mut substituted_head = publication;
    let substituted_bytes = b"substituted exact membership head".to_vec();
    let substituted_ref = crate::sync::storage::ExactObjectRef::new(
        substituted_head.head_object.reference().slot().clone(),
        substituted_bytes.len() as u64,
        ObjectHash::digest(&substituted_bytes),
    );
    substituted_head.head_object =
        crate::sync::storage::PreparedExactObject::new(substituted_ref.clone(), substituted_bytes)
            .expect("prepare substituted membership head object");
    substituted_head.head_ref.object = substituted_ref;
    assert!(validate_prepared_publication(&substituted_head).is_err());
}
