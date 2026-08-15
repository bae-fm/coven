use std::sync::Arc;

use super::commands::{CircleCancelEpochCloseRequest, CircleOperationRequest};
use super::*;
use crate::sync::test_helpers::{
    temp_store_dir, test_migrations, test_synced_tables, TestCustody, TestStore, TestStoreParts,
};
use coven_database::StoreDatabase;
use coven_database::{Database, DbError};
use coven_keys::encryption::{EncryptionService, MasterKeyring};
use coven_keys::keys::{self, UserKeypair};
use coven_protocol::circle::{
    circle_semantic_prefix, CircleAccessDisposition, CircleId, CircleOperationId,
    CircleOperationKind, CircleOperationState, CircleRole, CircleRosterDraftPolicy,
    CircleSemanticSlot, CircleTransitionDraft, CircleTransitionDraftPolicy,
    CircleTransitionPolicyObjects, PreparedCircleTransition,
};
use coven_protocol::membership::MemberRole;
use coven_protocol::objects::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain,
};
use coven_protocol::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, CircleAccessEnvelopeObjectRef,
    CircleAccessLeafObjectRef, CircleAccessObjectRef, GrantStreamAnchor, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StreamActivation,
};
use coven_storage::cloud::CloudHome;
use coven_storage::CloudSyncObjectStorage;

async fn create_test_store_in_its_own_task(
    db: &Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    name: &str,
    signer: &UserKeypair,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
) -> std::sync::Arc<TestStore> {
    let (store, _connection) =
        create_test_store_fixture_in_its_own_task(db, db_store_dir, name, signer, home).await;
    store
}

async fn create_test_store_fixture_in_its_own_task(
    db: &Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    name: &str,
    signer: &UserKeypair,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
) -> TestStoreParts {
    let db = db.clone();
    let name = name.to_string();
    let signer = signer.clone();
    tokio::spawn(async move {
        TestStore::create_with_connection(&db, db_store_dir.clone(), &name, signer, home).await
    })
    .await
    .expect("join Circle test Store creation")
    .expect("create exact Circle test Store")
}

async fn persist_merge_operation(
    db: &Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    name: &str,
) -> (
    std::sync::Arc<TestStore>,
    std::sync::Arc<coven_storage::InMemoryCloudHome>,
    UserKeypair,
    CircleOperationJournal,
) {
    let (fixture, home, signer, journal) =
        persist_merge_operation_fixture(db, db_store_dir, name).await;
    let (store, _connection) = fixture;
    (store, home, signer, journal)
}

async fn persist_merge_operation_fixture(
    db: &Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    name: &str,
) -> (
    TestStoreParts,
    std::sync::Arc<coven_storage::InMemoryCloudHome>,
    UserKeypair,
    CircleOperationJournal,
) {
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let fixture = create_test_store_fixture_in_its_own_task(
        db,
        db_store_dir.clone(),
        name,
        &signer,
        home.clone(),
    )
    .await;
    let (store, connection) = fixture;
    let prepared = store
        .bind_device_in(db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-creator", "Household")
        .await
        .expect("prepare circle operation");
    StoreDatabase::new(db)
        .insert_circle_operation(prepared.journal.clone(), prepared.prepared_objects)
        .await
        .expect("persist circle operation");
    ((store, connection), home, signer, prepared.journal)
}

/// The bytes an operation's durable row owns for one exact object.
async fn stored_bytes(db: &Database, object: &ExactObjectRef) -> Vec<u8> {
    StoreDatabase::new(db)
        .payload_for_test(object.stored_hash())
        .await
        .expect("read a prepared Circle object's payload")
}

/// Publish every object an operation names from the bytes its durable row owns.
async fn publish_prepared_objects(
    store: &TestStore,
    db: &Database,
    journal: &CircleOperationJournal,
) {
    for object in journal.operation().prepared_objects.values() {
        store
            .publish_exact_protocol_object(object, stored_bytes(db, object).await)
            .await
            .expect("publish a prepared Circle object");
    }
}

/// Which of an operation's objects still have storage owned by the database.
async fn stored_objects(db: &Database, journal: &CircleOperationJournal) -> Vec<String> {
    let database = StoreDatabase::new(db);
    let mut present = Vec::new();
    for (step, object) in &journal.operation().prepared_objects {
        if database
            .has_payload_for_test(object.stored_hash())
            .await
            .expect("check prepared Circle object storage")
        {
            present.push(step.clone());
        }
    }
    present
}

/// Install a substituted object's bytes before a test gives its reference to a
/// durable operation.
async fn install_substituted_object(db: &Database, prepared: &PreparedExactObject) {
    StoreDatabase::new(db)
        .install_payload_for_test(prepared.stored_bytes().to_vec())
        .await
        .expect("install a substituted Circle object");
}

fn promote_store_member_access_without_adding_to_circle_roster(
    creation: &mut CircleTransitionDraft,
    owner: &UserKeypair,
    recipient: &UserKeypair,
) {
    let recipient_pubkey = keys::public_key_hex(recipient);
    let access = creation
        .access
        .iter_mut()
        .find(|access| access.leaf.value.recipient_pubkey == recipient_pubkey)
        .expect("Store member has a prepared inactive access leaf");
    access.leaf.value.body_mut().disposition = CircleAccessDisposition::Active {
        keyring: creation.keyring.clone(),
        key_fingerprint: creation.control.value.key_fingerprint(),
        roster: creation.control.value.roster_state_ref(),
        bootstrap: None,
    };
    access.leaf.value.resign(owner);
    let recipient_key =
        keys::ed25519_to_x25519_public_key(&recipient.public_key()).expect("convert recipient key");
    access.leaf.bytes = keys::seal_box_encrypt(
        &serde_json::to_vec(&access.leaf.value).expect("serialize promoted access leaf"),
        &recipient_key,
    );
    access.leaf.leaf_hash = ObjectHash::digest(&access.leaf.bytes);

    let leaf_hashes = creation
        .access
        .iter()
        .map(|access| access.leaf.leaf_hash)
        .collect::<Vec<_>>();
    let (access_root, proofs) =
        coven_protocol::circle_control::merkle_root_and_proofs(&leaf_hashes);
    creation
        .control
        .value
        .body_mut()
        .value
        .state
        .active_epoch_mut()
        .expect("test transition has an active epoch")
        .common
        .access_root = access_root;
    creation.control.value.resign(owner);
    creation.control.coord = creation.control.value.coord();
    creation.control.bytes =
        serde_json::to_vec(&creation.control.value).expect("serialize promoted control");
    for (access, proof) in creation.access.iter_mut().zip(proofs) {
        let value_hash = ObjectHash::digest(
            &serde_json::to_vec(&access.leaf.value).expect("serialize access leaf value"),
        );
        let envelope = access.envelope.body_mut();
        envelope.control_hash = creation.control.coord.control_hash();
        envelope.leaf_hash = access.leaf.leaf_hash;
        envelope.value_hash = value_hash;
        envelope.proof = proof;
        access.envelope.resign(owner);
    }
}

/// Cloud storage backed by a Circle test's in-memory home, sealed with the fixed
/// routing key every case in this tree uses.
fn circle_test_cloud_storage(
    home: &Arc<coven_storage::InMemoryCloudHome>,
    store_id: &str,
    identity: &UserKeypair,
) -> coven_storage::CloudSyncConnection {
    coven_storage::CloudSyncConnection::new(
        home.clone(),
        coven_storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        coven_storage::BlobPathScheme::Hashed,
        store_id,
        identity.clone(),
    )
}

fn circle_test_custody() -> Arc<crate::sync::test_helpers::TestCustody> {
    let custody = Arc::new(crate::sync::test_helpers::TestCustody::default());
    custody.set_initial_key([42; 32]);
    custody
}

/// The initialized production sync components a Circle test's owner drives.
async fn prepare_owner_sync_components(
    db: &Database,
    store: &TestStore,
    home: &Arc<coven_storage::InMemoryCloudHome>,
    store_dir: &coven_foundation::store_dir::StoreDir,
    signer: &UserKeypair,
    store_id: &str,
    master_keys: Arc<crate::sync::test_helpers::TestCustody>,
) -> crate::sync::cycle::SyncComponents {
    crate::sync::cycle::PreparedSyncComponents::prepare(
        coven_database::StoreDatabase::new(db),
        store_dir.clone(),
        circle_test_cloud_storage(home, store_id, signer),
        signer.clone(),
        crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root: store.root().clone(),
        },
        Some(EncryptionService::from_key([42; 32])),
        master_keys,
    )
    .await
    .expect("prepare Circle owner sync")
    .initialize(None)
    .await
    .expect("initialize Circle owner sync")
}

/// Publishes the owner device's Circle epoch-close response and runs the cycle
/// that activates the close outcome — the pair every epoch-close case drives
/// after the operation that opened the close.
async fn finalize_circle_epoch_close(
    store: &TestStore,
    db: &Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    signer: &UserKeypair,
    components: &crate::sync::cycle::SyncComponents,
) {
    store
        .bind_device(db, db_store_dir, signer)
        .await
        .expect("bind Circle test Store")
        .publish_circle_epoch_close_response()
        .await
        .expect("publish local Circle epoch-close response");
    components
        .run_cycle(&coven_foundation::clock::SystemClock, None)
        .await
        .expect("activate the Circle epoch-close outcome");
}

fn draft_from_transition(creation: &PreparedCircleTransition) -> CircleTransitionDraft {
    let roster = creation.policy_objects.roster.as_ref().map_or(
        CircleRosterDraftPolicy::Inherited,
        |roster| {
            assert_eq!(roster.entry.seq, 1, "test transition must be a founder");
            assert!(
                roster.entry.previous_hash.is_none(),
                "test transition must be a founder"
            );
            CircleRosterDraftPolicy::Founder {
                entry: roster.entry.clone(),
            }
        },
    );
    let policy = CircleTransitionDraftPolicy {
        roster,
        metadata_successor: creation.policy_objects.metadata_head.is_some(),
    };
    CircleTransitionDraft {
        circle_id: creation.circle_id,
        epoch_id: creation.epoch_id,
        keyring: creation.keyring.clone(),
        roster: creation.roster.clone(),
        policy,
        metadata: creation.metadata.clone(),
        close_intent: creation.close_intent.clone(),
        close_finalization: None,
        close_cancellation: None,
        access: creation.access.clone(),
        control: creation.control.clone(),
    }
}

fn assert_exact_operation(expected: &CircleOperationJournal, actual: &CircleOperationJournal) {
    assert_eq!(actual.operation_id, expected.operation_id);
    assert_eq!(actual.circle_id, expected.circle_id);
    assert_eq!(actual.intent, expected.intent);
    assert_eq!(actual.operation().creation, expected.operation().creation);
    assert_eq!(
        actual.operation().commit_bytes,
        expected.operation().commit_bytes
    );
    assert_eq!(actual.operation().policy, expected.operation().policy);
}

mod journal;
mod local_validation;
mod publication;
mod recovery;
mod remote_validation;
mod resolution;
mod retained;
mod rotation_required;
