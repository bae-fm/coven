use std::sync::Arc;

use super::*;
use crate::database::{Database, DbError};
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{test_utils::InMemoryCloudHome, CloudHome};
use crate::sync::circle::{
    circle_semantic_prefix, CircleAccessDisposition, CircleId, CircleOperationId,
    CircleOperationKind, CircleOperationState, CircleRole, CircleRosterDraftPolicy,
    CircleSemanticSlot, CircleTransitionDraft, CircleTransitionDraftPolicy,
    CircleTransitionPolicyObjects, PreparedCircleTransition,
};
use crate::sync::cloud_storage::{CloudCipher, CloudCipherAccess};
use crate::sync::membership::MemberRole;
use crate::sync::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, CircleAccessEnvelopeObjectRef,
    CircleAccessLeafObjectRef, CircleAccessObjectRef, CircleActivationObjects, GrantStreamAnchor,
    ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StreamActivation, VerifiedStoreBatchCommit,
};
use crate::sync::test_helpers::{
    install_active_device_fixture, open_test_db, temp_store_dir, test_migrations,
    test_synced_tables, TestCustody, TestStore,
};

async fn local_device_id(db: &Database) -> String {
    db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device id")
        .expect("local Store device is active")
}

async fn create_test_store_in_its_own_task(
    db: &Database,
    name: &str,
    signer: &UserKeypair,
) -> TestStore {
    let db = db.clone();
    let name = name.to_string();
    let signer = signer.clone();
    tokio::spawn(async move { TestStore::create(&db, &name, signer).await })
        .await
        .expect("join Circle test Store creation")
        .expect("create exact Circle test Store")
}

async fn prepare_circle_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    let database = StoreDatabase::new(db);
    let root = database
        .local_store_root_ref()
        .await?
        .expect("Circle test Store root is installed");
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root)
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    super::prepare_circle_operation(
        &mut history_verifier,
        &database,
        device_id,
        metadata_stamp,
        name,
        signer,
    )
    .await
}

async fn publish_circle_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    operation_id: &CircleOperationId,
    signer: &UserKeypair,
) -> Result<(), CircleOperationError> {
    let root = StoreDatabase::new(db)
        .local_store_root_ref()
        .await?
        .expect("Circle test Store root is installed");
    let routing_key = crate::sync::circle::derive_row_routing_key(
        &crate::encryption::EncryptionService::from_key([42; 32]),
        root.store_root_hash,
    )
    .expect("derive Circle test routing key");
    super::publish_circle_operation(
        &StoreDatabase::new(db),
        storage,
        operation_id,
        signer,
        Some(&routing_key),
    )
    .await
}

async fn resume_circle_operations(
    db: &Database,
    storage: &Arc<crate::sync::cloud_storage::CloudSyncStorage>,
    signer: &UserKeypair,
) -> Result<(), CircleOperationError> {
    let store = crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone()).await?;
    let routing_key = crate::sync::circle::derive_row_routing_key(
        &crate::encryption::EncryptionService::from_key([42; 32]),
        store.store_root().store_root_hash,
    )
    .expect("derive Circle test routing key");
    let mut authority = store
        .authorize()
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    authority
        .resume_circle_operations(signer, Some(&routing_key))
        .await
}

async fn retry_circle_operation(
    db: &Database,
    storage: &Arc<crate::sync::cloud_storage::CloudSyncStorage>,
    operation_id: &CircleOperationId,
    signer: &UserKeypair,
) -> Result<(), CircleOperationError> {
    crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone())
        .await?
        .retry_circle_operation(
            operation_id,
            Some(&crate::encryption::EncryptionService::from_key([42; 32])),
            signer,
        )
        .await
}

async fn discard_circle_operation(
    db: &Database,
    storage: &Arc<crate::sync::cloud_storage::CloudSyncStorage>,
    operation_id: &CircleOperationId,
) -> Result<(), CircleOperationError> {
    crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone())
        .await?
        .discard_circle_operation(operation_id)
        .await
}

async fn remote_object_exists(db: &Database, object: &ExactObjectRef) -> bool {
    let object_id = crate::sync::remote_object::remote_object_id(object);
    db.call(move |conn| {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(DbError::from)
    })
    .await
    .expect("check stored remote object")
}

async fn exact_object_present(home: &InMemoryCloudHome, reference: &ExactObjectRef) -> bool {
    let storage = Arc::new(home.clone())
        .exact_slot_storage()
        .expect("in-memory storage supports exact slots");
    storage.read_at(reference.slot()).await.is_ok()
}

#[allow(clippy::too_many_arguments)]
async fn rename_circle(
    db: &Database,
    storage: &Arc<crate::sync::cloud_storage::CloudSyncStorage>,
    device_id: &str,
    metadata_stamp: &str,
    circle_id: CircleId,
    name: &str,
    signer: &UserKeypair,
) -> Result<(), CircleOperationError> {
    crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone())
        .await?
        .rename_circle(device_id, metadata_stamp, circle_id, name, signer)
        .await
}

async fn delete_circle(
    db: &Database,
    storage: &Arc<crate::sync::cloud_storage::CloudSyncStorage>,
    device_id: &str,
    circle_id: CircleId,
    signer: &UserKeypair,
) -> Result<(), CircleOperationError> {
    crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone())
        .await?
        .delete_circle(device_id, circle_id, signer)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn load_circle_activations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &crate::sync::store_commit::StoreDeviceRegistration,
    identity: &UserKeypair,
) -> Result<VerifiedCircleActivations, CircleOperationError> {
    let routing_key = crate::sync::circle::derive_row_routing_key(
        &crate::encryption::EncryptionService::from_key([42; 32]),
        root.store_root_hash,
    )
    .expect("derive Circle test routing key");
    let verified = VerifiedStoreBatchCommit::parse(
        &commit.to_bytes(),
        root.store_root_hash,
        commit_ref,
        author,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, root)
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    super::load_circle_activations(
        &StoreDatabase::new(db),
        &mut history_verifier,
        &verified,
        identity,
        Some(&routing_key),
    )
    .await
}

/// Publish a valid, different Store commit and activation head at a prepared
/// Circle operation's exact device-stream successor slot, so the operation's
/// candidate loses that slot to a verified Merge winner. Returns the winner's
/// commit and head objects so a test can assert they survive the loser's
/// candidate-exclusive cleanup.
async fn publish_competing_store_head(
    db: &Database,
    storage: &Arc<crate::sync::cloud_storage::CloudSyncStorage>,
    signer: &UserKeypair,
    journal: &CircleOperationJournal,
) -> (ExactObjectRef, ExactObjectRef) {
    let database = StoreDatabase::new(db);
    let root = database
        .local_store_root_ref()
        .await
        .expect("read Store root")
        .expect("Store root installed");
    let candidate = journal.commit().expect("parse candidate Store commit");
    let coord = journal.operation().commit_ref.coord.clone();
    let head = &journal.operation().policy.head;
    let registration = database
        .activated_store_device_registration(candidate.author_registration.clone())
        .await
        .expect("load candidate author registration");
    let device_signer = registration
        .device_signer(signer)
        .expect("derive candidate device signer");
    let package = crate::sync::audience_package::AudiencePackage::store(
        root.store_root_hash,
        candidate.candidate_family(),
        candidate.write_id.clone(),
        coord.clone(),
        db.schema_version(),
        b"competing valid package".to_vec(),
        Vec::new(),
    )
    .expect("construct competing package");
    let package_bytes = package.to_bytes();
    let package_prefix = crate::sync::store_commit::package_semantic_prefix(
        candidate.candidate_family(),
        &coord.stream_id.to_string(),
        candidate.seq(),
        ObjectHash::digest(&package_bytes),
    );
    let package_context = ProtocolObjectContext::store_encrypted(
        root.store_root_hash,
        ProtocolObjectDomain::StorePackage,
    );
    let package_slot = storage
        .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
        .await
        .expect("reserve competing package slot");
    let package_prepared = storage
        .prepare_protocol_object(
            &package_context,
            package_slot,
            &package_prefix,
            package_bytes.clone(),
        )
        .expect("prepare competing package");
    storage
        .create_protocol_object(&package_prepared)
        .await
        .expect("publish competing package");
    let membership = crate::sync::store::pull::load_cycle_membership(storage.as_ref(), &database)
        .await
        .expect("load competing commit membership");
    let predecessor = membership
        .write_grant_authority(&registration.author_pubkey)
        .expect("competing author has an active write grant");
    let winner = StoreBatchCommit::signed(
        root.store_root_hash,
        candidate.write_id.clone(),
        coord.clone(),
        candidate.author_registration.clone(),
        &registration,
        candidate.order.clone(),
        candidate.membership_state.clone(),
        candidate.device_state.clone(),
        crate::sync::store_commit::StoreOperationMembershipAuthority { predecessor },
        crate::sync::store_commit::StorePackageInput {
            candidate_family: candidate.candidate_family(),
            schema_version: db.schema_version(),
            bytes: &package_bytes,
            object: package_prepared.reference().clone(),
        },
        &device_signer,
    )
    .expect("sign competing commit");
    let commit_prefix = commit_semantic_prefix(
        winner.candidate_family(),
        &coord.stream_id.to_string(),
        winner.seq(),
        winner.commit_hash(),
    );
    let commit_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let commit_slot = storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .expect("reserve competing commit slot");
    let commit_prepared = storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            winner.to_bytes(),
        )
        .expect("prepare competing commit");
    storage
        .create_protocol_object(&commit_prepared)
        .await
        .expect("publish competing commit");
    let winner_ref = StoreBatchCommitRef::from_commit(
        &winner,
        coord.clone(),
        commit_prepared.reference().clone(),
    )
    .expect("reference competing commit");
    assert_ne!(winner_ref, journal.operation().commit_ref);
    let winner_head = StoreDeviceHead::signed(
        root.store_root_hash,
        candidate.author_registration.clone(),
        winner_ref,
        head.history_summary,
        head.successor.clone(),
        &device_signer,
    )
    .expect("sign competing head");
    let head_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_slot = journal
        .operation()
        .prepared_objects
        .get("store-head")
        .expect("candidate carries a prepared Store head")
        .reference()
        .slot()
        .clone();
    let head_prefix = head_slot_prefix(
        &candidate.author_registration.device_id.to_string(),
        candidate.seq(),
    );
    let head_prepared = storage
        .prepare_protocol_object(
            &head_context,
            head_slot,
            &head_prefix,
            winner_head.to_bytes(),
        )
        .expect("prepare competing head");
    storage
        .create_protocol_object(&head_prepared)
        .await
        .expect("publish competing head");
    (
        commit_prepared.reference().clone(),
        head_prepared.reference().clone(),
    )
}

async fn assert_exact_object_absent(home: &InMemoryCloudHome, reference: &ExactObjectRef) {
    let storage = Arc::new(home.clone())
        .exact_slot_storage()
        .expect("in-memory storage supports exact slots");
    let error = storage
        .read_at(reference.slot())
        .await
        .expect_err("rejected Store object must be absent");
    assert!(
        matches!(error, crate::storage::cloud::CloudHomeError::NotFound(_)),
        "{error}"
    );
}

async fn persist_merge_operation(
    db: &Database,
    name: &str,
) -> (TestStore, UserKeypair, CircleOperationJournal) {
    let signer = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(db, name, &signer).await;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read Circle creator device id")
        .expect("Circle creator has an active exact device");
    let journal = prepare_circle_operation(
        db,
        &store.storage,
        &device_id,
        "0000000001000-0000-creator",
        "Household",
        &signer,
    )
    .await
    .expect("prepare circle operation");
    StoreDatabase::new(db)
        .insert_circle_operation(journal.clone())
        .await
        .expect("persist circle operation");
    (store, signer, journal)
}

async fn resign_merge_journal_with_objects(
    db: &Database,
    storage: &dyn SyncStorage,
    signer: &UserKeypair,
    journal: &mut CircleOperationJournal,
    mutate: impl FnOnce(&mut CircleActivationObjects),
) {
    let old_commit = journal.commit().expect("parse prepared Circle commit");
    let [old_reference] = old_commit.circle_controls() else {
        panic!("Circle operation must carry one control")
    };
    let mut objects = old_reference.objects().clone();
    mutate(&mut objects);
    let reference = journal
        .operation()
        .creation
        .control_ref(objects, Some(old_reference.head_object().clone()));
    resign_merge_journal_with_reference(db, storage, signer, journal, reference, |_| {}).await;
}

async fn resign_merge_journal_with_reference(
    db: &Database,
    storage: &dyn SyncStorage,
    signer: &UserKeypair,
    journal: &mut CircleOperationJournal,
    reference: crate::sync::store_commit::CircleControlRef,
    mutate_commit: impl FnOnce(&mut StoreBatchCommit),
) {
    let old_commit = journal.commit().expect("parse prepared Circle commit");
    let author = crate::sync::store::database::StoreDatabase::new(db)
        .activated_store_device_registration(old_commit.author_registration.clone())
        .await
        .expect("load Circle commit author");
    let device_signer = author
        .device_signer(signer)
        .expect("derive Circle device signer");
    let coord = journal.operation().commit_ref.coord.clone();
    let stream_activations = old_commit.stream_activations().to_vec();
    let mut commit = signed_circle_commit(
        old_commit.store_root_hash,
        old_commit.write_id.clone(),
        coord.clone(),
        old_commit.author_registration.clone(),
        &author,
        old_commit.order.clone(),
        old_commit.membership_state.clone(),
        old_commit.device_state.clone(),
        old_commit
            .operations_membership_authority()
            .expect("prepared Circle commit carries validated operations"),
        reference,
        stream_activations,
        &device_signer,
    )
    .expect("re-sign Circle commit with substituted exact graph");
    mutate_commit(&mut commit);
    commit.signature = keys::sign_hex(&device_signer, &commit.canonical_signed_bytes()).1;
    let StoreCommitCoord { stream_id, .. } = coord.clone();
    let commit_prepared = prepare_circle_object(
        storage,
        &ProtocolObjectContext::signed_plaintext(
            commit.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        ),
        &commit_semantic_prefix(
            commit.candidate_family(),
            &stream_id.to_string(),
            commit.seq(),
            commit.commit_hash(),
        ),
        ".json",
        commit.to_bytes(),
    )
    .await
    .expect("prepare substituted Circle commit");
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .expect("bind substituted Circle commit");
    let policy = &journal.operation().policy;
    let old_head = &policy.head;
    let history_summary = &policy.history_summary;
    let history_summary = history_summary.clone();
    let head = StoreDeviceHead::signed(
        commit.store_root_hash,
        commit.author_registration.clone(),
        commit_ref.clone(),
        history_summary.digest(),
        old_head.successor.clone(),
        &device_signer,
    )
    .expect("sign substituted Circle Store head");
    let head_slot = journal
        .operation()
        .prepared_objects
        .get("store-head")
        .expect("Circle operation carries a Store head")
        .reference()
        .slot()
        .clone();
    let head_prepared = prepare_circle_object_at(
        storage,
        &ProtocolObjectContext::signed_plaintext(
            commit.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        ),
        head_slot,
        &head_slot_prefix(
            &commit.author_registration.device_id.to_string(),
            commit.seq(),
        ),
        head.to_bytes(),
    )
    .expect("prepare substituted Circle Store head");
    let operation = journal.operation_mut();
    operation.commit_bytes = commit.to_bytes();
    operation.commit_ref = commit_ref;
    operation
        .prepared_objects
        .insert("store-commit".to_string(), commit_prepared);
    operation
        .prepared_objects
        .insert("store-head".to_string(), head_prepared);
    operation.policy = CircleOperationPolicy {
        head,
        history_summary,
    };
    operation.uploaded.clear();
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
    access.leaf.value.disposition = CircleAccessDisposition::Active {
        keyring: creation.keyring.clone(),
        key_fingerprint: creation.control.value.key_fingerprint(),
        roster: creation.control.value.roster_state_ref(),
        bootstrap: None,
    };
    access.leaf.value.signature = keys::sign_hex(owner, &access.leaf.value.canonical_bytes()).1;
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
    let (access_root, proofs) = crate::sync::circle_control::merkle_root_and_proofs(&leaf_hashes);
    creation
        .control
        .value
        .value
        .state
        .active_epoch_mut()
        .expect("test transition has an active epoch")
        .common
        .access_root = access_root;
    creation.control.value.signature =
        keys::sign_hex(owner, &creation.control.value.canonical_bytes()).1;
    creation.control.coord = creation.control.value.coord();
    creation.control.bytes =
        serde_json::to_vec(&creation.control.value).expect("serialize promoted control");
    for (access, proof) in creation.access.iter_mut().zip(proofs) {
        access.envelope.control_hash = creation.control.coord.control_hash();
        access.envelope.leaf_hash = access.leaf.leaf_hash;
        access.envelope.value_hash = ObjectHash::digest(
            &serde_json::to_vec(&access.leaf.value).expect("serialize access leaf value"),
        );
        access.envelope.proof = proof;
        access.envelope.signature = keys::sign_hex(owner, &access.envelope.canonical_bytes()).1;
    }
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

async fn activation_count(db: &Database, circle_id: CircleId) -> i64 {
    let circle_id = circle_id.to_string();
    db.call(move |conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
            [circle_id],
            |row| row.get(0),
        )
        .map_err(DbError::from)
    })
    .await
    .expect("count circle activations")
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
