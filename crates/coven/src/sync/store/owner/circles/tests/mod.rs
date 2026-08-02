use std::sync::Arc;

use super::*;
use crate::database::StoreDatabase;
use crate::database::{Database, DbError};
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};
use crate::protocol::circle::{
    circle_semantic_prefix, CircleAccessDisposition, CircleId, CircleOperationId,
    CircleOperationKind, CircleOperationState, CircleRole, CircleRosterDraftPolicy,
    CircleSemanticSlot, CircleTransitionDraft, CircleTransitionDraftPolicy,
    CircleTransitionPolicyObjects, PreparedCircleTransition,
};
use crate::protocol::membership::MemberRole;
use crate::protocol::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, CircleAccessEnvelopeObjectRef,
    CircleAccessLeafObjectRef, CircleAccessObjectRef, CircleActivationObjects, GrantStreamAnchor,
    ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StreamActivation,
};
use crate::storage::cloud::CloudHome;
use crate::storage::{CloudCipher, CloudCipherAccess};
use crate::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use crate::sync::test_helpers::{
    open_test_db, temp_store_dir, test_migrations, test_synced_tables, TestCustody, TestStore,
};

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

async fn persist_merge_operation(
    db: &Database,
    name: &str,
) -> (TestStore, UserKeypair, CircleOperationJournal) {
    let signer = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(db, name, &signer).await;
    let journal = store
        .bind_device(db, &signer)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-creator", "Household")
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
    store: &TestStore,
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
    let device = store
        .bind_device(db, signer)
        .await
        .expect("bind substituted Circle object Store");
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize substituted Circle object Store");
    writer
        .circles()
        .resign_merge_journal_with_reference_for_test(journal, reference, |_| {})
        .await
        .expect("re-sign Circle commit with substituted exact graph");
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
    let (access_root, proofs) =
        crate::protocol::circle_control::merkle_root_and_proofs(&leaf_hashes);
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
