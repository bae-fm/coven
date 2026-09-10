use super::*;
use crate::circle_test_fixtures::{
    exact_logical_object, merge_device_authority, test_founder_entry,
};
use crate::objects::{ExactObjectRef, ObjectSlot};
use coven_keys::keys::{self, UserKeypair};
use std::collections::BTreeMap;

fn publication_ref(entry: &StorePublicationEntry) -> StorePublicationRef {
    let bytes = entry.to_bytes();
    StorePublicationRef::from_entry(
        entry,
        ExactObjectRef::new(
            ObjectSlot::logical(format!(
                "{}.json",
                store_publication_entry_semantic_prefix(entry)
            ))
            .expect("valid publication slot"),
            bytes.len() as u64,
            ObjectHash::digest(&bytes),
        ),
    )
    .expect("reference exact publication entry")
}

fn verified_fixture_commit() -> (
    UserKeypair,
    ReferencedStoreDeviceRegistration,
    VerifiedStoreBatchCommit,
    UserKeypair,
) {
    let identity = UserKeypair::generate();
    let root_hash = ObjectHash::digest(b"publication Store");
    let label = "publication-author";
    let author = merge_device_authority(&identity, root_hash, label);
    let founder_grant = crate::membership::MembershipGrantId::from_test_label(label);
    let recovery = GrantStreamAnchor::OwnerRecovery {
        first_slot: ObjectSlot::logical("store-v1/test/publication/recovery/1.json".to_string())
            .expect("valid recovery slot"),
    };
    let devices = ResolvedStoreDeviceState::founder(
        &author.registration().store_root,
        author.reference().clone(),
        &keys::public_key_hex(&identity),
        founder_grant.clone(),
        &recovery,
    )
    .expect("resolve founder devices");
    let device_state =
        StoreDeviceStateRef::from_resolved(CommitFrontier(BTreeMap::new()), &devices)
            .expect("reference founder devices");
    let founder = test_founder_entry(
        label,
        &identity,
        GrantStreamAnchor::StoreMembership {
            first_slot: ObjectSlot::logical(
                "store-v1/test/publication/membership/1.json".to_string(),
            )
            .expect("valid membership slot"),
        },
    );
    let membership = crate::membership::MembershipChain::from_entries(vec![founder.clone()])
        .expect("resolve founder membership");
    let membership_state =
        StoreMembershipStateRef::from_membership(&membership, devices.recovery.clone())
            .expect("reference founder membership");
    let acknowledgement_bytes = b"publication acknowledgement";
    let acknowledgement = StoreAckRef {
        registration: author.reference().clone(),
        sequence: 2,
        ack_hash: ObjectHash::digest(acknowledgement_bytes),
        object: exact_logical_object(
            format!(
                "{}.json",
                ack_slot_prefix(&author.reference().device_id.to_string(), 2)
            ),
            acknowledgement_bytes,
        ),
    };
    let coord = StoreCommitCoord {
        stream_id: author.stream_id(),
        sequence: 1,
    };
    let commit = author
        .sign_operations(
            root_hash,
            WriteId::from_generated("publication-write".to_string()),
            coord.clone(),
            StoreCommitOrder {
                seq: 1,
                predecessor: None,
                dependencies: BTreeMap::new(),
            },
            membership_state,
            device_state,
            founder.coord(),
            StoreCommitOperationsInput {
                acknowledgement: Some(acknowledgement),
                ..StoreCommitOperationsInput::empty()
            },
        )
        .expect("sign fixture commit");
    let commit_bytes = commit.to_bytes();
    let commit_ref = StoreBatchCommitRef::from_commit(
        &commit,
        coord,
        exact_logical_object(
            format!(
                "{}.json",
                commit_semantic_prefix(
                    commit.candidate_family(),
                    &author.stream_id().to_string(),
                    commit.seq(),
                    commit.commit_hash(),
                )
            ),
            &commit_bytes,
        ),
    )
    .expect("reference fixture commit");
    let commit = VerifiedStoreBatchCommit::parse(
        &commit_bytes,
        root_hash,
        &commit_ref,
        author.registration(),
    )
    .expect("verify fixture commit");
    let device_signer = author
        .registration()
        .device_signer(&identity)
        .expect("derive fixture device signer");
    let author = ReferencedStoreDeviceRegistration::verified(
        author.reference().clone(),
        author.registration().clone(),
    )
    .expect("reference fixture author");
    (identity, author, commit, device_signer)
}

#[test]
fn current_record_accepts_exactly_one_successor_of_its_accepted_boundary() {
    let (identity, _, commit, device_signer) = verified_fixture_commit();
    let root = commit.store_root_hash();
    let current = StoreCurrentPublicationRecord::genesis(root, &identity);
    let entry = StorePublicationEntry::signed_commit(&current, &commit, &device_signer)
        .expect("sign first publication entry");
    let entry_bytes = entry.to_bytes();
    let entry_ref = publication_ref(&entry);

    let accepted = StoreCurrentPublicationRecord::advance_commit(
        &current,
        &entry,
        entry_ref.clone(),
        &commit,
        &device_signer,
    )
    .expect("advance current record");
    accepted
        .verify_commit_transition(
            &current,
            &entry,
            &entry_ref,
            &commit,
            &keys::public_key_hex(&device_signer),
        )
        .expect("verify accepted transition");
    accepted
        .verify_accepted_commit(
            &entry,
            &entry_ref,
            &commit,
            &keys::public_key_hex(&device_signer),
        )
        .expect("verify accepted record directly");
    assert_eq!(accepted.accepted(), Some(&entry_ref));
    assert_eq!(accepted.latest_snapshot(), None);

    let stale = StorePublicationEntry::signed_commit(&current, &commit, &device_signer)
        .expect("sign stale publication entry");
    let stale_ref = publication_ref(&stale);
    assert!(StoreCurrentPublicationRecord::advance_commit(
        &accepted,
        &stale,
        stale_ref,
        &commit,
        &device_signer,
    )
    .is_err());

    StorePublicationEntry::parse_at(
        &entry_bytes,
        root,
        &entry_ref,
        &keys::public_key_hex(&device_signer),
    )
    .expect("parse exact publication entry");
}

#[test]
fn publication_wire_has_one_source_for_each_boundary_fact() {
    let (identity, _, commit, device_signer) = verified_fixture_commit();
    let current = StoreCurrentPublicationRecord::genesis(commit.store_root_hash(), &identity);
    let entry = StorePublicationEntry::signed_commit(&current, &commit, &device_signer)
        .expect("sign publication entry");
    let reference = publication_ref(&entry);
    let accepted = StoreCurrentPublicationRecord::advance_commit(
        &current,
        &entry,
        reference,
        &commit,
        &device_signer,
    )
    .expect("accept publication entry");
    let entry_json: serde_json::Value =
        serde_json::from_slice(&entry.to_bytes()).expect("decode publication entry JSON");
    let current_json: serde_json::Value =
        serde_json::from_slice(&accepted.to_bytes()).expect("decode current publication JSON");

    assert!(entry_json["body"].get("previous_state_hash").is_some());
    assert!(current_json["body"].get("publisher").is_none());
    assert!(current_json["body"].get("previous_state_hash").is_none());
    assert!(current_json["body"]["state"]["accepted"]
        .get("previous_state_hash")
        .is_none());
}

#[test]
fn publication_reference_rejects_another_protocol_slot() {
    let (identity, _, commit, device_signer) = verified_fixture_commit();
    let current = StoreCurrentPublicationRecord::genesis(commit.store_root_hash(), &identity);
    let entry = StorePublicationEntry::signed_commit(&current, &commit, &device_signer)
        .expect("sign publication entry");
    let bytes = entry.to_bytes();
    let wrong = ExactObjectRef::new(
        ObjectSlot::logical("store-v1/commits/not-a-publication.json".to_string())
            .expect("valid wrong slot"),
        bytes.len() as u64,
        ObjectHash::digest(&bytes),
    );

    assert!(StorePublicationRef::from_entry(&entry, wrong).is_err());
}

#[test]
fn commit_signed_at_genesis_cannot_cross_an_accepted_snapshot() {
    let (identity, registration, commit, device_signer) = verified_fixture_commit();
    let current = StoreCurrentPublicationRecord::genesis(commit.store_root_hash(), &identity);
    let commit_entry = StorePublicationEntry::signed_commit(&current, &commit, &device_signer)
        .expect("sign commit publication");
    let commit_ref = publication_ref(&commit_entry);
    let accepted_commit = StoreCurrentPublicationRecord::advance_commit(
        &current,
        &commit_entry,
        commit_ref,
        &commit,
        &device_signer,
    )
    .expect("accept commit publication");
    let snapshot_bytes = b"snapshot metadata";
    let snapshot = StoreSnapshotRef {
        snapshot_hash: ObjectHash::digest(snapshot_bytes),
        object: ExactObjectRef::new(
            ObjectSlot::logical("store-v1/test/snapshot.json".to_string())
                .expect("valid snapshot slot"),
            snapshot_bytes.len() as u64,
            ObjectHash::digest(snapshot_bytes),
        ),
    };
    let snapshot_entry = StorePublicationEntry::signed_snapshot(
        &accepted_commit,
        registration.reference().clone(),
        snapshot,
        &device_signer,
    )
    .expect("sign snapshot publication");
    let snapshot_ref = publication_ref(&snapshot_entry);
    let accepted_snapshot = StoreCurrentPublicationRecord::advance_snapshot(
        &accepted_commit,
        &snapshot_entry,
        snapshot_ref,
        &device_signer,
    )
    .expect("accept snapshot publication");

    assert!(
        StorePublicationEntry::signed_commit(&accepted_snapshot, &commit, &device_signer).is_err()
    );
}

#[test]
fn a_received_publication_cannot_accept_a_commit_from_before_its_snapshot_base() {
    assert_received_commit_cannot_cross_snapshot(false);
}

#[test]
fn an_earlier_receipt_cannot_validate_a_repeated_commit_after_a_snapshot() {
    assert_received_commit_cannot_cross_snapshot(true);
}

fn assert_received_commit_cannot_cross_snapshot(publish_before_snapshot: bool) {
    let (identity, author, commit, device_signer) = verified_fixture_commit();
    let genesis = StoreCurrentPublicationRecord::genesis(commit.store_root_hash(), &identity);
    let mut entries = Vec::new();
    let mut current = genesis.clone();
    if publish_before_snapshot {
        let entry = StorePublicationEntry::signed_commit(&current, &commit, &device_signer)
            .expect("sign commit before snapshot");
        let reference = publication_ref(&entry);
        current = StoreCurrentPublicationRecord::advance_commit(
            &current,
            &entry,
            reference.clone(),
            &commit,
            &device_signer,
        )
        .expect("accept commit before snapshot");
        entries.push(StorePublicationIntervalEntry::new(
            entry,
            reference,
            author.clone(),
        ));
    }
    let snapshot_bytes = b"snapshot preceding a delayed commit";
    let snapshot = StoreSnapshotRef {
        snapshot_hash: ObjectHash::digest(snapshot_bytes),
        object: exact_logical_object(
            "store-v1/test/publication/delayed-commit-snapshot.json".to_string(),
            snapshot_bytes,
        ),
    };
    let snapshot_entry = StorePublicationEntry::signed_snapshot(
        &current,
        author.reference().clone(),
        snapshot,
        &device_signer,
    )
    .expect("sign snapshot publication");
    let snapshot_ref = publication_ref(&snapshot_entry);
    let accepted_snapshot = StoreCurrentPublicationRecord::advance_snapshot(
        &current,
        &snapshot_entry,
        snapshot_ref.clone(),
        &device_signer,
    )
    .expect("accept snapshot");
    // Bypass the outgoing constructor to exercise validation of received bytes.
    let stale_entry = Signed::sign(
        StorePublicationEntryBody {
            store_root_hash: commit.store_root_hash(),
            position: accepted_snapshot.next_position().expect("next publication"),
            predecessor: accepted_snapshot.accepted().cloned(),
            previous_state_hash: accepted_snapshot.state_hash(),
            author_registration: author.reference().clone(),
            payload: StorePublicationPayload::Commit(commit.reference().clone()),
        },
        &device_signer,
    );
    let stale_ref = publication_ref(&stale_entry);
    let received_current = Signed::sign(
        StoreCurrentPublicationRecordBody {
            store_root_hash: commit.store_root_hash(),
            state: StorePublicationState::Accepted {
                entry: stale_ref.clone(),
                latest_snapshot: accepted_snapshot.latest_snapshot().cloned(),
            },
        },
        &device_signer,
    );
    entries.extend([
        StorePublicationIntervalEntry::new(snapshot_entry, snapshot_ref, author.clone()),
        StorePublicationIntervalEntry::new(stale_entry, stale_ref, author),
    ]);
    let receipt = VerifiedStorePublicationInterval::verified(genesis, received_current, entries)
        .and_then(|interval| interval.accepted_commit(&commit));
    assert!(
        receipt.is_err(),
        "the old commit must be rejected against the snapshot preceding its accepted entry"
    );
}

#[test]
fn publication_interval_verifies_every_entry_author_and_the_final_boundary() {
    let (identity, author, commit, device_signer) = verified_fixture_commit();
    let genesis = StoreCurrentPublicationRecord::genesis(commit.store_root_hash(), &identity);
    let commit_entry = StorePublicationEntry::signed_commit(&genesis, &commit, &device_signer)
        .expect("sign commit publication");
    let commit_ref = publication_ref(&commit_entry);
    let accepted_commit = StoreCurrentPublicationRecord::advance_commit(
        &genesis,
        &commit_entry,
        commit_ref.clone(),
        &commit,
        &device_signer,
    )
    .expect("accept commit publication");
    let snapshot_bytes = b"publication interval snapshot";
    let snapshot = StoreSnapshotRef {
        snapshot_hash: ObjectHash::digest(snapshot_bytes),
        object: exact_logical_object(
            "store-v1/test/publication/interval-snapshot.json".to_string(),
            snapshot_bytes,
        ),
    };
    let snapshot_entry = StorePublicationEntry::signed_snapshot(
        &accepted_commit,
        author.reference().clone(),
        snapshot,
        &device_signer,
    )
    .expect("sign snapshot publication");
    let snapshot_ref = publication_ref(&snapshot_entry);
    let current = StoreCurrentPublicationRecord::advance_snapshot(
        &accepted_commit,
        &snapshot_entry,
        snapshot_ref.clone(),
        &device_signer,
    )
    .expect("accept snapshot publication");

    let interval = VerifiedStorePublicationInterval::verified(
        genesis,
        current,
        vec![
            StorePublicationIntervalEntry::new(commit_entry, commit_ref, author.clone()),
            StorePublicationIntervalEntry::new(snapshot_entry, snapshot_ref, author),
        ],
    )
    .expect("verify publication interval");

    assert_eq!(interval.entries().len(), 2);
    assert_eq!(
        interval
            .accepted_commit(&commit)
            .unwrap()
            .reference()
            .position
            .get(),
        1
    );
}

#[test]
fn initial_publication_observation_authenticates_the_actual_genesis_record() {
    let founder = UserKeypair::generate();
    let founder_pubkey = keys::public_key_hex(&founder);
    let root_hash = ObjectHash::digest(b"initial publication root");
    let current = StoreCurrentPublicationRecord::genesis(root_hash, &founder);
    let interval = VerifiedStorePublicationInterval::from_genesis(
        root_hash,
        &founder_pubkey,
        current.clone(),
        Vec::new(),
    )
    .expect("observe genesis using only the founder public key");
    assert_eq!(interval.current(), &current);
    assert_eq!(interval.previous(), current.body());
    assert!(interval.entries().is_empty());

    let impostor = UserKeypair::generate();
    let forged = StoreCurrentPublicationRecord::genesis(root_hash, &impostor);
    assert!(VerifiedStorePublicationInterval::from_genesis(
        root_hash,
        &founder_pubkey,
        forged,
        Vec::new(),
    )
    .is_err());
    assert!(VerifiedStorePublicationInterval::from_genesis(
        ObjectHash::digest(b"another root"),
        &founder_pubkey,
        current,
        Vec::new(),
    )
    .is_err());
}

#[test]
fn initial_publication_observation_verifies_the_accepted_path_without_a_genesis_signature() {
    let (founder, author, commit, device_signer) = verified_fixture_commit();
    let root_hash = commit.store_root_hash();
    let genesis = StoreCurrentPublicationRecord::genesis(root_hash, &founder);
    let entry = StorePublicationEntry::signed_commit(&genesis, &commit, &device_signer)
        .expect("prepare accepted publication");
    let reference = publication_ref(&entry);
    let current = StoreCurrentPublicationRecord::advance_commit(
        &genesis,
        &entry,
        reference.clone(),
        &commit,
        &device_signer,
    )
    .expect("accept publication");
    let interval = VerifiedStorePublicationInterval::from_genesis(
        root_hash,
        &keys::public_key_hex(&founder),
        current.clone(),
        vec![StorePublicationIntervalEntry::new(entry, reference, author)],
    )
    .expect("read accepted history without holding the founder's signing key");
    assert_eq!(interval.current(), &current);
    interval
        .accepted_commit(&commit)
        .expect("authenticate accepted commit");
    assert!(VerifiedStorePublicationInterval::from_genesis(
        root_hash,
        &keys::public_key_hex(&founder),
        current,
        Vec::new(),
    )
    .is_err());
}
