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
    StoreDeviceRegistrationRef,
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
            StoreOperationMembershipAuthority {
                predecessor: crate::membership::MembershipGrantCreationAuthority::Entry(
                    founder.coord(),
                ),
            },
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
    (identity, author.reference().clone(), commit, device_signer)
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
        generation: 1,
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
        registration,
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
