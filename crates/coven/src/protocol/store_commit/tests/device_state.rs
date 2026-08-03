use super::*;
use crate::protocol::store_commit::device_state::merge_device_status;

fn merge_cut_reference(
    stream_byte: u8,
    sequence: u64,
    identity_byte: u8,
) -> (AuthorStreamId, StoreBatchCommitRef) {
    let stream = AuthorStreamId::from_bytes([stream_byte; 32]);
    (
        stream,
        StoreBatchCommitRef {
            coord: StoreCommitCoord {
                stream_id: stream,
                sequence,
            },
            commit_hash: ObjectHash::digest(&[identity_byte]),
            object: exact(
                format!("test/terminal-cut/{stream_byte}/{sequence}/{identity_byte}.json"),
                &[identity_byte],
            ),
        },
    )
}

fn terminal_ref(fixture: &Fixture, identity_byte: u8) -> StoreDeviceExclusionRef {
    let proposal_id =
        StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(&[identity_byte]));
    StoreDeviceExclusionRef {
        proposal: StoreDeviceExclusionProposalRef {
            proposal_id,
            target: fixture.registration_ref.clone(),
            proposal_hash: ObjectHash::digest(&[identity_byte, 1]),
            object: exact(
                format!("test/terminal-proposal/{identity_byte}.json"),
                &[identity_byte, 1],
            ),
        },
        outcome_hash: ObjectHash::digest(&[identity_byte, 2]),
        object: exact(
            format!("test/terminal/{identity_byte}.json"),
            &[identity_byte, 2],
        ),
    }
}

fn inactive_status(
    terminals: Vec<StoreDeviceExclusionRef>,
    cut: impl IntoIterator<Item = (AuthorStreamId, StoreBatchCommitRef)>,
) -> StoreDeviceStatus {
    StoreDeviceStatus::Inactive {
        terminals,
        accepted_cut: StoreHistoryCut(cut.into_iter().collect()),
    }
}

#[test]
fn concurrent_terminal_states_union_terminals_and_intersect_cuts_in_both_orders() {
    let fixture = fixture();
    let (stream_a, a3) = merge_cut_reference(1, 3, 31);
    let (_, a5) = merge_cut_reference(1, 5, 51);
    let (stream_b, b4) = merge_cut_reference(2, 4, 42);
    let left_terminal = terminal_ref(&fixture, 1);
    let right_terminal = terminal_ref(&fixture, 2);
    let left = inactive_status(
        vec![left_terminal.clone()],
        [(stream_a, a5), (stream_b, b4.clone())],
    );
    let right = inactive_status(vec![right_terminal.clone()], [(stream_a, a3.clone())]);
    let expected = inactive_status(vec![left_terminal, right_terminal], [(stream_a, a3)]);

    assert_eq!(
        merge_device_status(left.clone(), right.clone()).unwrap(),
        expected
    );
    assert_eq!(merge_device_status(right, left).unwrap(), expected);
}

#[test]
fn concurrent_terminal_cut_rejects_different_refs_at_the_same_coordinate() {
    let fixture = fixture();
    let (stream, left) = merge_cut_reference(1, 3, 31);
    let (_, right) = merge_cut_reference(1, 3, 32);
    let terminal = terminal_ref(&fixture, 1);

    assert_eq!(
        merge_device_status(
            inactive_status(vec![terminal.clone()], [(stream, left)]),
            inactive_status(vec![terminal], [(stream, right)]),
        ),
        Err(StoreProtocolError::DeviceStateMismatch)
    );
}

#[test]
fn concurrent_terminal_cut_intersection_is_associative_and_idempotent() {
    let fixture = fixture();
    let terminal = terminal_ref(&fixture, 1);
    let (stream_a, a2) = merge_cut_reference(1, 2, 21);
    let (_, a3) = merge_cut_reference(1, 3, 31);
    let (_, a4) = merge_cut_reference(1, 4, 41);
    let (stream_b, b1) = merge_cut_reference(2, 1, 12);
    let (_, b2) = merge_cut_reference(2, 2, 22);
    let left = inactive_status(
        vec![terminal.clone()],
        [(stream_a, a4), (stream_b, b2.clone())],
    );
    let middle = inactive_status(
        vec![terminal.clone()],
        [(stream_a, a3), (stream_b, b1.clone())],
    );
    let right = inactive_status(vec![terminal], [(stream_a, a2)]);

    assert_eq!(
        merge_device_status(
            merge_device_status(left.clone(), middle.clone()).unwrap(),
            right.clone(),
        )
        .unwrap(),
        merge_device_status(left.clone(), merge_device_status(middle, right).unwrap()).unwrap()
    );
    assert_eq!(
        merge_device_status(left.clone(), left.clone()).unwrap(),
        left
    );
}

#[test]
fn acknowledgement_cut_join_remains_componentwise_maximum() {
    let (stream_a, a2) = merge_cut_reference(1, 2, 21);
    let (_, a4) = merge_cut_reference(1, 4, 41);
    let (stream_b, b1) = merge_cut_reference(2, 1, 12);
    let joined = StoreHistoryCut(BTreeMap::from([(stream_a, a2)]))
        .join(StoreHistoryCut(BTreeMap::from([
            (stream_a, a4.clone()),
            (stream_b, b1.clone()),
        ])))
        .unwrap();

    assert_eq!(
        joined,
        StoreHistoryCut(BTreeMap::from([(stream_a, a4), (stream_b, b1),]))
    );
}

#[test]
fn device_exclusion_objects_drive_the_exact_pending_and_terminal_states() {
    let fixture = fixture();
    let resolved = ResolvedStoreDeviceState::founder(
        &fixture.root_ref,
        fixture.registration_ref.clone(),
        &fixture.root.descriptor.founder_pubkey,
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.root.descriptor.founder_recovery,
    )
    .expect("founder device state");
    let predecessor = fixture.commit.device_state.clone();
    let proposal_id =
        StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(b"device exclusion proposal"));
    let outcome_key = format!(
        "{}.json",
        device_exclusion_outcome_semantic_prefix(fixture.registration_ref.device_id, proposal_id,)
    );
    let device_signer = fixture
        .registration
        .device_signer(&fixture.signer)
        .expect("founder device signer");
    let proposal = StoreDeviceExclusionProposal::signed(
        fixture.root_ref.store_root_hash,
        proposal_id,
        fixture.registration_ref.clone(),
        &fixture.registration,
        predecessor.clone(),
        slot(outcome_key.clone()),
        fixture.registration_ref.clone(),
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.registration,
        &device_signer,
    )
    .expect("sign exclusion proposal");
    let proposal_bytes = proposal.to_bytes();
    let proposal_ref = StoreDeviceExclusionProposalRef::from_proposal(
        &proposal,
        exact(
            format!(
                "{}.json",
                device_exclusion_proposal_semantic_prefix(
                    fixture.registration_ref.device_id,
                    proposal_id,
                    proposal.proposal_hash(),
                )
            ),
            &proposal_bytes,
        ),
    )
    .expect("exact exclusion proposal ref");
    let parsed = StoreDeviceExclusionProposal::parse_at(
        &proposal_bytes,
        &proposal_ref,
        &fixture.registration,
        &fixture.registration,
    )
    .expect("parse exclusion proposal");
    assert_eq!(parsed, proposal);

    let pending = resolved
        .propose_exclusion(proposal_ref.clone(), &proposal, &predecessor)
        .expect("activate exclusion proposal");
    assert!(device_state_has_exact_pending_proposal(
        &pending,
        &proposal_ref
    ));

    let cancellation = StoreDeviceExclusionCancellation::signed(
        proposal_ref.clone(),
        &proposal,
        fixture.registration_ref.clone(),
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.registration,
        &device_signer,
    )
    .expect("sign exclusion cancellation");
    let cancellation_value = StoreDeviceExclusionOutcome::Cancelled(cancellation);
    let cancellation_bytes = cancellation_value.to_bytes();
    let cancellation_ref = StoreDeviceExclusionOutcomeRef::from_outcome(
        &cancellation_value,
        &proposal,
        exact(outcome_key.clone(), &cancellation_bytes),
    )
    .expect("exact exclusion cancellation ref");
    let parsed = StoreDeviceExclusionOutcome::parse_at(
        &cancellation_bytes,
        &cancellation_ref,
        &proposal,
        &fixture.registration,
        &fixture.registration,
    )
    .expect("parse exclusion cancellation");
    assert_eq!(parsed, cancellation_value);
    let StoreDeviceExclusionOutcomeRef::Cancelled(cancellation_ref) = cancellation_ref else {
        panic!("cancellation ref changed variant")
    };
    let cancelled = pending
        .cancel_exclusion(cancellation_ref.clone())
        .expect("activate exclusion cancellation");
    assert!(matches!(
        cancelled
            .devices
            .get(&fixture.registration_ref.device_id)
            .and_then(|record| record.proposals.get(&proposal_id)),
        Some(StoreDeviceProposalState::Cancelled { outcome }) if outcome == &cancellation_ref
    ));

    let exclusion = StoreDeviceExclusion::signed(
        proposal_ref.clone(),
        &proposal,
        fixture.registration_ref.clone(),
        &fixture.registration,
        StoreDeviceExclusionProof {
            frozen_device_state: proposal.frozen_device_state.clone(),
            remaining_device_acks: Vec::new(),
            cutoff: fixture
                .commit
                .order
                .predecessor_cut()
                .expect("derive exclusion cutoff"),
        },
        fixture.registration_ref.clone(),
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.registration,
        &device_signer,
    )
    .expect("sign device exclusion");
    let exclusion_value = StoreDeviceExclusionOutcome::Excluded(exclusion);
    let exclusion_bytes = exclusion_value.to_bytes();
    let exclusion_ref = StoreDeviceExclusionOutcomeRef::from_outcome(
        &exclusion_value,
        &proposal,
        exact(outcome_key, &exclusion_bytes),
    )
    .expect("exact exclusion ref");
    let StoreDeviceExclusionOutcomeRef::Excluded(exclusion_ref) = exclusion_ref else {
        panic!("exclusion ref changed variant")
    };
    let accepted_cut = fixture
        .commit
        .order
        .predecessor_cut()
        .expect("derive predecessor cut");
    let excluded = pending
        .exclude(exclusion_ref.clone(), accepted_cut.clone())
        .expect("activate device exclusion");
    assert!(matches!(
        &excluded
            .devices
            .get(&fixture.registration_ref.device_id)
            .expect("excluded record")
            .status,
        StoreDeviceStatus::Inactive { terminals, accepted_cut: cut }
            if terminals == &vec![exclusion_ref]
                && cut == &accepted_cut
    ));
}

#[test]
fn retained_registration_activations_reopen_exact_canonical_inputs() {
    let fixture = fixture();
    let replacement = UserKeypair::generate();
    let recovery_id = DeviceRecoveryId::from_hash(ObjectHash::digest(b"retained recovery"));
    let recovery_slot = slot("store-v1/recovery/retained/1.json".to_string());
    let replacement_registration = StoreDeviceRegistration::signed(
        fixture.root_ref.clone(),
        StoreDeviceRegistrationOrigin::Recovery {
            recovery_id,
            recovery_slot: recovery_slot.clone(),
            owner_grant: fixture.root.descriptor.founder_grant.clone(),
        },
        fixture.registration.provider.clone(),
        DeviceStreamAnchor::StoreAnnouncements {
            first_slot: slot("store-v1/announcements/retained/1.json".to_string()),
        },
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot("store-v1/acks/retained/1.json".to_string()),
        },
        DeviceStreamAnchor::StoreSnapshots {
            first_slot: slot("store-v1/snapshots/retained/1.json".to_string()),
        },
        &replacement,
    )
    .expect("sign replacement registration");
    let replacement_bytes = replacement_registration.to_bytes();
    let replacement_ref = StoreDeviceRegistrationRef::from_registration(
        &replacement_registration,
        exact(
            format!(
                "{}.json",
                registration_semantic_prefix(&replacement_registration.device_id.to_string())
            ),
            &replacement_bytes,
        ),
    );
    let recovery_node = OwnerRecoveryNodeRef {
        owner_pubkey: fixture.registration.author_pubkey.clone(),
        owner_grant: fixture.root.descriptor.founder_grant.clone(),
        sequence: 1,
        node_hash: ObjectHash::digest(b"retained recovery node"),
        object: exact(
            recovery_slot.logical_key().to_string(),
            b"retained recovery node",
        ),
    };
    let activated = ActivatedStoreDeviceRegistrationRef {
        registration: replacement_ref,
        authority: StoreDeviceRegistrationActivationRef::Recovery {
            recovery_id,
            node: recovery_node.clone(),
        },
    };
    let authority = StoreDeviceRegistrationActivation::Recovery {
        recovery_id,
        node: recovery_node,
    };
    let device_signer = fixture
        .registration
        .device_signer(&fixture.signer)
        .expect("founder device signer");
    let commit = StoreBatchCommit::signed_with_registrations(
        fixture.root_ref.store_root_hash,
        WriteId::from_generated("retained-registration".to_string()),
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.commit.order.clone(),
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        fixture
            .commit
            .operations_membership_authority()
            .expect("fixture carries membership authority"),
        vec![activated.clone()],
        &device_signer,
    )
    .expect("sign registration activation commit");
    let activated_value = ActivatedStoreDeviceRegistration::verified(
        ReferencedStoreDeviceRegistration::verified(
            activated.registration.clone(),
            replacement_registration.clone(),
        )
        .expect("verify exact replacement registration"),
        authority.clone(),
    )
    .expect("verify replacement activation");
    activated_value
        .verify_reference(&activated)
        .expect("verify exact replacement activation reference");
    let input = vec![activated_value];
    let retained = RetainedStoreDeviceRegistrationActivations::from_verified(
        &fixture.root_ref,
        &commit,
        &input,
    )
    .expect("retain registration activation");
    let encoded = serde_json::to_vec(&retained).expect("encode retained registration");
    let decoded: RetainedStoreDeviceRegistrationActivations =
        serde_json::from_slice(&encoded).expect("decode retained registration");
    assert_eq!(
        decoded
            .verify_for(&fixture.root_ref, &commit)
            .expect("verify retained registration"),
        input
    );

    let mut tampered = serde_json::to_value(&retained).expect("encode retained registration");
    tampered["registrations"][0]["canonical_registration"]
        .as_array_mut()
        .expect("canonical registration bytes")
        .push(serde_json::Value::from(b' '));
    let tampered: RetainedStoreDeviceRegistrationActivations =
        serde_json::from_value(tampered).expect("decode tampered retained registration");
    assert!(tampered.verify_for(&fixture.root_ref, &commit).is_err());

    let missing: RetainedStoreDeviceRegistrationActivations =
        serde_json::from_value(serde_json::json!({ "registrations": [] }))
            .expect("decode missing retained registration");
    assert!(missing.verify_for(&fixture.root_ref, &commit).is_err());

    let mut substituted = serde_json::to_value(&retained).expect("encode retained registration");
    substituted["registrations"][0]["canonical_registration"] =
        serde_json::to_value(fixture.registration.to_bytes()).expect("encode registration bytes");
    let substituted: RetainedStoreDeviceRegistrationActivations =
        serde_json::from_value(substituted).expect("decode substituted retained registration");
    assert!(substituted.verify_for(&fixture.root_ref, &commit).is_err());
}

#[test]
fn retained_device_operations_reopen_sources_and_derive_the_accepted_cut() {
    let fixture = fixture();
    let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
        b"retained exclusion proposal",
    ));
    let outcome_key = format!(
        "{}.json",
        device_exclusion_outcome_semantic_prefix(fixture.registration_ref.device_id, proposal_id,)
    );
    let device_signer = fixture
        .registration
        .device_signer(&fixture.signer)
        .expect("founder device signer");
    let proposal = StoreDeviceExclusionProposal::signed(
        fixture.root_ref.store_root_hash,
        proposal_id,
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.commit.device_state.clone(),
        slot(outcome_key.clone()),
        fixture.registration_ref.clone(),
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.registration,
        &device_signer,
    )
    .expect("sign retained exclusion proposal");
    let proposal_bytes = proposal.to_bytes();
    let proposal_ref = StoreDeviceExclusionProposalRef::from_proposal(
        &proposal,
        exact(
            format!(
                "{}.json",
                device_exclusion_proposal_semantic_prefix(
                    fixture.registration_ref.device_id,
                    proposal_id,
                    proposal.proposal_hash(),
                )
            ),
            &proposal_bytes,
        ),
    )
    .expect("exact retained exclusion proposal");
    let proposal_source = RetainedStoreDeviceExclusionProposal::from_exact(
        proposal_ref.clone(),
        &proposal,
        &fixture.registration,
        &fixture.registration,
    )
    .expect("retain exclusion proposal");
    let proposal_commit = StoreBatchCommit::signed_with_device_exclusions(
        fixture.root_ref.store_root_hash,
        WriteId::from_generated("retained-proposal".to_string()),
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.commit.order.clone(),
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        fixture
            .commit
            .operations_membership_authority()
            .expect("fixture carries membership authority"),
        vec![proposal_ref.clone()],
        Vec::new(),
        &device_signer,
    )
    .expect("sign retained proposal commit");
    let retained_proposal =
        RetainedStoreDeviceOperations::from_sources(vec![proposal_source.clone()], Vec::new());
    let verified_proposal = retained_proposal
        .verify_for(&fixture.root_ref, &proposal_commit)
        .expect("verify retained proposal input");
    assert_eq!(
        verified_proposal
            .proposals()
            .next()
            .map(|(reference, value)| (reference.clone(), value.clone())),
        Some((proposal_ref.clone(), proposal.clone()))
    );
    let mut tampered_proposal =
        serde_json::to_value(&retained_proposal).expect("encode retained proposal");
    tampered_proposal["proposals"][0]["canonical_proposal"]
        .as_array_mut()
        .expect("canonical proposal bytes")
        .push(serde_json::Value::from(b' '));
    let tampered_proposal: RetainedStoreDeviceOperations =
        serde_json::from_value(tampered_proposal).expect("decode tampered retained proposal");
    assert!(tampered_proposal
        .verify_for(&fixture.root_ref, &proposal_commit)
        .is_err());
    let exclusion = StoreDeviceExclusion::signed(
        proposal_ref,
        &proposal,
        fixture.registration_ref.clone(),
        &fixture.registration,
        StoreDeviceExclusionProof {
            frozen_device_state: proposal.frozen_device_state.clone(),
            remaining_device_acks: Vec::new(),
            cutoff: fixture
                .commit
                .order
                .predecessor_cut()
                .expect("derive exclusion cutoff"),
        },
        fixture.registration_ref.clone(),
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.registration,
        &device_signer,
    )
    .expect("sign retained exclusion outcome");
    let outcome = StoreDeviceExclusionOutcome::Excluded(exclusion);
    let outcome_bytes = outcome.to_bytes();
    let outcome_ref = StoreDeviceExclusionOutcomeRef::from_outcome(
        &outcome,
        &proposal,
        exact(outcome_key, &outcome_bytes),
    )
    .expect("exact retained exclusion outcome");
    let outcome_source = RetainedStoreDeviceExclusionOutcome::from_exact(
        &outcome_ref,
        proposal_source,
        &outcome,
        &fixture.registration,
    )
    .expect("retain exclusion outcome");
    let commit = StoreBatchCommit::signed_with_device_exclusions(
        fixture.root_ref.store_root_hash,
        WriteId::from_generated("retained-exclusion".to_string()),
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.commit.order.clone(),
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        fixture
            .commit
            .operations_membership_authority()
            .expect("fixture carries membership authority"),
        Vec::new(),
        vec![outcome_ref],
        &device_signer,
    )
    .expect("sign retained exclusion commit");
    let retained = RetainedStoreDeviceOperations::from_sources(Vec::new(), vec![outcome_source]);
    let encoded = serde_json::to_vec(&retained).expect("encode retained device operations");
    let decoded: RetainedStoreDeviceOperations =
        serde_json::from_slice(&encoded).expect("decode retained device operations");
    let verified = decoded
        .verify_for(&fixture.root_ref, &commit)
        .expect("verify retained device operations");
    assert_eq!(verified.to_retained(), retained);
    assert_eq!(
        verified.exclusions().next().map(|(_, cut)| cut.clone()),
        Some(
            commit
                .order
                .predecessor_cut()
                .expect("derive accepted predecessor cut")
        )
    );

    let mut tampered = serde_json::to_value(&retained).expect("encode retained operations");
    tampered["outcomes"][0]["excluded"]["canonical_outcome"]
        .as_array_mut()
        .expect("canonical outcome bytes")
        .push(serde_json::Value::from(b' '));
    let tampered: RetainedStoreDeviceOperations =
        serde_json::from_value(tampered).expect("decode tampered retained operations");
    assert!(tampered.verify_for(&fixture.root_ref, &commit).is_err());

    let missing = RetainedStoreDeviceOperations::from_sources(Vec::new(), Vec::new());
    assert!(missing.verify_for(&fixture.root_ref, &commit).is_err());

    let mut other_registration = fixture.registration.clone();
    other_registration.author_pubkey.push('0');
    let mut substituted = serde_json::to_value(&retained).expect("encode retained operations");
    substituted["outcomes"][0]["excluded"]["canonical_owner_registration"] =
        serde_json::to_value(other_registration.to_bytes()).expect("encode registration bytes");
    let substituted: RetainedStoreDeviceOperations =
        serde_json::from_value(substituted).expect("decode substituted retained operations");
    assert!(substituted.verify_for(&fixture.root_ref, &commit).is_err());
}

fn device_state_has_exact_pending_proposal(
    state: &ResolvedStoreDeviceState,
    expected: &StoreDeviceExclusionProposalRef,
) -> bool {
    state
            .devices
            .get(&expected.target.device_id)
            .and_then(|record| record.proposals.get(&expected.proposal_id))
            .is_some_and(|state| {
                matches!(state, StoreDeviceProposalState::Pending { proposal } if proposal == expected)
            })
}
