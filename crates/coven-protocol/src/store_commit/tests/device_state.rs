use super::*;
use crate::store_commit::device_state::merge_device_status;

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

fn proposal_value(fixture: &Fixture, identity_byte: u8) -> StoreDeviceExclusionProposal {
    let proposal_id =
        StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(&[identity_byte]));
    proposal_for(fixture, proposal_id)
}

fn proposal_for(
    fixture: &Fixture,
    proposal_id: StoreDeviceExclusionProposalId,
) -> StoreDeviceExclusionProposal {
    StoreDeviceExclusionProposal {
        proposal_id,
        target: fixture.registration_ref.clone(),
        outcome_slot: slot(format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(
                fixture.registration_ref.device_id,
                proposal_id,
            )
        )),
    }
}

/// A Store control whose exact membership entry issues `proposal`, and that
/// entry. The entry is what a reader projects the proposal out of.
fn proposing_control(
    fixture: &Fixture,
    proposal: &StoreDeviceExclusionProposal,
) -> (MembershipEntry, StoreControl) {
    let store_id = fixture.root_ref.store_root_id.to_string();
    let author_pubkey = keys::public_key_hex(&fixture.signer);
    let entry: MembershipEntry = Signed::sign(
        crate::membership::MembershipEntryBody {
            store_id: store_id.clone(),
            author_pubkey: author_pubkey.clone(),
            author_owner_grant: fixture.root.descriptor.founder_grant.clone(),
            stream_id: AuthorStreamId::from_bytes([7; 32]),
            seq: 2,
            previous_hash: None,
            dependencies: Vec::new(),
            created_at: "0000000002000-0000-device-a".to_string(),
            change: StoreAuthorityChange::DeviceExclusionProposal {
                proposal: proposal.clone(),
            },
            provider_admin: None,
        },
        &fixture.signer,
    );
    let coord = entry.coord();
    let entry_bytes = serde_json::to_vec(&entry).expect("serialize proposing entry");
    let entry_ref = MembershipEntryRef {
        coord: coord.clone(),
        object: exact(
            format!(
                "{}.json",
                membership_entry_semantic_prefix(
                    &coord.author_pubkey,
                    &coord.author_owner_grant,
                    coord.stream_id,
                    coord.seq,
                    coord.entry_hash,
                )
            ),
            &entry_bytes,
        ),
    };
    let control = StoreControl {
        transition: crate::membership::MergeMembershipHeadTransition {
            body: crate::membership::MembershipHeadBody {
                author_registration: fixture.registration_ref.clone(),
                entry: entry_ref,
                predecessor: None,
                successor: SuccessorLink {
                    activation: StreamActivation::grant_authorized(
                        fixture.root_ref.store_root_hash,
                        fixture.registration_ref.clone(),
                        fixture.root.descriptor.founder_grant.clone(),
                        GrantStreamAnchor::StoreMembership {
                            first_slot: slot(
                                "store-v1/membership/heads/proposing/1.json".to_string(),
                            ),
                        },
                    )
                    .activation_id(),
                    predecessor: None,
                    next_slot: slot("store-v1/membership/heads/proposing/2.json".to_string()),
                },
            },
            head_slot: slot("store-v1/membership/heads/proposing/head.json".to_string()),
        },
    };
    (entry, control)
}

fn terminal_ref(fixture: &Fixture, identity_byte: u8) -> StoreDeviceExclusionRef {
    StoreDeviceExclusionRef {
        proposal: proposal_value(fixture, identity_byte),
        outcome_hash: ObjectHash::digest(&[identity_byte, 2]),
        object: exact(
            format!("test/terminal/{identity_byte}.json"),
            &[identity_byte, 2],
        ),
    }
}

#[test]
fn terminal_states_preserve_each_exact_exclusion_in_either_merge_order() {
    let fixture = fixture();
    let left_terminal = terminal_ref(&fixture, 1);
    let right_terminal = terminal_ref(&fixture, 2);
    let left = StoreDeviceStatus::Inactive {
        terminals: vec![left_terminal.clone()],
    };
    let right = StoreDeviceStatus::Inactive {
        terminals: vec![right_terminal.clone()],
    };
    let expected = StoreDeviceStatus::Inactive {
        terminals: vec![left_terminal, right_terminal],
    };
    assert_eq!(
        merge_device_status(left.clone(), right.clone()).unwrap(),
        expected
    );
    assert_eq!(merge_device_status(right, left.clone()).unwrap(), expected);
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
fn exclusion_proposals_and_outcomes_drive_the_exact_pending_and_terminal_states() {
    let fixture = fixture();
    let resolved = ResolvedStoreDeviceState::founder(
        &fixture.root_ref,
        fixture.registration_ref.clone(),
        &fixture.root.descriptor.founder_pubkey,
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.root.descriptor.founder_recovery,
    )
    .expect("founder device state");
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
    let proposal = proposal_for(&fixture, proposal_id);
    proposal.validate().expect("canonical exclusion proposal");

    let pending = resolved
        .propose_exclusion(proposal.clone())
        .expect("activate exclusion proposal");
    assert!(device_state_has_exact_pending_proposal(&pending, &proposal));

    let cancellation = StoreDeviceExclusionCancellation::signed(
        proposal.clone(),
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
        proposal.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
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
    assert!(cancelled.exclude(exclusion_ref.clone()).is_err());
    let excluded = pending
        .exclude(exclusion_ref.clone())
        .expect("activate device exclusion");
    assert!(matches!(
        &excluded
            .devices
            .get(&fixture.registration_ref.device_id)
            .expect("excluded record")
            .status,
        StoreDeviceStatus::Inactive { terminals }
            if terminals == &vec![exclusion_ref.clone()]
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
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot("store-v1/acks/retained/1.json".to_string()),
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
        node: recovery_node.clone(),
    };
    let device_signer = fixture
        .registration
        .device_signer(&fixture.signer)
        .expect("founder device signer");
    let commit = StoreBatchCommit::signed_operations(
        fixture.root_ref.store_root_hash,
        WriteId::from_generated("retained-registration".to_string()),
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.commit.order.clone(),
        crate::store_commit::StorePublicationBase::Genesis,
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        fixture
            .commit
            .operations_membership_authority()
            .expect("fixture carries membership authority"),
        StoreCommitOperationsInput {
            device_registrations: vec![activated.clone()],
            ..StoreCommitOperationsInput::empty()
        },
        &device_signer,
    )
    .expect("sign registration activation commit");
    let referenced_registration = ReferencedStoreDeviceRegistration::verified(
        activated.registration.clone(),
        replacement_registration.clone(),
    )
    .expect("verify exact replacement registration");
    let activated_value =
        ActivatedStoreDeviceRegistration::verified(referenced_registration.clone(), authority)
            .expect("verify replacement activation");
    activated_value
        .verify_reference(&activated)
        .expect("verify exact replacement activation reference");
    assert_eq!(
        activated_value
            .recovery_cursor()
            .expect("derive exact recovery cursor"),
        Some(OwnerRecoveryCursor {
            owner_grant: fixture.root.descriptor.founder_grant.clone(),
            position: OwnerRecoveryPosition::At {
                node: recovery_node.clone(),
            },
        })
    );
    let mut wrong_node = recovery_node;
    wrong_node.owner_grant = MembershipGrantId(ObjectHash::digest(b"wrong recovery owner grant"));
    let mismatched = ActivatedStoreDeviceRegistration::verified(
        referenced_registration,
        StoreDeviceRegistrationActivation::Recovery {
            recovery_id,
            node: wrong_node,
        },
    )
    .expect("recovery activation still names the exact recovery slot");
    assert!(mismatched.recovery_cursor().is_err());
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
fn retained_device_operations_reopen_the_exact_exclusion_sources() {
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
    let proposal = proposal_for(&fixture, proposal_id);
    let proposal_source =
        RetainedStoreDeviceExclusionProposal::from_exact(proposal.clone(), &fixture.registration)
            .expect("retain exclusion proposal");
    let (proposal_entry, proposal_control) = proposing_control(&fixture, &proposal);
    let proposal_commit = StoreBatchCommit::signed_operations(
        fixture.root_ref.store_root_hash,
        WriteId::from_generated("retained-proposal".to_string()),
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.commit.order.clone(),
        crate::store_commit::StorePublicationBase::Genesis,
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        fixture
            .commit
            .operations_membership_authority()
            .expect("fixture carries membership authority"),
        StoreCommitOperationsInput {
            control: Some(proposal_control),
            ..StoreCommitOperationsInput::empty()
        },
        &device_signer,
    )
    .expect("sign retained proposal commit");
    let retained_proposal =
        RetainedStoreDeviceOperations::from_sources(Some(proposal_source.clone()), Vec::new());
    let verified_proposal = retained_proposal
        .verify_for(&fixture.root_ref, &proposal_commit, Some(&proposal_entry))
        .expect("verify retained proposal input");
    assert_eq!(verified_proposal.proposal(), Some(&proposal));
    assert!(retained_proposal
        .verify_for(&fixture.root_ref, &proposal_commit, None)
        .is_err());
    let mut tampered_proposal =
        serde_json::to_value(&retained_proposal).expect("encode retained proposal");
    tampered_proposal["proposal"]["canonical_target_registration"]
        .as_array_mut()
        .expect("canonical target registration bytes")
        .push(serde_json::Value::from(b' '));
    let tampered_proposal: RetainedStoreDeviceOperations =
        serde_json::from_value(tampered_proposal).expect("decode tampered retained proposal");
    assert!(tampered_proposal
        .verify_for(&fixture.root_ref, &proposal_commit, Some(&proposal_entry))
        .is_err());
    let exclusion = StoreDeviceExclusion::signed(
        proposal.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
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
    let commit = StoreBatchCommit::signed_operations(
        fixture.root_ref.store_root_hash,
        WriteId::from_generated("retained-exclusion".to_string()),
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.commit.order.clone(),
        crate::store_commit::StorePublicationBase::Genesis,
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        fixture
            .commit
            .operations_membership_authority()
            .expect("fixture carries membership authority"),
        StoreCommitOperationsInput {
            device_exclusion_outcomes: vec![outcome_ref.clone()],
            ..StoreCommitOperationsInput::empty()
        },
        &device_signer,
    )
    .expect("sign retained exclusion commit");
    let retained = RetainedStoreDeviceOperations::from_sources(None, vec![outcome_source]);
    let encoded = serde_json::to_vec(&retained).expect("encode retained device operations");
    let decoded: RetainedStoreDeviceOperations =
        serde_json::from_slice(&encoded).expect("decode retained device operations");
    let verified = decoded
        .verify_for(&fixture.root_ref, &commit, None)
        .expect("verify retained device operations");
    assert_eq!(verified.to_retained(), retained);
    let StoreDeviceExclusionOutcomeRef::Excluded(expected_exclusion) = outcome_ref else {
        panic!("exclusion fixture changed outcome")
    };
    assert_eq!(verified.exclusions().next(), Some(&expected_exclusion));

    let mut tampered = serde_json::to_value(&retained).expect("encode retained operations");
    tampered["outcomes"][0]["excluded"]["canonical_outcome"]
        .as_array_mut()
        .expect("canonical outcome bytes")
        .push(serde_json::Value::from(b' '));
    let tampered: RetainedStoreDeviceOperations =
        serde_json::from_value(tampered).expect("decode tampered retained operations");
    assert!(tampered
        .verify_for(&fixture.root_ref, &commit, None)
        .is_err());

    let missing = RetainedStoreDeviceOperations::from_sources(None, Vec::new());
    assert!(missing
        .verify_for(&fixture.root_ref, &commit, None)
        .is_err());

    let mut other_registration = fixture.registration.clone();
    other_registration.body_mut().author_pubkey.push('0');
    let mut substituted = serde_json::to_value(&retained).expect("encode retained operations");
    substituted["outcomes"][0]["excluded"]["canonical_owner_registration"] =
        serde_json::to_value(other_registration.to_bytes()).expect("encode registration bytes");
    let substituted: RetainedStoreDeviceOperations =
        serde_json::from_value(substituted).expect("decode substituted retained operations");
    assert!(substituted
        .verify_for(&fixture.root_ref, &commit, None)
        .is_err());
}

#[test]
fn a_proposal_with_a_relocated_outcome_slot_is_rejected() {
    let fixture = fixture();
    let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
        b"relocated outcome slot proposal",
    ));
    let mut proposal = proposal_for(&fixture, proposal_id);
    let canonical = proposal.outcome_slot.logical_key().to_string();
    proposal.outcome_slot = slot("store-v1/device-exclusion-outcomes/elsewhere.json".to_string());
    assert!(matches!(
        proposal.validate(),
        Err(StoreProtocolError::RelocatedSlot { expected, actual })
            if expected == canonical
                && actual == "store-v1/device-exclusion-outcomes/elsewhere.json"
    ));
    let resolved = ResolvedStoreDeviceState::founder(
        &fixture.root_ref,
        fixture.registration_ref.clone(),
        &fixture.root.descriptor.founder_pubkey,
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.root.descriptor.founder_recovery,
    )
    .expect("founder device state");
    assert!(resolved.propose_exclusion(proposal).is_err());
}

fn device_state_has_exact_pending_proposal(
    state: &ResolvedStoreDeviceState,
    expected: &StoreDeviceExclusionProposal,
) -> bool {
    state
            .devices
            .get(&expected.target.device_id)
            .and_then(|record| record.proposals.get(&expected.proposal_id))
            .is_some_and(|state| {
                matches!(state, StoreDeviceProposalState::Pending { proposal } if proposal == expected)
            })
}

/// Two predecessor states stand at different positions on one Owner grant's
/// recovery chain when a frontier spans a recovery commit: the stream head
/// from before it still names the activation, the one after names the node.
/// The merged state stands at the furthest; only positions that cannot both
/// be on the one chain are a mismatch.
#[test]
fn recovery_positions_merge_to_the_furthest_on_one_chain() {
    let fixture = fixture();
    let owner_pubkey = fixture.registration.author_pubkey.clone();
    let owner_grant = fixture.root.descriptor.founder_grant.clone();
    let node = |sequence: u64, bytes: &[u8]| OwnerRecoveryNodeRef {
        owner_pubkey: owner_pubkey.clone(),
        owner_grant: owner_grant.clone(),
        sequence,
        node_hash: ObjectHash::digest(bytes),
        object: exact(
            format!("store-v1/recovery/owner/grant/{sequence}.json"),
            bytes,
        ),
    };
    let before = OwnerRecoveryPosition::BeforeFirst {
        activation: OwnerRecoveryActivationId::derive(
            &fixture.root_ref,
            &owner_pubkey,
            &owner_grant,
            &fixture.root.descriptor.founder_recovery,
        )
        .expect("derive the recovery activation"),
    };
    let first = OwnerRecoveryPosition::At {
        node: node(1, b"first recovery node"),
    };
    let second = OwnerRecoveryPosition::At {
        node: node(2, b"second recovery node"),
    };

    assert_eq!(before.merge(&first).unwrap(), first);
    assert_eq!(first.merge(&before).unwrap(), first);
    assert_eq!(first.merge(&second).unwrap(), second);
    assert_eq!(second.merge(&first).unwrap(), second);
    assert_eq!(first.merge(&first).unwrap(), first);
    assert_eq!(before.merge(&before).unwrap(), before);

    let fork = OwnerRecoveryPosition::At {
        node: node(1, b"another first recovery node"),
    };
    assert!(matches!(
        first.merge(&fork),
        Err(StoreProtocolError::OwnerRecoveryMismatch)
    ));
}
