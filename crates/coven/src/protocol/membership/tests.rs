use super::*;
use crate::protocol::objects::ObjectSlot;
use crate::protocol::objects::{ProviderDeviceBinding, ProviderPrincipalId};
use crate::protocol::store_commit::{
    membership_entry_semantic_prefix, membership_head_semantic_prefix,
    membership_resolution_semantic_prefix, registration_semantic_prefix, CommitFrontier,
    DeviceStreamAnchor, GrantStreamAnchor, ResolvedStoreDeviceState, StoreCreationId,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreRootRef,
    StreamActivation,
};

fn key() -> UserKeypair {
    UserKeypair::generate()
}

fn stream(byte: u8) -> AuthorStreamId {
    AuthorStreamId::from_bytes([byte; 32])
}

fn slot(key: impl Into<String>) -> ObjectSlot {
    ObjectSlot::logical(key.into()).expect("valid test object slot")
}

fn exact(key: impl Into<String>, bytes: &[u8]) -> ExactObjectRef {
    ExactObjectRef::new(slot(key), bytes.len() as u64, ObjectHash::digest(bytes))
}

fn membership_anchor(store_id: &str) -> GrantStreamAnchor {
    GrantStreamAnchor::StoreMembership {
        first_slot: slot(format!("test/{store_id}/membership/1.json")),
    }
}

fn recovery_anchor(store_id: &str) -> GrantStreamAnchor {
    GrantStreamAnchor::OwnerRecovery {
        first_slot: slot(format!("test/{store_id}/recovery/1.json")),
    }
}

fn test_founder_entry(
    store_id: &str,
    owner: &UserKeypair,
    created_at: &str,
    membership: GrantStreamAnchor,
) -> MembershipEntry {
    founder_entry(
        store_id,
        owner,
        crate::protocol::causal_grants::MembershipGrantId::from_test_label(store_id),
        created_at,
        membership,
        crate::protocol::provider::FounderProviderAdminGrant::from_test_label(store_id),
    )
}

fn test_root(store_id: &str) -> StoreRootRef {
    let bytes = store_id.as_bytes();
    StoreRootRef {
        store_root_id: ObjectHash::digest(format!("{store_id} identity").as_bytes()),
        store_root_hash: ObjectHash::digest(bytes),
        object: exact(format!("test/{store_id}/root.json"), bytes),
    }
}

fn registration(
    root: &StoreRootRef,
    label: &str,
    signer: &UserKeypair,
) -> (StoreDeviceRegistration, StoreDeviceRegistrationRef) {
    let registration = StoreDeviceRegistration::signed(
        root.clone(),
        StoreDeviceRegistrationOrigin::Founder {
            creation_id: StoreCreationId::from_nonce(label),
        },
        ProviderDeviceBinding {
            principal: ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(label.as_bytes()),
            },
        },
        DeviceStreamAnchor::StoreAnnouncements {
            first_slot: slot(format!("test/{label}/announcements/1.json")),
        },
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot(format!("test/{label}/acks/1.json")),
        },
        DeviceStreamAnchor::StoreSnapshots {
            first_slot: slot(format!("test/{label}/snapshots/1.json")),
        },
        signer,
    )
    .expect("sign test registration");
    let bytes = registration.to_bytes();
    let reference = StoreDeviceRegistrationRef::from_registration(
        &registration,
        exact(
            format!(
                "{}.json",
                registration_semantic_prefix(&registration.device_id.to_string())
            ),
            &bytes,
        ),
    );
    (registration, reference)
}

fn conflict_acceptance(
    chain: &MembershipChain,
    store_root_hash: ObjectHash,
    membership: GrantStreamAnchor,
    signer: &UserKeypair,
) -> OwnerConflictResolutionAcceptance {
    let (conflict_hash, owner_grants) = match chain
        .conflict()
        .expect("test chain has a membership conflict")
    {
        MembershipConflict::ConcurrentMemberAssignments {
            conflict_hash,
            grants,
            ..
        } => (
            conflict_hash,
            grants
                .iter()
                .filter_map(|(grant, state)| {
                    state
                        .active()
                        .filter(|record| record.role.is_owner())
                        .map(|record| (grant.clone(), record.clone()))
                })
                .collect::<Vec<_>>(),
        ),
        MembershipConflict::RevocationCycle {
            conflict_hash,
            maximal_valid_branches,
            ..
        } => (
            conflict_hash,
            maximal_valid_branches
                .iter()
                .flat_map(StoreMembershipBranch::active_grants)
                .filter(|(_, record)| record.role.is_owner())
                .map(|(grant, record)| (grant.clone(), record.clone()))
                .collect::<Vec<_>>(),
        ),
    };
    let resolver_pubkey = keys::public_key_hex(signer);
    let root = StoreRootRef {
        store_root_id: ObjectHash::digest(b"test conflict-resolution root id"),
        store_root_hash,
        object: exact(
            "test/conflict-resolution/root.json",
            b"conflict resolution root",
        ),
    };
    let (registration, owner_registration) = registration(
        &root,
        &format!("conflict-resolution-{resolver_pubkey}"),
        signer,
    );
    let mut recovery = owner_grants
        .into_iter()
        .map(|(grant, record)| OwnerRecoveryCursor {
            owner_grant: grant.clone(),
            position: OwnerRecoveryPosition::At {
                node: OwnerRecoveryNodeRef {
                    owner_pubkey: record.member_pubkey,
                    owner_grant: grant.clone(),
                    sequence: 1,
                    node_hash: ObjectHash::digest(format!("conflict recovery {grant}").as_bytes()),
                    object: exact(
                        format!("test/conflict-recovery/{grant}/1.json"),
                        format!("conflict recovery {grant}").as_bytes(),
                    ),
                },
            },
        })
        .collect::<Vec<_>>();
    recovery.sort();
    recovery.dedup_by(|left, right| left.owner_grant == right.owner_grant);
    OwnerConflictResolutionAcceptance::unsigned_for_test(OwnerConflictResolutionAcceptanceBody {
        store_root_hash,
        owner_grant: derive_store_resolution_grant(conflict_hash, &resolver_pubkey),
        owner_registration,
        provider: registration.provider.clone(),
        membership,
        recovery: recovery_anchor(&format!("conflict-resolution-{resolver_pubkey}")),
        device_state: StoreDeviceStateRef::from_resolved(
            CommitFrontier(BTreeMap::new()),
            &ResolvedStoreDeviceState {
                devices: BTreeMap::new(),
                recovery,
                state_hash: ObjectHash::digest(b"test conflict-resolution device state"),
            },
        )
        .expect("construct conflict-resolution device state"),
    })
}

fn exact_head(entry: &MembershipEntry, signer: &UserKeypair) -> (MembershipHeadRef, AuthorHead) {
    exact_head_with_resolutions(entry, signer, entry.resolution_dependencies.clone())
}

fn exact_head_with_resolutions(
    entry: &MembershipEntry,
    signer: &UserKeypair,
    resolutions: Vec<StoreMembershipConflictResolutionRef>,
) -> (MembershipHeadRef, AuthorHead) {
    let root = test_root(&entry.store_id);
    let (registration, registration_ref) = registration(
        &root,
        &format!("{}-{}", entry.store_id, entry.author_pubkey),
        signer,
    );
    let entry_bytes = serde_json::to_vec(entry).expect("serialize membership entry");
    let coord = entry.coord();
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
    let anchor = membership_anchor(&entry.store_id);
    let successor = SuccessorLink {
        activation: StreamActivation::grant_authorized(
            root.store_root_hash,
            registration_ref.clone(),
            entry.author_owner_grant.clone(),
            anchor,
        )
        .activation_id(),
        predecessor: None,
        next_slot: slot(format!(
            "test/{}/membership-heads/{}/next.json",
            entry.store_id, coord.entry_hash
        )),
    };
    let device_signer = registration.device_signer(signer).unwrap();
    let head = AuthorHead::signed(
        entry.store_id.clone(),
        MembershipHeadBody {
            author_registration: registration_ref,
            entry: entry_ref,
            predecessor: None,
            resolutions,
            successor,
        },
        MembershipHeadActivation::Direct,
        &device_signer,
    );
    let head_bytes = serde_json::to_vec(&head).expect("serialize membership head");
    let reference = MembershipHeadRef {
        coord: coord.clone(),
        head_hash: head.head_hash(),
        object: exact(
            format!(
                "{}.json",
                membership_head_semantic_prefix(
                    &coord.author_pubkey,
                    &coord.author_owner_grant,
                    coord.stream_id,
                    coord.seq,
                    head.head_hash(),
                )
            ),
            &head_bytes,
        ),
    };
    (reference, head)
}

fn exact_resolution(
    resolution: StoreMembershipConflictResolution,
) -> (
    StoreMembershipConflictResolutionRef,
    StoreMembershipConflictResolution,
) {
    let bytes = serde_json::to_vec(&resolution).expect("serialize membership resolution");
    let reference = resolution.resolution_ref(exact(
        format!(
            "{}.json",
            membership_resolution_semantic_prefix(
                resolution.conflict_hash,
                &resolution.resolver_pubkey,
                resolution.resolution_hash(),
            )
        ),
        &bytes,
    ));
    (reference, resolution)
}

fn founded(store_id: &str, owner: &UserKeypair) -> MembershipChain {
    MembershipChain::from_entries(vec![test_founder_entry(
        store_id,
        owner,
        "founder",
        membership_anchor(store_id),
    )])
    .unwrap()
}

#[test]
fn membership_head_requires_an_explicit_activation_rule() {
    let owner = key();
    let entry = test_founder_entry(
        "required-head-activation",
        &owner,
        "founder",
        membership_anchor("required-head-activation"),
    );
    let (_, head) = exact_head(&entry, &owner);
    let mut encoded = serde_json::to_value(head).expect("serialize membership head");
    encoded
        .as_object_mut()
        .expect("membership head object")
        .remove("activation");
    assert!(serde_json::from_value::<AuthorHead>(encoded).is_err());
}

#[test]
fn reserved_membership_transition_and_published_head_share_one_body() {
    let owner = key();
    let entry = test_founder_entry(
        "shared-head-body",
        &owner,
        "founder",
        membership_anchor("shared-head-body"),
    );
    let (reference, head) = exact_head(&entry, &owner);
    let transition = MergeMembershipHeadTransition {
        body: head.body.clone(),
        head_slot: reference.object.slot().clone(),
    };
    let encoded = serde_json::to_vec(&transition).expect("serialize reserved transition");
    let decoded: MergeMembershipHeadTransition =
        serde_json::from_slice(&encoded).expect("parse reserved transition");
    assert_eq!(decoded, transition);
    assert!(decoded.matches_head(&head, &reference));

    let mut mismatched = decoded;
    mismatched.body.successor.next_slot = slot("test/shared-head-body/another-next.json");
    assert!(!mismatched.matches_head(&head, &reference));
}

#[test]
fn merge_active_grant_lookup_returns_only_the_exact_live_record() {
    let owner = key();
    let member = key();
    let member_pubkey = keys::public_key_hex(&member);
    let mut chain = founded("exact-live-merge-grant", &owner);
    let addition = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            member_pubkey.clone(),
            None,
            MemberRole::Member,
            "add member".to_string(),
        )
        .unwrap();
    let MembershipChange::SetMember { grant_id, .. } = &addition.change else {
        unreachable!()
    };
    let grant_id = grant_id.clone();
    chain.add_entry(addition).unwrap();
    let MembershipStatus::Resolved(resolved) = chain.status() else {
        panic!("membership must resolve")
    };
    assert_eq!(
        chain.active_grant(&grant_id),
        resolved.active_grant(&grant_id)
    );
    assert!(chain
        .active_grant(&MembershipGrantId(ObjectHash::digest(b"absent grant")))
        .is_none());

    let removal = chain
        .signed_remove_member_in_stream(
            &owner,
            stream(1),
            member_pubkey.clone(),
            "remove member".to_string(),
        )
        .unwrap();
    let retirement_authority = removal.coord();
    chain.add_entry(removal).unwrap();
    assert!(chain.active_grant(&grant_id).is_none());
    let MembershipStatus::Resolved(resolved) = chain.status() else {
        panic!("membership must resolve")
    };
    assert!(matches!(
        &resolved.grants[&grant_id],
        GrantState::Tombstoned { record, retirements }
            if record.member_pubkey == member_pubkey
                && retirements.as_set() == &BTreeSet::from([MembershipGrantRetirement::Entry {
                    authority: retirement_authority.clone(),
                    barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
                        author_streams: StoreGrantStreamBarrier {
                            observed_streams: Vec::new(),
                        },
                    },
                }])
    ));
    let mut altered = resolved.grants.clone();
    let GrantState::Tombstoned { retirements, .. } = altered
        .get_mut(&grant_id)
        .expect("retired Merge grant remains present")
    else {
        unreachable!()
    };
    retirements.insert(MembershipGrantRetirement::Entry {
        authority: MembershipCoord {
            entry_hash: ObjectHash::digest(b"different retirement entry"),
            ..retirement_authority.clone()
        },
        barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
            author_streams: StoreGrantStreamBarrier {
                observed_streams: Vec::new(),
            },
        },
    });
    assert_ne!(
        resolved.state_hash,
        store_membership_state_hash(&altered, &resolved.provider_admin)
    );

    let mut reuse = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            member_pubkey,
            None,
            MemberRole::Member,
            "reuse retired grant".to_string(),
        )
        .unwrap();
    let MembershipChange::SetMember {
        grant_id: candidate,
        ..
    } = &mut reuse.change
    else {
        unreachable!()
    };
    *candidate = grant_id.clone();
    sign_membership_entry(&mut reuse, &owner);
    assert!(matches!(
        chain.add_entry(reuse),
        Err(MembershipError::DuplicateGrant {
            grant,
            ..
        }) if grant == grant_id
    ));
}

#[test]
fn grant_mapping_returns_an_error_when_signed_retirement_evidence_is_absent() {
    let owner = key();
    let founder = test_founder_entry(
        "missing-retirement-evidence",
        &owner,
        "founder",
        membership_anchor("missing-retirement-evidence"),
    );
    let MembershipChange::Founder { owner_grant_id, .. } = &founder.change else {
        panic!("test entry is the founder")
    };
    let owner_grant_id = owner_grant_id.clone();
    let authority = MembershipCoord {
        author_pubkey: keys::public_key_hex(&owner),
        author_owner_grant: owner_grant_id.clone(),
        stream_id: stream(77),
        seq: 1,
        entry_hash: ObjectHash::digest(b"missing retirement authority"),
    };
    let state = GrantState::Tombstoned {
        record: causal_grants::GrantRecord {
            member_pubkey: keys::public_key_hex(&owner),
            assignment: StoreAssignment {
                role: StoreMembershipRoleGrant::Member,
                provider_account_email: None,
            },
            creation: causal_grants::CausalGrantCreation::Entry(founder.coord()),
        },
        retirements: GrantRetirements::new(causal_grants::CausalGrantRetirement::Entry {
            coord: authority.clone(),
            owner_barrier: None,
        }),
    };

    assert!(matches!(
        map_store_grant_state(&owner_grant_id, &state, None, &[founder]),
        Err(MembershipError::MissingRetirementBarrier {
            grant,
            authority: missing,
        }) if grant == owner_grant_id && *missing == authority
    ));
}

#[test]
fn concurrent_effective_removals_union_exact_retirement_entries() {
    let first_owner = key();
    let second_owner = key();
    let member = key();
    let member_pubkey = keys::public_key_hex(&member);
    let mut base = founded("concurrent-retirement-evidence", &first_owner);
    base.add_owner_for_test(
        &first_owner,
        stream(1),
        keys::public_key_hex(&second_owner),
        "add second Owner".to_string(),
    )
    .unwrap();
    let add_member = base
        .signed_set_member_in_stream(
            &first_owner,
            stream(1),
            member_pubkey.clone(),
            None,
            MemberRole::Member,
            "add member".to_string(),
        )
        .unwrap();
    let member_grant = match &add_member.change {
        MembershipChange::SetMember { grant_id, .. } => grant_id.clone(),
        _ => unreachable!(),
    };
    base.add_entry(add_member).unwrap();

    let first_removal = base
        .signed_remove_member_in_stream(
            &first_owner,
            stream(1),
            member_pubkey.clone(),
            "first removal".to_string(),
        )
        .unwrap();
    let second_removal = base
        .signed_remove_member_in_stream(
            &second_owner,
            stream(2),
            member_pubkey,
            "second removal".to_string(),
        )
        .unwrap();
    let expected = GrantRetirements::new(MembershipGrantRetirement::Entry {
        authority: first_removal.coord(),
        barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
            author_streams: StoreGrantStreamBarrier {
                observed_streams: Vec::new(),
            },
        },
    });
    let mut expected = expected;
    expected.insert(MembershipGrantRetirement::Entry {
        authority: second_removal.coord(),
        barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
            author_streams: StoreGrantStreamBarrier {
                observed_streams: Vec::new(),
            },
        },
    });
    let mut entries = base.entries().to_vec();
    entries.extend([first_removal, second_removal]);
    let chain = MembershipChain::from_entries(entries).unwrap();
    let MembershipStatus::Resolved(resolved) = chain.status() else {
        panic!("concurrent non-Owner removals must resolve")
    };

    assert!(matches!(
        &resolved.grants[&member_grant],
        GrantState::Tombstoned { retirements, .. }
            if retirements.as_set() == expected.as_set()
    ));
}

fn three_owner_store_cycle() -> (UserKeypair, UserKeypair, UserKeypair, MembershipChain) {
    let first = key();
    let second = key();
    let third = key();
    let first_pubkey = keys::public_key_hex(&first);
    let second_pubkey = keys::public_key_hex(&second);
    let third_pubkey = keys::public_key_hex(&third);
    let mut base = founded("three-owner-store", &first);
    base.add_owner_for_test(
        &first,
        stream(1),
        second_pubkey.clone(),
        "add second Owner".to_string(),
    )
    .expect("add second Owner");
    base.add_owner_for_test(
        &first,
        stream(1),
        third_pubkey,
        "add third Owner".to_string(),
    )
    .expect("add third Owner");
    let remove_second = base
        .signed_remove_member_in_stream(
            &first,
            stream(1),
            second_pubkey,
            "first branch".to_string(),
        )
        .expect("first branch");
    let remove_first = base
        .signed_remove_member_in_stream(
            &second,
            stream(92),
            first_pubkey,
            "second branch".to_string(),
        )
        .expect("second branch");
    let mut entries = base.entries().to_vec();
    entries.extend([remove_second.clone(), remove_first.clone()]);
    let heads = vec![
        exact_head(
            base.entries().first().expect("founder membership entry"),
            &first,
        ),
        exact_head(&remove_second, &first),
        exact_head(&remove_first, &second),
    ];
    let conflict = MembershipChain::from_entries_with_coords_and_heads(
        entries
            .into_iter()
            .map(|entry| (entry.coord(), entry))
            .collect(),
        heads,
    )
    .expect("three-Owner Store conflict");
    (first, second, third, conflict)
}

#[test]
fn unaffected_store_owner_resolution_retires_its_selected_branch_grant() {
    let (_first, _second, third, conflicted) = three_owner_store_cycle();
    let third_pubkey = keys::public_key_hex(&third);
    let (branch, old_grant) = match conflicted.conflict().expect("conflict") {
        MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } => {
            let branch = maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch.active_grants().any(|(_, record)| {
                        record.member_pubkey == third_pubkey && record.role.is_owner()
                    })
                })
                .expect("unaffected Owner branch");
            let old_grant = branch
                .active_grants()
                .find_map(|(grant, record)| {
                    (record.member_pubkey == third_pubkey).then_some(grant.clone())
                })
                .expect("unaffected Owner grant");
            (branch.heads.clone(), old_grant)
        }
        _ => panic!("expected revocation conflict"),
    };
    let store_root_hash = ObjectHash::digest(b"unaffected Store resolver root");
    let replacement_membership = membership_anchor("unaffected-store-resolver");
    let acceptance = conflict_acceptance(
        &conflicted,
        store_root_hash,
        replacement_membership.clone(),
        &third,
    );
    let resolution = conflicted
        .signed_conflict_resolution(
            store_root_hash,
            MembershipConflictSelection::RevocationBranch { heads: branch },
            replacement_membership,
            acceptance,
            &third,
        )
        .expect("unaffected Owner resolution");
    let resolution = exact_resolution(resolution);
    let resolved = conflicted
        .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
        .expect("unaffected Owner resolution is valid");

    assert!(resolution.1.retired_owner_grants.contains(&old_grant));
    assert!(resolved.grants[&old_grant].active().is_none());
    assert!(resolved
        .grants
        .get(&resolution.1.replacement_grant)
        .and_then(GrantState::active)
        .is_some());
    assert!(matches!(
        &resolved.grants[&old_grant],
        GrantState::Tombstoned { retirements, .. }
            if retirements.iter().any(|retirement| matches!(
                retirement,
                MembershipGrantRetirement::ConflictResolution { authority, .. }
                    if authority == &resolution.0
            ))
    ));
}

#[test]
fn store_revocation_cycle_over_protocol_bound_is_typed() {
    let owners = (0..13).map(|_| key()).collect::<Vec<_>>();
    let pubkeys = owners.iter().map(keys::public_key_hex).collect::<Vec<_>>();
    let mut base = founded("bounded-store-cycle", &owners[0]);
    for pubkey in pubkeys.iter().skip(1) {
        base.add_owner_for_test(
            &owners[0],
            stream(1),
            pubkey.clone(),
            format!("add {pubkey}"),
        )
        .expect("add ring Owner");
    }
    let removals = owners
        .iter()
        .enumerate()
        .map(|(index, owner)| {
            base.signed_remove_member_in_stream(
                owner,
                stream(index as u8 + 101),
                pubkeys[(index + 1) % pubkeys.len()].clone(),
                format!("remove ring successor {index}"),
            )
            .expect("sign ring removal")
        })
        .collect::<Vec<_>>();
    let mut entries = base.entries().to_vec();
    entries.extend(removals.iter().cloned());
    let heads = removals
        .iter()
        .zip(&owners)
        .map(|(entry, owner)| exact_head(entry, owner))
        .collect();

    assert!(matches!(
        MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        ),
        Err(MembershipError::RevocationCycleTooWide {
            sources: 13,
            maximum: 12,
        })
    ));
}

#[test]
fn timestamp_does_not_change_causal_authorization() {
    let owner = key();
    let member = key();
    let mut chain = founded("store", &owner);
    let add = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            keys::public_key_hex(&member),
            None,
            MemberRole::Member,
            "9999".to_string(),
        )
        .unwrap();
    chain.add_entry(add).unwrap();
    let remove = chain
        .signed_remove_member_in_stream(
            &owner,
            stream(1),
            keys::public_key_hex(&member),
            "0000".to_string(),
        )
        .unwrap();
    chain.add_entry(remove).unwrap();
    assert!(!chain.can_write_now(&keys::public_key_hex(&member)));
}

#[test]
fn signed_candidate_is_validated_before_it_is_returned() {
    let owner = key();
    let chain = founded("store", &owner);

    assert!(matches!(
        chain.signed_remove_member_in_stream(
            &owner,
            stream(1),
            keys::public_key_hex(&owner),
            "remove last owner".to_string(),
        ),
        Err(MembershipError::NoActiveOwner)
    ));
}

#[test]
fn direct_owner_assignment_is_rejected() {
    let founder = key();
    let candidate = key();
    let chain = founded("owner-promotion-required", &founder);
    let candidate_pubkey = keys::public_key_hex(&candidate);

    assert!(matches!(
        chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
            &founder,
            stream(1),
            candidate_pubkey.clone(),
            None,
            MemberRole::Owner,
            Some(membership_anchor("direct-owner-assignment")),
            test_wrapped_key_ref(
                &keys::public_key_hex(&founder),
                &candidate_pubkey,
                crate::encryption::INITIAL_KEY_GENERATION,
                b"direct Owner assignment",
            ),
            "direct Owner assignment".to_string(),
        ),
        Err(MembershipError::OwnerPromotionRequired)
    ));
}

#[test]
fn membership_candidates_require_exact_wrapped_key_recipient_coverage() {
    let owner = key();
    let member = key();
    let owner_pubkey = keys::public_key_hex(&owner);
    let member_pubkey = keys::public_key_hex(&member);
    let mut chain = founded("store", &owner);
    let wrong_recipient = test_wrapped_key_ref(
        &owner_pubkey,
        &owner_pubkey,
        crate::encryption::INITIAL_KEY_GENERATION,
        b"wrong invitation recipient",
    );
    assert!(matches!(
        chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
            &owner,
            stream(1),
            member_pubkey.clone(),
            None,
            MemberRole::Member,
            None,
            wrong_recipient,
            "invalid invitation".to_string(),
        ),
        Err(MembershipError::InvalidWrappedKeys(_))
    ));

    let add = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            member_pubkey.clone(),
            None,
            MemberRole::Member,
            "add member".to_string(),
        )
        .unwrap();
    chain.add_entry(add).unwrap();
    assert!(matches!(
        chain.signed_remove_member_with_wrapped_keys_in_stream(
            &owner,
            stream(1),
            member_pubkey,
            Vec::new(),
            "missing owner wrap".to_string(),
        ),
        Err(MembershipError::InvalidWrappedKeys(_))
    ));
}

#[test]
fn wrapped_key_generations_follow_the_causal_membership_history() {
    let owner = key();
    let first_member = key();
    let second_member = key();
    let later_member = key();
    let owner_pubkey = keys::public_key_hex(&owner);
    let first_pubkey = keys::public_key_hex(&first_member);
    let second_pubkey = keys::public_key_hex(&second_member);
    let later_pubkey = keys::public_key_hex(&later_member);
    let mut chain = founded("wrapped-generation-history", &owner);
    for member in [&first_pubkey, &second_pubkey] {
        let add = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                member.clone(),
                None,
                MemberRole::Member,
                format!("add {member}"),
            )
            .unwrap();
        chain.add_entry(add).unwrap();
    }
    let mut first_rotation_wraps = vec![
        test_wrapped_key_ref(&owner_pubkey, &owner_pubkey, 2, b"first owner rotation"),
        test_wrapped_key_ref(&owner_pubkey, &second_pubkey, 2, b"first member rotation"),
    ];
    first_rotation_wraps.sort();
    let first_rotation = chain
        .signed_remove_member_with_wrapped_keys_in_stream(
            &owner,
            stream(1),
            first_pubkey,
            first_rotation_wraps,
            "first rotation".to_string(),
        )
        .unwrap();
    chain.add_entry(first_rotation).unwrap();

    assert!(matches!(
        chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
            &owner,
            stream(1),
            later_pubkey.clone(),
            None,
            MemberRole::Member,
            None,
            test_wrapped_key_ref(&owner_pubkey, &later_pubkey, 1, b"stale later invitation",),
            "stale later invitation".to_string(),
        ),
        Err(MembershipError::InvalidWrappedKeys(_))
    ));
    assert!(matches!(
        chain.signed_remove_member_with_wrapped_keys_in_stream(
            &owner,
            stream(1),
            second_pubkey,
            vec![test_wrapped_key_ref(
                &owner_pubkey,
                &owner_pubkey,
                2,
                b"reused rotation generation",
            )],
            "reused rotation generation".to_string(),
        ),
        Err(MembershipError::InvalidWrappedKeys(_))
    ));
}

#[test]
fn concurrent_add_and_rotation_has_incomplete_wrapped_key_authority() {
    let owner = key();
    let removed = key();
    let concurrent_member = key();
    let owner_pubkey = keys::public_key_hex(&owner);
    let removed_pubkey = keys::public_key_hex(&removed);
    let concurrent_pubkey = keys::public_key_hex(&concurrent_member);
    let mut chain = founded("concurrent-add-rotation", &owner);
    let add_removed = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            removed_pubkey.clone(),
            None,
            MemberRole::Member,
            "add member that will be removed".to_string(),
        )
        .unwrap();
    chain.add_entry(add_removed).unwrap();

    let add_concurrent = chain
        .signed_set_member_in_stream(
            &owner,
            stream(2),
            concurrent_pubkey.clone(),
            None,
            MemberRole::Member,
            "concurrent add".to_string(),
        )
        .unwrap();
    let owner_rotation = test_wrapped_key_ref(
        &owner_pubkey,
        &owner_pubkey,
        2,
        b"rotation missing concurrent member",
    );
    let remove = chain
        .signed_remove_member_with_wrapped_keys_in_stream(
            &owner,
            stream(3),
            removed_pubkey,
            vec![owner_rotation],
            "concurrent removal".to_string(),
        )
        .unwrap();
    chain.add_entry(add_concurrent).unwrap();
    chain.add_entry(remove).unwrap();

    assert!(matches!(
        chain.wrapped_key_authority_for(&concurrent_pubkey),
        Err(MembershipError::MissingWrappedKeyCoverage { .. })
    ));

    let replacement_wrap = test_wrapped_key_ref(
        &owner_pubkey,
        &concurrent_pubkey,
        2,
        b"post-rotation replacement invitation",
    );
    let replacement = chain
        .signed_set_member_with_anchor_and_wrapped_key_in_stream(
            &owner,
            stream(4),
            concurrent_pubkey.clone(),
            None,
            MemberRole::Member,
            None,
            replacement_wrap.clone(),
            "replace concurrent invitation after rotation".to_string(),
        )
        .unwrap();
    chain.add_entry(replacement).unwrap();
    assert_eq!(
        chain.wrapped_key_authority_for(&concurrent_pubkey).unwrap(),
        vec![replacement_wrap],
    );
}

#[test]
fn concurrent_member_assignments_are_validated_conflict_state() {
    let owner = key();
    let target = key();
    let target_pubkey = keys::public_key_hex(&target);
    let mut chain = founded("store", &owner);
    let member = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            target_pubkey.clone(),
            None,
            MemberRole::Member,
            "initial Member".to_string(),
        )
        .unwrap();
    chain.add_entry(member).unwrap();
    let first = chain
        .signed_set_member_in_stream(
            &owner,
            stream(21),
            target_pubkey.clone(),
            None,
            MemberRole::Follower,
            "first".to_string(),
        )
        .unwrap();
    let second = chain
        .signed_promote_member_in_stream_for_test(
            &owner,
            stream(22),
            target_pubkey.clone(),
            "second".to_string(),
        )
        .unwrap();
    let mut entries = chain.entries().to_vec();
    entries.extend([first.clone(), second.clone()]);
    let heads = entries
        .iter()
        .filter(|entry| {
            !entries.iter().any(|candidate| {
                candidate
                    .dependencies
                    .iter()
                    .any(|dependency| dependency == &entry.coord())
                    && candidate.stream_id == entry.stream_id
            })
        })
        .map(|entry| exact_head(entry, &owner))
        .collect();

    let conflicted = MembershipChain::from_entries_with_coords_and_heads(
        entries
            .into_iter()
            .map(|entry| (entry.coord(), entry))
            .collect(),
        heads,
    )
    .expect("well-formed conflict");
    let MembershipConflict::ConcurrentMemberAssignments {
        member_pubkey,
        conflicting_grants,
        ..
    } = conflicted.conflict().expect("assignment conflict")
    else {
        panic!("concurrent assignments must produce an assignment conflict")
    };
    assert_eq!(member_pubkey, &target_pubkey);
    assert_eq!(conflicting_grants.len(), 2);

    let selected_grant = conflicting_grants
        .iter()
        .find_map(|(grant, record)| {
            (record.role.role() == MemberRole::Follower).then(|| grant.clone())
        })
        .expect("Follower assignment");
    let retired_grant = conflicting_grants
        .keys()
        .find(|grant| **grant != selected_grant)
        .expect("other assignment")
        .clone();
    let opaque_choice = MembershipConflictChoice::new(
        "opaque-choice".to_string(),
        Vec::new(),
        ObjectHash::digest(b"hidden conflict"),
        MembershipConflictSelection::MemberAssignment {
            grant: selected_grant.clone(),
        },
    );
    assert_eq!(
        format!("{opaque_choice:?}"),
        "MembershipConflictChoice { id: \"opaque-choice\", members: [] }",
    );
    let store_root_hash = ObjectHash::digest(b"assignment-resolution Store root");
    let replacement_membership = membership_anchor("assignment-resolution");
    let acceptance = conflict_acceptance(
        &conflicted,
        store_root_hash,
        replacement_membership.clone(),
        &owner,
    );
    let resolution_value = conflicted
        .signed_conflict_resolution(
            store_root_hash,
            MembershipConflictSelection::MemberAssignment {
                grant: selected_grant.clone(),
            },
            replacement_membership,
            acceptance,
            &owner,
        )
        .expect("Owner selects an assignment");
    let mut incomplete_resolution = resolution_value.clone();
    incomplete_resolution
        .retirement_barriers
        .remove(&retired_grant);
    incomplete_resolution.signature =
        keys::sign_hex(&owner, &incomplete_resolution.canonical_bytes()).1;
    assert!(!incomplete_resolution.verify_against(
        store_root_hash,
        conflicted.conflict().expect("assignment conflict"),
    ));
    let resolution = exact_resolution(resolution_value);
    let resolved_once = conflicted
        .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
        .expect("assignment resolution applies");
    let resolved_retry = conflicted
        .resolved_with(store_root_hash, &[resolution.clone(), resolution.clone()])
        .expect("exact assignment resolution retry is idempotent");

    assert_eq!(resolved_once, resolved_retry);
    assert_eq!(
        resolved_once
            .grants
            .get(&selected_grant)
            .and_then(GrantState::active)
            .map(|record| record.role.role()),
        Some(MemberRole::Follower),
    );
    assert!(matches!(
        resolved_once.grants.get(&retired_grant),
        Some(GrantState::Tombstoned { .. })
    ));
    assert!(resolution
        .1
        .retired_owner_grants
        .iter()
        .all(|grant| resolved_once
            .grants
            .get(grant)
            .and_then(GrantState::active)
            .is_none()));
    assert!(resolved_once
        .grants
        .get(&resolution.1.replacement_grant)
        .and_then(GrantState::active)
        .is_some());
}

#[test]
fn assignment_resolvers_keep_only_a_choice_they_all_selected() {
    let first_owner = key();
    let second_owner = key();
    let target = key();
    let first_owner_pubkey = keys::public_key_hex(&first_owner);
    let second_owner_pubkey = keys::public_key_hex(&second_owner);
    let target_pubkey = keys::public_key_hex(&target);
    let mut base = founded("assignment-consensus", &first_owner);
    base.add_owner_for_test(
        &first_owner,
        stream(1),
        second_owner_pubkey.clone(),
        "add second Owner".to_string(),
    )
    .unwrap();
    let initial = base
        .signed_set_member_in_stream(
            &first_owner,
            stream(1),
            target_pubkey.clone(),
            None,
            MemberRole::Member,
            "initial target assignment".to_string(),
        )
        .unwrap();
    base.add_entry(initial).unwrap();
    let follower_assignment = base
        .signed_set_member_in_stream(
            &first_owner,
            stream(21),
            target_pubkey.clone(),
            None,
            MemberRole::Follower,
            "Follower assignment".to_string(),
        )
        .unwrap();
    let member_assignment = base
        .signed_set_member_in_stream(
            &second_owner,
            stream(22),
            target_pubkey.clone(),
            None,
            MemberRole::Member,
            "Member assignment".to_string(),
        )
        .unwrap();
    let mut entries = base.entries().to_vec();
    entries.extend([follower_assignment, member_assignment]);
    let heads = entries
        .iter()
        .filter(|entry| {
            !entries.iter().any(|candidate| {
                candidate
                    .dependencies
                    .iter()
                    .any(|dependency| dependency == &entry.coord())
                    && candidate.stream_id == entry.stream_id
            })
        })
        .map(|entry| {
            let signer = if entry.author_pubkey == first_owner_pubkey {
                &first_owner
            } else {
                assert_eq!(entry.author_pubkey, second_owner_pubkey);
                &second_owner
            };
            exact_head(entry, signer)
        })
        .collect();
    let conflicted = MembershipChain::from_entries_with_coords_and_heads(
        entries
            .into_iter()
            .map(|entry| (entry.coord(), entry))
            .collect(),
        heads,
    )
    .expect("well-formed assignment conflict");
    let MembershipConflict::ConcurrentMemberAssignments {
        conflicting_grants, ..
    } = conflicted.conflict().expect("assignment conflict")
    else {
        panic!("concurrent assignments must produce an assignment conflict")
    };
    let follower_grant = conflicting_grants
        .iter()
        .find_map(|(grant, record)| {
            (record.role.role() == MemberRole::Follower).then(|| grant.clone())
        })
        .expect("Follower assignment");
    let member_grant = conflicting_grants
        .iter()
        .find_map(|(grant, record)| {
            (record.role.role() == MemberRole::Member).then(|| grant.clone())
        })
        .expect("Member assignment");
    let store_root_hash = ObjectHash::digest(b"assignment consensus Store root");

    let first_membership = membership_anchor("first-assignment-resolution");
    let first_acceptance = conflict_acceptance(
        &conflicted,
        store_root_hash,
        first_membership.clone(),
        &first_owner,
    );
    let first_resolution = exact_resolution(
        conflicted
            .signed_conflict_resolution(
                store_root_hash,
                MembershipConflictSelection::MemberAssignment {
                    grant: follower_grant.clone(),
                },
                first_membership,
                first_acceptance,
                &first_owner,
            )
            .expect("first Owner selects the Follower assignment"),
    );
    let second_membership = membership_anchor("second-assignment-resolution");
    let second_acceptance = conflict_acceptance(
        &conflicted,
        store_root_hash,
        second_membership.clone(),
        &second_owner,
    );
    let second_resolution = exact_resolution(
        conflicted
            .signed_conflict_resolution(
                store_root_hash,
                MembershipConflictSelection::MemberAssignment {
                    grant: member_grant.clone(),
                },
                second_membership,
                second_acceptance,
                &second_owner,
            )
            .expect("second Owner selects the Member assignment"),
    );

    let resolved = conflicted
        .resolved_with(
            store_root_hash,
            &[first_resolution.clone(), second_resolution.clone()],
        )
        .expect("disagreeing assignment resolutions converge");

    assert!(matches!(
        resolved.grants.get(&follower_grant),
        Some(GrantState::Tombstoned { .. })
    ));
    assert!(matches!(
        resolved.grants.get(&member_grant),
        Some(GrantState::Tombstoned { .. })
    ));
    assert!(!resolved
        .grants
        .values()
        .filter_map(GrantState::active)
        .any(|record| record.member_pubkey == target_pubkey));
    for resolution in [&first_resolution, &second_resolution] {
        assert!(resolved
            .grants
            .get(&resolution.1.replacement_grant)
            .and_then(GrantState::active)
            .is_some());
        assert!(resolution
            .1
            .retired_owner_grants
            .iter()
            .all(|grant| resolved
                .grants
                .get(grant)
                .and_then(GrantState::active)
                .is_none()));
    }
}

#[test]
fn concurrent_cross_revocation_is_a_validated_cycle_conflict() {
    let first_owner = key();
    let second_owner = key();
    let first_pubkey = keys::public_key_hex(&first_owner);
    let second_pubkey = keys::public_key_hex(&second_owner);
    let mut base = founded("store", &first_owner);
    base.add_owner_for_test(
        &first_owner,
        stream(1),
        second_pubkey.clone(),
        "add second".to_string(),
    )
    .unwrap();
    let remove_second = base
        .signed_remove_member_in_stream(
            &first_owner,
            stream(1),
            second_pubkey.clone(),
            "remove second".to_string(),
        )
        .unwrap();
    let remove_first = base
        .signed_remove_member_in_stream(
            &second_owner,
            stream(23),
            first_pubkey.clone(),
            "remove first".to_string(),
        )
        .unwrap();
    let mut entries = base.entries().to_vec();
    entries.extend([remove_second.clone(), remove_first.clone()]);
    let heads = vec![
        exact_head(
            base.entries().first().expect("founder membership entry"),
            &first_owner,
        ),
        exact_head(&remove_second, &first_owner),
        exact_head(&remove_first, &second_owner),
    ];

    let conflicted = MembershipChain::from_entries_with_coords_and_heads(
        entries
            .into_iter()
            .map(|entry| (entry.coord(), entry))
            .collect(),
        heads,
    )
    .expect("well-formed conflict");
    assert!(matches!(
        conflicted.status(),
        MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
            cyclic_sources,
            involved_owner_grants,
            maximal_valid_branches,
            ..

        }) if cyclic_sources.len() == 2
            && involved_owner_grants.len() == 2
            && maximal_valid_branches.len() == 2
    ));

    let MembershipConflict::RevocationCycle {
        maximal_valid_branches,
        ..
    } = conflicted.conflict().expect("cycle conflict")
    else {
        unreachable!();
    };
    let resolver_branch_state = maximal_valid_branches
        .iter()
        .find(|branch| {
            branch
                .active_grants()
                .any(|(_, record)| record.member_pubkey == first_pubkey && record.role.is_owner())
        })
        .expect("first Owner branch")
        .clone();
    let resolver_branch = resolver_branch_state.heads.clone();
    let second_resolver_branch = maximal_valid_branches
        .iter()
        .find(|branch| {
            branch
                .active_grants()
                .any(|(_, record)| record.member_pubkey == second_pubkey && record.role.is_owner())
        })
        .expect("second Owner branch")
        .heads
        .clone();
    let store_root_hash = ObjectHash::digest(b"resolution Store root");
    let first_membership = membership_anchor("first-cycle-resolution");
    let first_acceptance = conflict_acceptance(
        &conflicted,
        store_root_hash,
        first_membership.clone(),
        &first_owner,
    );
    let resolution_value = conflicted
        .signed_conflict_resolution(
            store_root_hash,
            MembershipConflictSelection::RevocationBranch {
                heads: resolver_branch.clone(),
            },
            first_membership.clone(),
            first_acceptance.clone(),
            &first_owner,
        )
        .expect("branch Owner resolves the conflict");
    let mut forged_resolution = resolution_value.clone();
    forged_resolution.signature =
        keys::sign_hex(&second_owner, &forged_resolution.canonical_bytes()).1;
    assert!(!forged_resolution.verify_signature());
    assert!(!forged_resolution.verify_against(
        store_root_hash,
        conflicted.conflict().expect("cycle conflict"),
    ));
    let second_membership = membership_anchor("second-cycle-resolution");
    let second_acceptance = conflict_acceptance(
        &conflicted,
        store_root_hash,
        second_membership.clone(),
        &second_owner,
    );
    let second_resolution_value = conflicted
        .signed_conflict_resolution(
            store_root_hash,
            MembershipConflictSelection::RevocationBranch {
                heads: second_resolver_branch,
            },
            second_membership,
            second_acceptance,
            &second_owner,
        )
        .expect("other branch Owner resolves the conflict");
    let retried = conflicted
        .signed_conflict_resolution(
            store_root_hash,
            MembershipConflictSelection::RevocationBranch {
                heads: resolver_branch,
            },
            first_membership,
            first_acceptance,
            &first_owner,
        )
        .expect("same resolver retry");
    assert_eq!(resolution_value, retried);
    assert!(resolution_value.verify_against(
        store_root_hash,
        conflicted.conflict().expect("cycle conflict"),
    ));
    let resolution = exact_resolution(resolution_value);
    let second_resolution = exact_resolution(second_resolution_value);
    let resolved_once = conflicted
        .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
        .expect("one resolution applies");
    let resolved_duplicate = conflicted
        .resolved_with(store_root_hash, &[resolution.clone(), resolution.clone()])
        .expect("an exact retry is idempotent");
    assert_eq!(resolved_once, resolved_duplicate);
    assert!(resolved_once
        .grants
        .get(&resolution.1.replacement_grant)
        .and_then(GrantState::active)
        .is_some());
    assert!(resolution
        .1
        .retired_owner_grants
        .iter()
        .all(|grant| resolved_once
            .grants
            .get(grant)
            .and_then(GrantState::active)
            .is_none()));

    let resolved_union = conflicted
        .resolved_with(
            store_root_hash,
            &[resolution.clone(), second_resolution.clone()],
        )
        .expect("distinct resolvers are unioned");
    assert!(resolved_union
        .grants
        .get(&resolution.1.replacement_grant)
        .and_then(GrantState::active)
        .is_some());
    assert!(resolved_union
        .grants
        .get(&second_resolution.1.replacement_grant)
        .and_then(GrantState::active)
        .is_some());

    let mut branch_specific = conflicted.conflict().expect("cycle conflict").clone();
    let MembershipConflict::RevocationCycle {
        maximal_valid_branches,
        ..
    } = &mut branch_specific
    else {
        unreachable!()
    };
    let branch_only_grant = MembershipGrantId(ObjectHash::digest(b"branch-only grant"));
    let branch_only_creation = maximal_valid_branches[0].effective_frontier[0].clone();
    maximal_valid_branches[0].grants.insert(
        branch_only_grant.clone(),
        GrantState::Active {
            record: MembershipGrantRecord {
                member_pubkey: keys::public_key_hex(&key()),
                role: StoreMembershipRoleGrant::Member,
                provider_account_email: None,
                creation_authority: MembershipGrantCreationAuthority::Entry(branch_only_creation),
            },
        },
    );
    let branch_barrier = MergeMembershipGrantRetirementBarrier::NonOwner {
        author_streams: StoreGrantStreamBarrier {
            observed_streams: Vec::new(),
        },
    };
    let mut branch_resolution_value = resolution.1.clone();
    branch_resolution_value
        .retirement_barriers
        .insert(branch_only_grant.clone(), branch_barrier.clone());
    branch_resolution_value.signature =
        keys::sign_hex(&first_owner, &branch_resolution_value.canonical_bytes()).1;
    let branch_resolution = exact_resolution(branch_resolution_value);
    let mut branch_second_resolution_value = second_resolution.1.clone();
    branch_second_resolution_value
        .retirement_barriers
        .insert(branch_only_grant.clone(), branch_barrier);
    branch_second_resolution_value.signature = keys::sign_hex(
        &second_owner,
        &branch_second_resolution_value.canonical_bytes(),
    )
    .1;
    let branch_second_resolution = exact_resolution(branch_second_resolution_value);
    let composed = resolve_store_membership_conflict(
        store_root_hash,
        &branch_specific,
        &[branch_resolution.clone(), branch_second_resolution.clone()],
    )
    .expect("retire grants not agreed by every valid branch");
    let branch_only_retirements = composed
        .grants
        .get(&branch_only_grant)
        .and_then(GrantState::retirements)
        .expect("branch-only grant is retained as retired");
    assert!(branch_only_retirements.iter().any(|retirement| matches!(
        retirement,
        MembershipGrantRetirement::ConflictResolution { authority, .. }
            if authority == &branch_resolution.0
    )));
    assert!(branch_only_retirements.iter().any(|retirement| matches!(
        retirement,
        MembershipGrantRetirement::ConflictResolution { authority, .. }
            if authority == &branch_second_resolution.0
    )));

    let mut duplicate_member = branch_specific;
    let MembershipConflict::RevocationCycle {
        maximal_valid_branches,
        ..
    } = &mut duplicate_member
    else {
        unreachable!()
    };
    let duplicate_pubkey = keys::public_key_hex(&key());
    let duplicate_creation = resolution.1.conflicting_heads[0].coord.clone();
    for branch in maximal_valid_branches {
        for suffix in [b'a', b'b'] {
            branch.grants.insert(
                MembershipGrantId(ObjectHash::digest(&[suffix])),
                GrantState::Active {
                    record: MembershipGrantRecord {
                        member_pubkey: duplicate_pubkey.clone(),
                        role: StoreMembershipRoleGrant::Member,
                        provider_account_email: None,
                        creation_authority: MembershipGrantCreationAuthority::Entry(
                            duplicate_creation.clone(),
                        ),
                    },
                },
            );
        }
    }
    assert!(matches!(
        resolve_store_membership_conflict(
            store_root_hash,
            &duplicate_member,
            &[resolution.clone(), second_resolution.clone()],
        ),
        Err(MembershipError::InvalidConflictResolution)
    ));

    let mut resumed = conflicted.clone();
    let raw_heads = resumed.author_heads();
    resumed
        .apply_resolutions(store_root_hash, std::slice::from_ref(&resolution))
        .expect("resolution activates replacement Owner grant");
    assert_eq!(resumed.author_heads(), raw_heads);
    let accepted_controls = [remove_second.coord(), remove_first.coord()];
    assert!(accepted_controls
        .iter()
        .all(|coord| resumed.contains_coord(coord)));
    assert!(accepted_controls
        .iter()
        .any(|coord| !resumed.included.contains(coord)));
    let raw_losing_control = accepted_controls
        .iter()
        .find(|coord| !resumed.included.contains(*coord))
        .expect("resolved history retains one raw losing control")
        .clone();
    let checkpoint_floor = crate::protocol::store_commit::MembershipCausalFloor {
        effective_coordinates: vec![raw_losing_control],
        resolutions: resumed.resolution_refs().to_vec(),
    };
    assert!(
            !checkpoint_floor.is_included_in(&resumed),
            "a coordinate present only in the raw losing branch cannot satisfy a retained effective checkpoint floor",
        );
    assert_eq!(
        resumed.effective_frontier(),
        resolver_branch_state.effective_frontier
    );
    assert_eq!(
        resumed.resolution_refs(),
        std::slice::from_ref(&resolution.0)
    );
    let after_resolution = resumed
        .signed_set_member_in_stream(
            &first_owner,
            stream(37),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "write after resolution".to_string(),
        )
        .expect("replacement Owner can author from a fresh stream");
    assert_eq!(
        after_resolution.author_owner_grant,
        resolution.1.replacement_grant
    );
    let activated_head = exact_head(&after_resolution, &first_owner).1;
    resumed
        .add_entry(after_resolution)
        .expect("future authoring validates from the resolved checkpoint");
    assert_eq!(activated_head.body.resolutions, vec![resolution.0.clone()]);
    let authority = MembershipGrantCreationAuthority::ConflictResolution(resolution.0.clone());
    assert!(resumed.authorizes_write_authority(&authority, &first_pubkey));
    let outsider = key();
    let outsider_membership = membership_anchor("non-owner-cycle-resolution");
    let outsider_acceptance = conflict_acceptance(
        &conflicted,
        store_root_hash,
        outsider_membership.clone(),
        &outsider,
    );
    assert!(matches!(
        conflicted.signed_conflict_resolution(
            store_root_hash,
            resolution.1.selection.clone(),
            outsider_membership,
            outsider_acceptance,
            &outsider,
        ),
        Err(MembershipError::SignerIsNotOwner(_))
    ));
}

#[test]
fn dependency_frontier_must_be_strictly_ordered_by_author_stream() {
    let founder = key();
    let second_owner = key();
    let mut chain = founded("store", &founder);
    chain
        .add_owner_for_test(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add owner".to_string(),
        )
        .unwrap();
    let second_stream = chain
        .signed_set_member_in_stream(
            &second_owner,
            stream(31),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "second stream".to_string(),
        )
        .unwrap();
    chain.add_entry(second_stream).unwrap();
    let mut unsorted = chain
        .signed_set_member_in_stream(
            &founder,
            stream(1),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "candidate".to_string(),
        )
        .unwrap();
    assert!(unsorted.dependencies.len() > 1);
    unsorted.dependencies.reverse();
    sign_membership_entry(&mut unsorted, &founder);

    assert!(matches!(
        chain.add_entry(unsorted),
        Err(MembershipError::NonCanonicalDependencyFrontier { .. })
    ));
}

#[test]
fn owner_barrier_must_be_strictly_ordered_by_author_stream() {
    let founder = key();
    let second_owner = key();
    let second_owner_pubkey = keys::public_key_hex(&second_owner);
    let mut chain = founded("store", &founder);
    chain
        .add_owner_for_test(
            &founder,
            stream(1),
            second_owner_pubkey.clone(),
            "add owner".to_string(),
        )
        .unwrap();
    for (stream_id, timestamp) in [(stream(41), "first stream"), (stream(42), "second stream")] {
        let authored = chain
            .signed_set_member_in_stream(
                &second_owner,
                stream_id,
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                timestamp.to_string(),
            )
            .unwrap();
        chain.add_entry(authored).unwrap();
    }
    let mut removal = chain
        .signed_remove_member_in_stream(
            &founder,
            stream(1),
            second_owner_pubkey,
            "remove owner".to_string(),
        )
        .unwrap();
    let MembershipChange::RemoveMember {
        retirement_barriers,
        ..
    } = &mut removal.change
    else {
        unreachable!();
    };
    let observed = &mut retirement_barriers
        .values_mut()
        .next()
        .expect("owner removal barrier")
        .author_streams()
        .observed_streams
        .clone();
    assert!(observed.len() > 1);
    let barrier = retirement_barriers
        .values_mut()
        .next()
        .expect("owner removal barrier");
    match barrier {
        MergeMembershipGrantRetirementBarrier::Owner { barrier } => {
            barrier.author_streams.observed_streams.reverse();
        }
        MergeMembershipGrantRetirementBarrier::NonOwner { .. } => {
            panic!("Owner removal carries non-Owner barrier")
        }
    }
    sign_membership_entry(&mut removal, &founder);

    assert!(matches!(
        chain.add_entry(removal),
        Err(MembershipError::InvalidOwnerRevocationBarrier { .. })
    ));
}

#[test]
fn owner_readd_uses_a_new_sequence_one_stream() {
    let owner = key();
    let second = key();
    let mut chain = founded("store", &owner);
    chain
        .add_owner_for_test(
            &owner,
            stream(1),
            keys::public_key_hex(&second),
            "add".to_string(),
        )
        .unwrap();
    let old_grant = chain
        .active_owner_grant(&keys::public_key_hex(&second))
        .unwrap();
    let remove = chain
        .signed_remove_member_in_stream(
            &owner,
            stream(1),
            keys::public_key_hex(&second),
            "remove".to_string(),
        )
        .unwrap();
    chain.add_entry(remove).unwrap();
    chain
        .add_owner_for_test(
            &owner,
            stream(1),
            keys::public_key_hex(&second),
            "readd".to_string(),
        )
        .unwrap();
    let new_grant = chain
        .active_owner_grant(&keys::public_key_hex(&second))
        .unwrap();
    assert_ne!(old_grant, new_grant);
    let authored = chain
        .signed_set_member_in_stream(
            &second,
            stream(32),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "authored".to_string(),
        )
        .unwrap();
    assert_eq!(authored.seq, 1);
    assert_eq!(authored.author_owner_grant, new_grant);
}

#[test]
fn owner_self_removal_remains_effective_when_its_grant_is_capped_before_first() {
    let founder = key();
    let departing_owner = key();
    let departing_pubkey = keys::public_key_hex(&departing_owner);
    let mut chain = founded("store", &founder);
    chain
        .add_owner_for_test(
            &founder,
            stream(1),
            departing_pubkey.clone(),
            "add owner".to_string(),
        )
        .unwrap();

    let self_removal = chain
        .signed_remove_member_in_stream(
            &departing_owner,
            stream(33),
            departing_pubkey.clone(),
            "self removal".to_string(),
        )
        .unwrap();
    assert!(matches!(
        &self_removal.change,
        MembershipChange::RemoveMember { retirement_barriers, .. }
            if retirement_barriers.values().all(|barrier| barrier.author_streams().observed_streams.is_empty())
    ));
    chain.add_entry(self_removal).unwrap();

    assert!(!chain.is_owner_now(&departing_pubkey));
}

#[test]
fn before_first_barrier_excludes_every_entry_from_the_revoked_owner_stream() {
    let founder = key();
    let second_owner = key();
    let target = key();
    let mut observed = founded("store", &founder);
    observed
        .add_owner_for_test(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add owner".to_string(),
        )
        .unwrap();

    let stale_entry = observed
        .signed_set_member_in_stream(
            &second_owner,
            stream(34),
            keys::public_key_hex(&target),
            None,
            MemberRole::Member,
            "stale entry".to_string(),
        )
        .unwrap();
    let removal = observed
        .signed_remove_member_in_stream(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "remove owner".to_string(),
        )
        .unwrap();
    assert!(matches!(
        &removal.change,
        MembershipChange::RemoveMember { retirement_barriers, .. }
            if retirement_barriers.values().all(|barrier| barrier.author_streams().observed_streams.is_empty())
    ));

    let mut entries = observed.entries().to_vec();
    entries.extend([removal, stale_entry]);
    let chain = MembershipChain::from_entries(entries).unwrap();
    assert!(!chain.can_write_now(&keys::public_key_hex(&target)));
    assert!(chain
        .author_heads()
        .iter()
        .any(|coord| coord.author_pubkey == keys::public_key_hex(&second_owner)));
    assert!(chain
        .effective_frontier()
        .iter()
        .all(|coord| coord.author_pubkey != keys::public_key_hex(&second_owner)));
}

#[test]
fn through_barrier_keeps_its_exact_prefix_and_prunes_the_stale_suffix() {
    let founder = key();
    let second_owner = key();
    let first_target = key();
    let second_target = key();
    let third_target = key();
    let mut observed = founded("store", &founder);
    observed
        .add_owner_for_test(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add owner".to_string(),
        )
        .unwrap();
    let first = observed
        .signed_set_member_in_stream(
            &second_owner,
            stream(35),
            keys::public_key_hex(&first_target),
            None,
            MemberRole::Member,
            "first".to_string(),
        )
        .unwrap();
    observed.add_entry(first.clone()).unwrap();

    let removal = observed
        .signed_remove_member_in_stream(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "remove owner".to_string(),
        )
        .unwrap();
    assert!(matches!(
        &removal.change,
        MembershipChange::RemoveMember { retirement_barriers, .. }
            if retirement_barriers.values().any(|barrier| barrier.author_streams().observed_streams == vec![first.coord()])
    ));

    let second = observed
        .signed_set_member_in_stream(
            &second_owner,
            stream(35),
            keys::public_key_hex(&second_target),
            None,
            MemberRole::Member,
            "second".to_string(),
        )
        .unwrap();
    let mut exact_entries = observed.entries().to_vec();
    exact_entries.extend([removal.clone(), second.clone()]);
    let exact = MembershipChain::from_entries(exact_entries).unwrap();
    assert!(exact.can_write_now(&keys::public_key_hex(&first_target)));
    assert!(!exact.can_write_now(&keys::public_key_hex(&second_target)));

    let mut stale = observed;
    stale.add_entry(second).unwrap();
    let third = stale
        .signed_set_member_in_stream(
            &second_owner,
            stream(35),
            keys::public_key_hex(&third_target),
            None,
            MemberRole::Member,
            "third".to_string(),
        )
        .unwrap();
    stale.add_entry(third.clone()).unwrap();
    let mut beyond_entries = stale.entries().to_vec();
    beyond_entries.push(removal);
    let pruned = MembershipChain::from_entries(beyond_entries).unwrap();
    assert!(pruned.can_write_now(&keys::public_key_hex(&first_target)));
    assert!(!pruned.can_write_now(&keys::public_key_hex(&second_target)));
    assert!(!pruned.can_write_now(&keys::public_key_hex(&third_target)));
}

#[test]
fn through_barrier_rejects_a_coordinate_hash_that_is_not_its_dependency() {
    let founder = key();
    let second_owner = key();
    let mut chain = founded("store", &founder);
    chain
        .add_owner_for_test(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add owner".to_string(),
        )
        .unwrap();
    let authored = chain
        .signed_set_member_in_stream(
            &second_owner,
            stream(36),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "authored".to_string(),
        )
        .unwrap();
    chain.add_entry(authored).unwrap();
    let mut removal = chain
        .signed_remove_member_in_stream(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "remove owner".to_string(),
        )
        .unwrap();
    let MembershipChange::RemoveMember {
        retirement_barriers,
        ..
    } = &mut removal.change
    else {
        unreachable!();
    };
    let barrier = retirement_barriers
        .values_mut()
        .next()
        .expect("owner removal barrier");
    let MergeMembershipGrantRetirementBarrier::Owner { barrier } = barrier else {
        panic!("Owner removal carries non-Owner barrier")
    };
    let barrier = barrier
        .author_streams
        .observed_streams
        .first_mut()
        .expect("observed owner stream");
    barrier.entry_hash = ObjectHash::digest(b"wrong barrier hash");
    sign_membership_entry(&mut removal, &founder);
    assert!(matches!(
        chain.add_entry(removal),
        Err(MembershipError::InvalidOwnerRevocationBarrier { .. })
    ));
}

#[test]
fn cross_store_replay_fails_even_with_the_same_founder_key() {
    let owner = key();
    let from_a = test_founder_entry("store-a", &owner, "founder", membership_anchor("store-a"));
    let mut replayed = from_a.clone();
    replayed.store_id = "store-b".to_string();
    assert!(!verify_membership_entry(&replayed));
    assert!(MembershipChain::from_entries(vec![from_a])
        .unwrap()
        .is_founded_by(&keys::public_key_hex(&owner)));
}

#[test]
fn created_at_is_signed_but_never_orders_entries() {
    let owner = key();
    let entry = test_founder_entry("store", &owner, "display-time", membership_anchor("store"));
    let mut tampered = entry.clone();
    tampered.created_at = "other".to_string();
    assert!(!verify_membership_entry(&tampered));
}

#[test]
fn membership_head_resolution_cut_must_equal_its_tip_entry_cut() {
    let owner = UserKeypair::generate();
    let entry = test_founder_entry(
        "head-tip-resolution-cut",
        &owner,
        "founder",
        membership_anchor("head-tip-resolution-cut"),
    );
    let fake = StoreMembershipConflictResolutionRef {
        conflict_hash: ObjectHash::digest(b"head-tip conflict"),
        resolver_pubkey: keys::public_key_hex(&owner),
        resolution_hash: ObjectHash::digest(b"head-tip resolution"),
        object: exact(
            "test/head-tip-resolution-cut/resolution.json",
            b"head-tip resolution",
        ),
    };
    let head = exact_head_with_resolutions(&entry, &owner, vec![fake]);

    assert!(matches!(
        MembershipChain::from_entries_with_coords_and_heads(
            vec![(entry.coord(), entry)],
            vec![head],
        ),
        Err(MembershipError::MissingConflictHeads)
    ));
}

#[test]
fn membership_entry_rejects_unsorted_or_duplicate_resolution_dependencies() {
    let owner = UserKeypair::generate();
    let founder = test_founder_entry(
        "entry-resolution-cut",
        &owner,
        "founder",
        membership_anchor("entry-resolution-cut"),
    );
    let chain = MembershipChain::from_entries(vec![founder]).unwrap();
    let entry = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            keys::public_key_hex(&UserKeypair::generate()),
            None,
            MemberRole::Member,
            "member".to_string(),
        )
        .unwrap();
    let mut refs = [b"first".as_slice(), b"second".as_slice()]
        .into_iter()
        .map(|label| StoreMembershipConflictResolutionRef {
            conflict_hash: ObjectHash::digest(label),
            resolver_pubkey: keys::public_key_hex(&owner),
            resolution_hash: ObjectHash::digest(&[label, b" resolution"].concat()),
            object: exact(
                format!(
                    "test/entry-resolution-cut/{}.json",
                    String::from_utf8_lossy(label)
                ),
                label,
            ),
        })
        .collect::<Vec<_>>();
    refs.sort();

    let mut unsorted = entry.clone();
    unsorted.resolution_dependencies = refs.iter().rev().cloned().collect();
    sign_membership_entry(&mut unsorted, &owner);
    assert!(!verify_membership_entry(&unsorted));

    let mut duplicate = entry;
    duplicate.resolution_dependencies = vec![refs[0].clone(), refs[0].clone()];
    sign_membership_entry(&mut duplicate, &owner);
    assert!(!verify_membership_entry(&duplicate));
}
