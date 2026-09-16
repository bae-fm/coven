use super::*;
use crate::causal_grants::GrantRetirements;
use crate::objects::ObjectSlot;
use crate::objects::{ProviderDeviceBinding, ProviderPrincipalId};
use crate::store_commit::{
    membership_entry_semantic_prefix, membership_head_semantic_prefix,
    registration_semantic_prefix, DeviceStreamAnchor, GrantStreamAnchor, MembershipCausalFloor,
    StoreCreationId, StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreRootRef,
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

fn test_founder_entry(
    store_id: &str,
    owner: &UserKeypair,
    created_at: &str,
    membership: GrantStreamAnchor,
) -> MembershipEntry {
    founder_entry(
        store_id,
        owner,
        crate::causal_grants::MembershipGrantId::from_test_label(store_id),
        created_at,
        membership,
        crate::provider::FounderProviderAdminGrant::from_test_label(store_id),
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
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot(format!("test/{label}/acks/1.json")),
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

fn exact_head(entry: &MembershipEntry, signer: &UserKeypair) -> (MembershipHeadRef, AuthorHead) {
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
        .get_mut("body")
        .and_then(serde_json::Value::as_object_mut)
        .expect("membership head body object")
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
    let StoreAuthorityChange::SetMember { grant_id, .. } = &addition.change else {
        unreachable!()
    };
    let grant_id = grant_id.clone();
    chain.add_entry(addition).unwrap();
    let resolved = chain.resolved();
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
    let resolved = chain.resolved();
    assert!(matches!(
        &resolved.grants[&grant_id],
        GrantState::Tombstoned { record, retirements }
            if record.member_pubkey == member_pubkey
                && retirements.as_set() == &BTreeSet::from([MembershipGrantRetirement {
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
    retirements.insert(MembershipGrantRetirement {
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
        store_membership_state_hash(&altered, &resolved.provider_administrator)
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
    let StoreAuthorityChange::SetMember {
        grant_id: candidate,
        ..
    } = &mut reuse.body_mut().change
    else {
        unreachable!()
    };
    *candidate = grant_id.clone();
    reuse.resign(&owner);
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
    let StoreAuthorityChange::Founder { owner_grant_id, .. } = &founder.change else {
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
            creation: founder.coord(),
        },
        retirements: GrantRetirements::new(causal_grants::CausalGrantRetirement {
            coord: authority.clone(),
            owner_barrier: None,
        }),
    };

    assert!(matches!(
        map_store_grant_state(&owner_grant_id, &state, &[founder]),
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
        StoreAuthorityChange::SetMember { grant_id, .. } => grant_id.clone(),
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
    let expected = GrantRetirements::new(MembershipGrantRetirement {
        authority: first_removal.coord(),
        barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
            author_streams: StoreGrantStreamBarrier {
                observed_streams: Vec::new(),
            },
        },
    });
    let mut expected = expected;
    expected.insert(MembershipGrantRetirement {
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
    let resolved = chain.resolved();

    assert!(matches!(
        &resolved.grants[&member_grant],
        GrantState::Tombstoned { retirements, .. }
            if retirements.as_set() == expected.as_set()
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
        chain.signed_set_member_with_anchor_and_sealed_key_in_stream(
            &founder,
            stream(1),
            candidate_pubkey,
            None,
            MemberRole::Owner,
            Some(membership_anchor("direct-owner-assignment")),
            test_sealed_store_key(b"direct Owner assignment"),
            "direct Owner assignment".to_string(),
        ),
        Err(MembershipError::OwnerPromotionRequired)
    ));
}

#[test]
fn cross_store_replay_fails_even_with_the_same_founder_key() {
    let owner = key();
    let from_a = test_founder_entry("store-a", &owner, "founder", membership_anchor("store-a"));
    let mut replayed = from_a.clone();
    replayed.body_mut().store_id = "store-b".to_string();
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
    tampered.body_mut().created_at = "other".to_string();
    assert!(!verify_membership_entry(&tampered));
}

#[path = "tests/sealed_keys.rs"]
mod sealed_keys;

#[path = "tests/conflicting_authority.rs"]
mod conflicting_authority;

#[path = "tests/owner_grant_barriers.rs"]
mod owner_grant_barriers;

#[path = "tests/provider_administration.rs"]
mod provider_administration;
