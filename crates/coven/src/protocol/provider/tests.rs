use super::*;
use crate::protocol::provider::test_fixtures::{
    test_device_binding, test_exact_receipt, test_store_binding,
};
use crate::protocol::store_commit::ObjectHash;

#[test]
fn credential_rotation_generation_exhaustion_is_not_a_successor() {
    assert!(ProviderAccessWithdrawal::S3CredentialRotation {
        retired_generation: u64::MAX,
        active_generation: u64::MAX,
        retired_credential_verified_rejected: true,
    }
    .validate()
    .is_err());
}

#[test]
fn exact_probe_verifier_rejects_two_created_contenders() {
    let mut receipt = test_exact_receipt();
    receipt
        .verify(&test_store_binding(), &test_device_binding())
        .expect("baseline exact receipt verifies");
    receipt.transcript.contenders[1].outcome = ProbeCreateOutcome::Created;

    assert!(receipt
        .verify(&test_store_binding(), &test_device_binding())
        .is_err());
}

#[test]
fn custom_s3_origin_rejects_paths_and_canonicalizes_default_port() {
    assert_eq!(
        canonical_custom_s3_origin("HTTPS://Objects.Example:443").unwrap(),
        "https://objects.example"
    );
    assert!(canonical_custom_s3_origin("https://objects.example/").is_err());
    assert!(canonical_custom_s3_origin("https://objects.example/bucket").is_err());
}

#[test]
fn private_cloudkit_owner_exposes_its_exact_administrator_locator() {
    let binding = private_cloudkit_binding();

    let locator = ProviderAccessLocator::for_current_administrator(&binding)
        .expect("private CloudKit owner exposes its exact administrator locator");

    assert_eq!(
        locator,
        ProviderAccessLocator::CloudKitPrivateZoneOwner {
            owner_name: "private-owner".to_string(),
            zone_name: "private-zone".to_string(),
            owner_record_name: "current-user".to_string(),
        }
    );
    locator
        .validate_for(&binding.store, &binding.device)
        .expect("private CloudKit owner locator matches its binding");
}

#[test]
fn shared_cloudkit_participant_is_not_treated_as_the_zone_owner() {
    let mut binding = private_cloudkit_binding();
    binding.device.principal =
        crate::protocol::objects::ProviderPrincipalId::CloudKitSharedZoneParticipant {
            record_name: "current-user".to_string(),
        };

    assert!(ProviderAccessLocator::for_current_administrator(&binding).is_err());
}

#[test]
fn provider_admin_grants_coexist_and_replay_exactly() {
    let founder = admin_record(1, "founder");
    let mut state = ProviderAdminState::founder(founder.clone());
    let second = admin_record(2, "second");
    let change = set_change(&second, BTreeSet::new());
    state
        .apply(change.clone(), second.created_at.clone())
        .expect("a second administrator may coexist");
    state
        .apply(change, second.created_at.clone())
        .expect("an exact replay is idempotent");
    assert_eq!(state.active().len(), 2);
    assert!(state.tombstones().is_empty());
}

#[test]
fn provider_admin_rejects_conflicting_id_reuse() {
    let founder = admin_record(1, "founder");
    let mut state = ProviderAdminState::founder(founder);
    let second = admin_record(2, "second");
    state
        .apply(
            set_change(&second, BTreeSet::new()),
            second.created_at.clone(),
        )
        .unwrap();
    let mut conflicting = second.clone();
    conflicting.provider = ProviderDeviceBinding {
        principal: crate::protocol::objects::ProviderPrincipalId::Aws {
            account_id: "999999999999".to_string(),
            principal: crate::protocol::objects::AwsPrincipal::Root,
        },
    };
    assert_eq!(
        state.apply(
            set_change(&conflicting, BTreeSet::new()),
            conflicting.created_at.clone()
        ),
        Err(ProviderAdminReducerError::GrantIdReuse)
    );
}

#[test]
fn provider_admin_removal_and_replacement_retain_tombstones() {
    let founder = admin_record(1, "founder");
    let founder_id = founder.grant_id.clone();
    let mut state = ProviderAdminState::founder(founder);
    let replacement = admin_record(2, "replacement");
    state
        .apply(
            set_change(&replacement, BTreeSet::from([founder_id.clone()])),
            replacement.created_at.clone(),
        )
        .unwrap();
    assert!(state.records().contains_key(&founder_id));
    assert!(state.tombstones().contains(&founder_id));
    assert_eq!(
        state.apply(
            ProviderAdminChange::Remove {
                removes: BTreeSet::from([replacement.grant_id.clone()]),
            },
            replacement.created_at.clone(),
        ),
        Err(ProviderAdminReducerError::NoEffectiveAdministrator)
    );
    assert!(state.records().contains_key(&replacement.grant_id));
    assert!(!state.tombstones().contains(&replacement.grant_id));
}

#[test]
fn provider_admin_replay_cannot_tombstone_a_newly_active_replacement() {
    let founder = admin_record(1, "founder");
    let mut state = ProviderAdminState::founder(founder);
    let second = admin_record(2, "second");
    state
        .apply(
            set_change(&second, BTreeSet::new()),
            second.created_at.clone(),
        )
        .unwrap();
    let third = admin_record(3, "third");
    state
        .apply(
            set_change(&third, BTreeSet::new()),
            third.created_at.clone(),
        )
        .unwrap();
    assert_eq!(
        state.apply(
            set_change(&second, BTreeSet::from([third.grant_id.clone()])),
            second.created_at.clone(),
        ),
        Err(ProviderAdminReducerError::UnknownReplacement)
    );
    assert!(state.active().contains(&third.grant_id));
}

fn admin_record(id: u8, label: &str) -> ProviderAdminGrantRecord {
    let root_object = crate::protocol::objects::ExactObjectRef::new(
        crate::protocol::objects::ObjectSlot::logical(format!("roots/{label}")).unwrap(),
        1,
        ObjectHash::digest(&[id]),
    );
    let root = StoreRootRef {
        store_root_id: ObjectHash::digest(format!("{label} id").as_bytes()),
        store_root_hash: ObjectHash::digest(label.as_bytes()),
        object: root_object,
    };
    let registration: StoreDeviceRegistrationRef = serde_json::from_value(serde_json::json!({
            "device_id": ObjectHash::digest(&[id, 1]),
            "registration_hash": ObjectHash::digest(&[id, 2]),
            "object": {
                "slot": {"logical_key": format!("registrations/{label}"), "physical": {"kind": "logical_key"}},
                "stored_size": 1,
                "stored_hash": ObjectHash::digest(&[id, 3]),
            }
        }))
        .unwrap();
    ProviderAdminGrantRecord {
        grant_id: ProviderAdminGrantId(ObjectHash::digest(&[id, 4])),
        administrator: registration,
        provider: test_device_binding(),
        access: ProviderAccessLocator::S3SharedCredentialGeneration {
            generation: 1,
            access_key_id_hash: ObjectHash::digest(b"test access key"),
        },
        capability: ProviderCapabilityProof {
            exact_slots: test_exact_receipt(),
        },
        created_at: ProviderAdminGrantOrigin::Founder { root },
    }
}

fn set_change(
    record: &ProviderAdminGrantRecord,
    replaces: BTreeSet<ProviderAdminGrantId>,
) -> ProviderAdminChange {
    ProviderAdminChange::Set {
        administrator: record.administrator.clone(),
        provider: record.provider.clone(),
        access: record.access.clone(),
        capability: record.capability.clone(),
        grant_id: record.grant_id.clone(),
        replaces,
    }
}

fn private_cloudkit_binding() -> crate::protocol::objects::ResolvedProviderBinding {
    crate::protocol::objects::ResolvedProviderBinding {
        store: crate::protocol::objects::StoreProviderBinding::CloudKit {
            container_id: "iCloud.example.coven".to_string(),
            environment: crate::protocol::objects::CloudKitEnvironment::Development,
            owner_name: "private-owner".to_string(),
            zone_name: "private-zone".to_string(),
        },
        device: crate::protocol::objects::ProviderDeviceBinding {
            principal: crate::protocol::objects::ProviderPrincipalId::CloudKitPrivateZoneOwner {
                record_name: "current-user".to_string(),
            },
        },
    }
}
