//! The founder Circle activation every Circle test starts from: the signed
//! protocol values, and the current state they derive.

use crate::circle_activation::{CircleCurrentState, VerifiedCircleReference};
use coven_keys::keys::{UserKeypair, SIGN_SECRETKEYBYTES};

pub fn test_circle_owner_keypair() -> UserKeypair {
    let keypair_bytes: [u8; SIGN_SECRETKEYBYTES] = hex::decode(concat!(
        "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
        "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c"
    ))
    .expect("fixed Circle signing key is hexadecimal")
    .try_into()
    .expect("fixed Circle signing key is 64 bytes");
    UserKeypair::from_signing_key_bytes(&keypair_bytes).expect("fixed Circle signing key is valid")
}

/// The founder Circle every layer's Circle tests are built on: a signed device
/// registration, a resolved founder membership, and the founder transition
/// whose verified reference derives the current state. `active` decides whether
/// this device holds Circle access — an inactive Circle is one it was never
/// granted.
pub struct TestCircleActivation {
    pub circle_id: crate::circle::CircleId,
    pub control: crate::circle::PreparedCircleControl,
    pub owner_pubkey: String,
    pub current: CircleCurrentState,
}

pub fn test_circle_activation(label: &str, active: bool) -> TestCircleActivation {
    use std::collections::BTreeMap;

    use crate::circle::{
        CircleRole, CircleRosterDraftPolicy, CircleRosterPolicyObjects, CircleTransitionDraft,
        CircleTransitionPolicyObjects, PreparedCircleTransition, StoreMembershipStateRef,
    };
    use crate::circle_activation::{VerifiedCircleAccess, VerifiedCircleActive};
    use crate::membership::{MemberRole, MembershipChain, MembershipHeadRef};
    use crate::objects::ExactObjectRef;
    use crate::objects::ObjectSlot;
    use crate::store_commit::{
        CandidateFamilyId, CircleActivationObjects, CircleEntryOrigin, CircleMetadataObjectRef,
        CircleRosterEntryRef, DeviceStreamAnchor, GrantStreamAnchor, ObjectHash, StoreCreationId,
        StoreDeviceRegistration, StoreDeviceRegistrationOrigin, StoreRootRef,
    };

    fn exact_object(label: &str, bytes: &[u8]) -> ExactObjectRef {
        ExactObjectRef::new(
            ObjectSlot::logical(format!("store-v1/test/{label}.json"))
                .expect("valid test object slot"),
            bytes.len() as u64,
            ObjectHash::digest(bytes),
        )
    }

    let owner = test_circle_owner_keypair();
    let owner_pubkey = coven_keys::keys::public_key_hex(&owner);
    let store_root_hash = ObjectHash::digest(format!("{label} Store root").as_bytes());
    let root_bytes = format!("{label} root").into_bytes();
    let root = StoreRootRef {
        store_root_id: ObjectHash::digest(format!("{label} identity").as_bytes()),
        store_root_hash,
        object: exact_object(&format!("{label}/root"), &root_bytes),
    };
    let registration_origin = StoreDeviceRegistrationOrigin::Founder {
        creation_id: StoreCreationId::from_random_bytes(
            *ObjectHash::digest(label.as_bytes()).as_bytes(),
        ),
    };
    let registration = StoreDeviceRegistration::signed(
        root.clone(),
        registration_origin,
        crate::objects::ProviderDeviceBinding {
            principal: crate::objects::ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(
                    format!("{label} registration access key").as_bytes(),
                ),
            },
        },
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: ObjectSlot::logical(format!(
                "store-v1/test/{label}/acknowledgements/1.json"
            ))
            .expect("valid test Store acknowledgement slot"),
        },
        &owner,
    )
    .expect("sign test Store device registration");
    let membership_anchor = GrantStreamAnchor::StoreMembership {
        first_slot: ObjectSlot::logical(format!("store-v1/test/{label}/membership/1.json"))
            .expect("valid test membership slot"),
    };
    let founder = crate::membership::founder_entry(
        label,
        &owner,
        crate::causal_grants::MembershipGrantId::from_test_label(label),
        "founder",
        membership_anchor,
        crate::provider::FounderProviderAdminGrant::from_test_label(label),
    );
    let founder_coord = founder.coord();
    let chain =
        MembershipChain::from_entries(vec![founder.clone()]).expect("found test membership");
    let resolved = chain.resolved();
    let head = MembershipHeadRef {
        coord: founder_coord.clone(),
        head_hash: ObjectHash::digest(format!("{label} membership head").as_bytes()),
        object: exact_object(&format!("{label}/membership-head"), b"test membership head"),
    };
    let membership =
        StoreMembershipStateRef::from_parts(vec![head], Vec::new(), resolved.state_hash)
            .expect("valid test membership reference");
    let membership_authority = founder_coord;
    let candidate_family = CandidateFamilyId::from_hash(ObjectHash::digest(
        format!("{label} candidate family").as_bytes(),
    ));
    let ids = coven_foundation::id_provider::SequentialIdProvider::new(label);
    let draft = CircleTransitionDraft::founder(
        store_root_hash,
        candidate_family,
        &registration.device_id.to_string(),
        label,
        "0000000001000-0000-test",
        membership,
        membership_authority,
        vec![(owner_pubkey.clone(), MemberRole::Owner)],
        &ids,
        &owner,
    )
    .expect("construct test Circle");
    let control_object = exact_object(&format!("{label}/control"), &draft.control.bytes);
    let metadata_bytes = serde_json::to_vec(&draft.metadata).expect("serialize test metadata");
    let metadata_object = exact_object(&format!("{label}/metadata"), &metadata_bytes);
    let mut roster_entries = BTreeMap::new();
    let metadata_entries = BTreeMap::from([(
        draft.metadata.coord(),
        CircleMetadataObjectRef {
            key_fingerprint: draft.metadata.key_fingerprint,
            object: metadata_object.clone(),
            origin: CircleEntryOrigin::Introduced,
        },
    )]);
    let policy_objects = {
        let CircleRosterDraftPolicy::Founder {
            entry: roster_entry,
        } = &draft.policy.roster
        else {
            panic!("founder Circle contains a founder roster entry");
        };
        let roster_entry = roster_entry.clone();
        let roster_bytes =
            serde_json::to_vec(&roster_entry).expect("serialize test Circle roster entry");
        let roster_object = exact_object(&format!("{label}/roster-entry"), &roster_bytes);
        roster_entries.insert(
            roster_entry.coord(),
            CircleRosterEntryRef {
                object: roster_object,
                origin: CircleEntryOrigin::Introduced,
            },
        );
        CircleTransitionPolicyObjects {
            roster: Some(CircleRosterPolicyObjects {
                entry: roster_entry,
            }),
            metadata: Some(draft.metadata.clone()),
        }
    };
    let creation = PreparedCircleTransition {
        circle_id: draft.circle_id,
        epoch_id: draft.epoch_id,
        keyring: draft.keyring,
        roster: draft.roster,
        policy_objects,
        metadata: draft.metadata,
        close_intent: draft.close_intent,
        close_outcome: None,
        close_cancellation: None,
        access: draft.access,
        control: draft.control,
    };
    let objects = CircleActivationObjects {
        control: control_object,
        close_intent: None,
        close_outcome: None,
        close_cancellation: None,
        roster_entries,
        metadata_entries,
        bootstraps: Vec::new(),
    };
    let reference = creation.control_ref(objects);
    let control = creation.control.clone();
    let own_access = creation
        .access
        .iter()
        .find(|access| access.value.recipient_pubkey == owner_pubkey)
        .expect("test Circle owner access");
    let activation = VerifiedCircleReference {
        reference,
        circle_id: creation.circle_id,
        control: control.clone(),
        local_access: active.then(|| VerifiedCircleAccess {
            leaf: own_access.clone(),
            active: Some(VerifiedCircleActive {
                roster: creation.resolved_roster(),
                metadata: creation.metadata.clone(),
            }),
        }),
    };
    let current = CircleCurrentState::from_verified(candidate_family, &activation)
        .expect("derive test Circle current state");
    assert_eq!(
        creation.resolved_roster().members().get(&owner_pubkey),
        Some(&CircleRole::Owner)
    );
    TestCircleActivation {
        circle_id: creation.circle_id,
        control,
        owner_pubkey,
        current,
    }
}
