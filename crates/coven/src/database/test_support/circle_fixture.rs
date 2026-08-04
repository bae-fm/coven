use crate::database::DatabaseTestSql;
use crate::keys::{UserKeypair, SIGN_SECRETKEYBYTES};

pub(crate) fn test_circle_owner_keypair() -> UserKeypair {
    let keypair_bytes: [u8; SIGN_SECRETKEYBYTES] = hex::decode(concat!(
        "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
        "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c"
    ))
    .expect("fixed Circle signing key is hexadecimal")
    .try_into()
    .expect("fixed Circle signing key is 64 bytes");
    UserKeypair::from_signing_key_bytes(&keypair_bytes).expect("fixed Circle signing key is valid")
}

impl DatabaseTestSql<'_> {
    pub(crate) fn install_test_active_circle(
        &self,
        label: &str,
    ) -> (
        crate::protocol::circle::CircleId,
        crate::protocol::circle::CircleControlCoord,
    ) {
        self.install_test_circle_current_state(label, true)
    }

    pub(crate) fn install_test_inactive_circle(
        &self,
        label: &str,
    ) -> (
        crate::protocol::circle::CircleId,
        crate::protocol::circle::CircleControlCoord,
    ) {
        self.install_test_circle_current_state(label, false)
    }

    fn install_test_circle_current_state(
        &self,
        label: &str,
        active: bool,
    ) -> (
        crate::protocol::circle::CircleId,
        crate::protocol::circle::CircleControlCoord,
    ) {
        use std::collections::BTreeMap;

        use crate::protocol::circle::{
            CircleMetadataHead, CircleRole, CircleRosterDraftPolicy, CircleRosterHead,
            CircleRosterPolicyObjects, CircleTransitionDraft, CircleTransitionPolicyObjects,
            PreparedCircleTransition, StoreMembershipStateRef,
        };
        use crate::protocol::membership::{
            MemberRole, MembershipChain, MembershipGrantCreationAuthority, MembershipHeadRef,
            MembershipStatus,
        };
        use crate::protocol::objects::ExactObjectRef;
        use crate::protocol::objects::ObjectSlot;
        use crate::protocol::store_commit::{
            CandidateFamilyId, CircleActivationObjects, CircleMetadataObjectRef,
            DeviceStreamAnchor, GrantStreamAnchor, ObjectHash, StoreCreationId,
            StoreDeviceRegistration, StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef,
            StoreRootRef, StreamActivation, SuccessorLink,
        };
        use crate::sync::store::{
            CircleCurrentState, VerifiedCircleAccess, VerifiedCircleActive, VerifiedCircleReference,
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
        let owner_pubkey = crate::keys::public_key_hex(&owner);
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
        let store_commits = DeviceStreamAnchor::StoreAnnouncements {
            first_slot: ObjectSlot::logical(format!("store-v1/test/{label}/announcements/1.json"))
                .expect("valid test Store announcement slot"),
        };
        let registration = StoreDeviceRegistration::signed(
            root.clone(),
            registration_origin,
            crate::protocol::objects::ProviderDeviceBinding {
                principal: crate::protocol::objects::ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: ObjectHash::digest(
                        format!("{label} registration access key").as_bytes(),
                    ),
                },
            },
            store_commits,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: ObjectSlot::logical(format!(
                    "store-v1/test/{label}/acknowledgements/1.json"
                ))
                .expect("valid test Store acknowledgement slot"),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: ObjectSlot::logical(format!("store-v1/test/{label}/snapshots/1.json"))
                    .expect("valid test Store snapshot slot"),
            },
            &owner,
        )
        .expect("sign test Store device registration");
        let registration_bytes = registration.to_bytes();
        let author_registration = StoreDeviceRegistrationRef::from_registration(
            &registration,
            exact_object(&format!("{label}/registration"), &registration_bytes),
        );
        let device_signer = registration
            .device_signer(&owner)
            .expect("derive test Store device signer");
        let membership_anchor = GrantStreamAnchor::StoreMembership {
            first_slot: ObjectSlot::logical(format!("store-v1/test/{label}/membership/1.json"))
                .expect("valid test membership slot"),
        };
        let founder = crate::protocol::membership::founder_entry(
            label,
            &owner,
            crate::protocol::causal_grants::MembershipGrantId::from_test_label(label),
            "founder",
            membership_anchor,
            crate::protocol::provider::FounderProviderAdminGrant::from_test_label(label),
        );
        let founder_coord = founder.coord();
        let chain =
            MembershipChain::from_entries(vec![founder.clone()]).expect("found test membership");
        let MembershipStatus::Resolved(resolved) = chain.status() else {
            panic!("founder membership must resolve")
        };
        let head = MembershipHeadRef {
            coord: founder_coord.clone(),
            head_hash: ObjectHash::digest(format!("{label} membership head").as_bytes()),
            object: exact_object(&format!("{label}/membership-head"), b"test membership head"),
        };
        let membership = StoreMembershipStateRef::from_parts(
            vec![head],
            Vec::new(),
            Vec::new(),
            resolved.state_hash,
        )
        .expect("valid test membership reference");
        let membership_authority = MembershipGrantCreationAuthority::Entry(founder_coord);
        let candidate_family = CandidateFamilyId::from_hash(ObjectHash::digest(
            format!("{label} candidate family").as_bytes(),
        ));
        let ids = crate::id_provider::SequentialIdProvider::new(label);
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
        let mut roster_heads = Vec::new();
        let metadata_entries = BTreeMap::from([(
            draft.metadata.coord(),
            CircleMetadataObjectRef {
                key_fingerprint: draft.metadata.key_fingerprint,
                object: metadata_object.clone(),
            },
        )]);
        let mut metadata_heads = Vec::new();
        let (policy_objects, head_object) = {
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
            roster_entries.insert(roster_entry.coord(), roster_object.clone());

            let roster_head_slot =
                ObjectSlot::logical(format!("store-v1/test/{label}/circle-roster-head/1.json"))
                    .expect("valid test Circle roster-head slot");
            let roster_activation = StreamActivation::grant_authorized(
                store_root_hash,
                author_registration.clone(),
                roster_entry.author_owner_grant.clone(),
                GrantStreamAnchor::CircleRoster {
                    circle_id: draft.circle_id,
                    first_slot: roster_head_slot.clone(),
                },
            );
            let roster_head = CircleRosterHead::signed(
                &roster_entry,
                roster_object,
                SuccessorLink {
                    activation: roster_activation.activation_id(),
                    predecessor: None,
                    next_slot: ObjectSlot::logical(format!(
                        "store-v1/test/{label}/circle-roster-head/2.json"
                    ))
                    .expect("valid next test Circle roster-head slot"),
                },
                &device_signer,
            );
            let roster_head_bytes =
                serde_json::to_vec(&roster_head).expect("serialize test Circle roster head");
            let roster_head_object = ExactObjectRef::new(
                roster_head_slot,
                roster_head_bytes.len() as u64,
                ObjectHash::digest(&roster_head_bytes),
            );
            roster_heads.push(
                crate::protocol::circle::CircleRosterHeadRef::from_stored_head(
                    &roster_head,
                    roster_head_object,
                ),
            );

            let metadata_head_slot =
                ObjectSlot::logical(format!("store-v1/test/{label}/circle-metadata-head/1.json"))
                    .expect("valid test Circle metadata-head slot");
            let metadata_activation = StreamActivation::grant_authorized(
                store_root_hash,
                author_registration.clone(),
                draft.metadata.author_owner_grant.clone(),
                GrantStreamAnchor::CircleMetadata {
                    circle_id: draft.circle_id,
                    first_slot: metadata_head_slot.clone(),
                },
            );
            let metadata_head = CircleMetadataHead::signed(
                &draft.metadata,
                metadata_object,
                SuccessorLink {
                    activation: metadata_activation.activation_id(),
                    predecessor: None,
                    next_slot: ObjectSlot::logical(format!(
                        "store-v1/test/{label}/circle-metadata-head/2.json"
                    ))
                    .expect("valid next test Circle metadata-head slot"),
                },
                &device_signer,
            );
            let metadata_head_bytes =
                serde_json::to_vec(&metadata_head).expect("serialize test Circle metadata head");
            let metadata_head_object = ExactObjectRef::new(
                metadata_head_slot,
                metadata_head_bytes.len() as u64,
                ObjectHash::digest(&metadata_head_bytes),
            );
            metadata_heads.push(
                crate::protocol::circle::CircleMetadataHeadRef::from_stored_head(
                    &metadata_head,
                    metadata_head_object,
                ),
            );

            let control_head_slot =
                ObjectSlot::logical(format!("store-v1/test/{label}/circle-control-head/1.json"))
                    .expect("valid test Circle control-head slot");
            let control_activation = StreamActivation::grant_authorized(
                store_root_hash,
                author_registration.clone(),
                draft.metadata.author_owner_grant.clone(),
                GrantStreamAnchor::CircleControl {
                    circle_id: draft.circle_id,
                    first_slot: control_head_slot.clone(),
                },
            );
            let control_head = crate::protocol::circle::CircleControlHead::signed(
                &draft.control.value,
                control_object.clone(),
                SuccessorLink {
                    activation: control_activation.activation_id(),
                    predecessor: None,
                    next_slot: ObjectSlot::logical(format!(
                        "store-v1/test/{label}/circle-control-head/2.json"
                    ))
                    .expect("valid next test Circle control-head slot"),
                },
                &device_signer,
            );
            let control_head_bytes =
                serde_json::to_vec(&control_head).expect("serialize test Circle control head");
            let control_head_object = ExactObjectRef::new(
                control_head_slot,
                control_head_bytes.len() as u64,
                ObjectHash::digest(&control_head_bytes),
            );
            (
                CircleTransitionPolicyObjects {
                    roster: Some(CircleRosterPolicyObjects {
                        entry: roster_entry,
                        head: roster_head,
                    }),
                    metadata_head: Some(metadata_head),
                    control_head,
                },
                Some(control_head_object),
            )
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
            roster_heads,
            roster_resolutions: BTreeMap::new(),
            metadata_entries,
            metadata_heads,
            access: Vec::new(),
        };
        let reference = creation.control_ref(objects, head_object);
        let control = creation.control.clone();
        let own_access = creation
            .access
            .iter()
            .find(|access| access.leaf.value.recipient_pubkey == owner_pubkey)
            .expect("test Circle owner access");
        let activation = VerifiedCircleReference {
            reference,
            circle_id: creation.circle_id,
            control: control.clone(),
            local_access: active.then(|| VerifiedCircleAccess {
                envelope: own_access.envelope.clone(),
                leaf: own_access.leaf.clone(),
                active: Some(VerifiedCircleActive {
                    roster: creation.resolved_roster(),
                    metadata: creation.metadata.clone(),
                }),
            }),
        };
        let current = CircleCurrentState::from_verified(candidate_family, &activation)
            .expect("derive test Circle current state");
        let control_coord = serde_json::to_string(&control.coord)
            .expect("serialize test Circle control coordinate");
        self.install_circle_current_state(
            creation.circle_id,
            &control_coord,
            &format!("{label}-device"),
            ObjectHash::digest(format!("{label} commit").as_bytes()),
            &control.bytes,
            active.then_some(owner_pubkey.as_str()),
            &serde_json::to_vec(&current).expect("serialize test Circle current state"),
        )
        .expect("install test Circle state");
        assert_eq!(
            creation
                .resolved_roster()
                .members()
                .get(&crate::keys::public_key_hex(&owner)),
            Some(&CircleRole::Owner)
        );
        (creation.circle_id, control.coord)
    }
}
