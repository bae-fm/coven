//! Signed Circle and membership values a test needs before it can exercise
//! anything that consumes them: a resolved membership reference, a registered
//! device authority, and the Circle control reference that device signs.

use std::collections::BTreeMap;

use crate::circle::{CircleControlHead, PreparedCircleControl};
use crate::circle_control::StoreMembershipStateRef;
use crate::store_commit::ObjectHash;
use crate::{membership, store_commit};
use coven_keys::keys::{self, UserKeypair};

pub(crate) fn exact_object(label: &str, bytes: &[u8]) -> crate::objects::ExactObjectRef {
    crate::objects::ExactObjectRef::new(
        crate::objects::ObjectSlot::logical(format!("store-v1/test/{label}.json")).unwrap(),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
}

pub fn exact_logical_object(logical_key: String, bytes: &[u8]) -> crate::objects::ExactObjectRef {
    crate::objects::ExactObjectRef::new(
        crate::objects::ObjectSlot::logical(logical_key).unwrap(),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
}

pub(crate) fn test_founder_entry(
    label: &str,
    owner: &UserKeypair,
    membership: store_commit::GrantStreamAnchor,
) -> membership::MembershipEntry {
    membership::founder_entry(
        label,
        owner,
        crate::causal_grants::MembershipGrantId::from_test_label(label),
        "founder",
        membership,
        crate::provider::FounderProviderAdminGrant::from_test_label(label),
    )
}

pub fn merge_membership_ref(
    owner: &UserKeypair,
    members: &[(String, membership::MemberRole)],
    label: &str,
) -> (
    StoreMembershipStateRef,
    membership::MembershipGrantCreationAuthority,
) {
    let founder = test_founder_entry(
        label,
        owner,
        store_commit::GrantStreamAnchor::StoreMembership {
            first_slot: crate::objects::ObjectSlot::logical(format!(
                "store-v1/test/{label}/membership/1.json"
            ))
            .unwrap(),
        },
    );
    let founder_coord = founder.coord();
    let mut chain = membership::MembershipChain::from_entries(vec![founder])
        .expect("found merge-concurrent membership");
    for (index, (pubkey, role)) in members.iter().enumerate() {
        if pubkey == &keys::public_key_hex(owner) {
            continue;
        }
        if role == &membership::MemberRole::Owner {
            chain
                .add_owner_for_test(
                    owner,
                    founder_coord.stream_id,
                    pubkey.clone(),
                    format!("member-{index}"),
                )
                .expect("promote merge-concurrent Owner");
            continue;
        }
        let entry = chain
            .signed_set_member_in_stream(
                owner,
                founder_coord.stream_id,
                pubkey.clone(),
                None,
                role.clone(),
                format!("member-{index}"),
            )
            .expect("sign merge-concurrent member");
        chain
            .add_entry(entry)
            .expect("apply merge-concurrent member");
    }
    let resolved = match chain.status() {
        membership::MembershipStatus::Resolved(resolved) => resolved,
        membership::MembershipStatus::Conflict(_) => {
            panic!("membership fixture must resolve")
        }
    };
    let tip = chain.entries().last().expect("membership tip").coord();
    let head = membership::MembershipHeadRef {
        coord: tip,
        head_hash: ObjectHash::digest(format!("{label} head").as_bytes()),
        object: exact_object(&format!("{label}/membership-head"), b"membership head"),
    };
    (
        StoreMembershipStateRef::from_parts(
            vec![head],
            Vec::new(),
            Vec::new(),
            resolved.state_hash,
        )
        .expect("valid merge-concurrent membership reference"),
        membership::MembershipGrantCreationAuthority::Entry(founder_coord),
    )
}

pub struct MergeDeviceAuthority {
    pub registration: store_commit::StoreDeviceRegistration,
    pub reference: store_commit::StoreDeviceRegistrationRef,
    pub device_signer: UserKeypair,
    pub stream_id: membership::AuthorStreamId,
}

impl MergeDeviceAuthority {
    pub fn circle_control_reference(
        &self,
        control: &PreparedCircleControl,
        label: &str,
    ) -> store_commit::CircleControlRef {
        let control_object = exact_object(&format!("{label}/control"), &control.bytes);
        let head_slot = crate::objects::ObjectSlot::logical(format!(
            "store-v1/test/{label}/control-head/1.json"
        ))
        .expect("valid test Circle control-head slot");
        let activation = store_commit::StreamActivation::grant_authorized(
            control.value.store_root_hash,
            self.reference.clone(),
            control.value.author_grant_id(),
            store_commit::GrantStreamAnchor::CircleControl {
                circle_id: control.value.circle_id,
                first_slot: head_slot.clone(),
            },
        );
        let head = CircleControlHead::signed(
            &control.value,
            control_object.clone(),
            store_commit::SuccessorLink {
                activation: activation.activation_id(),
                predecessor: None,
                next_slot: crate::objects::ObjectSlot::logical(format!(
                    "store-v1/test/{label}/control-head/2.json"
                ))
                .expect("valid next test Circle control-head slot"),
            },
            &self.device_signer,
        );
        let head_bytes = serde_json::to_vec(&head).expect("serialize test Circle control head");
        let head_object = crate::objects::ExactObjectRef::new(
            head_slot,
            head_bytes.len() as u64,
            ObjectHash::digest(&head_bytes),
        );
        let objects = store_commit::CircleActivationObjects {
            control: control_object,
            close_intent: None,
            close_outcome: None,
            close_cancellation: None,
            roster_entries: BTreeMap::new(),
            roster_heads: Vec::new(),
            roster_resolutions: BTreeMap::new(),
            metadata_entries: BTreeMap::new(),
            metadata_heads: Vec::new(),
            access: Vec::new(),
        };
        store_commit::CircleControlRef {
            circle_id: control.value.circle_id,
            control: control.coord.clone(),
            head_hash: head.head_hash(),
            head_object,
            objects,
        }
    }
}

pub fn merge_device_authority(
    identity: &UserKeypair,
    store_root_hash: ObjectHash,
    label: &str,
) -> MergeDeviceAuthority {
    let root = store_commit::StoreRootRef {
        store_root_id: ObjectHash::digest(format!("{label} identity").as_bytes()),
        store_root_hash,
        object: exact_object(&format!("{label}/root"), label.as_bytes()),
    };
    let slot = |stream: &str| {
        crate::objects::ObjectSlot::logical(format!("store-v1/test/{label}/{stream}/1.json"))
            .unwrap()
    };
    let registration = store_commit::StoreDeviceRegistration::signed(
        root.clone(),
        store_commit::StoreDeviceRegistrationOrigin::Founder {
            creation_id: store_commit::StoreCreationId::from_nonce(label),
        },
        crate::objects::ProviderDeviceBinding {
            principal: crate::objects::ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(label.as_bytes()),
            },
        },
        store_commit::DeviceStreamAnchor::StoreAnnouncements {
            first_slot: slot("announcements"),
        },
        store_commit::DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot("acknowledgements"),
        },
        store_commit::DeviceStreamAnchor::StoreSnapshots {
            first_slot: slot("snapshots"),
        },
        identity,
    )
    .expect("sign test device registration");
    let bytes = registration.to_bytes();
    let reference = store_commit::StoreDeviceRegistrationRef::from_registration(
        &registration,
        exact_object(&format!("{label}/registration"), &bytes),
    );
    let device_signer = registration
        .device_signer(identity)
        .expect("derive registered device signer");
    let stream_id = store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        &reference,
        store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    MergeDeviceAuthority {
        registration,
        reference,
        device_signer,
        stream_id,
    }
}
