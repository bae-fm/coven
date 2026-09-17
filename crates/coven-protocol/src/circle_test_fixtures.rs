//! Signed Circle and membership values a test needs before it can exercise
//! anything that consumes them: a resolved membership reference, a registered
//! device authority, and the Circle control reference that device signs.

use std::collections::BTreeMap;

use crate::circle::PreparedCircleControl;
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
) -> (StoreMembershipStateRef, membership::MembershipCoord) {
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
    let resolved = chain.resolved();
    let tip = chain.entries().last().expect("membership tip").coord();
    let head = membership::MembershipHeadRef {
        coord: tip,
        head_hash: ObjectHash::digest(format!("{label} head").as_bytes()),
        object: exact_object(&format!("{label}/membership-head"), b"membership head"),
    };
    (
        StoreMembershipStateRef::from_parts(vec![head], Vec::new(), resolved.state_hash)
            .expect("valid merge-concurrent membership reference"),
        founder_coord,
    )
}

pub struct MergeDeviceAuthority {
    registration: store_commit::StoreDeviceRegistration,
    reference: store_commit::StoreDeviceRegistrationRef,
    device_signer: UserKeypair,
    stream_id: membership::AuthorStreamId,
}

impl MergeDeviceAuthority {
    pub fn registration(&self) -> &store_commit::StoreDeviceRegistration {
        &self.registration
    }

    pub fn reference(&self) -> &store_commit::StoreDeviceRegistrationRef {
        &self.reference
    }

    pub fn stream_id(&self) -> membership::AuthorStreamId {
        self.stream_id
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sign_operations(
        &self,
        store_root_hash: ObjectHash,
        write_id: crate::write::WriteId,
        coord: store_commit::StoreCommitCoord,
        order: store_commit::StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: store_commit::StoreDeviceStateRef,
        membership_authority: membership::MembershipCoord,
        input: store_commit::StoreCommitOperationsInput<'_>,
    ) -> Result<store_commit::StoreBatchCommit, store_commit::StoreProtocolError> {
        store_commit::StoreBatchCommit::signed_operations(
            store_root_hash,
            write_id,
            coord,
            self.reference.clone(),
            &self.registration,
            order,
            crate::store_commit::StorePublicationBase::Genesis,
            membership_state,
            device_state,
            membership_authority,
            input,
            &self.device_signer,
        )
    }

    pub fn circle_control_reference(
        &self,
        control: &PreparedCircleControl,
        label: &str,
    ) -> store_commit::CircleControlRef {
        let control_object = exact_object(&format!("{label}/control"), &control.bytes);
        let objects = store_commit::CircleActivationObjects {
            control: control_object,
            close_intent: None,
            close_outcome: None,
            close_cancellation: None,
            roster_entries: BTreeMap::new(),
            metadata_entries: BTreeMap::new(),
            bootstraps: Vec::new(),
        };
        store_commit::CircleControlRef {
            circle_id: control.value.circle_id,
            control: control.coord.clone(),
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
        store_commit::DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot("acknowledgements"),
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
