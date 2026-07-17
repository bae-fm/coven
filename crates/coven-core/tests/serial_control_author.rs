use coven_core::database::Database;
use coven_core::keys::{public_key_hex, UserKeypair};
use coven_core::storage::cloud::ObjectSlot;
use coven_core::sync::circle_control::StoreMembershipStateRef;
use coven_core::sync::membership::{
    founder_entry, MemberRole, MembershipGrantId, SerialAuthorizationState, SerialMembershipState,
};
use coven_core::sync::store_commit::{
    GrantStreamAnchor, ObjectHash, ResolvedStoreDeviceState, StoreBatchCommit, StoreCommitCoord,
    StoreCommitOrder, StoreControl, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreDeviceStateRef, StoreProtocolError, StoreProtocolRoot, StoreSerialPredecessor,
};
use coven_core::sync::store_objects::load_store_protocol_root;
use coven_core::sync::test_helpers::{open_serial_test_db, TestStore};

struct SerialFixture {
    db: Database,
    identity: UserKeypair,
    store: TestStore,
    root: StoreProtocolRoot,
    registration_ref: StoreDeviceRegistrationRef,
    registration: StoreDeviceRegistration,
    device_signer: UserKeypair,
}

impl SerialFixture {
    async fn create(store_id: &str) -> Self {
        let db = open_serial_test_db();
        let identity = UserKeypair::generate();
        let store = TestStore::create(&db, store_id, identity.clone())
            .await
            .expect("create Serial Store");
        let root = load_store_protocol_root(&store.storage, &store.root)
            .await
            .expect("load exact Store root")
            .value;
        let (registration_ref, registration, device_signer) = store
            .founder_device_authority()
            .await
            .expect("load founder device authority");
        Self {
            db,
            identity,
            store,
            root,
            registration_ref,
            registration,
            device_signer,
        }
    }

    fn sign_control(
        &self,
        entry: coven_core::sync::membership::SerialMembershipEntry,
    ) -> Result<StoreBatchCommit, StoreProtocolError> {
        let genesis = StoreSerialPredecessor::Genesis {
            root: self.store.root.clone(),
            founder_registration: self.registration_ref.clone(),
        };
        let authorization = SerialAuthorizationState::from_founder(
            &self.store.root,
            &self.root,
            &self.registration_ref,
            &self.registration,
        )
        .expect("derive Serial founder authorization");
        let resolved_devices = ResolvedStoreDeviceState::founder(
            &self.store.root,
            self.registration_ref.clone(),
            &self.root.descriptor.founder_pubkey,
            self.root.descriptor.founder_grant.clone(),
            &self.root.descriptor.founder_recovery,
        )
        .expect("derive Serial founder device state");
        let membership_state = StoreMembershipStateRef::serial(
            genesis.clone(),
            resolved_devices.recovery.clone(),
            &authorization,
        )
        .expect("derive Serial membership state reference");
        let device_state = StoreDeviceStateRef::serial(genesis.clone(), &resolved_devices)
            .expect("derive Serial device state reference");
        StoreBatchCommit::signed_with_control(
            self.store.root.store_root_hash,
            self.db.new_write_id(),
            StoreCommitCoord::Serial { sequence: 1 },
            self.registration_ref.clone(),
            &self.registration,
            StoreCommitOrder::Serial {
                seq: 1,
                predecessor: genesis,
            },
            membership_state,
            device_state,
            None,
            Some(StoreControl::SerialMembership { entry }),
            None,
            &self.device_signer,
        )
    }
}

#[tokio::test]
async fn serial_control_accepts_the_registered_identity_as_its_author() {
    let fixture = SerialFixture::create("serial-control-author").await;
    let membership = SerialAuthorizationState::from_founder(
        &fixture.store.root,
        &fixture.root,
        &fixture.registration_ref,
        &fixture.registration,
    )
    .expect("derive Serial founder authorization")
    .membership;
    let member = UserKeypair::generate();
    let entry = membership
        .signed_set_member(
            &fixture.identity,
            public_key_hex(&member),
            None,
            MemberRole::Member,
            "add member".to_string(),
        )
        .expect("sign membership control with the registered identity");

    fixture
        .sign_control(entry)
        .expect("identity-authored control is carried by its registered device");
}

#[tokio::test]
async fn serial_control_rejects_the_device_key_as_its_membership_author() {
    let fixture = SerialFixture::create("serial-control-device-author").await;
    let device_grant = MembershipGrantId(ObjectHash::digest(b"device founder grant"));
    let device_founder = founder_entry(
        "serial-control-device-author",
        &fixture.device_signer,
        device_grant,
        "device founder",
        GrantStreamAnchor::StoreMembership {
            first_slot: ObjectSlot::logical(
                "store-v1/membership/heads/device-author/1.json".to_string(),
            )
            .expect("valid device-author membership slot"),
        },
        fixture.root.descriptor.founder_provider_admin.clone(),
    );
    let device_membership =
        SerialMembershipState::from_founder(fixture.store.root.store_root_id, &device_founder)
            .expect("derive device-authored membership fixture");
    let member = UserKeypair::generate();
    let entry = device_membership
        .signed_set_member(
            &fixture.device_signer,
            public_key_hex(&member),
            None,
            MemberRole::Member,
            "device-authored add".to_string(),
        )
        .expect("sign membership control with the device key");

    assert!(matches!(
        fixture.sign_control(entry),
        Err(StoreProtocolError::InvalidSerialControl)
    ));
}
