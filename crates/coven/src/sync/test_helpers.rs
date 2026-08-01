/// Shared test helpers for sync module tests.
///
/// These drive a real [`Database`] over an in-memory connection carrying the
/// synthetic test schema, so tests exercise the engine through the same path
/// production does.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension};

use crate::database::resolve_and_apply_changeset;
use crate::database::{Database, DbError};
use crate::encryption::MasterKeyring;
use crate::keys::{KeyError, MasterKeyCustody, UserKeypair};
use crate::protocol::store_commit::ObjectHash;
use crate::store_dir::StoreDir;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::Migration;

#[cfg(test)]
pub(crate) fn test_cache_locator_hash(label: &str) -> ObjectHash {
    ObjectHash::digest(label.as_bytes())
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn test_membership_grant_id(
    label: &str,
) -> crate::protocol::causal_grants::MembershipGrantId {
    crate::protocol::causal_grants::MembershipGrantId(
        crate::protocol::store_commit::ObjectHash::digest(label.as_bytes()),
    )
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn test_founder_provider_admin(
    label: &str,
) -> crate::protocol::provider::FounderProviderAdminGrant {
    use crate::protocol::provider::{
        ExactSlotProbeReceipt, ExactSlotProbeTranscript, LostResponseProbeReceipt,
        ProbeCreateAttempt, ProbeCreateOutcome, ProbePayloadLabel, ProbeRangeReceipt,
        ProviderAdminGrantId, ProviderCapabilityProof, ProviderProbeId, PROBE_RANGE_END,
        PROBE_RANGE_START,
    };
    use crate::storage::{
        ProviderDeviceBinding, ProviderPrincipalId, S3EndpointBinding, StoreProviderBinding,
    };
    let probe_id = ProviderProbeId::from_bytes(*ObjectHash::digest(label.as_bytes()).as_bytes());
    let slot = crate::storage::cloud::ObjectSlot::logical(format!(
        "store-v1/test/{label}/provider-probe/exact"
    ))
    .expect("valid exact-probe test slot");
    let first =
        crate::protocol::provider::probe_payload(&probe_id, ProbePayloadLabel::ExactCreateFirst);
    let second =
        crate::protocol::provider::probe_payload(&probe_id, ProbePayloadLabel::ExactCreateSecond);
    let accepted = crate::storage::ExactObjectRef::new(
        slot.clone(),
        first.len() as u64,
        ObjectHash::digest(&first),
    );
    let lost_slot = crate::storage::cloud::ObjectSlot::logical(format!(
        "store-v1/test/{label}/provider-probe/lost-response"
    ))
    .expect("valid lost-response test slot");
    let lost_payload =
        crate::protocol::provider::probe_payload(&probe_id, ProbePayloadLabel::LostResponse);
    let lost_ref = crate::storage::ExactObjectRef::new(
        lost_slot.clone(),
        lost_payload.len() as u64,
        ObjectHash::digest(&lost_payload),
    );
    let device = ProviderDeviceBinding {
        principal: ProviderPrincipalId::CustomS3Credential {
            access_key_id_hash: ObjectHash::digest(format!("{label} access key").as_bytes()),
        },
    };
    let store = StoreProviderBinding::S3 {
        endpoint: S3EndpointBinding::Custom {
            origin: "https://test.invalid".to_string(),
        },
        region: "test-region".to_string(),
        bucket: format!("{label}-bucket"),
        key_prefix: None,
    };
    let transcript = ExactSlotProbeTranscript {
        probe_id,
        logical_key: slot.logical_key().to_string(),
        slot,
        contenders: [
            ProbeCreateAttempt {
                payload_hash: ObjectHash::digest(&first),
                outcome: ProbeCreateOutcome::Created,
            },
            ProbeCreateAttempt {
                payload_hash: ObjectHash::digest(&second),
                outcome: ProbeCreateOutcome::RejectedOccupied,
            },
        ],
        accepted: accepted.clone(),
        full_read_hash: accepted.stored_hash(),
        range: ProbeRangeReceipt {
            start: PROBE_RANGE_START,
            end: PROBE_RANGE_END,
            bytes_hash: ObjectHash::digest(
                &first[PROBE_RANGE_START as usize..PROBE_RANGE_END as usize],
            ),
        },
        delete_verified_absent: true,
        lost_response: LostResponseProbeReceipt {
            logical_key: lost_slot.logical_key().to_string(),
            slot: lost_slot,
            payload_hash: ObjectHash::digest(&lost_payload),
            settled: lost_ref,
            readback_hash: ObjectHash::digest(&lost_payload),
            delete_verified_absent: true,
        },
    };
    crate::protocol::provider::FounderProviderAdminGrant {
        grant_id: ProviderAdminGrantId(ObjectHash::digest(
            format!("{label} provider admin grant").as_bytes(),
        )),
        provider: device.clone(),
        access: crate::protocol::provider::ProviderAccessLocator::S3SharedCredentialGeneration {
            generation: 1,
            access_key_id_hash: ObjectHash::digest(format!("{label} access key").as_bytes()),
        },
        capability: ProviderCapabilityProof {
            exact_slots: ExactSlotProbeReceipt::from_transcript(transcript, &store, &device),
        },
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn install_test_store_root_authority(
    conn: &Connection,
    label: &str,
) -> crate::protocol::store_commit::ObjectHash {
    use crate::protocol::store_commit::{
        GrantStreamAnchor, ObjectHash, StoreCreationDescriptor, StoreCreationId, StoreProtocolRoot,
        STORE_PROTOCOL_VERSION,
    };
    use crate::storage::cloud::ObjectSlot;
    use crate::storage::{ExactObjectRef, S3EndpointBinding, StoreProviderBinding};

    let keypair_bytes: [u8; crate::keys::SIGN_SECRETKEYBYTES] = hex::decode(concat!(
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    ))
    .expect("fixed test signing key is hexadecimal")
    .try_into()
    .expect("fixed test signing key is 64 bytes");
    let signer = UserKeypair::from_signing_key_bytes(&keypair_bytes)
        .expect("fixed test signing key is valid");
    let sync_routing_hash: ObjectHash = crate::database::required_protocol_state_on(
        conn,
        crate::database::SYNC_ROUTING_HASH_STATE_KEY,
    )
    .expect("test Store has a sync-routing hash")
    .parse()
    .expect("test Store sync-routing hash is valid");
    let root_slot = ObjectSlot::logical(
        crate::protocol::store_commit::STORE_PROTOCOL_ROOT_LOGICAL_KEY.to_string(),
    )
    .expect("valid test Store root slot");
    let descriptor = StoreCreationDescriptor {
        version: STORE_PROTOCOL_VERSION,
        creation_id: StoreCreationId::from_random_bytes(
            *ObjectHash::digest(label.as_bytes()).as_bytes(),
        ),
        provider: StoreProviderBinding::S3 {
            endpoint: S3EndpointBinding::Custom {
                origin: "https://test.invalid".to_string(),
            },
            region: "test-region".to_string(),
            bucket: format!("{label}-bucket"),
            key_prefix: None,
        },
        schema_version: 1,
        sync_routing_hash,
        founder_pubkey: crate::keys::public_key_hex(&signer),
        founder_grant: test_membership_grant_id(&format!("{label} founder grant")),
        root_slot: root_slot.clone(),
        founder_registration: ObjectSlot::logical(format!(
            "store-v1/test/{label}/registration.json"
        ))
        .expect("valid test founder registration slot"),
        founder_provider_admin: test_founder_provider_admin(label),
        founder_membership: GrantStreamAnchor::StoreMembership {
            first_slot: ObjectSlot::logical(format!("store-v1/test/{label}/membership/1.json"))
                .expect("valid test founder membership slot"),
        },
        founder_recovery: GrantStreamAnchor::OwnerRecovery {
            first_slot: ObjectSlot::logical(format!("store-v1/test/{label}/recovery/1.json"))
                .expect("valid test founder recovery slot"),
        },
    };
    let root = StoreProtocolRoot::signed(descriptor, &signer).expect("sign test Store root");
    let bytes = root.to_bytes();
    let hash = root.object_hash();
    let object = ExactObjectRef::new(root_slot, bytes.len() as u64, ObjectHash::digest(&bytes));
    conn.execute(
        "INSERT INTO store_protocol_root_authority
         (singleton, store_root_hash, store_protocol_root_bytes, store_root_object)
         VALUES (1, ?1, ?2, ?3)
         ON CONFLICT(singleton) DO NOTHING",
        rusqlite::params![
            hash.to_string(),
            bytes,
            serde_json::to_string(&object).expect("serialize test Store root object")
        ],
    )
    .expect("install test Store root authority");
    hash
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn install_test_active_circle(
    conn: &Connection,
    label: &str,
) -> (
    crate::protocol::circle::CircleId,
    crate::protocol::circle::CircleControlCoord,
) {
    install_test_circle_current_state(conn, label, true)
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn test_circle_owner_keypair() -> UserKeypair {
    let keypair_bytes: [u8; crate::keys::SIGN_SECRETKEYBYTES] = hex::decode(concat!(
        "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
        "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c"
    ))
    .expect("fixed Circle signing key is hexadecimal")
    .try_into()
    .expect("fixed Circle signing key is 64 bytes");
    UserKeypair::from_signing_key_bytes(&keypair_bytes).expect("fixed Circle signing key is valid")
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn install_test_inactive_circle(
    conn: &Connection,
    label: &str,
) -> (
    crate::protocol::circle::CircleId,
    crate::protocol::circle::CircleControlCoord,
) {
    install_test_circle_current_state(conn, label, false)
}

#[cfg(any(test, feature = "test-utils"))]
fn install_test_circle_current_state(
    conn: &Connection,
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
    use crate::protocol::store_commit::{
        CandidateFamilyId, CircleActivationObjects, CircleMetadataObjectRef, DeviceStreamAnchor,
        GrantStreamAnchor, ObjectHash, StoreCreationId, StoreDeviceRegistration,
        StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreRootRef, StreamActivation,
        SuccessorLink,
    };
    use crate::storage::cloud::ObjectSlot;
    use crate::storage::ExactObjectRef;
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

    fn founder_entry(
        label: &str,
        owner: &UserKeypair,
        membership: GrantStreamAnchor,
    ) -> crate::protocol::membership::MembershipEntry {
        crate::protocol::membership::founder_entry(
            label,
            owner,
            test_membership_grant_id(label),
            "founder",
            membership,
            test_founder_provider_admin(label),
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
        crate::storage::ProviderDeviceBinding {
            principal: crate::storage::ProviderPrincipalId::CustomS3Credential {
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
    let founder = founder_entry(label, &owner, membership_anchor);
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
    let control_coord =
        serde_json::to_string(&control.coord).expect("serialize test Circle control coordinate");
    conn.execute(
        "INSERT INTO circle_control_activations
         (circle_id, control_coord, stream_id, seq, commit_hash, control_bytes)
         VALUES (?1, ?2, ?3, 1, ?4, ?5)",
        rusqlite::params![
            creation.circle_id.to_string(),
            &control_coord,
            format!("{label}-device"),
            ObjectHash::digest(format!("{label} commit").as_bytes()).to_string(),
            &control.bytes,
        ],
    )
    .expect("insert test Circle activation");
    if active {
        conn.execute(
            "INSERT INTO circle_access_cache
             (circle_id, control_coord, owner_pubkey, disposition)
             VALUES (?1, ?2, ?3, 'active')",
            rusqlite::params![creation.circle_id.to_string(), &control_coord, owner_pubkey,],
        )
        .expect("insert test Circle access history");
    }
    conn.execute(
        "INSERT INTO circle_current_state (circle_id, state) VALUES (?1, ?2)",
        rusqlite::params![
            creation.circle_id.to_string(),
            serde_json::to_vec(&current).expect("serialize test Circle current state"),
        ],
    )
    .expect("insert test Circle current state");
    assert_eq!(
        creation
            .resolved_roster()
            .members()
            .get(&crate::keys::public_key_hex(&owner)),
        Some(&CircleRole::Owner)
    );
    (creation.circle_id, control.coord)
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn test_row_routing_id(
    conn: &Connection,
    generation_one_key: [u8; 32],
    table: &str,
    row_id: &str,
) -> crate::protocol::circle::RowRoutingId {
    let root_hash: crate::protocol::store_commit::ObjectHash = conn
        .query_row(
            "SELECT store_root_hash FROM store_protocol_root_authority WHERE singleton = 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .expect("test Store root authority is installed")
        .parse()
        .expect("test Store root hash is valid");
    let encryption = crate::encryption::EncryptionService::from_key(generation_one_key);
    let key = crate::protocol::circle::derive_row_routing_key(&encryption, root_hash)
        .expect("test keyring has one generation-one key");
    crate::protocol::circle::row_routing_id(&key, table, row_id)
}

/// In-memory [`MasterKeyCustody`] for tests, with a switch to force `persist`
/// to fail. The switch models a device whose keyring is momentarily
/// unwritable, so a test can drive a key adoption into its failure path and then
/// clear the switch to prove the retry converges. Stores the serialized form
/// (like the real `Keyring` preset), so `stored_key` reflects exactly what a
/// caller wrote.
#[derive(Clone, Default)]
pub(crate) struct TestCustody {
    value: Arc<Mutex<Option<String>>>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl TestCustody {
    pub(crate) fn set_initial_key(&self, key: [u8; 32]) {
        *self.value.lock().unwrap() = Some(
            MasterKeyring::from(crate::encryption::EncryptionService::from_key(key))
                .to_serialized(),
        );
    }

    pub(crate) fn stored_key(&self) -> Option<String> {
        self.value.lock().unwrap().clone()
    }

    /// Make the next and every subsequent `persist` fail until cleared.
    pub(crate) fn fail_writes(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Let `persist` succeed again.
    pub(crate) fn allow_writes(&self) {
        self.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl MasterKeyCustody for TestCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        self.value
            .lock()
            .unwrap()
            .as_deref()
            .map(MasterKeyring::from_serialized)
            .transpose()
            .map_err(|e| KeyError::Crypto(e.to_string()))
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KeyError::Persistence(
                "forced keyring write failure".to_string(),
            ));
        }
        *self.value.lock().unwrap() = Some(keyring.to_serialized());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.value.lock().unwrap() = None;
        Ok(())
    }
}

pub(crate) fn test_store_security(
    store_id: &str,
    master_keys: Arc<dyn MasterKeyCustody>,
) -> crate::store_security::StoreSecurity {
    let store_keys = crate::keys::StoreKeys::bind(store_id.to_string());
    let identity = crate::identity_custody::IdentityCustody::InMemory(UserKeypair::generate())
        .resolve(
            &store_keys,
            &StoreDir::new(format!("{store_id}-unused-test-identity-directory")),
        );
    crate::store_security::StoreSecurity::new(
        store_keys,
        master_keys,
        identity,
        crate::oauth::OAuthClients::empty(),
    )
}

/// The synthetic, domain-free schema the sync tests run against. Three synced
/// tables exercising the engine's generic mechanics: a *gated root* (`notes`,
/// gated by its `shared` boolean), a child with a foreign key (`note_tags`,
/// which inherits the gate and exercises FK-violation retry), and a child that
/// CAN carry a blob (`note_photos`, also FK-to-`notes`, so it inherits the gate).
/// `note_photos` carries no blob here; blob tests declare one with
/// [`test_synced_tables_with_blob`].
pub(crate) fn test_synced_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).gated_by("shared"),
        SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey),
    ]
}

/// [`test_synced_tables`] with `note_photos` declared blob-bearing per `decl`, for
/// tests exercising the blob push/pull/backfill paths. The blob id defaults to the
/// `note_photos` primary key; `note_photos.cloud_path` holds a readable key for
/// plain-scheme tests, and `note_photos.blob_id` is there for a decl that names a
/// blob id apart from the PK — the shape a row repointed at a new blob needs, since
/// the row keeps its primary key.
pub(crate) fn test_synced_tables_with_blob(decl: BlobDecl) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).gated_by("shared"),
        SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey)
            .carries_blob(decl),
    ]
}

/// [`test_synced_tables`] with TWO blob-bearing children of the gated `notes` root:
/// `note_photos` per `photo_decl` (a release file, user-provided) and `note_covers`
/// per `cover_decl` (a host-provided asset). Both inherit the `notes` gate, so a
/// make_remote of a note carries both — the user-provided file through the durable
/// outbox and the host-provided cover through the inline push — exercising the
/// per-provenance split in one subtree.
pub(crate) fn test_synced_tables_with_user_and_host_blobs(
    photo_decl: BlobDecl,
    cover_decl: BlobDecl,
) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).gated_by("shared"),
        SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey)
            .carries_blob(photo_decl),
        SyncedTable::new("note_covers", crate::sync::session::RowIdentity::SharedKey)
            .carries_blob(cover_decl),
    ]
}

/// Open a test [`Database`] over the synthetic schema with `note_photos` declared
/// blob-bearing per `decl`.
pub(crate) fn open_test_db_with_blob(decl: BlobDecl) -> Database {
    open_test_db_schema(test_synced_tables_with_blob(decl), test_migrations())
}

/// Open a read-test [`Database`] whose `note_photos` child carries a blob in
/// `namespace`, so `read_blob`'s locality dispatch can resolve a
/// blob in that namespace up to its gated `notes` root. The decl's namespace MUST
/// match the blobs the test reads (the read path resolves the carrying table from the
/// blob's namespace); its provenance/fill don't matter to that resolution (the read
/// reads the row → root → gate, and takes provenance off the `BlobRef`), so this fixes
/// them. Pair with [`plant_blob_row`].
pub(crate) fn read_test_db(namespace: &str) -> Database {
    open_test_db_with_blob(BlobDecl::new(
        namespace,
        crate::blob::Provenance::UserProvided,
        crate::blob::CacheFill::CacheLazy,
    ))
}

/// Like [`read_test_db`] but with a chosen `max_concurrent_downloads`, so a pin test
/// can drive the download loop concurrently. Uploads run one at a time (not exercised here).
pub(crate) fn read_test_db_with_download_limit(namespace: &str, downloads: usize) -> Database {
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        namespace,
        crate::blob::Provenance::UserProvided,
        crate::blob::CacheFill::CacheLazy,
    ));
    let limits = crate::blob::TransferLimits {
        uploads: std::num::NonZeroUsize::MIN,
        downloads: std::num::NonZeroUsize::new(downloads).expect("downloads limit is nonzero"),
    };
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        tables,
        crate::blob::BLOB_TOMBSTONE_GRACE,
        limits,
        "test-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &test_migrations(),
    )
    .expect("open test database");
    db
}

/// Plant the backing row `read_blob` resolves a blob's locality
/// from: a gated `notes` root with `shared = remote` and a `note_photos` child whose
/// id is `blob_id`, carrying `bytes`'s length and content hash so a download of those
/// exact bytes verifies. `remote = true` ⇒ the blob resolves **Remote** (cache/cloud);
/// `remote = false` ⇒ **Local** (and the read then dispatches on the `BlobRef`'s
/// provenance — external file vs local store). Requires a db whose `note_photos`
/// carries a blob (e.g. [`read_test_db`] / [`open_test_db_with_blob`]).
pub(crate) async fn plant_blob_row(db: &Database, blob_id: &str, remote: bool, bytes: &[u8]) {
    plant_blob_row_with_size_hash(
        db,
        blob_id,
        remote,
        bytes.len() as u64,
        Some(&crate::blob::content_hash(bytes)),
    )
    .await;
}

/// Plant a blob-bearing row with a caller-chosen `size` and `hash`, for the tests
/// that deliberately declare a size or hash that does not match the bytes served
/// (the size-mismatch and hash-mismatch refusals) or that never download at all
/// (a missing-blob row, `hash = None`).
pub(crate) async fn plant_blob_row_with_size_hash(
    db: &Database,
    blob_id: &str,
    remote: bool,
    size: u64,
    hash: Option<&str>,
) {
    let note = format!("note-{blob_id}");
    let blob_id = blob_id.to_string();
    let hash = hash.map(str::to_string);
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES (?1, 'read-test', ?2, '0000000001000-0000-dev1', '2026-01-01')",
            (note.as_str(), remote as i64),
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES (?1, ?2, 'attach', ?3, ?4, '0000000001000-0000-dev1', '2026-01-01')",
            rusqlite::params![blob_id.as_str(), note.as_str(), size as i64, hash],
        )
        .map_err(DbError::from)?;
        Ok(())
    })
    .await
    .expect("plant blob row");
}

/// Flip the gate on a blob's planted `notes` root — `shared = remote` for the row
/// [`plant_blob_row`] created — so a read re-resolves the blob's locality. Models the
/// gate side of a make_remote (Local → Remote) / make_local (Remote → Local) without
/// running the whole transition.
pub(crate) async fn set_blob_remote(db: &Database, blob_id: &str, remote: bool) {
    let note = format!("note-{blob_id}");
    db.call(move |conn| {
        conn.execute(
            "UPDATE notes SET shared = ?1 WHERE id = ?2",
            (remote as i64, note.as_str()),
        )
        .map_err(DbError::from)?;
        Ok(())
    })
    .await
    .expect("flip blob gate");
}

/// Open a test [`Database`] with both `note_photos` (per `photo_decl`) and
/// `note_covers` (per `cover_decl`) declared blob-bearing — the schema for the
/// per-provenance transition tests.
pub(crate) fn open_test_db_with_user_and_host_blobs(
    photo_decl: BlobDecl,
    cover_decl: BlobDecl,
) -> Database {
    open_test_db_schema(
        test_synced_tables_with_user_and_host_blobs(photo_decl, cover_decl),
        test_migrations(),
    )
}

/// The synthetic test schema as a single-migration ladder, so a test db opens at
/// `schema_version() == 1`. The host-schema ladder for every `open_test_db*`
/// helper.
pub(crate) fn test_migrations() -> Vec<Migration> {
    vec![Migration::run(1, "test-schema", create_synced_schema)]
}

/// Create the synthetic test schema on a connection. Run as the host migration
/// step for [`open_test_db`] (see [`test_migrations`]).
pub(crate) fn create_synced_schema(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE notes (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            shared INTEGER NOT NULL DEFAULT 0,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        ) STRICT;
        CREATE TABLE note_tags (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;
        CREATE TABLE note_photos (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            hash TEXT,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
            blob_id TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;
        CREATE TABLE note_covers (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            hash TEXT,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;",
    )
    .map_err(DbError::from)
}

/// Open a [`Database`] over a fresh in-memory connection with the synthetic test
/// schema and the [`test_synced_tables`] synced set. The returned stamper is
/// dropped (tests stamp `_updated_at` literally in their SQL).
pub(crate) fn open_test_db() -> Database {
    open_test_db_with(test_synced_tables())
}

/// Like [`open_test_db`] but with an explicit synced set and migration ladder, for
/// tests that exercise a different schema (gate tests).
pub(crate) fn open_test_db_schema(
    tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
) -> Database {
    // `:memory:` is unique per connection; the Database owns exactly one.
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        tables,
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &migrations,
    )
    .expect("open test database");
    db
}

fn open_test_db_with(tables: Vec<SyncedTable>) -> Database {
    open_test_db_schema(tables, test_migrations())
}

/// Open a test [`Database`] over the synthetic schema with a caller-supplied
/// register clock (so a test can control the wall clock), plus an extra `seed`
/// step run after the host schema is created to plant host rows before
/// `Database::open` reads its floor.
///
/// Used only by the register-clock tests (`hlc_register_tests`).
pub(crate) fn open_test_db_with_hlc(
    hlc: std::sync::Arc<crate::sync::hlc::Hlc>,
    seed: impl Fn(&Connection) -> Result<(), DbError> + Send + Sync + 'static,
) -> Database {
    let migrations = vec![Migration::run(1, "test-schema", move |conn| {
        create_synced_schema(conn)?;
        seed(conn)
    })];
    let (db, _stamper) = Database::open_with_hlc(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        hlc,
        &migrations,
    )
    .expect("open test database with hlc");
    db
}

/// Run a write statement on the test database (blocking on the current runtime).
pub(crate) async fn exec(db: &Database, sql: &str) {
    let sql = sql.to_string();
    db.call(move |conn| conn.execute_batch(&sql).map_err(DbError::from))
        .await
        .unwrap_or_else(|e| panic!("exec failed: {e}"));
}

pub(crate) async fn host_exec(db: &Database, sql: &str) {
    let sql = sql.to_string();
    let tables = db.synced_tables().to_vec();
    let write_id = db.new_write_id();
    db.call(move |conn| {
        crate::database::StoreDatabase::run_prepared_blob_transition_transaction_on(
            conn,
            &tables,
            None,
            write_id,
            |tx| tx.execute_batch(&sql).map(|_| ()).map_err(DbError::from),
        )
    })
    .await
    .unwrap_or_else(|e| panic!("exec failed: {e}"));
}

/// Query a single text value from the test database.
pub(crate) async fn query_text(db: &Database, sql: &str) -> String {
    let sql = sql.to_string();
    db.call(move |conn| {
        conn.query_row(&sql, [], |r| r.get::<_, String>(0))
            .map_err(DbError::from)
    })
    .await
    .unwrap_or_else(|e| panic!("query_text failed: {e}"))
}

/// Whether a row exists for `sql` (a `SELECT 1 ...`).
pub(crate) async fn row_exists(db: &Database, sql: &str) -> bool {
    let sql = sql.to_string();
    db.call(move |conn| {
        conn.query_row(&sql, [], |_| Ok(()))
            .optional()
            .map(|o| o.is_some())
            .map_err(DbError::from)
    })
    .await
    .unwrap_or_else(|e| panic!("row_exists failed: {e}"))
}

/// Run `stmts` as one journaled host transaction (the same path a host write
/// takes), then drain the pending-changeset journal and return the combined
/// changeset bytes. With no `stmts`, this just drains whatever the journal already
/// holds — the "clear the captured changes" idiom. Draining clears the journal, so
/// a later `capture_bytes` returns only the writes since this one.
pub(crate) async fn capture_bytes(db: &Database, stmts: &[&str]) -> Vec<u8> {
    let statements: Vec<String> = stmts
        .iter()
        .map(|statement| statement.to_string())
        .collect();
    let tables: Vec<String> = db
        .synced_tables()
        .iter()
        .map(|table| table.name().to_string())
        .collect();
    db.call(move |conn| {
        let mut session = rusqlite::session::Session::new(conn).map_err(DbError::from)?;
        for table in tables {
            session
                .attach(Some(table.as_str()))
                .map_err(DbError::from)?;
        }
        for statement in statements {
            conn.execute_batch(&statement).map_err(DbError::from)?;
        }
        let mut bytes = Vec::new();
        session.changeset_strm(&mut bytes).map_err(DbError::from)?;
        Ok(bytes)
    })
    .await
    .unwrap_or_else(|error| panic!("capture failed: {error}"))
}

/// Apply a changeset to the test database with the production conflict-resolving
/// apply path, scoped to `tables`. A plain `call`, like the cycle's apply: an apply
/// is never journaled, so the applied rows are not recorded as this device's own
/// outgoing changes.
pub(crate) async fn apply_to_db(db: &Database, bytes: &[u8], tables: &[SyncedTable]) {
    let bytes = bytes.to_vec();
    let tables = tables.to_vec();
    let receiver_wall_ms = db.receive_wall_ms();
    db.call(move |conn| {
        resolve_and_apply_changeset(conn, &bytes, &tables, receiver_wall_ms).map(|_| ())
    })
    .await
    .expect("apply changeset");
}

/// A temp dir plus a [`StoreDir`] rooted at it. The returned `TempDir` must be
/// held for the directory to outlive the test.
pub(crate) fn temp_store_dir() -> (tempfile::TempDir, StoreDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new(tmp.path());
    (tmp, dir)
}

/// Hex-encoded ed25519 public key, as membership entries and the wrapped-key
/// store identify a member.
pub(crate) fn pubkey_hex(kp: &UserKeypair) -> String {
    hex::encode(kp.public_key())
}

/// Ed25519 identity derived from exact test-owned seed bytes.
pub(crate) fn user_keypair_from_seed(seed: [u8; 32]) -> UserKeypair {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
    UserKeypair::from_signing_key_bytes(&signing_key.to_keypair_bytes())
        .expect("seed-derived signing key is valid")
}

pub(crate) async fn create_exact_protocol_object(
    storage: &dyn crate::storage::SyncStorage,
    context: &crate::storage::ProtocolObjectContext,
    semantic_prefix: &str,
    extension: &str,
    bytes: &[u8],
) -> Result<crate::storage::ExactObjectRef, String> {
    let slot = storage
        .allocate_protocol_slot(context, semantic_prefix, extension)
        .await
        .map_err(|error| error.to_string())?;
    let prepared = storage
        .prepare_protocol_object(context, slot, semantic_prefix, bytes.to_vec())
        .map_err(|error| error.to_string())?;
    storage
        .create_protocol_object(&prepared)
        .await
        .map_err(|error| error.to_string())?;
    Ok(prepared.reference().clone())
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn load_exact_materialized_commit(
    store: &crate::sync::store::Store,
    stream_id: &str,
    sequence: u64,
) -> Result<
    Option<(
        crate::protocol::store_commit::StoreBatchCommitRef,
        crate::protocol::store_commit::VerifiedStoreBatchCommit,
    )>,
    String,
> {
    store
        .load_exact_materialized_commit(stream_id, sequence)
        .await
}

pub(crate) async fn create_exact_test_store(
    db: &Database,
    storage: &std::sync::Arc<crate::storage::CloudSyncStorage>,
    store_id: &str,
    signer: &UserKeypair,
) -> Result<crate::protocol::store_commit::StoreRootRef, String> {
    let initialized = crate::sync::store::Store::create(
        crate::database::StoreDatabase::new(db),
        storage.clone(),
        store_id,
        signer,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(initialized.store.store_root().clone())
}

pub(crate) async fn open_exact_test_store_as(
    db: &Database,
    storage: &std::sync::Arc<crate::storage::CloudSyncStorage>,
    root: &crate::protocol::store_commit::StoreRootRef,
    identity: &UserKeypair,
) -> Result<(), String> {
    crate::sync::store::Store::open(
        crate::database::StoreDatabase::new(db),
        storage.clone(),
        root,
        identity,
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

pub(crate) async fn run_cycle_fixture(
    db: &Database,
    storage: crate::storage::CloudSyncStorage,
    store_dir: &StoreDir,
) -> Result<crate::sync::cycle::SyncComponents, String> {
    let database = crate::database::StoreDatabase::new(db);
    let local_blob_access =
        crate::sync::test_owner_graph::local_blob_access(database.clone(), store_dir.clone());
    let expected_store_root = database
        .local_store_root_ref()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "cycle fixture database has no exact Store root".to_string())?;
    let components = Box::pin(crate::sync::cycle::init_sync_over_storage(
        &database,
        local_blob_access,
        storage,
        crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root,
        },
        None,
    ))
    .await
    .map_err(|error| error.to_string())?;
    Box::pin(components.run_cycle(&crate::clock::SystemClock, None, store_dir, None))
        .await
        .map_err(|error| error.to_string())?;
    Ok(components)
}

pub(crate) async fn promote_active_member_fixture(
    store: &TestStore,
    owner_db: &Database,
    member_db: &Database,
    owner: &UserKeypair,
    member: &UserKeypair,
    encryption: &crate::encryption::EncryptionService,
) -> Result<crate::protocol::circle_control::StoreMembershipStateRef, String> {
    let member_database = crate::database::StoreDatabase::new(member_db);
    let owner_store = store.bind_device(owner_db, owner).await?;
    let member_store = store.bind_device(member_db, member).await?;
    let (member_registration, _) = member_database
        .local_blob_write_authority()
        .await
        .map_err(|error| error.to_string())?;
    let request = owner_store
        .store
        .begin_owner_promotion(member_registration)
        .await
        .map_err(|error| format!("begin Owner promotion: {error}"))?;
    let acceptance = member_store
        .store
        .accept_owner_promotion(request)
        .await
        .map_err(|error| format!("accept Owner promotion: {error}"))?;
    let finalized = owner_store
        .store
        .finalize_owner_promotion(encryption, acceptance)
        .await
        .map_err(|error| format!("finalize Owner promotion: {error}"))?;
    let (_temp, store_dir) = temp_store_dir();
    let mut authorized_member_store = member_store
        .store
        .authorize_writer()
        .await
        .map_err(|error| error.to_string())?;
    let pull = authorized_member_store
        .pull(&store_dir, Some(encryption))
        .await
        .map_err(|error| error.to_string())?;
    if !pull.held_positions.is_empty() {
        return Err(format!(
            "Owner promotion pull held signed positions: {:?}",
            pull.held_positions
        ));
    }
    Ok(finalized)
}

/// Grants a Dropbox shared-folder membership to whichever peer account asks —
/// the provider-side step a cross-principal admission needs before the joining
/// device can write to the store's namespace.
pub(crate) struct TestDropboxAccessAdministrator {
    pub namespace_id: String,
}

#[async_trait::async_trait]
impl crate::sync::store::DeviceProviderAccessAdministrator for TestDropboxAccessAdministrator {
    async fn grant_member_access(
        &self,
        _member_pubkey: &str,
        _provider_account_email: Option<&str>,
        peer: &crate::storage::ProviderDeviceBinding,
    ) -> Result<crate::protocol::provider::ProviderAccessLocator, crate::sync::store::DeviceJoinError>
    {
        let crate::storage::ProviderPrincipalId::Dropbox { account_id } = &peer.principal else {
            return Err(crate::sync::store::DeviceJoinError::Provider(
                "test Dropbox access administrator received a non-Dropbox peer".to_string(),
            ));
        };
        Ok(
            crate::protocol::provider::ProviderAccessLocator::DropboxSharedFolderMember {
                namespace_id: self.namespace_id.clone(),
                account_id: account_id.clone(),
            },
        )
    }
}

#[cfg(test)]
pub(crate) fn install_cross_principal_device_fixture<'a>(
    store: &'a TestStore,
    observer_db: &'a Database,
    local_db: &'a Database,
    identity: &'a UserKeypair,
    peer_account_id: &'a str,
    published_at: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + 'a>> {
    Box::pin(async move {
        let local_database = crate::database::StoreDatabase::new(local_db);
        let observer = store.bind_device(observer_db, &store.signer).await?;
        let provider_binding = crate::storage::SyncStorage::provider_binding(&*store.storage)
            .await
            .map_err(|error| error.to_string())?;
        let crate::storage::StoreProviderBinding::Dropbox { namespace_id } =
            &provider_binding.store
        else {
            return Err("cross-principal test Store is not Dropbox".to_string());
        };
        let namespace_id = namespace_id.clone();
        let peer_binding = crate::storage::ResolvedProviderBinding {
            store: provider_binding.store.clone(),
            device: crate::storage::ProviderDeviceBinding {
                principal: crate::storage::ProviderPrincipalId::Dropbox {
                    account_id: peer_account_id.to_string(),
                },
            },
        };
        let peer_home = std::sync::Arc::new(
            store
                .home
                .as_ref()
                .clone()
                .with_provider_binding(peer_binding),
        );
        let peer_storage = crate::storage::CloudSyncStorage::new(
            peer_home.clone(),
            crate::storage::CloudCipher::Encrypted(crate::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            crate::storage::BlobPathScheme::Hashed,
            "cross-principal-test-store",
            identity.clone(),
        )
        .map_err(|error| error.to_string())?;
        let pending_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .map_err(|error| error.to_string())?;
        let offer = observer
            .store
            .begin_device_join(&pubkey_hex(identity))
            .await
            .map_err(|error| error.to_string())?;
        let mut pending_join = crate::sync::store::open_pending_device_join_authority(
            &pending,
            &peer_storage,
            identity,
            offer,
        )
        .await
        .map_err(|error| error.to_string())?;
        let access_request = pending_join
            .prepare_provider_access_request()
            .await
            .map_err(|error| error.to_string())?;
        let access_administrator = TestDropboxAccessAdministrator { namespace_id };
        let approval = observer
            .store
            .authorize_device_provider_access(access_request, Some(&access_administrator))
            .await
            .map_err(|error| error.to_string())?;
        if !matches!(
            approval.admission,
            crate::sync::store::DeviceProviderAdmissionChallenge::CrossPrincipal(_)
        ) {
            return Err("distinct provider principals produced same-principal admission".into());
        }
        let registration_request = pending_join
            .prepare_registration_request(approval)
            .await
            .map_err(|error| error.to_string())?;
        let provisional = observer
            .store
            .accept_device_registration_request(registration_request)
            .await
            .map_err(|error| error.to_string())?;
        let provider_ready = observer
            .store
            .publish_device_provider_challenge(provisional)
            .await
            .map_err(|error| error.to_string())?;
        let mut joining =
            crate::sync::store::begin_joining_store_from_pending(pending_join, local_database)
                .await
                .map_err(|error| error.to_string())?;
        let readiness = joining
            .bootstrap(provider_ready, published_at)
            .await
            .map_err(|error| error.to_string())?;
        if !matches!(
            readiness.provider,
            crate::sync::store::DeviceProviderReadiness::CrossPrincipal(_)
        ) {
            return Err("distinct provider principals produced same-principal readiness".into());
        }
        let completion = observer
            .store
            .complete_device_provider_admission(readiness)
            .await
            .map_err(|error| error.to_string())?;
        if !matches!(
            completion.admission,
            crate::sync::store::DeviceProviderAdmission::CrossPrincipal(_)
        ) {
            return Err("distinct provider principals produced same-principal completion".into());
        }
        let activation = observer
            .store
            .finalize_device_join(completion)
            .await
            .map_err(|error| error.to_string())?;
        joining
            .complete(activation)
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    })
}

pub(crate) struct TestStore {
    pub home: std::sync::Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
    pub storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
    pub root: crate::protocol::store_commit::StoreRootRef,
    pub signer: UserKeypair,
    producers: tokio::sync::Mutex<TestStoreProducers>,
}

impl std::ops::Deref for TestStore {
    type Target = crate::storage::CloudSyncStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

#[derive(Clone)]
pub(crate) struct TestDevice {
    pub db: crate::database::StoreDatabase,
    pub store: std::sync::Arc<crate::sync::store::Store>,
    pub device_id: String,
}

impl std::ops::Deref for TestDevice {
    type Target = crate::sync::store::Store;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl TestDevice {
    pub(crate) async fn create(
        db: &Database,
        storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
        founder_timestamp: &str,
        identity: UserKeypair,
    ) -> Result<Self, String> {
        Self::create_with_database(
            crate::database::StoreDatabase::new(db),
            storage,
            founder_timestamp,
            identity,
        )
        .await
    }

    pub(crate) async fn create_with_database(
        database: crate::database::StoreDatabase,
        storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
        founder_timestamp: &str,
        identity: UserKeypair,
    ) -> Result<Self, String> {
        let initialized = crate::sync::store::Store::create(
            database.clone(),
            storage,
            founder_timestamp,
            &identity,
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(Self {
            db: database,
            store: std::sync::Arc::new(initialized.store),
            device_id: initialized.device_id,
        })
    }

    pub(crate) async fn load(
        db: &Database,
        storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
        identity: UserKeypair,
    ) -> Result<Self, String> {
        Self::load_with_database(crate::database::StoreDatabase::new(db), storage, identity).await
    }

    pub(crate) async fn load_with_database(
        database: crate::database::StoreDatabase,
        storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
        identity: UserKeypair,
    ) -> Result<Self, String> {
        let store = crate::sync::store::Store::load(database.clone(), storage, identity)
            .await
            .map_err(|error| error.to_string())?;
        let device_id = database
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "loaded test device has no local device id".to_string())?;
        Ok(Self {
            db: database,
            store: std::sync::Arc::new(store),
            device_id,
        })
    }

    pub(crate) fn store_root(&self) -> &crate::protocol::store_commit::StoreRootRef {
        self.store.store_root()
    }

    pub(crate) async fn create_circle(
        &self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<crate::CircleId, crate::sync::store::CircleOperationError> {
        self.store
            .circles()
            .create_circle(metadata_stamp, name)
            .await
    }

    pub(crate) async fn rename_circle(
        &self,
        metadata_stamp: &str,
        circle_id: crate::CircleId,
        name: &str,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.store
            .circles()
            .rename_circle(metadata_stamp, circle_id, name)
            .await
    }

    pub(crate) async fn delete_circle(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.store.circles().delete_circle(circle_id).await
    }

    pub(crate) async fn membership(
        &self,
    ) -> Result<crate::protocol::membership::MembershipChain, String> {
        self.store
            .membership_for_test()
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) fn protocol_root(&self) -> &crate::protocol::store_commit::StoreProtocolRoot {
        self.store.protocol_root_for_test()
    }

    #[cfg(test)]
    pub(crate) async fn prepare_wrapped_key(
        &self,
        recipient: &str,
        value: crate::protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<crate::protocol::wrapped_store_key::PreparedWrappedStoreKey, String> {
        self.store
            .prepare_wrapped_key_for_test(recipient, value)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn open_membership_keyring(
        &self,
    ) -> Result<crate::encryption::EncryptionService, String> {
        self.store.open_membership_keyring_for_test().await
    }

    pub(crate) async fn publish_snapshot(
        &self,
        db_image: Vec<u8>,
        coverage: crate::protocol::store_commit::CommitFrontier,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, String> {
        self.store
            .publish_snapshot_for_test(
                crate::database::CreatedSnapshot {
                    db_image,
                    blobs: Vec::new(),
                },
                coverage,
                "2026-07-16T00:00:00Z".to_string(),
            )
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn publish_acknowledgement(
        &self,
        frontier: crate::protocol::store_commit::CommitFrontier,
    ) -> Result<(), String> {
        self.store
            .stage_acknowledgement_for_test(frontier, "2026-07-16T00:00:01Z".to_string())
            .await
            .map_err(|error| error.to_string())?;
        let published = self
            .store
            .drain_acknowledgements_for_test()
            .await
            .map_err(|error| error.to_string())?;
        if published != 1 {
            return Err(format!(
                "snapshot acknowledgement fixture published {published} acknowledgements instead of one"
            ));
        }
        Ok(())
    }

    pub(crate) async fn stage_acknowledgement(
        &self,
        frontier: crate::protocol::store_commit::CommitFrontier,
        sync_time: String,
    ) -> Result<crate::protocol::store_commit::StoreAck, String> {
        self.store
            .stage_acknowledgement_for_test(frontier, sync_time)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn drain_acknowledgements(&self) -> Result<u64, String> {
        self.store
            .drain_acknowledgements_for_test()
            .await
            .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub(crate) async fn stage_acknowledgement_exact(
        &self,
        frontier: crate::protocol::store_commit::CommitFrontier,
        sync_time: String,
    ) -> Result<crate::protocol::store_commit::StoreAck, crate::sync::store::StoreAckError> {
        self.store
            .stage_acknowledgement_for_test(frontier, sync_time)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn drain_acknowledgements_exact(
        &self,
    ) -> Result<u64, crate::sync::store::StoreAckError> {
        self.store.drain_acknowledgements_for_test().await
    }

    #[cfg(test)]
    pub(crate) async fn stage_circle_acknowledgements(
        &self,
        frontier: &crate::protocol::store_commit::CommitFrontier,
        sync_time: &str,
    ) -> Result<(), crate::sync::store::StoreAckError> {
        self.store
            .stage_circle_acknowledgements_for_test(frontier, sync_time)
            .await
    }

    pub(crate) async fn load_commit_ancestry_until(
        &self,
        start: crate::protocol::store_commit::StoreBatchCommitRef,
        coverage: &crate::protocol::store_commit::CommitFrontier,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
        )>,
        String,
    > {
        self.store
            .load_commit_ancestry_until_for_test(start, coverage)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn export_activated_device_continuation(
        &self,
    ) -> Result<crate::restoration::ActivatedContinuation, String> {
        self.store
            .export_activated_device_continuation_for_test()
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn latest_store_position(
        &self,
    ) -> Result<Option<crate::protocol::store_commit::StoreBatchCommitRef>, String> {
        self.store
            .latest_local_store_position()
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn pull_store(
        &self,
        store_dir: &StoreDir,
    ) -> Result<
        (
            std::collections::BTreeMap<String, u64>,
            crate::sync::store::StorePullResult,
        ),
        crate::sync::store::StorePullError,
    > {
        let routing_encryption = crate::encryption::EncryptionService::from_key([42; 32]);
        let mut authorization = self
            .store
            .authorize_writer()
            .await
            .map_err(|error| crate::sync::store::StorePullError::Database(error.to_string()))?;
        let result = authorization
            .pull(store_dir, Some(&routing_encryption))
            .await
            .map_err(|error| crate::sync::store::StorePullError::Database(error.to_string()))?;
        let sequences = result
            .frontier
            .iter()
            .map(|(stream, reference)| (stream.clone(), reference.coord.sequence()))
            .collect();
        Ok((sequences, result))
    }
}

struct TestStoreProducers {
    founder: TestDevice,
    unassigned: Option<TestDevice>,
    by_name: HashMap<String, TestDevice>,
}

impl TestStore {
    pub(crate) async fn create(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
    ) -> Result<Self, String> {
        let home = std::sync::Arc::new(
            crate::storage::cloud::test_utils::InMemoryCloudHome::new().with_provider_binding(
                crate::storage::ResolvedProviderBinding {
                    store: crate::storage::StoreProviderBinding::GoogleDrive {
                        corpus: crate::storage::GoogleDriveCorpus::SharedDrive {
                            drive_id: "test-drive".to_string(),
                            folder_id: "test-folder".to_string(),
                        },
                    },
                    device: crate::storage::ProviderDeviceBinding {
                        principal: crate::storage::ProviderPrincipalId::GoogleDrive {
                            permission_id: "test-permission".to_string(),
                        },
                    },
                },
            ),
        );
        Box::pin(Self::create_with_home(db, store_id, signer, home)).await
    }

    pub(crate) async fn create_with_database(
        database: crate::database::StoreDatabase,
        store_id: &str,
        signer: UserKeypair,
    ) -> Result<Self, String> {
        let home = std::sync::Arc::new(
            crate::storage::cloud::test_utils::InMemoryCloudHome::new().with_provider_binding(
                crate::storage::ResolvedProviderBinding {
                    store: crate::storage::StoreProviderBinding::GoogleDrive {
                        corpus: crate::storage::GoogleDriveCorpus::SharedDrive {
                            drive_id: "test-drive".to_string(),
                            folder_id: "test-folder".to_string(),
                        },
                    },
                    device: crate::storage::ProviderDeviceBinding {
                        principal: crate::storage::ProviderPrincipalId::GoogleDrive {
                            permission_id: "test-permission".to_string(),
                        },
                    },
                },
            ),
        );
        Box::pin(Self::create_with_protection_database(
            database,
            store_id,
            signer,
            home,
            crate::storage::CloudCipher::Encrypted(crate::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            crate::storage::BlobPathScheme::Hashed,
        ))
        .await
    }

    /// A store whose home keeps blobs **browsable**: stored in the clear under
    /// readable paths. The counterpart of [`Self::create`], whose home is opaque
    /// (sealed under the store key, hashed paths). The pair is fixed per home,
    /// so a test that needs the browsable verification story needs this store.
    pub(crate) async fn create_browsable(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
    ) -> Result<Self, String> {
        let home = std::sync::Arc::new(
            crate::storage::cloud::test_utils::InMemoryCloudHome::new().with_provider_binding(
                crate::storage::ResolvedProviderBinding {
                    store: crate::storage::StoreProviderBinding::GoogleDrive {
                        corpus: crate::storage::GoogleDriveCorpus::SharedDrive {
                            drive_id: "test-drive".to_string(),
                            folder_id: "test-folder".to_string(),
                        },
                    },
                    device: crate::storage::ProviderDeviceBinding {
                        principal: crate::storage::ProviderPrincipalId::GoogleDrive {
                            permission_id: "test-permission".to_string(),
                        },
                    },
                },
            ),
        );
        Box::pin(Self::create_with_protection(
            db,
            store_id,
            signer,
            home,
            crate::storage::CloudCipher::Plaintext,
            crate::storage::BlobPathScheme::Plain,
        ))
        .await
    }

    pub(crate) async fn create_with_provider_binding(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
        binding: crate::storage::ResolvedProviderBinding,
    ) -> Result<Self, String> {
        let home = std::sync::Arc::new(
            crate::storage::cloud::test_utils::InMemoryCloudHome::new()
                .with_provider_binding(binding),
        );
        Box::pin(Self::create_with_home(db, store_id, signer, home)).await
    }

    async fn create_with_home(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Self, String> {
        Box::pin(Self::create_with_protection(
            db,
            store_id,
            signer,
            home,
            crate::storage::CloudCipher::Encrypted(crate::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            crate::storage::BlobPathScheme::Hashed,
        ))
        .await
    }

    async fn create_with_protection(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
        cipher: crate::storage::CloudCipher,
        blob_paths: crate::storage::BlobPathScheme,
    ) -> Result<Self, String> {
        Self::create_with_protection_database(
            crate::database::StoreDatabase::new(db),
            store_id,
            signer,
            home,
            cipher,
            blob_paths,
        )
        .await
    }

    async fn create_with_protection_database(
        database: crate::database::StoreDatabase,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
        cipher: crate::storage::CloudCipher,
        blob_paths: crate::storage::BlobPathScheme,
    ) -> Result<Self, String> {
        let storage = std::sync::Arc::new(
            crate::storage::CloudSyncStorage::new(
                home.clone(),
                cipher,
                blob_paths,
                store_id,
                signer.clone(),
            )
            .map_err(|error| error.to_string())?,
        );
        let founder =
            TestDevice::create_with_database(database, storage.clone(), store_id, signer.clone())
                .await?;
        let root = founder.store_root().clone();
        Ok(Self {
            home,
            storage,
            root,
            signer,
            producers: tokio::sync::Mutex::new(TestStoreProducers {
                founder: founder.clone(),
                unassigned: Some(founder),
                by_name: HashMap::new(),
            }),
        })
    }

    pub(crate) async fn new() -> Self {
        Box::pin(Self::for_store("test-store")).await
    }

    pub(crate) async fn for_store(store_id: &str) -> Self {
        Box::pin(Self::with_store_and_keypair(
            store_id,
            UserKeypair::generate(),
        ))
        .await
    }

    pub(crate) async fn with_store_and_keypair(store_id: &str, signer: UserKeypair) -> Self {
        let db = open_test_db();
        Box::pin(Self::create(&db, store_id, signer))
            .await
            .expect("create exact test Store")
    }

    pub(crate) fn protocol_founder_pubkey(&self) -> String {
        crate::keys::public_key_hex(&self.signer)
    }

    pub(crate) fn protocol_founder_keypair(&self) -> UserKeypair {
        self.signer.clone()
    }

    pub(crate) async fn bind_device(
        &self,
        db: &Database,
        identity: &UserKeypair,
    ) -> Result<TestDevice, String> {
        self.bind_store_device(&crate::database::StoreDatabase::new(db), identity)
            .await
    }

    pub(crate) async fn activate_joined_device(
        &self,
        observer_db: &Database,
        joining_db: &Database,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<TestDevice, String> {
        self.activate_joined_device_from_store_database(
            crate::database::StoreDatabase::new(observer_db),
            joining_db,
            joining_identity,
            published_at,
        )
        .await
    }

    async fn activate_joined_device_from_store_database(
        &self,
        observer_database: crate::database::StoreDatabase,
        joining_db: &Database,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<TestDevice, String> {
        let joining_database = crate::database::StoreDatabase::new(joining_db);
        let observer = self
            .bind_store_device(&observer_database, &self.signer)
            .await?;
        let pending_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .map_err(|error| error.to_string())?;
        let offer = observer
            .store
            .begin_device_join(&pubkey_hex(joining_identity))
            .await
            .map_err(|error| format!("begin device join: {error}"))?;
        let mut pending_join = crate::sync::store::open_pending_device_join_authority(
            &pending,
            &*self.storage,
            joining_identity,
            offer,
        )
        .await
        .map_err(|error| format!("open pending device join: {error}"))?;
        let access_request = pending_join
            .prepare_provider_access_request()
            .await
            .map_err(|error| format!("prepare provider access request: {error}"))?;
        let approval = observer
            .store
            .authorize_device_provider_access(access_request, None)
            .await
            .map_err(|error| format!("authorize device provider access: {error}"))?;
        let registration_request = pending_join
            .prepare_registration_request(approval)
            .await
            .map_err(|error| format!("prepare device registration request: {error}"))?;
        let provisional = observer
            .store
            .accept_device_registration_request(registration_request)
            .await
            .map_err(|error| format!("accept device registration request: {error}"))?;
        let provider_ready = observer
            .store
            .publish_device_provider_challenge(provisional)
            .await
            .map_err(|error| format!("publish device provider challenge: {error}"))?;
        let mut joining =
            crate::sync::store::begin_joining_store_from_pending(pending_join, joining_database)
                .await
                .map_err(|error| format!("begin joining Store: {error}"))?;
        let (_bootstrap_temp, bootstrap_store_dir) = temp_store_dir();
        let routing_encryption = crate::encryption::EncryptionService::from_key([42; 32]);
        let bootstrap_pull = joining
            .pull_store_history(&bootstrap_store_dir, Some(&routing_encryption))
            .await
            .map_err(|error| format!("pull joining Store history: {error}"))?;
        if !bootstrap_pull.held_positions.is_empty() {
            return Err(format!(
                "device join bootstrap pull held signed positions: {:?}",
                bootstrap_pull.held_positions
            ));
        }
        let readiness = joining
            .bootstrap(provider_ready, published_at)
            .await
            .map_err(|error| format!("bootstrap joining Store: {error}"))?;
        let completion = observer
            .store
            .complete_device_provider_admission(readiness)
            .await
            .map_err(|error| format!("complete device provider admission: {error}"))?;
        let activation = observer
            .store
            .finalize_device_join(completion)
            .await
            .map_err(|error| format!("finalize device join: {error}"))?;
        joining
            .complete(activation)
            .await
            .map_err(|error| format!("complete joining Store: {error}"))?;
        self.bind_device(joining_db, joining_identity).await
    }

    pub(crate) async fn bind_store_device(
        &self,
        database: &crate::database::StoreDatabase,
        identity: &UserKeypair,
    ) -> Result<TestDevice, String> {
        let store = crate::sync::store::Store::load(
            database.clone(),
            self.storage.clone(),
            identity.clone(),
        )
        .await
        .map_err(|error| error.to_string())?;
        let device_id = database
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "bound test device has no local device id".to_string())?;
        Ok(TestDevice {
            db: database.clone(),
            store: std::sync::Arc::new(store),
            device_id,
        })
    }

    pub(crate) async fn invite_member(
        &self,
        db: &Database,
        identity: &UserKeypair,
        hlc: &crate::sync::hlc::Hlc,
        member_pubkey: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_name: &str,
    ) -> Result<crate::joining::InviteCode, crate::sync::store::MembershipOpsError> {
        let device = self.bind_device(db, identity).await.map_err(|error| {
            crate::sync::store::MembershipOpsError::Chain(
                crate::sync::store::AnchoredChainError::LoadFailed(error),
            )
        })?;
        device
            .store
            .invite_member(
                hlc,
                member_pubkey,
                invitee_email,
                role,
                encryption,
                self.storage.store_id(),
                store_name,
            )
            .await
    }

    pub(crate) async fn remove_member(
        &self,
        db: &Database,
        identity: &UserKeypair,
        hlc: &crate::sync::hlc::Hlc,
        member_pubkey: &str,
        encryption: &crate::encryption::EncryptionService,
        security: &crate::store_security::StoreSecurity,
    ) -> Result<String, crate::sync::store::MembershipOpsError> {
        let device = self.bind_device(db, identity).await.map_err(|error| {
            crate::sync::store::MembershipOpsError::Chain(
                crate::sync::store::AnchoredChainError::LoadFailed(error),
            )
        })?;
        let pending_rotation = self.storage.shared_pending_rotation();
        device
            .store
            .remove_member(
                hlc,
                member_pubkey,
                encryption,
                security,
                self.storage.cipher_state(),
                &pending_rotation,
            )
            .await
    }

    pub(crate) async fn device_id(&self, name: &str) -> Result<String, String> {
        Ok(self.ensure_producer(name).await?.device_id)
    }

    pub(crate) async fn founder_device(&self) -> Result<TestDevice, String> {
        Ok(self.producers.lock().await.founder.clone())
    }

    pub(crate) async fn next_commit_sequence(&self, name: &str) -> Result<u64, String> {
        self.ensure_producer(name)
            .await?
            .store
            .latest_local_store_position()
            .await
            .map_err(|error| error.to_string())?
            .map_or(Ok(1), |reference| {
                reference
                    .coord
                    .sequence()
                    .checked_add(1)
                    .ok_or_else(|| "test producer sequence exhausted u64".to_string())
            })
    }

    pub(crate) async fn founder_device_authority(
        &self,
    ) -> Result<
        (
            crate::protocol::store_commit::StoreDeviceRegistrationRef,
            crate::protocol::store_commit::StoreDeviceRegistration,
            UserKeypair,
        ),
        String,
    > {
        let device = self.ensure_producer("founder").await?;
        let (reference, registration) = device
            .db
            .activated_store_device_registration_records()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|(_, registration)| registration.device_id.to_string() == device.device_id)
            .ok_or_else(|| "founder device registration is not active".to_string())?;
        let device_signer = registration
            .device_signer(&self.signer)
            .map_err(|error| error.to_string())?;
        Ok((reference, registration, device_signer))
    }

    #[cfg(test)]
    pub(crate) async fn retained_merge_history_summary(
        &self,
        device_id: &crate::protocol::store_commit::StoreDeviceId,
        reference: crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary, String> {
        let db = {
            let producers = self.producers.lock().await;
            producers
                .by_name
                .values()
                .chain(producers.unassigned.iter())
                .find(|producer| producer.device_id == device_id.to_string())
                .map(|producer| producer.db.clone())
                .ok_or_else(|| format!("test Store has no producer for device {device_id}"))?
        };
        Ok(db
            .retained_merge_materialization(self.root.clone(), reference)
            .await
            .map_err(|error| error.to_string())?
            .history_summary()
            .clone())
    }

    #[cfg(test)]
    pub(crate) async fn publish_changeset(
        &self,
        name: &str,
        sequence: u64,
        changeset: &[u8],
        schema_version: u32,
    ) -> Result<crate::protocol::store_commit::StoreBatchCommitRef, String> {
        let device = self.ensure_producer(name).await?;
        let db = &device.db;
        if schema_version != db.schema_version() {
            return Err(format!(
                "test changeset schema version {schema_version} differs from producer schema {}",
                db.schema_version()
            ));
        }
        let before = device
            .store
            .latest_local_store_position()
            .await
            .map_err(|error| error.to_string())?;
        let expected = before
            .as_ref()
            .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
        if sequence != expected {
            return Err(format!(
                "test producer {name:?} expected sequence {expected}, got {sequence}"
            ));
        }
        db.enqueue_store_changeset_for_test(changeset.to_vec())
            .await
            .map_err(|error| error.to_string())?;
        let (_tmp, store_dir) = temp_store_dir();
        let mut writer = device
            .store
            .authorize_writer()
            .await
            .map_err(|error| error.to_string())?;
        let published = writer
            .publish_pending_store_writes(&store_dir)
            .await
            .map_err(|error| error.to_string())?;
        if published == 0 {
            return Err("test changeset did not prepare a Store commit".to_string());
        }
        device
            .store
            .latest_local_store_position()
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "published test changeset has no Store position".to_string())
    }

    async fn ensure_producer(&self, name: &str) -> Result<TestDevice, String> {
        {
            let producers = self.producers.lock().await;
            if let Some(producer) = producers.by_name.get(name) {
                return Ok(producer.clone());
            }
        }

        let unassigned = {
            let mut producers = self.producers.lock().await;
            producers.unassigned.take()
        };
        let producer = match unassigned {
            Some(producer) => producer,
            None => {
                let db = open_test_db();
                let observer_db = {
                    let producers = self.producers.lock().await;
                    producers
                        .by_name
                        .values()
                        .next()
                        .ok_or_else(|| "test Store has no active device observer".to_string())?
                        .db
                        .clone()
                };
                self.activate_joined_device_from_store_database(
                    observer_db,
                    &db,
                    &self.signer,
                    "2026-07-16T00:00:00Z",
                )
                .await?
            }
        };
        let mut producers = self.producers.lock().await;
        if producers
            .by_name
            .insert(name.to_string(), producer)
            .is_some()
        {
            return Err(format!("test producer {name:?} was registered twice"));
        }
        Ok(producers
            .by_name
            .get(name)
            .expect("inserted test producer exists")
            .clone())
    }

    pub(crate) async fn open_into(&self, db: &Database) -> Result<TestDevice, String> {
        self.open_into_store_database(&crate::database::StoreDatabase::new(db))
            .await
    }

    pub(crate) async fn open_into_store_database(
        &self,
        database: &crate::database::StoreDatabase,
    ) -> Result<TestDevice, String> {
        let initialized = crate::sync::store::Store::open(
            database.clone(),
            self.storage.clone(),
            &self.root,
            &self.signer,
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(TestDevice {
            db: database.clone(),
            store: std::sync::Arc::new(initialized.store),
            device_id: initialized.device_id,
        })
    }

    pub(crate) async fn publish_pending(
        &self,
        db: &Database,
        store_dir: &StoreDir,
    ) -> Result<bool, String> {
        self.publish_pending_store_database(&crate::database::StoreDatabase::new(db), store_dir)
            .await
    }

    pub(crate) async fn publish_pending_store_database(
        &self,
        database: &crate::database::StoreDatabase,
        store_dir: &StoreDir,
    ) -> Result<bool, String> {
        let device = self.bind_store_device(database, &self.signer).await?;
        let mut writer = device
            .store
            .authorize_writer()
            .await
            .map_err(|error| error.to_string())?;
        let prepared = writer
            .prepare_pending_store_write(store_dir)
            .await
            .map_err(|error| error.to_string())?;
        let published = writer
            .drain_store_writes()
            .await
            .map_err(|error| error.to_string())?;
        if published > 0 {
            crate::sync::test_owner_graph::local_blob_access(database.clone(), store_dir.clone())
                .drain_published_blob_drop_intents(u64::MAX)
                .await?;
            database
                .drain_local_blob_cleanup(store_dir)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(prepared || published > 0)
    }
}

pub(crate) async fn pull_into_result(
    db: &Database,
    store: &TestStore,
    store_dir: &StoreDir,
) -> Result<
    (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store::StorePullResult,
    ),
    crate::sync::store::StorePullError,
> {
    let device = Box::pin(store.open_into(db)).await.map_err(|error| {
        crate::sync::store::StorePullError::Membership(
            crate::sync::store::StorePullMembershipError::Message(error),
        )
    })?;
    device.pull_store(store_dir).await
}

pub(crate) async fn pull_into(
    db: &Database,
    store: &TestStore,
    store_dir: &StoreDir,
) -> (
    std::collections::BTreeMap<String, u64>,
    crate::sync::store::StorePullResult,
) {
    pull_into_result(db, store, store_dir)
        .await
        .expect("pull exact test Store")
}

/// The Store view of a test database. Every sync test builds one; naming it here
/// keeps the three test modules that used to declare it from drifting apart.
#[cfg(test)]
pub(crate) fn store_database(db: &Database) -> crate::database::StoreDatabase {
    crate::database::StoreDatabase::new(db)
}

/// A plaintext cloud cipher — the default for tests that are not exercising
/// sealing.
#[cfg(test)]
pub(crate) fn plaintext_cipher() -> std::sync::RwLock<crate::storage::CloudCipher> {
    std::sync::RwLock::new(crate::storage::CloudCipher::Plaintext)
}

/// The host-provided, eagerly-cached photo blob declaration most blob tests use.
#[cfg(test)]
pub(crate) fn photo_decl() -> BlobDecl {
    BlobDecl::new(
        "photos",
        crate::blob::Provenance::HostProvided,
        crate::blob::CacheFill::CacheEager,
    )
}

/// The notes schema with a remote-root parent, carrying `decl` on `note_photos`.
#[cfg(test)]
pub(crate) fn remote_root_db(decl: BlobDecl) -> Database {
    open_test_db_schema(
        vec![
            SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).remote_root(),
            SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
            SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey)
                .carries_blob(decl),
        ],
        test_migrations(),
    )
}

/// The cloud key a tombstone for `stored` is written under.
#[cfg(test)]
pub(crate) fn exact_tombstone_key(stored: &crate::blob::locator::StoredBlobRef) -> String {
    format!(
        "blob_tombstones/{}",
        crate::protocol::remote_object::remote_object_id(stored.object())
    )
}

/// Bind an already-present local file to a row's blob reference.
#[cfg(test)]
pub(crate) async fn register_external_blob(
    db: &Database,
    table: &str,
    row_id: &str,
    path: &std::path::Path,
) {
    let reference = db
        .row_blob_ref(table, row_id)
        .await
        .expect("load exact Local row blob reference");
    let path = path.to_path_buf();
    db.call(move |conn| Database::register_external_blob_on(conn, &reference, &path))
        .await
        .expect("register exact external blob reference");
}

/// Which protocol read an interceptor hook is running ahead of.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProtocolRead {
    Object,
    Slot,
    PreparedSlot,
}

/// Test-side observation of a [`SyncStorage`] call.
///
/// Every hook runs before the wrapped storage does the work, and returning `Err`
/// fails the call without reaching it. All hooks default to doing nothing, so an
/// interceptor states only the operations its test is about — which is the point:
/// a test that intercepts two reads should not also have to restate the sixteen
/// operations it does not care about.
#[cfg(test)]
#[async_trait::async_trait]
pub(crate) trait StorageInterceptor: Send + Sync {
    async fn before_protocol_create(
        &self,
        _prepared: &crate::storage::PreparedExactObject,
    ) -> Result<(), crate::storage::StorageError> {
        Ok(())
    }

    async fn before_protocol_read(
        &self,
        _read: ProtocolRead,
        _semantic_prefix: &str,
    ) -> Result<(), crate::storage::StorageError> {
        Ok(())
    }

    async fn before_blob_allocate(&self) -> Result<(), crate::storage::StorageError> {
        Ok(())
    }

    async fn before_blob_prepare(&self) -> Result<(), crate::storage::StorageError> {
        Ok(())
    }

    async fn before_blob_create(
        &self,
        _blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::storage::StorageError> {
        Ok(())
    }

    async fn before_blob_stage(&self) -> Result<(), crate::storage::StorageError> {
        Ok(())
    }
}

/// A [`SyncStorage`] that forwards every call to `inner`, giving `interceptor`
/// its chance first.
#[cfg(test)]
pub(crate) struct InterceptedStorage<S, I: StorageInterceptor>
where
    S: std::ops::Deref,
{
    inner: S,
    interceptor: I,
}

#[cfg(test)]
impl<S, I: StorageInterceptor> InterceptedStorage<S, I>
where
    S: std::ops::Deref,
{
    pub(crate) fn new(inner: S, interceptor: I) -> Self {
        Self { inner, interceptor }
    }

    pub(crate) fn interceptor(&self) -> &I {
        &self.interceptor
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl<S, I> crate::storage::SyncStorage for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: crate::storage::SyncStorage,
    I: StorageInterceptor,
{
    fn blob_path_scheme(&self) -> crate::storage::BlobPathScheme {
        self.inner.blob_path_scheme()
    }

    fn self_uploader(&self) -> String {
        self.inner.self_uploader()
    }

    fn cloud_home(&self) -> &dyn crate::storage::cloud::CloudHome {
        self.inner.cloud_home()
    }

    fn exact_slot_storage(&self) -> &dyn crate::storage::cloud::ExactSlotStorage {
        self.inner.exact_slot_storage()
    }

    fn exact_slot_probe_clients(
        &self,
    ) -> (
        &dyn crate::storage::cloud::ExactSlotStorage,
        &dyn crate::storage::cloud::ExactSlotStorage,
    ) {
        self.inner.exact_slot_probe_clients()
    }

    fn store_blob_protection(
        &self,
    ) -> Result<crate::storage::BlobSpoolProtection, crate::storage::StorageError> {
        self.inner.store_blob_protection()
    }

    async fn provider_binding(
        &self,
    ) -> Result<crate::storage::ResolvedProviderBinding, crate::storage::StorageError> {
        self.inner.provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &crate::storage::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<crate::storage::cloud::ObjectSlot, crate::storage::StorageError> {
        self.inner
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &crate::storage::ProtocolObjectContext,
        slot: crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<crate::storage::PreparedExactObject, crate::storage::StorageError> {
        self.inner
            .prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn create_protocol_object(
        &self,
        prepared: &crate::storage::PreparedExactObject,
    ) -> Result<(), crate::storage::StorageError> {
        self.interceptor.before_protocol_create(prepared).await?;
        self.inner.create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &crate::storage::ProtocolObjectContext,
        object: &crate::storage::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, crate::storage::StorageError> {
        self.interceptor
            .before_protocol_read(ProtocolRead::Object, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &crate::storage::ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, crate::storage::ExactObjectRef), crate::storage::StorageError> {
        self.interceptor
            .before_protocol_read(ProtocolRead::Slot, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &crate::storage::ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, crate::storage::PreparedExactObject), crate::storage::StorageError> {
        self.interceptor
            .before_protocol_read(ProtocolRead::PreparedSlot, semantic_prefix)
            .await?;
        self.inner
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &crate::storage::ExactObjectRef,
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::storage::BlobWriteAuthority<'_>,
    ) -> Result<crate::storage::cloud::ObjectSlot, crate::storage::StorageError> {
        self.interceptor.before_blob_allocate().await?;
        self.inner.allocate_blob_slot(locator, authority).await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::storage::BlobWriteAuthority<'_>,
        protection: crate::storage::BlobSpoolProtection,
        plaintext_file: &std::path::Path,
        spool_file: &std::path::Path,
    ) -> Result<crate::storage::BlobSpoolWrite, crate::storage::StorageError> {
        self.inner
            .seal_blob_to_spool(locator, authority, protection, plaintext_file, spool_file)
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::storage::BlobWriteAuthority<'_>,
        slot: crate::storage::cloud::ObjectSlot,
        stored_file: &std::path::Path,
    ) -> Result<crate::blob::locator::StoredBlobRef, crate::storage::StorageError> {
        self.interceptor.before_blob_prepare().await?;
        self.inner
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        authority: &crate::storage::BlobWriteAuthority<'_>,
        stored_file: &std::path::Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), crate::storage::StorageError> {
        self.interceptor.before_blob_create(blob).await?;
        self.inner
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.verify_blob_object(blob).await
    }

    async fn stage_exact_blob_download(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        dest: &std::path::Path,
    ) -> Result<crate::storage::StagedBlobFile, crate::storage::StorageError> {
        self.interceptor.before_blob_stage().await?;
        self.inner.stage_exact_blob_download(blob, dest).await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        protection: crate::storage::BlobSpoolProtection,
        dest: &std::path::Path,
    ) -> Result<crate::storage::StagedBlobFile, crate::storage::StorageError> {
        self.interceptor.before_blob_stage().await?;
        self.inner
            .stage_verified_blob_plaintext(blob, protection, dest)
            .await
    }

    async fn open_blob_range_reader(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        protection: crate::storage::BlobSpoolProtection,
    ) -> Result<crate::storage::BlobRangeReader, crate::storage::StorageError> {
        self.inner.open_blob_range_reader(blob, protection).await
    }

    async fn delete_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.delete_blob_object(blob).await
    }
}
