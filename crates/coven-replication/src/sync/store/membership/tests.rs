use super::*;
use crate::sync::test_helpers::{
    open_test_db, pubkey_hex, temp_store_dir, TestCustody, TestStore, TestStoreFixture,
};
use coven_database::StoreDatabase;
use coven_database::SyntheticStoreFixture;
use coven_keys::encryption::{EncryptionService, MasterKeyring};
use coven_keys::keys::{MasterKeyCustody, UserKeypair};
use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;
use coven_protocol::membership::{
    validate_membership_floor, AuthorHead, AuthorStreamId, MemberRole, MembershipChain,
    MembershipCoord, MembershipGrantId, MembershipHeadRef,
};
use coven_protocol::objects::ObjectSlot;
use coven_protocol::objects::{ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain};
use coven_storage::CloudSyncObjectStorage;
use coven_storage::{CloudCipher, CloudSyncCipherStateAccess};
use std::sync::{Arc, RwLock};

struct MergeFixture {
    store: std::sync::Arc<TestStore>,
    storage: Arc<coven_storage::CloudSyncConnection>,
    home: Arc<coven_storage::InMemoryCloudHome>,
    store_id: String,
    device: crate::sync::test_helpers::TestDevice,
    db: SyntheticStoreFixture,
    database: StoreDatabase,
    owner: UserKeypair,
    owner_pubkey: String,
    _store_dir_temp: tempfile::TempDir,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl MergeFixture {
    async fn new(store_id: &str) -> Self {
        let db = open_test_db();
        let owner = UserKeypair::generate();
        let owner_pubkey = pubkey_hex(&owner);
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, storage) =
            (TestStoreFixture::create(&db, store_id, owner.clone(), home.clone())
                .await
                .expect("create exact Store"))
            .into_parts();
        let device = store
            .bind_device(&db, &owner)
            .await
            .expect("bind exact Store");
        let database = coven_database::StoreDatabase::new(db.database());
        let (store_dir_temp, store_dir) = temp_store_dir();
        Self {
            store,
            storage,
            home,
            store_id: store_id.to_string(),
            device,
            db,
            database,
            owner,
            owner_pubkey,
            _store_dir_temp: store_dir_temp,
            store_dir,
        }
    }

    async fn load(&self) -> MembershipChain {
        self.device
            .membership_for_test()
            .await
            .expect("load exact membership chain")
    }

    async fn load_result(&self) -> Result<MembershipChain, crate::sync::store::StoreError> {
        self.device.membership_for_test().await
    }

    async fn invite_member(
        &self,
        member: &UserKeypair,
        role: MemberRole,
    ) -> crate::sync::store::MemberInvitation {
        self.store
            .invite_member(
                &self.db,
                &self.owner,
                &pubkey_hex(member),
                None,
                role,
                &EncryptionService::from_key([42; 32]),
                "Test Store",
            )
            .await
            .expect("invite exact member")
    }

    async fn try_remove_member(&self, member: &UserKeypair) -> Result<String, MembershipOpsError> {
        let custody = TestCustody::default();
        self.store
            .remove_member(
                &self.db,
                &self.owner,
                &pubkey_hex(member),
                &EncryptionService::from_key([42; 32]),
                &custody,
            )
            .await
    }

    async fn remove_member(&self, member: &UserKeypair) {
        self.try_remove_member(member)
            .await
            .expect("remove exact member");
    }
}

fn altered_exact(reference: &ExactObjectRef, label: &[u8]) -> ExactObjectRef {
    ExactObjectRef::new(
        reference.slot().clone(),
        label.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(label),
    )
}

#[tokio::test]
async fn anchored_chain_loads_the_root_named_by_its_authoritative_hash() {
    let fixture = MergeFixture::new("pinned-root").await;
    let unrelated = MergeFixture::new("unrelated-root").await;
    assert_ne!(
        fixture.store.root().store_root_hash,
        unrelated.store.root().store_root_hash
    );

    let loaded = fixture.load().await;
    let expected_store_id = fixture.store.root().store_root_id.to_string();
    assert_eq!(loaded.store_id(), Some(expected_store_id.as_str()));
    assert_eq!(loaded.founder_pubkey(), Some(fixture.owner_pubkey.as_str()));
}

#[tokio::test]
async fn current_floor_is_the_exact_signed_head_cut() {
    let fixture = MergeFixture::new("exact-floor").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;

    let chain = fixture.load().await;
    let floor = fixture
        .device
        .restore_membership()
        .await
        .expect("read exact membership floor")
        .membership_floor
        .0;

    assert_eq!(floor, chain.head_refs());
    assert!(floor.iter().all(|reference| reference.coord.seq > 0));
}

#[tokio::test]
async fn current_floor_requires_every_exact_entry() {
    let fixture = MergeFixture::new("missing-entry").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    let chain = fixture.load().await;
    let head = chain.head_refs().last().expect("current head").clone();
    let loaded_head = fixture
        .device
        .load_membership_head_for_test(&head)
        .await
        .expect("load exact head");
    fixture
        .storage
        .delete_protocol_object(&loaded_head.body.entry.object)
        .await
        .expect("remove exact selected entry");

    assert!(
        fixture.device.restore_membership().await.is_err(),
        "a signed head whose exact entry is absent must fail"
    );
}

#[tokio::test]
async fn persisted_author_floor_requires_readable_head() {
    let fixture = MergeFixture::new("missing-head").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    let chain = fixture.load().await;
    let head = chain.head_refs().last().expect("current head").clone();
    fixture
        .storage
        .delete_protocol_object(&head.object)
        .await
        .expect("remove exact head");

    fixture
        .load_result()
        .await
        .expect_err("a durable exact cursor requires its head");
}

#[tokio::test]
async fn membership_head_must_match_its_exact_author_coordinate() {
    let fixture = MergeFixture::new("head-author").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    let chain = fixture.load().await;
    let reference = chain.head_refs().last().expect("current head").clone();
    let mut head = fixture
        .device
        .load_membership_head_for_test(&reference)
        .await
        .expect("load exact head");
    head.body_mut().body.entry.coord.author_pubkey = hex::encode([9; 32]);
    fixture
        .store
        .overwrite_membership_head(&reference, &head)
        .await;

    fixture
        .load_result()
        .await
        .expect_err("a head selecting another author coordinate must fail");
}

#[tokio::test]
async fn invalid_membership_head_signature_preserves_owner_and_cursor() {
    let fixture = MergeFixture::new("bad-head-signature").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    let chain = fixture.load().await;
    let before_owner = fixture
        .db
        .database()
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .unwrap();
    let before_cursors = fixture
        .database
        .membership_head_cursors()
        .await
        .unwrap()
        .head_refs;
    let reference = chain.head_refs().last().expect("current head").clone();
    let mut head = fixture
        .device
        .load_membership_head_for_test(&reference)
        .await
        .expect("load exact head");
    head.corrupt_signature_for_test();
    fixture
        .store
        .overwrite_membership_head(&reference, &head)
        .await;

    fixture
        .load_result()
        .await
        .expect_err("an invalid exact head signature must fail");
    assert_eq!(
        fixture
            .db
            .database()
            .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
            .await
            .unwrap(),
        before_owner
    );
    assert_eq!(
        fixture
            .database
            .membership_head_cursors()
            .await
            .unwrap()
            .head_refs,
        before_cursors
    );
}

#[tokio::test]
async fn forked_membership_cursor_preserves_the_accepted_reference() {
    let fixture = MergeFixture::new("forked-cursor").await;
    let current = fixture
        .load()
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    fixture
        .database
        .persist_membership_head_cursors(vec![current.clone()])
        .await
        .unwrap();
    let mut fork = current.clone();
    fork.head_hash = coven_protocol::store_commit::ObjectHash::digest(b"forked head");
    fork.object = altered_exact(&current.object, b"forked object");

    assert!(fixture
        .database
        .persist_membership_head_cursors(vec![fork])
        .await
        .is_err());
    assert_eq!(
        fixture
            .database
            .membership_head_cursors()
            .await
            .unwrap()
            .head_refs,
        vec![current]
    );
}

#[tokio::test]
async fn missing_membership_head_is_rejected() {
    let fixture = MergeFixture::new("missing-founder-head").await;
    let chain = fixture.load().await;
    let head = chain.head_refs().first().expect("founder head");
    fixture
        .storage
        .delete_protocol_object(&head.object)
        .await
        .expect("remove founder head");

    fixture
        .device
        .load_membership_at_exact_heads_for_test(&[], &[])
        .await
        .expect_err("a founder entry without its exact signed head is uncommitted");
}

#[tokio::test]
async fn entry_beyond_membership_head_is_not_committed() {
    let fixture = MergeFixture::new("unheaded-entry").await;
    let member = UserKeypair::generate();
    let chain = fixture.load().await;
    let founder = chain.entries().first().expect("founder");
    let entry = chain
        .signed_set_member_in_stream(
            &fixture.owner,
            founder.stream_id,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "unheaded member".to_string(),
        )
        .expect("sign entry after exact head");
    let (prepared, _) = coven_storage::prepare_membership_entry(
        &*fixture.storage,
        fixture.store.root().store_root_hash,
        &entry,
    )
    .await
    .expect("prepare unheaded entry");
    fixture
        .storage
        .create_protocol_object(&prepared)
        .await
        .expect("publish unheaded entry");

    let loaded = fixture.load().await;
    assert!(!loaded.can_write_now(&pubkey_hex(&member)));
}

#[tokio::test]
async fn complete_chain_still_validates() {
    let fixture = MergeFixture::new("complete-chain").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;

    assert!(fixture.load().await.can_write_now(&pubkey_hex(&member)));
}

#[tokio::test]
async fn store_owns_membership_conflict_reads_and_rejects_a_foreign_choice_atomically() {
    let fixture = MergeFixture::new("store-membership-conflict-boundary").await;
    let storage = Arc::new(coven_storage::CloudSyncConnection::new(
        fixture.home.clone(),
        CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        coven_storage::BlobPathScheme::Hashed,
        &fixture.store_id,
        fixture.owner.clone(),
    ));
    let store = crate::sync::store::Store::load(
        fixture.database.clone(),
        storage,
        fixture.store_dir.clone(),
        fixture.owner.clone(),
    )
    .await
    .expect("load Store owner");
    assert!(store
        .membership_conflict()
        .await
        .expect("read membership conflict")
        .is_none());

    let chain = fixture.load().await;
    let choice = coven_protocol::membership::MembershipConflictChoice::new(
        "foreign-choice".to_string(),
        Vec::new(),
        coven_protocol::store_commit::ObjectHash::digest(b"foreign conflict"),
        coven_protocol::membership::MembershipConflictSelection::RevocationBranch {
            heads: vec![chain
                .head_refs()
                .first()
                .expect("founder membership head")
                .clone()],
        },
    );
    let result = store
        .resolve_membership_conflict(&choice, "2026-07-22T00:00:00Z")
        .await;

    assert!(
        matches!(
            &result,
            Err(MembershipOpsError::Invite(InviteError::Membership(
                coven_protocol::membership::MembershipError::InvalidConflictResolution
            )))
        ),
        "foreign conflict choice returned {result:?}"
    );
    assert!(fixture
        .database
        .outbound_membership_mutation()
        .await
        .expect("read membership mutation journal")
        .is_none());
}

#[tokio::test]
async fn store_membership_reads_require_the_installed_owner_anchor() {
    let fixture = MergeFixture::new("store-membership-owner-anchor").await;
    let storage = Arc::new(coven_storage::CloudSyncConnection::new(
        fixture.home.clone(),
        CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        coven_storage::BlobPathScheme::Hashed,
        &fixture.store_id,
        fixture.owner.clone(),
    ));
    let store = crate::sync::store::Store::load(
        fixture.database.clone(),
        storage,
        fixture.store_dir.clone(),
        fixture.owner.clone(),
    )
    .await
    .expect("load Store owner");
    fixture
        .db
        .database()
        .delete_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .expect("remove the installed owner anchor");

    let error = store
        .members()
        .await
        .expect_err("membership reads must not recreate a missing owner anchor");
    assert!(
        matches!(
            &error,
            MembershipOpsError::Chain(AnchoredChainError::LoadFailed(message))
                if message.contains("owner anchor is absent")
        ),
        "{error}"
    );
}

#[tokio::test]
async fn store_membership_reads_reject_tampered_founder_state() {
    let fixture = MergeFixture::new("store-membership-founder-state").await;
    let storage = Arc::new(coven_storage::CloudSyncConnection::new(
        fixture.home.clone(),
        CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        coven_storage::BlobPathScheme::Hashed,
        &fixture.store_id,
        fixture.owner.clone(),
    ));
    let store = crate::sync::store::Store::load(
        fixture.database.clone(),
        storage,
        fixture.store_dir.clone(),
        fixture.owner.clone(),
    )
    .await
    .expect("load Store owner");
    fixture
        .db
        .database()
        .set_protocol_state(coven_database::STORE_DEVICE_GENESIS_STATE_KEY, "{}")
        .await
        .expect("tamper with the installed founder state");

    let error = store
        .members()
        .await
        .expect_err("membership reads must validate installed founder state");
    assert!(
        matches!(
            &error,
            MembershipOpsError::Chain(AnchoredChainError::LoadFailed(message))
                if message.contains("Store device genesis")
        ),
        "{error}"
    );
}

#[tokio::test]
async fn open_store_reuses_its_verified_replay_baseline() {
    let fixture = MergeFixture::new("store-membership-retained-replay-baseline").await;
    fixture.load().await;
    fixture
        .database
        .replace_generation_zero_replay_authority_for_test(
            b"invalid retained replay authority".to_vec(),
        )
        .await
        .expect("replace retained replay authority after verification");

    fixture
        .load_result()
        .await
        .expect("reuse the replay baseline verified by the open connection");
}

#[tokio::test]
async fn store_prefix_projection_retains_direct_membership_heads() {
    let fixture = MergeFixture::new("project-direct-membership").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    let current = fixture.load().await;
    let projected = fixture
        .device
        .project_membership_for_test(current.head_refs())
        .await
        .expect("project direct membership to the empty Store prefix");

    assert_eq!(projected.head_refs(), current.head_refs());
    assert!(projected.can_write_now(&pubkey_hex(&member)));
}

#[tokio::test]
async fn store_prefix_projection_excludes_store_bound_membership_and_its_direct_suffix() {
    let fixture = MergeFixture::new("project-store-bound-membership").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    let member_db = open_test_db();
    fixture
        .store
        .activate_joined_device(&fixture.db, &member_db, &member, "2026-07-21T00:00:00Z")
        .await
        .expect("activate member device");
    let before_promotion = fixture.load().await;
    fixture
        .store
        .promote_active_member_fixture(
            &fixture.db,
            &member_db,
            &fixture.owner,
            &member,
            &EncryptionService::from_key([42; 32]),
        )
        .await
        .expect("promote member to Owner");
    let after_promotion = fixture.load().await;
    assert_ne!(after_promotion.head_refs(), before_promotion.head_refs());
    let later_member = UserKeypair::generate();
    fixture
        .invite_member(&later_member, MemberRole::Member)
        .await;
    let candidate = fixture.load().await;
    assert!(candidate.can_write_now(&pubkey_hex(&later_member)));
    let projected = fixture
        .device
        .project_membership_for_test(candidate.head_refs())
        .await
        .expect("project membership before the Owner promotion Store control");

    assert_eq!(projected.head_refs(), before_promotion.head_refs());
    assert!(projected.can_write_now(&pubkey_hex(&member)));
    assert!(!projected.is_owner_now(&pubkey_hex(&member)));
    assert!(!projected.can_write_now(&pubkey_hex(&later_member)));
}

#[tokio::test]
async fn exact_membership_heads_must_begin_at_their_grant_anchor() {
    let fixture = MergeFixture::new("relocated-exact-membership-head").await;
    let current = fixture.load().await;
    let founder_ref = current
        .head_refs()
        .first()
        .expect("founder membership head")
        .clone();
    let founder_head = fixture
        .device
        .load_membership_head_for_test(&founder_ref)
        .await
        .expect("load founder membership head");
    let registration = coven_database::StoreDatabase::new(fixture.db.database())
        .activated_store_device_registration(founder_head.body.author_registration.clone())
        .await
        .expect("load founder device registration");
    let signer = registration
        .value()
        .device_signer(&fixture.owner)
        .expect("derive founder device signer");
    let relocated = AuthorHead::signed(
        founder_head.store_id.clone(),
        founder_head.body.clone(),
        founder_head.activation.clone(),
        &signer,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        fixture.store.root().store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let prefix = coven_protocol::store_commit::membership_head_slot_prefix(
        &founder_ref.coord.author_pubkey,
        &founder_ref.coord.author_owner_grant,
        AuthorStreamId::from_bytes([99; 32]),
        founder_ref.coord.seq,
    );
    let slot = fixture
        .storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .expect("allocate relocated membership head slot");
    let prepared = fixture
        .storage
        .prepare_protocol_object(
            &context,
            slot,
            &prefix,
            serde_json::to_vec(&relocated).expect("serialize relocated membership head"),
        )
        .expect("prepare relocated membership head");
    fixture
        .storage
        .create_protocol_object(&prepared)
        .await
        .expect("publish relocated membership head");
    let relocated_ref = MembershipHeadRef {
        coord: founder_ref.coord,
        head_hash: relocated.head_hash(),
        object: prepared.reference().clone(),
    };

    fixture
        .device
        .load_membership_at_exact_heads_for_test(&[relocated_ref], &[])
        .await
        .expect_err("a membership head relocated outside its grant anchor must fail");
}

#[tokio::test]
async fn invite_carries_the_founder_and_exact_root() {
    let fixture = MergeFixture::new("invite-authority").await;
    let invitee = UserKeypair::generate();
    let invite = fixture.invite_member(&invitee, MemberRole::Member).await;

    assert_eq!(invite.owner_pubkey, fixture.owner_pubkey);
    assert_eq!(invite.store_root, fixture.store.root());
    assert!(matches!(
        invite.membership_floor,
        coven_protocol::membership::MembershipFloor(ref floor) if !floor.is_empty()
    ));
}

#[tokio::test]
async fn inviting_yourself_is_a_typed_self_invite_error() {
    let fixture = MergeFixture::new("self-invite").await;
    let result = fixture
        .store
        .invite_member(
            &fixture.db,
            &fixture.owner,
            &fixture.owner_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await;

    assert!(matches!(result, Err(MembershipOpsError::SelfInvite)));
}

#[tokio::test]
async fn remove_member_completes_when_the_home_reports_no_per_member_revocation() {
    let fixture = MergeFixture::new("unsupported-revocation").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    fixture.remove_member(&member).await;

    assert!(!fixture.load().await.can_write_now(&pubkey_hex(&member)));
}

#[tokio::test]
async fn suppressed_remove_is_detected_by_the_exact_cursor() {
    let fixture = MergeFixture::new("suppressed-remove").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    fixture.remove_member(&member).await;
    let chain = fixture.load().await;
    let remove_head = chain.head_refs().last().expect("remove head").clone();
    fixture
        .storage
        .delete_protocol_object(&remove_head.object)
        .await
        .expect("suppress exact remove head");

    fixture
        .load_result()
        .await
        .expect_err("the accepted exact remove cursor cannot be suppressed");
}

#[test]
fn apply_key_rotation_replays_an_already_adopted_keyring() {
    let live = EncryptionService::from_key([1; 32])
        .with_appended_generation(2, [2; 32])
        .unwrap();
    let custody = TestCustody::default();
    custody
        .persist(&MasterKeyring::from(live.clone()))
        .expect("seed custody");
    let cipher = RwLock::new(CloudCipher::Encrypted(live.clone()));
    let fingerprint = cipher
        .adopt_key_rotation(&EncryptionService::from_key([1; 32]), &custody)
        .expect("an already-covered keyring is an idempotent adoption");
    assert_eq!(fingerprint.fingerprint(), live.fingerprint());
    assert_eq!(
        custody.unlock().unwrap().unwrap().fingerprint(),
        live.fingerprint()
    );
}

#[tokio::test]
async fn head_cursor_persist_never_regresses() {
    let fixture = MergeFixture::new("cursor-monotonic").await;
    let current = fixture
        .load()
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    let mut higher = current.clone();
    higher.coord.seq = 10;
    higher.coord.entry_hash = coven_protocol::store_commit::ObjectHash::digest(b"entry 10");
    higher.head_hash = coven_protocol::store_commit::ObjectHash::digest(b"head 10");
    higher.object = altered_exact(&current.object, b"object 10");
    let mut lower = higher.clone();
    lower.coord.seq = 9;
    lower.coord.entry_hash = coven_protocol::store_commit::ObjectHash::digest(b"entry 9");
    lower.head_hash = coven_protocol::store_commit::ObjectHash::digest(b"head 9");
    lower.object = altered_exact(&current.object, b"object 9");

    fixture
        .database
        .persist_membership_head_cursors(vec![higher.clone()])
        .await
        .unwrap();
    fixture
        .database
        .persist_membership_head_cursors(vec![lower])
        .await
        .unwrap();

    assert_eq!(
        fixture
            .database
            .membership_head_cursors()
            .await
            .unwrap()
            .head_refs,
        vec![higher]
    );
}

#[tokio::test]
async fn head_cursor_rejects_a_reference_from_another_author_stream() {
    let fixture = MergeFixture::new("cursor-stream").await;
    let current = fixture
        .load()
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    fixture
        .database
        .persist_membership_head_cursors(vec![current.clone()])
        .await
        .unwrap();
    let mut mismatched = current.clone();
    mismatched.coord.author_pubkey = hex::encode([3; 32]);
    mismatched.coord.seq = current.coord.seq + 1;

    assert!(fixture
        .database
        .persist_membership_head_cursors(vec![mismatched])
        .await
        .is_err());
}

#[tokio::test]
async fn pruned_membership_author_stream_is_replaced_and_persisted() {
    let db = open_test_db();
    let author = hex::encode([3; coven_keys::keys::SIGN_PUBLICKEYBYTES]);
    let grant = MembershipGrantId(coven_protocol::store_commit::ObjectHash::digest(
        b"local author stream grant",
    ));
    let database = coven_database::StoreDatabase::new(db.database());
    let first = database
        .select_membership_author_stream(&author, &grant, Default::default())
        .await
        .unwrap();
    let reused = database
        .select_membership_author_stream(&author, &grant, std::collections::BTreeSet::from([first]))
        .await
        .unwrap();
    assert_eq!(reused, first);
    let replacement = database
        .select_membership_author_stream(&author, &grant, Default::default())
        .await
        .unwrap();
    assert_ne!(replacement, first);
}

#[test]
fn membership_floor_rejects_unsorted_author_streams() {
    let grant = MembershipGrantId(coven_protocol::store_commit::ObjectHash::digest(
        b"floor ordering grant",
    ));
    let object = ExactObjectRef::new(
        ObjectSlot::logical("test/floor/head.json".to_string()).unwrap(),
        1,
        coven_protocol::store_commit::ObjectHash::digest(b"x"),
    );
    let make = |author: &str, stream: u8| MembershipHeadRef {
        coord: MembershipCoord {
            author_pubkey: author.to_string(),
            author_owner_grant: grant.clone(),
            stream_id: AuthorStreamId::from_bytes([stream; 32]),
            seq: 1,
            entry_hash: coven_protocol::store_commit::ObjectHash::digest(author.as_bytes()),
        },
        head_hash: coven_protocol::store_commit::ObjectHash::digest(&[stream]),
        object: object.clone(),
    };
    let later = make("bbbb", 2);
    let earlier = make("aaaa", 1);

    assert!(validate_membership_floor(&[later, earlier]).is_err());
}

#[tokio::test]
async fn seeding_a_complete_head_floor_is_atomic() {
    let fixture = MergeFixture::new("atomic-floor").await;
    let db = open_test_db();
    let first = fixture
        .load()
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    let mut second = first.clone();
    second.coord.author_pubkey = hex::encode([8; 32]);
    second.coord.author_owner_grant = MembershipGrantId(
        coven_protocol::store_commit::ObjectHash::digest(b"second grant"),
    );
    second.coord.stream_id = AuthorStreamId::from_bytes([8; 32]);
    second.object = altered_exact(&first.object, b"second exact head");
    let mut floor = vec![first, second.clone()];
    floor.sort_by_key(|reference| reference.coord.stream_key());
    let rejected_key =
        coven_database::InitialStoreMembershipAuthority::cursor_state_key_for_test(&second);
    db.database()
        .install_protocol_state_key_insert_failure_for_test(rejected_key)
        .await
        .unwrap();

    let database = coven_database::StoreDatabase::new(db.database());
    assert!(database
        .persist_membership_head_cursors(floor)
        .await
        .is_err());
    assert!(database
        .membership_head_cursors()
        .await
        .unwrap()
        .head_refs
        .is_empty());
}

#[tokio::test]
async fn owner_pin_and_complete_head_floor_commit_atomically() {
    let fixture = MergeFixture::new("atomic-owner-pin").await;
    let db = open_test_db();
    let head = fixture
        .load()
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    let rejected_key =
        coven_database::InitialStoreMembershipAuthority::cursor_state_key_for_test(&head);
    db.database()
        .install_protocol_state_key_insert_failure_for_test(rejected_key)
        .await
        .unwrap();

    assert!(crate::sync::store::Store::open(
        coven_database::StoreDatabase::new(db.database()),
        fixture.storage.clone(),
        fixture.store_dir.clone(),
        &fixture.store.root(),
        &fixture.owner,
    )
    .await
    .is_err());
    assert_eq!(
        db.database()
            .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
            .await
            .unwrap(),
        None
    );
    assert!(coven_database::StoreDatabase::new(db.database())
        .membership_head_cursors()
        .await
        .unwrap()
        .head_refs
        .is_empty());
}

#[tokio::test]
async fn reader_refuses_a_head_that_regresses_below_its_cursor() {
    let fixture = MergeFixture::new("cursor-regression").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    fixture.remove_member(&member).await;
    let chain = fixture.load().await;
    let latest = chain.head_refs().last().expect("latest head").clone();
    let latest_head = fixture
        .device
        .load_membership_head_for_test(&latest)
        .await
        .expect("load latest head");
    let predecessor = latest_head
        .body
        .predecessor
        .clone()
        .expect("remove predecessor");
    fixture
        .storage
        .delete_protocol_object(&latest.object)
        .await
        .expect("remove latest exact head");

    let error = fixture
        .load_result()
        .await
        .expect_err("the accepted cursor cannot regress to its predecessor");
    assert!(error.to_string().contains("regressed"));
    assert!(predecessor.coord.seq < latest.coord.seq);
}

#[tokio::test]
async fn membership_projection_handles_a_deep_valid_predecessor_path_iteratively() {
    let fixture = MergeFixture::new("deep-membership-projection").await;
    let chain = fixture.load().await;
    fixture
        .device
        .assert_deep_membership_projection_for_test(chain.head_refs())
        .await
        .expect("project deep membership path");
}

/// A Store-activated removal composes its candidate against this device's next
/// stream position, stages the mutation durably, then publishes — releasing the
/// turn that claimed the position in between. A queued host write that drains in
/// that window takes the position, and the staged candidate is bound to that
/// create-once head slot, so it can never activate there. Publication reads the
/// occupant, verifies it is a real winner, and ends the removal on that evidence:
/// the staged mutation is cleared rather than retried against a position that is
/// gone, and the initiator's next removal composes at the position that follows.
/// The mutation journal names the objects it publishes and does not carry their
/// bytes: every entry, head, commit, and resolution in it sits beside the exact
/// reference the upload rebuilds it under. The one payload it does carry is the
/// sealed keyring inside each replacement wrapped key, which no other field
/// holds.
#[tokio::test]
async fn the_membership_mutation_journal_carries_no_object_it_already_names() {
    let fixture = MergeFixture::new("mutation-journal-names-its-objects").await;
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    let member_db = open_test_db();
    fixture
        .store
        .activate_joined_device(&fixture.db, &member_db, &member, "2026-07-21T00:00:00Z")
        .await
        .expect("activate the member's device");

    // Stop the removal before it publishes, leaving its plan durable to read.
    fixture.home.fail_exact_create_before_call(1);
    Box::pin(fixture.try_remove_member(&member))
        .await
        .expect_err("the interrupted removal cannot publish its membership authority");
    let staged = fixture
        .database
        .outbound_membership_mutation()
        .await
        .expect("read the staged removal")
        .expect("the interrupted removal stays durable");
    let plan = String::from_utf8(staged.plan_bytes).expect("the plan is JSON");

    for carried in [
        "entry_object",
        "head_object",
        "resolution_object",
        "prepared_head",
    ] {
        assert!(
            !plan.contains(carried),
            "the journal carries {carried}, whose bytes its own reference already names"
        );
    }
    // One replacement wrapped key, for the one member who remains, and its
    // sealed keyring is the only value in the plan without a sibling field the
    // upload could rebuild it from.
    assert_eq!(
        plan.matches("stored_bytes").count(),
        1,
        "the journal's only carried payload is the replacement wrapped key"
    );
}

#[tokio::test]
async fn a_removal_whose_stream_position_was_taken_ends_and_re_issues() {
    let fixture = MergeFixture::new("removal-loses-its-position").await;
    let encryption = EncryptionService::from_key([42; 32]);
    let member = UserKeypair::generate();
    fixture.invite_member(&member, MemberRole::Member).await;
    let member_db = open_test_db();
    fixture
        .store
        .activate_joined_device(&fixture.db, &member_db, &member, "2026-07-21T00:00:00Z")
        .await
        .expect("activate the member's device");
    Box::pin(fixture.store.promote_active_member_fixture(
        &fixture.db,
        &member_db,
        &fixture.owner,
        &member,
        &encryption,
    ))
    .await
    .expect("promote the member to Owner");

    // A queued host write composes against the same next position the removal
    // will, and takes it the moment it drains.
    fixture
        .db
        .database()
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('contended-note', 'contended', NULL, 1, \
                 '0000000001000-0000-owner', '2026-07-21')",
        )
        .await;
    let loaded_store = fixture
        .store
        .bind_device(&fixture.db, &fixture.owner)
        .await
        .expect("load owner Store");
    let mut writer = loaded_store
        .authorize_writer()
        .await
        .expect("authorize owner writer");
    assert!(Box::pin(writer.prepare_pending_store_write())
        .await
        .expect("queue a host write at the contended position"));

    // Stop the removal before it publishes anything, leaving its candidate
    // durable and bound to the position it composed against.
    fixture.home.fail_exact_create_before_call(1);
    Box::pin(fixture.try_remove_member(&member))
        .await
        .expect_err("the interrupted removal cannot publish its membership authority");
    assert!(
        fixture
            .database
            .outbound_membership_mutation()
            .await
            .expect("read the staged removal")
            .is_some(),
        "the interrupted removal stays durable",
    );

    assert_eq!(
        Box::pin(writer.drain_store_writes())
            .await
            .expect("publish the queued host write"),
        1,
    );

    let lost = Box::pin(fixture.try_remove_member(&member))
        .await
        .expect_err("a candidate whose position was taken can never activate");
    assert!(
        lost.to_string().contains("did not activate"),
        "the removal ends on the verified winner: {lost}",
    );
    assert!(
        fixture
            .database
            .outbound_membership_mutation()
            .await
            .expect("read the cleared removal")
            .is_none(),
        "the lost removal is cleared rather than left staged against a position that is gone",
    );

    Box::pin(fixture.try_remove_member(&member))
        .await
        .expect("the re-issued removal publishes at the position that follows");
    assert!(!fixture.load().await.can_write_now(&pubkey_hex(&member)));
}
