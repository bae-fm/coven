use super::*;
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{MasterKeyCustody, UserKeypair};
use crate::storage::cloud::ObjectSlot;
use crate::sync::cloud_storage::{CloudCipher, CloudCipherAccess};
use crate::sync::hlc::Hlc;
use crate::sync::membership::{
    validate_membership_floor, AuthorHead, AuthorStreamId, MemberRole, MembershipChain,
    MembershipCoord, MembershipGrantId, MembershipHeadRef,
};
use crate::sync::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use crate::sync::store::database::StoreDatabase;
use crate::sync::test_helpers::{
    install_active_device_fixture, open_test_db, promote_active_member_fixture, pubkey_hex,
    TestCustody, TestStore,
};
use std::sync::{Arc, RwLock};

struct MergeFixture {
    store: TestStore,
    device: crate::sync::test_helpers::TestDevice,
    db: Database,
    database: StoreDatabase,
    owner: UserKeypair,
    owner_pubkey: String,
}

async fn merge_fixture(store_id: &str) -> MergeFixture {
    let db = open_test_db();
    let owner = UserKeypair::generate();
    let owner_pubkey = pubkey_hex(&owner);
    let store = TestStore::create(&db, store_id, owner.clone())
        .await
        .expect("create exact Store");
    let device = store
        .bind_device(&db, &owner)
        .await
        .expect("bind exact Store");
    let database = StoreDatabase::new(&db);
    MergeFixture {
        store,
        device,
        db,
        database,
        owner,
        owner_pubkey,
    }
}

async fn load_fixture(fixture: &MergeFixture) -> MembershipChain {
    fixture
        .device
        .membership_for_test()
        .await
        .expect("load exact membership chain")
}

async fn invite_fixture_member(
    fixture: &MergeFixture,
    member: &UserKeypair,
    role: MemberRole,
) -> crate::join_code::InviteCode {
    fixture
        .store
        .invite_member(
            &fixture.db,
            &fixture.owner,
            &Hlc::new("owner-device".to_string()),
            &pubkey_hex(member),
            None,
            role,
            &EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await
        .expect("invite exact member")
}

async fn try_remove_fixture_member(
    fixture: &MergeFixture,
    member: &UserKeypair,
) -> Result<String, MembershipOpsError> {
    let custody = TestCustody::default();
    fixture
        .store
        .remove_member(
            &fixture.db,
            &fixture.owner,
            &Hlc::new("owner-device".to_string()),
            &pubkey_hex(member),
            &EncryptionService::from_key([42; 32]),
            &custody,
        )
        .await
}

async fn remove_fixture_member(fixture: &MergeFixture, member: &UserKeypair) {
    try_remove_fixture_member(fixture, member)
        .await
        .expect("remove exact member");
}

fn altered_exact(reference: &ExactObjectRef, label: &[u8]) -> ExactObjectRef {
    ExactObjectRef::new(
        reference.slot().clone(),
        label.len() as u64,
        crate::sync::store_commit::ObjectHash::digest(label),
    )
}

async fn overwrite_head(fixture: &MergeFixture, reference: &MembershipHeadRef, head: &AuthorHead) {
    fixture
        .store
        .storage
        .delete_protocol_object(&reference.object)
        .await
        .expect("delete exact head before replacement");
    let context = ProtocolObjectContext::signed_plaintext(
        fixture.store.root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let prefix = crate::sync::store_commit::membership_head_slot_prefix(
        &reference.coord.author_pubkey,
        &reference.coord.author_owner_grant,
        reference.coord.stream_id,
        reference.coord.seq,
    );
    let prepared = fixture
        .store
        .storage
        .prepare_protocol_object(
            &context,
            reference.object.slot().clone(),
            &prefix,
            serde_json::to_vec(head).expect("serialize replacement head"),
        )
        .expect("prepare replacement head");
    fixture
        .store
        .storage
        .create_protocol_object(&prepared)
        .await
        .expect("write replacement head");
}

#[tokio::test]
async fn anchored_chain_loads_the_root_named_by_its_authoritative_hash() {
    let fixture = merge_fixture("pinned-root").await;
    let unrelated = merge_fixture("unrelated-root").await;
    assert_ne!(
        fixture.store.root.store_root_hash,
        unrelated.store.root.store_root_hash
    );

    let loaded = load_fixture(&fixture).await;
    let expected_store_id = fixture.store.root.store_root_id.to_string();
    assert_eq!(loaded.store_id(), Some(expected_store_id.as_str()));
    assert_eq!(loaded.founder_pubkey(), Some(fixture.owner_pubkey.as_str()));
}

#[tokio::test]
async fn current_floor_is_the_exact_signed_head_cut() {
    let fixture = merge_fixture("exact-floor").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;

    let chain = load_fixture(&fixture).await;
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
    let fixture = merge_fixture("missing-entry").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    let chain = load_fixture(&fixture).await;
    let head = chain.head_refs().last().expect("current head").clone();
    let loaded_head = fixture
        .device
        .load_membership_head_for_test(&head)
        .await
        .expect("load exact head");
    fixture
        .store
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
    let fixture = merge_fixture("missing-head").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    let chain = load_fixture(&fixture).await;
    let head = chain.head_refs().last().expect("current head").clone();
    fixture
        .store
        .storage
        .delete_protocol_object(&head.object)
        .await
        .expect("remove exact head");

    load_fixture_result(&fixture)
        .await
        .expect_err("a durable exact cursor requires its head");
}

async fn load_fixture_result(
    fixture: &MergeFixture,
) -> Result<MembershipChain, crate::sync::store::StoreError> {
    fixture.device.membership_for_test().await
}

#[tokio::test]
async fn membership_head_must_match_its_exact_author_coordinate() {
    let fixture = merge_fixture("head-author").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    let chain = load_fixture(&fixture).await;
    let reference = chain.head_refs().last().expect("current head").clone();
    let mut head = fixture
        .device
        .load_membership_head_for_test(&reference)
        .await
        .expect("load exact head");
    head.body.entry.coord.author_pubkey = hex::encode([9; 32]);
    overwrite_head(&fixture, &reference, &head).await;

    load_fixture_result(&fixture)
        .await
        .expect_err("a head selecting another author coordinate must fail");
}

#[tokio::test]
async fn invalid_membership_head_signature_preserves_owner_and_cursor() {
    let fixture = merge_fixture("bad-head-signature").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    let chain = load_fixture(&fixture).await;
    let before_owner = fixture
        .db
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
    head.signature = hex::encode([0; 64]);
    overwrite_head(&fixture, &reference, &head).await;

    load_fixture_result(&fixture)
        .await
        .expect_err("an invalid exact head signature must fail");
    assert_eq!(
        fixture
            .db
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
    let fixture = merge_fixture("forked-cursor").await;
    let current = load_fixture(&fixture)
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
    fork.head_hash = crate::sync::store_commit::ObjectHash::digest(b"forked head");
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
    let fixture = merge_fixture("missing-founder-head").await;
    let chain = load_fixture(&fixture).await;
    let head = chain.head_refs().first().expect("founder head");
    fixture
        .store
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
    let fixture = merge_fixture("unheaded-entry").await;
    let member = UserKeypair::generate();
    let chain = load_fixture(&fixture).await;
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
    let (prepared, _) = crate::sync::store_objects::prepare_membership_entry(
        &fixture.store.storage,
        fixture.store.root.store_root_hash,
        &entry,
    )
    .await
    .expect("prepare unheaded entry");
    fixture
        .store
        .storage
        .create_protocol_object(&prepared)
        .await
        .expect("publish unheaded entry");

    let loaded = load_fixture(&fixture).await;
    assert!(!loaded.can_write_now(&pubkey_hex(&member)));
}

#[tokio::test]
async fn complete_chain_still_validates() {
    let fixture = merge_fixture("complete-chain").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;

    assert!(load_fixture(&fixture)
        .await
        .can_write_now(&pubkey_hex(&member)));
}

#[tokio::test]
async fn store_owns_membership_conflict_reads_and_rejects_a_foreign_choice_atomically() {
    let fixture = merge_fixture("store-membership-conflict-boundary").await;
    let storage = Arc::new(
        crate::sync::cloud_storage::CloudSyncStorage::new(
            fixture.store.home.clone(),
            CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
            crate::sync::cloud_storage::BlobPathScheme::Hashed,
            fixture.store.storage.store_id(),
            fixture.owner.clone(),
        )
        .expect("open a Store-owned storage session"),
    );
    let store =
        crate::sync::store::Store::load(fixture.database.clone(), storage, fixture.owner.clone())
            .await
            .expect("load Store owner");
    assert!(store
        .membership_conflict(Some(&fixture.owner.public_key()))
        .await
        .expect("read membership conflict")
        .is_none());

    let chain = load_fixture(&fixture).await;
    let choice = crate::sync::membership::MembershipConflictChoice::new(
        "foreign-choice".to_string(),
        Vec::new(),
        crate::sync::store_commit::ObjectHash::digest(b"foreign conflict"),
        crate::sync::membership::MembershipConflictSelection::RevocationBranch {
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
                crate::sync::membership::MembershipError::InvalidConflictResolution
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
    let fixture = merge_fixture("store-membership-owner-anchor").await;
    let storage = Arc::new(
        crate::sync::cloud_storage::CloudSyncStorage::new(
            fixture.store.home.clone(),
            CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
            crate::sync::cloud_storage::BlobPathScheme::Hashed,
            fixture.store.storage.store_id(),
            fixture.owner.clone(),
        )
        .expect("open a Store-owned storage session"),
    );
    let store =
        crate::sync::store::Store::load(fixture.database.clone(), storage, fixture.owner.clone())
            .await
            .expect("load Store owner");
    fixture
        .db
        .delete_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .expect("remove the installed owner anchor");

    let error = store
        .members(Some(&fixture.owner.public_key()))
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
    let fixture = merge_fixture("store-membership-founder-state").await;
    let storage = Arc::new(
        crate::sync::cloud_storage::CloudSyncStorage::new(
            fixture.store.home.clone(),
            CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
            crate::sync::cloud_storage::BlobPathScheme::Hashed,
            fixture.store.storage.store_id(),
            fixture.owner.clone(),
        )
        .expect("open a Store-owned storage session"),
    );
    let store =
        crate::sync::store::Store::load(fixture.database.clone(), storage, fixture.owner.clone())
            .await
            .expect("load Store owner");
    fixture
        .db
        .set_protocol_state(crate::database::STORE_DEVICE_GENESIS_STATE_KEY, "{}")
        .await
        .expect("tamper with the installed founder state");

    let error = store
        .members(Some(&fixture.owner.public_key()))
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
async fn store_prefix_projection_retains_direct_membership_heads() {
    let fixture = merge_fixture("project-direct-membership").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    let current = load_fixture(&fixture).await;
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
    let fixture = merge_fixture("project-store-bound-membership").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    let member_db = open_test_db();
    install_active_device_fixture(
        &fixture.store,
        &fixture.db,
        &member_db,
        &member,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate member device");
    let before_promotion = load_fixture(&fixture).await;
    promote_active_member_fixture(
        &fixture.store,
        &fixture.db,
        &member_db,
        &fixture.owner,
        &member,
        &EncryptionService::from_key([42; 32]),
    )
    .await
    .expect("promote member to Owner");
    let after_promotion = load_fixture(&fixture).await;
    assert_ne!(after_promotion.head_refs(), before_promotion.head_refs());
    let later_member = UserKeypair::generate();
    invite_fixture_member(&fixture, &later_member, MemberRole::Member).await;
    let candidate = load_fixture(&fixture).await;
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
    let fixture = merge_fixture("relocated-exact-membership-head").await;
    let current = load_fixture(&fixture).await;
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
    let registration = crate::sync::store::database::StoreDatabase::new(&fixture.db)
        .activated_store_device_registration(founder_head.body.author_registration.clone())
        .await
        .expect("load founder device registration");
    let signer = registration
        .device_signer(&fixture.owner)
        .expect("derive founder device signer");
    let relocated = AuthorHead::signed(
        founder_head.store_id.clone(),
        founder_head.body.clone(),
        founder_head.activation.clone(),
        &signer,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        fixture.store.root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let prefix = crate::sync::store_commit::membership_head_slot_prefix(
        &founder_ref.coord.author_pubkey,
        &founder_ref.coord.author_owner_grant,
        AuthorStreamId::from_bytes([99; 32]),
        founder_ref.coord.seq,
    );
    let slot = fixture
        .store
        .storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .expect("allocate relocated membership head slot");
    let prepared = fixture
        .store
        .storage
        .prepare_protocol_object(
            &context,
            slot,
            &prefix,
            serde_json::to_vec(&relocated).expect("serialize relocated membership head"),
        )
        .expect("prepare relocated membership head");
    fixture
        .store
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
    let fixture = merge_fixture("invite-authority").await;
    let invitee = UserKeypair::generate();
    let invite = invite_fixture_member(&fixture, &invitee, MemberRole::Member).await;

    assert_eq!(invite.owner_pubkey, fixture.owner_pubkey);
    assert_eq!(invite.store_root, fixture.store.root);
    assert!(matches!(
        invite.membership_floor,
        crate::join_code::MembershipFloor(ref floor) if !floor.is_empty()
    ));
}

#[tokio::test]
async fn inviting_yourself_is_a_typed_self_invite_error() {
    let fixture = merge_fixture("self-invite").await;
    let result = fixture
        .store
        .invite_member(
            &fixture.db,
            &fixture.owner,
            &Hlc::new("owner-device".to_string()),
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
    let fixture = merge_fixture("unsupported-revocation").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    remove_fixture_member(&fixture, &member).await;

    assert!(!load_fixture(&fixture)
        .await
        .can_write_now(&pubkey_hex(&member)));
}

#[tokio::test]
async fn suppressed_remove_is_detected_by_the_exact_cursor() {
    let fixture = merge_fixture("suppressed-remove").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    remove_fixture_member(&fixture, &member).await;
    let chain = load_fixture(&fixture).await;
    let remove_head = chain.head_refs().last().expect("remove head").clone();
    fixture
        .store
        .storage
        .delete_protocol_object(&remove_head.object)
        .await
        .expect("suppress exact remove head");

    load_fixture_result(&fixture)
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
    assert_eq!(fingerprint, live.fingerprint());
    assert_eq!(
        custody.unlock().unwrap().unwrap().fingerprint(),
        live.fingerprint()
    );
}

#[tokio::test]
async fn head_cursor_persist_never_regresses() {
    let fixture = merge_fixture("cursor-monotonic").await;
    let current = load_fixture(&fixture)
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    let mut higher = current.clone();
    higher.coord.seq = 10;
    higher.coord.entry_hash = crate::sync::store_commit::ObjectHash::digest(b"entry 10");
    higher.head_hash = crate::sync::store_commit::ObjectHash::digest(b"head 10");
    higher.object = altered_exact(&current.object, b"object 10");
    let mut lower = higher.clone();
    lower.coord.seq = 9;
    lower.coord.entry_hash = crate::sync::store_commit::ObjectHash::digest(b"entry 9");
    lower.head_hash = crate::sync::store_commit::ObjectHash::digest(b"head 9");
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
    let fixture = merge_fixture("cursor-stream").await;
    let current = load_fixture(&fixture)
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
    let author = hex::encode([3; crate::keys::SIGN_PUBLICKEYBYTES]);
    let grant = MembershipGrantId(crate::sync::store_commit::ObjectHash::digest(
        b"local author stream grant",
    ));
    let database = StoreDatabase::new(&db);
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
    let grant = MembershipGrantId(crate::sync::store_commit::ObjectHash::digest(
        b"floor ordering grant",
    ));
    let object = ExactObjectRef::new(
        ObjectSlot::logical("test/floor/head.json".to_string()).unwrap(),
        1,
        crate::sync::store_commit::ObjectHash::digest(b"x"),
    );
    let make = |author: &str, stream: u8| MembershipHeadRef {
        coord: MembershipCoord {
            author_pubkey: author.to_string(),
            author_owner_grant: grant.clone(),
            stream_id: AuthorStreamId::from_bytes([stream; 32]),
            seq: 1,
            entry_hash: crate::sync::store_commit::ObjectHash::digest(author.as_bytes()),
        },
        head_hash: crate::sync::store_commit::ObjectHash::digest(&[stream]),
        object: object.clone(),
    };
    let later = make("bbbb", 2);
    let earlier = make("aaaa", 1);

    assert!(validate_membership_floor(&[later, earlier]).is_err());
}

#[tokio::test]
async fn seeding_a_complete_head_floor_is_atomic() {
    let fixture = merge_fixture("atomic-floor").await;
    let db = open_test_db();
    let first = load_fixture(&fixture)
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    let mut second = first.clone();
    second.coord.author_pubkey = hex::encode([8; 32]);
    second.coord.author_owner_grant = MembershipGrantId(
        crate::sync::store_commit::ObjectHash::digest(b"second grant"),
    );
    second.coord.stream_id = AuthorStreamId::from_bytes([8; 32]);
    second.object = altered_exact(&first.object, b"second exact head");
    let mut floor = vec![first, second.clone()];
    floor.sort_by_key(|reference| reference.coord.stream_key());
    let rejected_key =
        crate::database::InitialStoreMembershipAuthority::cursor_state_key_for_test(&second);
    db.call(move |conn| {
        conn.execute_batch(&format!(
            "CREATE TRIGGER reject_second_membership_floor \
                 BEFORE INSERT ON protocol_state \
                 WHEN NEW.key = '{rejected_key}' \
                 BEGIN SELECT RAISE(ABORT, 'forced cursor failure'); END;"
        ))
        .map_err(crate::database::DbError::from)
    })
    .await
    .unwrap();

    let database = StoreDatabase::new(&db);
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
    let fixture = merge_fixture("atomic-owner-pin").await;
    let db = open_test_db();
    let head = load_fixture(&fixture)
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    let rejected_key =
        crate::database::InitialStoreMembershipAuthority::cursor_state_key_for_test(&head);
    db.call(move |conn| {
        conn.execute_batch(&format!(
            "CREATE TRIGGER reject_anchor_cursor \
             BEFORE INSERT ON protocol_state \
             WHEN NEW.key = '{rejected_key}' \
             BEGIN SELECT RAISE(ABORT, 'forced cursor failure'); END;"
        ))
        .map_err(crate::database::DbError::from)
    })
    .await
    .unwrap();

    assert!(crate::sync::store::Store::open(
        StoreDatabase::new(&db),
        fixture.store.storage.clone(),
        &fixture.store.root,
        &fixture.owner,
    )
    .await
    .is_err());
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None
    );
    assert!(StoreDatabase::new(&db)
        .membership_head_cursors()
        .await
        .unwrap()
        .head_refs
        .is_empty());
}

#[tokio::test]
async fn reader_refuses_a_head_that_regresses_below_its_cursor() {
    let fixture = merge_fixture("cursor-regression").await;
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    remove_fixture_member(&fixture, &member).await;
    let chain = load_fixture(&fixture).await;
    let latest = chain.head_refs().last().expect("latest head").clone();
    let latest_head = fixture
        .device
        .load_membership_head_for_test(&latest)
        .await
        .expect("load latest head");
    let predecessor = latest_head.body.predecessor.expect("remove predecessor");
    fixture
        .store
        .storage
        .delete_protocol_object(&latest.object)
        .await
        .expect("remove latest exact head");

    let error = load_fixture_result(&fixture)
        .await
        .expect_err("the accepted cursor cannot regress to its predecessor");
    assert!(error.to_string().contains("regressed"));
    assert!(predecessor.coord.seq < latest.coord.seq);
}

#[tokio::test]
async fn membership_projection_handles_a_deep_valid_predecessor_path_iteratively() {
    let fixture = merge_fixture("deep-membership-projection").await;
    let chain = load_fixture(&fixture).await;
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
#[tokio::test]
async fn a_removal_whose_stream_position_was_taken_ends_and_re_issues() {
    let fixture = merge_fixture("removal-loses-its-position").await;
    let encryption = EncryptionService::from_key([42; 32]);
    let member = UserKeypair::generate();
    invite_fixture_member(&fixture, &member, MemberRole::Member).await;
    let member_db = open_test_db();
    install_active_device_fixture(
        &fixture.store,
        &fixture.db,
        &member_db,
        &member,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate the member's device");
    Box::pin(promote_active_member_fixture(
        &fixture.store,
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
    crate::sync::test_helpers::host_exec(
        &fixture.db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('contended-note', 'contended', NULL, 1, \
                 '0000000001000-0000-owner', '2026-07-21')",
    )
    .await;
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let loaded_store = fixture
        .store
        .bind_device(&fixture.db, &fixture.owner)
        .await
        .expect("load owner Store");
    let mut writer = loaded_store
        .authorize_writer()
        .await
        .expect("authorize owner writer");
    assert!(Box::pin(writer.prepare_pending_store_write(&store_dir))
        .await
        .expect("queue a host write at the contended position"));

    // Stop the removal before it publishes anything, leaving its candidate
    // durable and bound to the position it composed against.
    fixture.store.home.fail_exact_create_before_call(1);
    Box::pin(try_remove_fixture_member(&fixture, &member))
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

    let lost = Box::pin(try_remove_fixture_member(&fixture, &member))
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

    Box::pin(try_remove_fixture_member(&fixture, &member))
        .await
        .expect("the re-issued removal publishes at the position that follows");
    assert!(!load_fixture(&fixture)
        .await
        .can_write_now(&pubkey_hex(&member)));
}
