use super::*;
use crate::sync::test_helpers::{pubkey_hex, TestCustody, TestStore};
use coven_database::Database;
use coven_database::StoreDatabase;
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
    db: Database,
    database: StoreDatabase,
    owner: UserKeypair,
    owner_pubkey: String,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl MergeFixture {
    async fn new(store_id: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let owner = UserKeypair::generate();
        let owner_pubkey = pubkey_hex(&owner);
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, storage) = TestStore::create_with_connection(
            &db,
            db_store_dir.clone(),
            store_id,
            owner.clone(),
            home.clone(),
        )
        .await
        .expect("create exact Store");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &owner)
            .await
            .expect("bind exact Store");
        let database = coven_database::StoreDatabase::new(&db);
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
            store_dir: db_store_dir,
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

    async fn admit_member(
        &self,
        member: &UserKeypair,
        role: MemberRole,
    ) -> crate::sync::store::MemberAdmission {
        self.store
            .admit_member(
                &self.db,
                self.store_dir.clone(),
                &self.owner,
                &pubkey_hex(member),
                None,
                role,
                &EncryptionService::from_key([42; 32]),
                "Test Store",
            )
            .await
            .expect("admit exact member")
    }

    async fn try_remove_member(&self, member: &UserKeypair) -> Result<String, MembershipOpsError> {
        let custody = TestCustody::default();
        self.store
            .remove_member(
                &self.db,
                self.store_dir.clone(),
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
    fixture.admit_member(&member, MemberRole::Member).await;

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
async fn retrying_the_same_admission_reuses_the_active_membership_grant() {
    let fixture = MergeFixture::new("idempotent-admission").await;
    let member = UserKeypair::generate();
    let first = fixture.admit_member(&member, MemberRole::Member).await;
    let first_heads = fixture.load().await.head_refs().to_vec();

    let second = fixture.admit_member(&member, MemberRole::Member).await;

    assert_eq!(fixture.load().await.head_refs(), first_heads);
    assert_eq!(second.grant_id, first.grant_id);
    assert_eq!(second.membership_floor, first.membership_floor);
    assert_eq!(second.join_info, first.join_info);
}

#[tokio::test]
async fn retrying_join_after_admission_reuses_the_active_attempt() {
    let fixture = MergeFixture::new("idempotent-device-join").await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
    let member_pubkey = pubkey_hex(&member);

    let first = fixture
        .device
        .begin_device_join(&member_pubkey)
        .await
        .expect("begin device join");
    let resumed = fixture
        .device
        .begin_device_join(&member_pubkey)
        .await
        .expect("resume device join");

    assert_eq!(resumed.attempt_id, first.attempt_id);
}

#[tokio::test]
async fn retained_floor_reuses_verified_entries_while_a_cold_reader_requires_them() {
    let fixture = MergeFixture::new("missing-entry").await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
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

    assert_eq!(
        fixture
            .device
            .restore_membership()
            .await
            .expect("the installed accepted proof retains its exact entry")
            .membership_floor
            .0,
        chain.head_refs(),
    );
    fixture.home.clear_exact_reads();
    let cold = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(fixture.storage.as_ref(), &fixture.store.root())
        .await
        .expect("open a reader without retained membership proof");
    let error = cold
        .load_accepted_anchored_membership(chain.head_refs(), Some(&fixture.owner_pubkey))
        .await
        .expect_err("a cold reader must obtain every selected exact entry");
    assert!(
        matches!(
            error,
            AnchoredChainError::Object(coven_protocol::objects::StoreObjectError::Storage(
                coven_protocol::objects::StorageError::NotFound(_)
            ))
        ),
        "{error:?}"
    );
    assert!(fixture
        .home
        .exact_reads()
        .contains(loaded_head.body.entry.object.slot()));
}

#[tokio::test]
async fn persisted_author_floor_requires_readable_head() {
    let fixture = MergeFixture::new("missing-head").await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
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
    fixture.admit_member(&member, MemberRole::Member).await;
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
    fixture.admit_member(&member, MemberRole::Member).await;
    let chain = fixture.load().await;
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
        .load_membership_at_exact_heads_for_test(&[])
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
    fixture.admit_member(&member, MemberRole::Member).await;

    assert!(fixture.load().await.can_write_now(&pubkey_hex(&member)));
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
        Some(coven_keys::encryption::EncryptionService::from_key(
            [42; 32],
        )),
    )
    .await
    .expect("load Store owner");
    fixture
        .db
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
            MembershipOpsError::Store(crate::sync::store::StoreError::SyncCycle(failure))
                if failure.contains("Store owner anchor is absent")
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
        Some(coven_keys::encryption::EncryptionService::from_key(
            [42; 32],
        )),
    )
    .await
    .expect("load Store owner");
    fixture
        .db
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
            MembershipOpsError::Store(crate::sync::store::StoreError::SyncCycle(failure))
                if failure.contains("Store device genesis state")
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
        .replace_replay_authority_for_test(b"invalid retained replay authority".to_vec())
        .await
        .expect("replace retained replay authority after verification");

    fixture
        .load_result()
        .await
        .expect("reuse the replay baseline verified by the open connection");
}

#[path = "projection_tests.rs"]
mod projection;

#[path = "head_acceptance_tests.rs"]
mod head_acceptance;

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
    let registration = coven_database::StoreDatabase::new(&fixture.db)
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
        .load_membership_at_exact_heads_for_test(&[relocated_ref])
        .await
        .expect_err("a membership head relocated outside its grant anchor must fail");
}

#[tokio::test]
async fn admission_carries_the_founder_and_exact_root() {
    let fixture = MergeFixture::new("admission-authority").await;
    let candidate = UserKeypair::generate();
    let admission = fixture.admit_member(&candidate, MemberRole::Member).await;

    assert_eq!(admission.owner_pubkey, fixture.owner_pubkey);
    assert_eq!(admission.store_root, fixture.store.root());
    assert!(matches!(
        admission.membership_floor,
        coven_protocol::membership::MembershipFloor(ref floor) if !floor.is_empty()
    ));
}

#[tokio::test]
async fn admitting_yourself_is_a_typed_self_admission_error() {
    let fixture = MergeFixture::new("self-admission").await;
    let result = fixture
        .store
        .admit_member(
            &fixture.db,
            fixture.store_dir.clone(),
            &fixture.owner,
            &fixture.owner_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await;

    assert!(matches!(result, Err(MembershipOpsError::SelfAdmission)));
}

#[tokio::test]
async fn remove_member_completes_when_the_home_reports_no_per_member_revocation() {
    let fixture = MergeFixture::new("unsupported-revocation").await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
    fixture.remove_member(&member).await;

    assert!(!fixture.load().await.can_write_now(&pubkey_hex(&member)));
}

#[tokio::test]
async fn suppressed_remove_is_detected_by_the_exact_cursor() {
    let fixture = MergeFixture::new("suppressed-remove").await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
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
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let author = hex::encode([3; coven_keys::keys::SIGN_PUBLICKEYBYTES]);
    let grant = MembershipGrantId(coven_protocol::store_commit::ObjectHash::digest(
        b"local author stream grant",
    ));
    let database = coven_database::StoreDatabase::new(&db);
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
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
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
    db.install_protocol_state_key_insert_failure_for_test(rejected_key)
        .await
        .unwrap();

    let database = coven_database::StoreDatabase::new(&db);
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
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let head = fixture
        .load()
        .await
        .head_refs()
        .first()
        .expect("founder head")
        .clone();
    let rejected_key =
        coven_database::InitialStoreMembershipAuthority::cursor_state_key_for_test(&head);
    db.install_protocol_state_key_insert_failure_for_test(rejected_key)
        .await
        .unwrap();

    assert!(crate::sync::store::Store::open(
        coven_database::StoreDatabase::new(&db),
        fixture.storage.clone(),
        fixture.store_dir.clone(),
        &fixture.store.root(),
        &fixture.owner,
        Some(coven_keys::encryption::EncryptionService::from_key(
            [42; 32]
        )),
    )
    .await
    .is_err());
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None
    );
    assert!(coven_database::StoreDatabase::new(&db)
        .membership_head_cursors()
        .await
        .unwrap()
        .head_refs
        .is_empty());
}

#[path = "publication_tests.rs"]
mod publication;

/// Open the Store the way an admitted device does — pinned root, nothing else
/// — and resolve its membership, with or without the published rollup.
async fn walk_admission_membership(
    fixture: &MergeFixture,
    admission: &crate::sync::store::MemberAdmission,
    adopt_rollup: bool,
) -> MembershipChain {
    let history = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(&*fixture.storage, &admission.store_root)
        .await
        .expect("open admission history");
    if adopt_rollup {
        assert!(
            history.adopt_published_membership_rollup().await,
            "the Store published a snapshot, so its rollup has to be adoptable"
        );
    }
    history
        .load_accepted_anchored_membership(
            &admission.membership_floor.0,
            Some(&admission.owner_pubkey),
        )
        .await
        .expect("walk membership from the cloud")
}
