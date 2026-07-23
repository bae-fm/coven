use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};
use crate::sync::membership::MembershipGrantId;
use crate::sync::storage::{ExactObjectRef, PreparedExactObject};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::ObjectHash;

use super::authority::{load_current_merge_membership, target_key};
use super::journal::OwnerPromotionJournalState;

#[tokio::test]
async fn second_merge_owner_promotion_verifies_existing_promotion_history() {
    let founder_db = crate::sync::test_helpers::open_test_db();
    let founder = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &founder_db,
        "successive-owner-promotions",
        founder.clone(),
    )
    .await
    .expect("create Merge Store");
    let first_owner = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    for member in [&first_owner, &second_owner] {
        crate::sync::store::membership::invite_member(
            &store.storage,
            store.home.as_ref(),
            &founder,
            &crate::sync::hlc::Hlc::new("successive-owner-promotions".to_string()),
            &keys::public_key_hex(member),
            None,
            crate::sync::membership::MemberRole::Member,
            &encryption,
            store.storage.store_id(),
            "Merge Store",
            &StoreDatabase::new(&founder_db),
        )
        .await
        .expect("invite Member identity");
    }

    let first_owner_db = crate::sync::test_helpers::open_test_db();
    let second_owner_db = crate::sync::test_helpers::open_test_db();
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &founder_db,
        &first_owner_db,
        &first_owner,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate first Owner device");
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &founder_db,
        &second_owner_db,
        &second_owner,
        "2026-07-21T00:01:00Z",
    )
    .await
    .expect("activate second Owner device");
    crate::sync::test_helpers::promote_active_member_fixture(
        &store,
        &founder_db,
        &first_owner_db,
        &founder,
        &first_owner,
        &encryption,
    )
    .await
    .expect("promote first Owner");

    let membership = crate::sync::store::pull::load_cycle_membership(
        &store.storage,
        &crate::sync::store::database::StoreDatabase::new(&second_owner_db),
    )
    .await
    .expect("load second Owner membership");
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let pull = crate::sync::store::pull_store_commits(
        &StoreDatabase::new(&second_owner_db),
        second_owner_db.synced_tables(),
        &store.storage,
        store.root.store_root_hash,
        &store_dir,
        membership
            .chain
            .as_ref()
            .expect("opened Store has membership"),
        Some(&second_owner),
    )
    .await
    .expect("pull second Owner through the first promotion");
    assert!(pull.held_positions.is_empty());

    crate::sync::test_helpers::promote_active_member_fixture(
        &store,
        &founder_db,
        &second_owner_db,
        &founder,
        &second_owner,
        &encryption,
    )
    .await
    .expect("promote second Owner");

    let membership = load_current_merge_membership(
        &StoreDatabase::new(&founder_db),
        &store.storage,
        &store.root,
    )
    .await
    .expect("load membership after successive promotions");
    assert!(membership.is_owner_now(&keys::public_key_hex(&first_owner)));
    assert!(membership.is_owner_now(&keys::public_key_hex(&second_owner)));
}

#[tokio::test]
async fn merge_owner_promotion_activates_through_its_store_bound_head_and_persists_exact_receipt() {
    let owner_db = crate::sync::test_helpers::open_test_db();
    let owner = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &owner_db,
        "merge-owner-promotion",
        owner.clone(),
    )
    .await
    .expect("create Merge Store");
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    crate::sync::store::membership::invite_member(
        &store.storage,
        store.home.as_ref(),
        &owner,
        &crate::sync::hlc::Hlc::new("owner-device".to_string()),
        &keys::public_key_hex(&member),
        None,
        crate::sync::membership::MemberRole::Member,
        &encryption,
        store.storage.store_id(),
        "Merge Store",
        &StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite Member identity");
    let member_db = crate::sync::test_helpers::open_test_db();
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &owner_db,
        &member_db,
        &member,
        "2026-07-20T00:00:00Z",
    )
    .await
    .expect("activate Member device");
    let member_device_id = member_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read Member device id")
        .expect("Member device id");
    let (_, member_registration, _, _) =
        crate::sync::store::operations::load_local_store_authority(
            &StoreDatabase::new(&member_db),
            &member_device_id,
            &member,
        )
        .await
        .expect("load Member registration");

    Box::pin(crate::sync::test_helpers::promote_active_member_fixture(
        &store,
        &owner_db,
        &member_db,
        &owner,
        &member,
        &encryption,
    ))
    .await
    .expect("activate Owner promotion");

    assert!(
        StoreDatabase::new(&member_db)
            .load_owner_promotion_target(target_key(&member_registration).unwrap())
            .await
            .expect("load candidate target index")
            .is_none(),
        "the accepting candidate does not own the initiating Owner's target index"
    );

    let membership =
        load_current_merge_membership(&StoreDatabase::new(&owner_db), &store.storage, &store.root)
            .await
            .expect("load activated membership");
    assert!(membership.is_owner_now(&keys::public_key_hex(&member)));
    let promoted_head = membership
        .head_refs()
        .iter()
        .find(|reference| reference.coord.author_pubkey == keys::public_key_hex(&owner))
        .expect("promoter stream head");
    let opened = crate::sync::store::membership::load_exact_membership_head(
        &store.storage,
        &store.root,
        promoted_head,
    )
    .await
    .expect("load activated promotion head");
    assert!(matches!(
        opened.activation,
        crate::sync::membership::MembershipHeadActivation::StoreCommit { .. }
    ));

    let mut journal = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load finalized promotion journal")
        .expect("finalized promotion journal exists");
    let OwnerPromotionJournalState::Finalized {
        membership: state,
        receipt,
        ..
    } = &mut journal.state
    else {
        panic!("promotion journal is finalized with Merge membership")
    };
    let publication = &receipt.publication;
    let exact_head = publication.head_ref.clone();
    let index = state
        .heads
        .binary_search(&exact_head)
        .expect("finalized membership contains the exact published head");
    let mut substituted = exact_head;
    substituted.head_hash = ObjectHash::digest(b"substituted same-coordinate head");
    state.heads[index] = substituted;
    state.heads.sort();
    let encoded = serde_json::to_string(&journal).expect("serialize substituted receipt journal");
    owner_db
        .set_protocol_state(
            &format!("owner_promotion/{}", journal.promotion_id),
            &encoded,
        )
        .await
        .expect("install substituted receipt journal");

    assert!(StoreDatabase::new(&owner_db)
        .load_owner_promotion_journal(journal.promotion_id)
        .await
        .is_err());
}

#[tokio::test]
async fn journal_load_rejects_substituted_request_or_prepared_commit_bytes() {
    let owner_db = crate::sync::test_helpers::open_test_db();
    let owner = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &owner_db,
        "corrupt-owner-promotion-request",
        owner.clone(),
    )
    .await
    .expect("create Merge Store");
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    crate::sync::store::membership::invite_member(
        &store.storage,
        store.home.as_ref(),
        &owner,
        &crate::sync::hlc::Hlc::new("owner-device".to_string()),
        &keys::public_key_hex(&member),
        None,
        crate::sync::membership::MemberRole::Member,
        &encryption,
        store.storage.store_id(),
        "Merge Store",
        &StoreDatabase::new(&owner_db),
    )
    .await
    .expect("invite Member identity");
    let member_db = crate::sync::test_helpers::open_test_db();
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &owner_db,
        &member_db,
        &member,
        "2026-07-20T00:00:00Z",
    )
    .await
    .expect("activate Member device");
    let owner_device_id = owner_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read Owner device id")
        .expect("Owner device id");
    let (_, member_registration, _, _) =
        crate::sync::store::operations::load_local_store_authority(
            &StoreDatabase::new(&member_db),
            &member_db
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await
                .expect("read Member device id")
                .expect("Member device id"),
            &member,
        )
        .await
        .expect("load Member registration");
    store.home.fail_exact_create_before_call(1);
    store
        .loaded_store(&owner_db)
        .await
        .expect("load Owner Store")
        .begin_owner_promotion(&owner_device_id, &owner, member_registration.clone())
        .await
        .expect_err("interrupted publication retains RequestPrepared");
    let journal = StoreDatabase::new(&owner_db)
        .load_owner_promotion_target(target_key(&member_registration).unwrap())
        .await
        .expect("load prepared request journal")
        .expect("prepared request journal exists");
    let mut substituted_request = journal.clone();
    let OwnerPromotionJournalState::RequestPrepared { request, .. } =
        &mut substituted_request.state
    else {
        panic!("interrupted request remains RequestPrepared")
    };
    request.member_grant = MembershipGrantId(ObjectHash::digest(b"another exact Member grant"));
    let encoded =
        serde_json::to_string(&substituted_request).expect("serialize corrupt request journal");
    owner_db
        .set_protocol_state(
            &format!("owner_promotion/{}", journal.promotion_id),
            &encoded,
        )
        .await
        .expect("install corrupt id journal");
    owner_db
        .set_protocol_state(&target_key(&journal.target).unwrap(), &encoded)
        .await
        .expect("install corrupt target journal");

    assert!(StoreDatabase::new(&owner_db)
        .load_owner_promotion_journal(journal.promotion_id)
        .await
        .is_err());

    let mut substituted_bytes = journal;
    let OwnerPromotionJournalState::RequestPrepared { candidate, .. } =
        &mut substituted_bytes.state
    else {
        panic!("interrupted request remains RequestPrepared")
    };
    let bytes = b"another exact prepared object".to_vec();
    let reference = ExactObjectRef::new(
        candidate.prepared.reference().slot().clone(),
        bytes.len() as u64,
        ObjectHash::digest(&bytes),
    );
    candidate.prepared = PreparedExactObject::new(reference.clone(), bytes)
        .expect("prepare substituted exact object");
    candidate.reference.object = reference;
    let encoded = serde_json::to_string(&substituted_bytes)
        .expect("serialize substituted prepared bytes journal");
    owner_db
        .set_protocol_state(
            &format!("owner_promotion/{}", substituted_bytes.promotion_id),
            &encoded,
        )
        .await
        .expect("install substituted id journal");
    owner_db
        .set_protocol_state(&target_key(&substituted_bytes.target).unwrap(), &encoded)
        .await
        .expect("install substituted target journal");

    assert!(StoreDatabase::new(&owner_db)
        .load_owner_promotion_journal(substituted_bytes.promotion_id)
        .await
        .is_err());
}
