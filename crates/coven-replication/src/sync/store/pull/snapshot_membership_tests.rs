use crate::sync::store::commit_verification::merge_history::VerifiedMergePrefixHeadStatus;
use crate::sync::store::HistoryConstructionAuthority;
use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::{public_key_hex, UserKeypair};

#[tokio::test]
async fn snapshot_membership_prefix_keeps_its_accepted_owner_promotion() {
    Box::pin(assert_snapshot_membership_prefix()).await;
}

async fn assert_snapshot_membership_prefix() {
    let owner_dir = test_store_dir();
    let owner_db = open_test_db(owner_dir.clone());
    let owner_identity = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &owner_db,
        owner_dir.clone(),
        "snapshot-membership-prefix",
        owner_identity.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let member_dir = test_store_dir();
    let member_db = open_test_db(member_dir.clone());
    let member_identity = UserKeypair::generate();
    store
        .admit_and_activate_peer(
            &owner_db,
            owner_dir.clone(),
            &member_db,
            member_dir.clone(),
            &member_identity,
        )
        .await
        .expect("activate the promotion candidate");
    store
        .promote_active_member_fixture(
            &owner_db,
            owner_dir.clone(),
            &member_db,
            member_dir,
            &owner_identity,
            &member_identity,
            &EncryptionService::from_key([42; 32]),
        )
        .await
        .expect("publish the accepted owner promotion");
    let owner = store
        .bind_device_in(&owner_db, owner_dir, &owner_identity)
        .await
        .expect("bind the snapshot publisher");
    let snapshot = owner
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish and adopt a snapshot covering the promotion");
    let baseline = StoreDatabase::new(&owner_db)
        .installed_replay_baseline()
        .await
        .expect("open the installed verified baseline");
    assert!(baseline.stands_on(&snapshot.reference));
    let proof = baseline
        .history_summary()
        .expect("snapshot retains its membership authority")
        .summary
        .membership_proofs
        .values()
        .find(|proof| {
            matches!(
                &proof.entry_value.change,
                coven_protocol::membership::StoreAuthorityChange::SetMember { user_pubkey, .. }
                    if *user_pubkey == public_key_hex(&member_identity)
            )
        })
        .expect("snapshot retains the accepted promotion proof")
        .clone();
    assert!(baseline.covers(&proof.commit));
    let coverage = baseline.coverage().clone();

    // A snapshot supplies its covered authority without requiring the retired
    // commits to be loaded into the verifier again.
    let mut history = HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &store.root())
        .await
        .expect("open the rooted snapshot reader");
    history
        .admit_installed_baseline(baseline)
        .expect("stand on the installed baseline");
    let empty_prefix = history
        .verified_membership_prefix(std::iter::empty())
        .expect("derive a prefix that does not reach the snapshot");
    assert_eq!(
        empty_prefix
            .classify_head(&proof.head, &proof.head_value, &proof.commit)
            .expect("classify the promotion outside the requested history"),
        VerifiedMergePrefixHeadStatus::OutsidePrefix,
    );
    history
        .verify_refs(coverage.commits().values().cloned())
        .await
        .expect("resolve the snapshot coverage without retired commits");
    let prefix = history
        .verified_membership_prefix(coverage.commits().values().cloned())
        .expect("derive the snapshot's membership prefix");
    assert_eq!(
        prefix
            .classify_head(&proof.head, &proof.head_value, &proof.commit)
            .expect("classify the snapshot's accepted promotion"),
        VerifiedMergePrefixHeadStatus::Included,
    );
    let membership = history
        .load_membership_at_verified_prefix(&snapshot.meta.state.membership.heads, &prefix)
        .await
        .expect("resolve membership from the snapshot prefix");
    assert!(membership.is_owner_now(&public_key_hex(&member_identity)));
}
