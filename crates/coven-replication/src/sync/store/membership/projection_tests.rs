use super::*;

#[tokio::test]
async fn empty_store_prefix_projection_retains_only_the_founder() {
    let fixture = MergeFixture::new("project-empty-membership").await;
    let founder = fixture.load().await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
    let current = fixture.load().await;
    let projected = fixture
        .device
        .project_membership_for_test(current.head_refs())
        .await
        .expect("project accepted membership to the empty Store prefix");

    assert_eq!(projected.head_refs(), founder.head_refs());
    assert!(!projected.can_write_now(&pubkey_hex(&member)));
    assert!(projected.is_owner_now(&fixture.owner_pubkey));
}

#[tokio::test]
async fn empty_store_prefix_projection_excludes_admission_promotion_and_their_suffix() {
    let fixture = MergeFixture::new("project-store-bound-membership").await;
    let founder = fixture.load().await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    fixture
        .store
        .activate_joined_device(
            &fixture.db,
            fixture.store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate member device");
    let before_promotion = fixture.load().await;
    fixture
        .store
        .promote_active_member_fixture(
            &fixture.db,
            fixture.store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
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
        .admit_member(&later_member, MemberRole::Member)
        .await;
    let candidate = fixture.load().await;
    assert!(candidate.can_write_now(&pubkey_hex(&later_member)));
    let projected = fixture
        .device
        .project_membership_for_test(candidate.head_refs())
        .await
        .expect("project membership before every accepted Store control");

    assert_eq!(projected.head_refs(), founder.head_refs());
    assert!(!projected.can_write_now(&pubkey_hex(&member)));
    assert!(!projected.is_owner_now(&pubkey_hex(&member)));
    assert!(!projected.can_write_now(&pubkey_hex(&later_member)));
}
