use coven_keys::keys::{self, UserKeypair};
use coven_protocol::membership::MemberRole;
use coven_protocol::store_commit::ObjectHash;

#[tokio::test]
async fn prepared_membership_transition_rejects_substituted_slots_and_bytes() {
    let db = crate::sync::test_helpers::open_test_db();
    let owner = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "prepared-membership-binding",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Merge Store");
    let device = store
        .bind_device(&db, &owner)
        .await
        .expect("bind membership publication Store");
    let chain = device
        .membership_for_test()
        .await
        .expect("load exact membership chain");
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize membership publication writer");
    let stream_id = writer
        .select_membership_author_stream(&chain)
        .await
        .expect("select membership stream");
    let entry = chain
        .signed_set_member_in_stream(
            &owner,
            stream_id,
            keys::public_key_hex(&UserKeypair::generate()),
            None,
            MemberRole::Member,
            "2026-07-21T00:00:00Z".to_string(),
        )
        .expect("sign membership entry");
    let prepared = writer
        .prepare_membership_transition(&chain, entry)
        .await
        .expect("prepare membership transition");
    prepared.validate().expect("validate prepared transition");

    let mut redirected_head = prepared.clone();
    redirected_head.transition.head_slot = coven_protocol::objects::ObjectSlot::logical(
        "store-v1/tests/redirected-membership-head.json".to_string(),
    )
    .expect("valid redirected head slot");
    assert!(redirected_head.validate().is_err());

    let mut redirected_successor = prepared.clone();
    redirected_successor.transition.body.successor.next_slot =
        coven_protocol::objects::ObjectSlot::logical(
            "store-v1/tests/redirected-membership-successor.json".to_string(),
        )
        .expect("valid redirected successor slot");
    assert!(redirected_successor.validate().is_err());

    let mut substituted_entry = prepared.clone();
    let substituted_bytes = b"substituted exact membership entry".to_vec();
    let substituted_ref = coven_protocol::objects::ExactObjectRef::new(
        substituted_entry.entry_object.reference().slot().clone(),
        substituted_bytes.len() as u64,
        ObjectHash::digest(&substituted_bytes),
    );
    substituted_entry.entry_object = coven_protocol::objects::PreparedExactObject::new(
        substituted_ref.clone(),
        substituted_bytes,
    )
    .expect("prepare substituted membership entry object");
    substituted_entry.entry_ref.object = substituted_ref.clone();
    substituted_entry.transition.body.entry.object = substituted_ref;
    assert!(substituted_entry.validate().is_err());

    let mut substituted_head = writer
        .finish_membership_transition(
            prepared,
            coven_protocol::membership::MembershipHeadActivation::Direct,
        )
        .await
        .expect("finish membership transition");
    let substituted_bytes = b"substituted exact membership head".to_vec();
    let substituted_ref = coven_protocol::objects::ExactObjectRef::new(
        substituted_head.head_object.reference().slot().clone(),
        substituted_bytes.len() as u64,
        ObjectHash::digest(&substituted_bytes),
    );
    substituted_head.head_object = coven_protocol::objects::PreparedExactObject::new(
        substituted_ref.clone(),
        substituted_bytes,
    )
    .expect("prepare substituted membership head object");
    substituted_head.head_ref.object = substituted_ref;
    assert!(substituted_head.validate().is_err());
}
