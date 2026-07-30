use crate::keys::{self, UserKeypair};
use crate::protocol::membership::MemberRole;
use crate::protocol::store_commit::ObjectHash;

use super::{
    select_mutation_author_stream, validate_prepared_publication, validate_prepared_transition,
    AuthorizedMembershipPublication,
};

#[tokio::test]
async fn prepared_membership_transition_rejects_substituted_slots_and_bytes() {
    let db = crate::sync::test_helpers::open_test_db();
    let owner = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "prepared-membership-binding",
        owner.clone(),
    )
    .await
    .expect("create Merge Store");
    let database = crate::database::StoreDatabase::new(&db);
    let chain = store
        .bind_device(&db, &owner)
        .await
        .expect("bind membership transition Store")
        .membership_for_test()
        .await
        .expect("load exact membership chain");
    let stream_id = select_mutation_author_stream(&database, &chain, &owner)
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
    let device = store
        .bind_device(&db, &owner)
        .await
        .expect("bind membership publication Store");
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize membership publication writer");
    let mut publication = AuthorizedMembershipPublication::new(&mut writer);
    let prepared = publication
        .prepare_transition(&chain, entry)
        .await
        .expect("prepare membership transition");
    validate_prepared_transition(&prepared).expect("validate prepared transition");

    let mut redirected_head = prepared.clone();
    redirected_head.transition.head_slot = crate::storage::cloud::ObjectSlot::logical(
        "store-v1/tests/redirected-membership-head.json".to_string(),
    )
    .expect("valid redirected head slot");
    assert!(validate_prepared_transition(&redirected_head).is_err());

    let mut redirected_successor = prepared.clone();
    redirected_successor.transition.body.successor.next_slot =
        crate::storage::cloud::ObjectSlot::logical(
            "store-v1/tests/redirected-membership-successor.json".to_string(),
        )
        .expect("valid redirected successor slot");
    assert!(validate_prepared_transition(&redirected_successor).is_err());

    let mut substituted_entry = prepared.clone();
    let substituted_bytes = b"substituted exact membership entry".to_vec();
    let substituted_ref = crate::storage::ExactObjectRef::new(
        substituted_entry.entry_object.reference().slot().clone(),
        substituted_bytes.len() as u64,
        ObjectHash::digest(&substituted_bytes),
    );
    substituted_entry.entry_object =
        crate::storage::PreparedExactObject::new(substituted_ref.clone(), substituted_bytes)
            .expect("prepare substituted membership entry object");
    substituted_entry.entry_ref.object = substituted_ref.clone();
    substituted_entry.transition.body.entry.object = substituted_ref;
    assert!(validate_prepared_transition(&substituted_entry).is_err());

    let mut substituted_head = publication
        .finish_transition(
            prepared,
            crate::protocol::membership::MembershipHeadActivation::Direct,
        )
        .await
        .expect("finish membership transition");
    let substituted_bytes = b"substituted exact membership head".to_vec();
    let substituted_ref = crate::storage::ExactObjectRef::new(
        substituted_head.head_object.reference().slot().clone(),
        substituted_bytes.len() as u64,
        ObjectHash::digest(&substituted_bytes),
    );
    substituted_head.head_object =
        crate::storage::PreparedExactObject::new(substituted_ref.clone(), substituted_bytes)
            .expect("prepare substituted membership head object");
    substituted_head.head_ref.object = substituted_ref;
    assert!(validate_prepared_publication(&substituted_head).is_err());
}
