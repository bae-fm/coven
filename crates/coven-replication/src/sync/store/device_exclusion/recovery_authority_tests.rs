use crate::sync::test_helpers::*;
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::{
    AuthorHead, MembershipFloor, MembershipHeadAcceptance, MembershipHeadActivation,
    MembershipHeadRef,
};
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::*;
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn an_active_device_cannot_issue_another_devices_recovery_acceptance() {
    let directory = test_store_dir();
    let database = open_test_db(directory.clone());
    let identity = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "recovery-principal-acceptance",
        identity.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&database, directory, &identity)
        .await
        .expect("bind founder");
    let root = store.root();
    let local = owner
        .latest_local_store_device_registration()
        .await
        .expect("read founder")
        .expect("founder registration");
    let author =
        StoreDeviceRegistration::parse_at(&local.registration_bytes, &root, local.device_id)
            .expect("verify founder registration");
    let author_ref =
        StoreDeviceRegistrationRef::from_registration(&author, local.prepared.reference().clone());
    let device_key = author
        .device_signer(&identity)
        .expect("founder's device key");
    let chain = owner
        .membership_for_test()
        .await
        .expect("actual accepted authority");
    let authority = store.founder_recovery_authority().await;
    let database_owner = coven_database::StoreDatabase::new(&database);
    let before = database_owner
        .store_current_publication()
        .await
        .expect("accepted predecessor");
    let mut recovery = owner
        .owner_recovery_for_test()
        .await
        .expect("authorize real recovery");
    home.fail_exact_create_before_call(4);
    recovery
        .recover_owner_device(&authority, None)
        .await
        .expect_err("interrupt the real recovery before its authority head upload");
    let staged = database_owner
        .owner_recovery_publication()
        .await
        .expect("read recovery journal")
        .expect("durable recovery candidate");
    let publication = staged
        .membership_publication()
        .expect("exact prepared recovery authority");
    let [activation] = staged.commit.value.device_registrations() else {
        panic!("sole recovery activation");
    };
    assert_ne!(activation.registration, author_ref);
    assert!(database_owner
        .activated_store_device_registration_for_device(activation.registration.device_id)
        .await
        .expect("read activation")
        .is_none());
    assert_eq!(
        database_owner.store_current_publication().await.unwrap(),
        before
    );
    assert!(storage
        .observe_exact_slot(publication.head_ref.object.slot())
        .await
        .expect("read unuploaded head")
        .is_none());

    // The principal prepared this entry before acceptance. Possession of an
    // existing device key must not turn that preparation into an accepted
    // recovery for a different registration.
    let mut body = publication.head.body.clone();
    body.author_registration = author_ref.clone();
    let anchor = chain
        .membership_anchor(&publication.head_ref.coord.author_owner_grant)
        .expect("rooted author anchor");
    body.successor.activation = StreamActivation::grant_authorized(
        root.store_root_hash,
        author_ref.clone(),
        publication.head_ref.coord.author_owner_grant.clone(),
        anchor.clone(),
    )
    .activation_id();
    let head = AuthorHead::signed(
        publication.head.store_id.clone(),
        body,
        publication.head.activation.clone(),
        &device_key,
    );
    let head_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let head_prefix = semantic_prefix_from_exact_object(&publication.head_ref.object, ".json")
        .expect("head path");
    let prepared_head = storage
        .prepare_protocol_object(
            &head_context,
            publication.head_ref.object.slot().clone(),
            &head_prefix,
            head.to_bytes(),
        )
        .expect("prepare device-signed head");
    let head_ref = MembershipHeadRef {
        coord: head.entry_coord(),
        head_hash: head.head_hash(),
        object: prepared_head.reference().clone(),
    };
    let mut entry = staged.publication.entry.clone();
    entry.body_mut().author_registration = author_ref;
    entry.resign(&device_key);
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StorePublicationEntry,
    );
    let prefix = store_publication_entry_semantic_prefix(&entry);
    let object = store
        .create_exact_protocol_object(&context, &prefix, ".json", &entry.to_bytes())
        .await
        .expect("upload unaccepted envelope");
    let entry_ref =
        StorePublicationRef::from_entry(&entry, object).expect("exact unaccepted publication");
    let mut claimed_current = staged.publication.replacement.clone();
    let StorePublicationState::Accepted {
        entry: claimed_entry,
        ..
    } = &mut claimed_current.body_mut().state
    else {
        panic!("prepared recovery replacement names its proposed publication");
    };
    *claimed_entry = entry_ref;
    claimed_current.resign(&device_key);
    let result = MembershipHeadAcceptance::signed(
        root.store_root_hash,
        head_ref.clone(),
        &head,
        &entry,
        &claimed_current,
        MembershipFloor(staged.commit.value.membership_state.heads.clone()),
        &author,
        &device_key,
    )
    .expect("existing device can sign its own acceptance assertion");
    result
        .verify_for(root.store_root_hash, &head_ref, &head, &author)
        .expect("valid device signature and exact head binding");
    let MembershipHeadActivation::StoreCommit {
        acceptance_slot, ..
    } = &head.activation
    else {
        panic!("Store activation");
    };
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHeadAcceptance,
    );
    let prefix =
        coven_protocol::membership::membership_head_acceptance_semantic_prefix(&head_ref.coord);
    let prepared_result = storage
        .prepare_protocol_object(
            &context,
            acceptance_slot.clone(),
            &prefix,
            result.to_bytes(),
        )
        .expect("prepare fabricated result");
    for prepared in [
        publication
            .prepared_entry()
            .expect("principal-prepared entry"),
        prepared_head,
        prepared_result,
    ] {
        storage
            .create_protocol_object(&prepared)
            .await
            .expect("upload malicious authority objects");
    }
    let loaded = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &root)
        .await
        .expect("cold authority owner")
        .load_accepted_anchored_membership(&[], Some(&pubkey_hex(&identity)))
        .await;
    assert_eq!(
        database_owner.store_current_publication().await.unwrap(),
        before,
        "no recovery publication was accepted"
    );
    let error = loaded
        .expect_err("a device-signed result cannot activate another device's principal recovery");
    assert!(
        error
            .to_string()
            .contains("Recovery activation requires its principal acceptance"),
        "{error}"
    );
}
