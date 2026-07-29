use super::*;

#[tokio::test]
async fn merge_resume_blocks_revoked_journals_without_stopping_later_operations() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store =
        create_test_store_in_its_own_task(&db, "circle-merge-revoked-grant", &founder).await;
    let successor = UserKeypair::generate();
    let successor_pubkey = keys::public_key_hex(&successor);
    let encryption = EncryptionService::from_key([42; 32]);
    store
        .invite_member(
            &db,
            &founder,
            &crate::sync::hlc::Hlc::new("founder-device".to_string()),
            &successor_pubkey,
            None,
            MemberRole::Member,
            &encryption,
            "Revocation test Store",
        )
        .await
        .expect("invite successor member through the production membership path");

    let successor_db = open_test_db();
    install_active_device_fixture(
        &store,
        &db,
        &successor_db,
        &successor,
        "0000000001003-0000-successor",
    )
    .await
    .expect("activate successor exact device fixture");
    let journal = prepare_circle_operation(
        &successor_db,
        &store.storage,
        "0000000001003-0000-successor",
        "Revoked Circle",
        &successor,
    )
    .await
    .expect("prepare operation while successor is authorized");
    StoreDatabase::new(&successor_db)
        .insert_circle_operation(journal.clone())
        .await
        .expect("persist operation that will lose authorization");
    let custody = TestCustody::default();
    custody.set_initial_key([42; 32]);
    let cipher = store.storage.cipher_state().clone();
    store
        .remove_member(
            &db,
            &founder,
            &crate::sync::hlc::Hlc::new("founder-device".to_string()),
            &successor_pubkey,
            &encryption,
            &custody,
        )
        .await
        .expect("remove successor through the production membership path");
    let rotated_encryption = match cipher.snapshot() {
        CloudCipher::Encrypted(encryption) => encryption,
        CloudCipher::Plaintext => panic!("member removal requires encrypted storage"),
    };
    store
        .invite_member(
            &db,
            &founder,
            &crate::sync::hlc::Hlc::new("founder-device".to_string()),
            &successor_pubkey,
            None,
            MemberRole::Member,
            &rotated_encryption,
            "Revocation test Store",
        )
        .await
        .expect("re-add successor under a new exact membership grant");
    store
        .open_into(&successor_db)
        .await
        .expect("load successor's replacement membership grant");
    let later = prepare_circle_operation(
        &successor_db,
        &store.storage,
        "0000000001004-0000-successor",
        "Later Circle",
        &successor,
    )
    .await
    .expect("prepare still-authorized operation");
    StoreDatabase::new(&successor_db)
        .insert_circle_operation(later.clone())
        .await
        .expect("persist still-authorized operation");

    resume_circle_operations(&successor_db, &store.storage, &successor)
        .await
        .expect("revoked journal is blocked without interrupting the resume loop");

    let blocked = StoreDatabase::new(&successor_db)
        .circle_operation(&journal.operation_id)
        .await
        .expect("read revoked journal")
        .expect("revoked journal remains durable");
    assert!(matches!(
        blocked.state(),
        CircleOperationState::Blocked { .. }
    ));
    assert!(StoreDatabase::new(&successor_db)
        .circle_operation(&later.operation_id)
        .await
        .expect("read later journal")
        .is_none());
    assert_eq!(
        StoreDatabase::new(&successor_db)
            .get_circles(
                &successor_pubkey,
                std::collections::BTreeSet::from([successor_pubkey.clone()]),
            )
            .await
            .expect("read successor circles"),
        vec![crate::sync::circle::CircleInfo::Active {
            id: later.circle_id(),
            name: "Later Circle".to_string(),
            role: CircleRole::Owner,
            rotation_required: false,
        }]
    );
    assert_eq!(
        activation_count(&successor_db, journal.circle_id()).await,
        0
    );
}

#[tokio::test]
async fn retained_circle_activation_reverifies_every_retained_boundary() {
    fn replace_once(bytes: &[u8], original: &[u8], replacement: &[u8]) -> Vec<u8> {
        let positions = bytes
            .windows(original.len())
            .enumerate()
            .filter_map(|(index, candidate)| (candidate == original).then_some(index))
            .collect::<Vec<_>>();
        let [position] = positions.as_slice() else {
            panic!("retained fixture must contain exactly one replacement target")
        };
        let mut replaced = Vec::with_capacity(bytes.len() - original.len() + replacement.len());
        replaced.extend_from_slice(&bytes[..*position]);
        replaced.extend_from_slice(replacement);
        replaced.extend_from_slice(&bytes[*position + original.len()..]);
        replaced
    }

    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(&db, "retained-circle-activation", founder.clone())
        .await
        .expect("create retained Circle Store");
    let peer = UserKeypair::generate();
    let peer_pubkey = keys::public_key_hex(&peer);
    store
        .invite_member(
            &db,
            &founder,
            &crate::sync::hlc::Hlc::new("founder-device".to_string()),
            &peer_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([73; 32]),
            "Retained Circle activation Store",
        )
        .await
        .expect("invite retained Circle peer");
    let journal = prepare_circle_operation(
        &db,
        &store.storage,
        "0000000001000-0000-founder",
        "Household",
        &founder,
    )
    .await
    .expect("prepare retained Circle activation");
    for object in journal.operation().prepared_objects.values() {
        store
            .storage
            .create_protocol_object(object)
            .await
            .expect("publish retained Circle activation object");
    }
    let commit = journal.commit().expect("parse retained Circle commit");
    let commit_ref = &journal.operation().commit_ref;
    let author = crate::sync::store::database::StoreDatabase::new(&db)
        .activated_store_device_registration(commit.author_registration.clone())
        .await
        .expect("load retained Circle commit author");
    let verified =
        load_circle_activations(&db, &store.storage, commit_ref, &commit, &author, &founder)
            .await
            .expect("verify retained Circle activation fixture");
    let retained = verified
        .to_retained()
        .expect("serialize retained Circle activation");
    let founder_pubkey = keys::public_key_hex(&founder);
    assert_eq!(
        VerifiedCircleActivations::parse_retained(
            &retained,
            &commit,
            commit_ref,
            &author,
            Some(&founder_pubkey),
        )
        .expect("parse retained Circle activation"),
        verified
    );
    assert_eq!(
        VerifiedCircleActivations::parse_retained(&retained, &commit, commit_ref, &author, None,)
            .expect("parse retained Circle activation before local registration"),
        verified
    );

    let local_access = verified.circles()[0]
        .local_access
        .as_ref()
        .expect("founder has retained Circle access");
    let envelope_bytes =
        serde_json::to_vec(&local_access.envelope).expect("serialize retained Circle envelope");
    let mut envelope_field = b",\"envelope\":".to_vec();
    envelope_field.extend_from_slice(&envelope_bytes);
    let omitted = replace_once(&retained, &envelope_field, &[]);
    let omitted_error = VerifiedCircleActivations::parse_retained(
        &omitted,
        &commit,
        commit_ref,
        &author,
        Some(&founder_pubkey),
    )
    .expect_err("retained Circle access cannot omit its envelope");
    assert!(omitted_error
        .to_string()
        .contains("missing field `envelope`"));

    let peer_envelope = journal
        .operation()
        .creation
        .access
        .iter()
        .find(|access| access.leaf.value.recipient_pubkey == peer_pubkey)
        .expect("peer has an exact retained Circle envelope");
    let local_pair = serde_json::to_vec(&crate::sync::circle::PreparedCircleAccess {
        leaf: local_access.leaf.clone(),
        envelope: local_access.envelope.clone(),
    })
    .expect("serialize local retained Circle access pair");
    let substituted_pair =
        serde_json::to_vec(peer_envelope).expect("serialize substituted Circle access pair");
    let substituted = replace_once(&retained, &local_pair, &substituted_pair);
    let substituted_error = VerifiedCircleActivations::parse_retained(
        &substituted,
        &commit,
        commit_ref,
        &author,
        Some(&founder_pubkey),
    )
    .expect_err("retained Circle access cannot substitute another signed access pair");
    assert!(
        substituted_error
            .to_string()
            .contains("access names another local recipient"),
        "{substituted_error}"
    );

    let mut tampered_envelope = local_access.envelope.clone();
    tampered_envelope.signature.push('0');
    let tampered_envelope = serde_json::to_vec(&tampered_envelope)
        .expect("serialize tampered retained Circle envelope");
    let tampered = replace_once(&retained, &envelope_bytes, &tampered_envelope);
    let tampered_error = VerifiedCircleActivations::parse_retained(
        &tampered,
        &commit,
        commit_ref,
        &author,
        Some(&founder_pubkey),
    )
    .expect_err("retained Circle access cannot alter a signed envelope");
    assert!(
        tampered_error
            .to_string()
            .contains("access leaf and envelope failed verification"),
        "{tampered_error}"
    );

    let mut noncanonical = retained;
    noncanonical.push(b'\n');
    let canonical_error = VerifiedCircleActivations::parse_retained(
        &noncanonical,
        &commit,
        commit_ref,
        &author,
        Some(&founder_pubkey),
    )
    .expect_err("retained Circle activation bytes must be canonical");
    assert!(canonical_error.to_string().contains("not canonical"));
}
