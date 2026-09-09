use super::*;
use coven_database::{HostWriteOperation, StoreRowWrites, WriteBatch};
use coven_protocol::remote_object::{OwnedObjectState, RemoteObjectRecord};

#[derive(Clone, Copy)]
enum BootstrapOperation {
    MemberAddition,
    CloseFinalization,
}

enum DiscardAuthority {
    AcceptedRetirement,
    UnacceptedRetirement,
}

#[tokio::test]
async fn discarding_a_member_addition_releases_its_borrowed_blob_claim() {
    check_bootstrap_discard(
        BootstrapOperation::MemberAddition,
        DiscardAuthority::AcceptedRetirement,
    )
    .await;
}

#[tokio::test]
async fn discarding_a_close_finalization_preserves_its_identity_and_releases_borrowed_blobs() {
    check_bootstrap_discard(
        BootstrapOperation::CloseFinalization,
        DiscardAuthority::AcceptedRetirement,
    )
    .await;
}

async fn check_bootstrap_discard(operation: BootstrapOperation, authority: DiscardAuthority) {
    let founder_dir = crate::sync::test_helpers::test_store_dir();
    let founder_db = open_circle_blob_test_db(founder_dir.clone());
    let founder = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, storage) = create_test_store_fixture_in_its_own_task(
        &founder_db,
        founder_dir.clone(),
        "circle-bootstrap-discard",
        &founder,
        home.clone(),
    )
    .await;
    let author = UserKeypair::generate();
    let invitee = UserKeypair::generate();
    let routing = EncryptionService::from_key([42; 32]);
    for member in [&author, &invitee] {
        store
            .admit_member(
                &founder_db,
                founder_dir.clone(),
                &founder,
                &keys::public_key_hex(member),
                None,
                MemberRole::Member,
                &routing,
                "Circle bootstrap Store",
            )
            .await
            .expect("admit the Circle author and prospective member");
    }
    let author_dir = crate::sync::test_helpers::test_store_dir();
    let author_db = open_circle_blob_test_db(author_dir.clone());
    store
        .activate_joined_device(
            &founder_db,
            founder_dir.clone(),
            &author_db,
            author_dir.clone(),
            &author,
            "0000000001000-0000-circle-author",
        )
        .await
        .expect("activate the Circle author's device");
    let device = store
        .bind_device_in(&author_db, author_dir.clone(), &author)
        .await
        .expect("bind the Circle author");
    let circle_id = device
        .create_circle("0000000002000-0000-circle-author", "Shared files")
        .await
        .expect("publish the author's Circle");
    let blob_id = "00000000-0000-4000-8000-000000000011";
    let bytes = b"Accepted attachment borrowed by an unaccepted bootstrap";
    let mut batch = WriteBatch::new();
    batch.put_blob("files", blob_id, bytes.to_vec());
    let database = StoreDatabase::new(&author_db);
    let captured = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO documents(id, audience, size, hash, _updated_at)
                 VALUES ('{blob_id}', '{circle_id}', {}, '{}', '0000000003000-0000-circle-author')",
                    bytes.len(),
                    coven_protocol::blob::content_hash(bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            Some(routing.clone()),
            Some(Box::new(device.host_write_blob_staging())),
        )
        .await
        .expect("capture the Circle attachment through the host write owner");
    let mut writer = device.authorize_writer().await.unwrap();
    assert_eq!(
        writer
            .publish_pending_store_writes(Some(&routing))
            .await
            .unwrap(),
        1
    );
    drop(writer);
    let accepted_commit = match database.write_status(&captured.write_id).await.unwrap() {
        coven_protocol::write::WriteStatus::Published(receipt) => {
            receipt.exact_commit().unwrap().clone()
        }
        status => panic!("attachment was not accepted: {status:?}"),
    };
    let blob = database
        .row_blob_ref("documents", blob_id)
        .await
        .unwrap()
        .stored()
        .unwrap()
        .clone();
    storage
        .verify_blob_object(&blob)
        .await
        .expect("the accepted attachment exists");
    let components = prepare_owner_sync_components(
        &author_db,
        &store,
        &home,
        &author_dir,
        &author,
        "circle-bootstrap-discard",
        circle_test_custody(),
    )
    .await;
    match operation {
        BootstrapOperation::MemberAddition => {
            home.fail_exact_create_before_call(1);
            components
                .add_circle_member(
                    circle_id,
                    keys::public_key_hex(&invitee),
                    CircleRole::Member,
                )
                .await
                .expect_err("interrupt after staging the bootstrap and before its first upload");
        }
        BootstrapOperation::CloseFinalization => {
            components
                .add_circle_member(
                    circle_id,
                    keys::public_key_hex(&invitee),
                    CircleRole::Member,
                )
                .await
                .expect("accept member addition before closing the epoch");
            components
                .remove_circle_member(circle_id, keys::public_key_hex(&invitee))
                .await
                .expect("accept the epoch close");
            let mut writer = device.authorize_writer().await.unwrap();
            writer
                .circles()
                .publish_circle_epoch_close_responses()
                .await
                .expect("publish the remaining participant's exact close response");
            home.fail_exact_create_before_call(1);
            let error = writer
                .circles()
                .finalize_ready_circle_epoch_closes(&routing)
                .await
                .expect_err("interrupt the durably staged finalization before its first upload");
            assert!(
                error
                    .to_string()
                    .contains("forced failure before exact create"),
                "{error:?}"
            );
        }
    }
    let journal = database
        .oldest_pending_circle_operation()
        .await
        .unwrap()
        .unwrap();
    let candidate = journal.operation().commit_ref().clone();
    match operation {
        BootstrapOperation::MemberAddition => {
            assert_eq!(journal.kind(), CircleOperationKind::AddMember)
        }
        BootstrapOperation::CloseFinalization => {
            assert_eq!(journal.state(), CircleOperationState::Finalizing);
            assert!(journal.operation().creation.close_outcome.is_some());
        }
    }
    assert!(
        journal
            .operation()
            .creation
            .access
            .iter()
            .any(|access| matches!(
                &access.leaf.value.disposition,
                CircleAccessDisposition::Active { bootstrap: Some(bootstrap), .. }
                    if bootstrap.blobs.iter().any(|binding| binding.stored() == Some(&blob))
            )),
        "the candidate borrows this exact accepted attachment"
    );
    let before = author_db
        .remote_object_for_test(blob.object().clone())
        .await
        .unwrap();
    let RemoteObjectRecord::SharedLiveSet(before) = before else {
        panic!("accepted blob has shared ownership")
    };
    let OwnedObjectState::UploadedVerified { ownership: before } = before.state else {
        panic!("accepted blob has verified upload")
    };
    assert!(before.pending.contains(&candidate));
    assert!(before.activated.contains(
        &coven_protocol::remote_object::SharedObjectOwner::StoreCommit(accepted_commit.clone())
    ));

    if matches!(authority, DiscardAuthority::UnacceptedRetirement) {
        let mut membership = device.membership().await.unwrap();
        let stream = membership.founder_coord().unwrap().stream_id;
        let removal = membership
            .signed_remove_member_in_stream(
                &founder,
                stream,
                keys::public_key_hex(&author),
                "0000000004000-0000-founder".into(),
            )
            .expect("sign a real removal without accepting its publication");
        membership
            .add_entry(removal)
            .expect("reduce the signed removal");
        assert!(
            membership
                .write_authority_retirement(
                    journal
                        .operation()
                        .commit()
                        .membership_authority
                        .as_ref()
                        .unwrap(),
                    &keys::public_key_hex(&author),
                )
                .is_some(),
            "the unaccepted chain claims the candidate's grant is retired"
        );
        let boundary = database.store_current_publication().await.unwrap();
        let active = database.active_store_publication().await.unwrap();
        database
            .begin_circle_operation_discard(
                journal.clone(),
                membership,
                boundary.record().accepted().unwrap().clone(),
            )
            .await
            .expect_err("signed unaccepted retirement cannot authorize candidate deletion");
        assert_eq!(
            database
                .circle_operation(&journal.operation_id)
                .await
                .unwrap(),
            Some(journal)
        );
        assert_eq!(database.active_store_publication().await.unwrap(), active);
        assert_eq!(
            database.store_current_publication().await.unwrap(),
            boundary
        );
        let RemoteObjectRecord::SharedLiveSet(after) = author_db
            .remote_object_for_test(blob.object().clone())
            .await
            .unwrap()
        else {
            panic!("rejected discard preserves accepted blob ownership")
        };
        let OwnedObjectState::UploadedVerified { ownership: after } = after.state else {
            panic!("rejected discard preserves verified upload")
        };
        assert_eq!(after, before);
        return;
    }

    let custody = circle_test_custody();
    store
        .remove_member(
            &founder_db,
            founder_dir,
            &founder,
            &keys::public_key_hex(&author),
            &routing,
            custody.as_ref(),
        )
        .await
        .expect("retire the candidate author's exact Store grant");
    device
        .pull_store()
        .await
        .expect("install the accepted grant retirement");
    home.clear_exact_creates();
    device
        .circles()
        .discard_circle_operation(&journal.operation_id)
        .await
        .expect("discard the unaccepted Circle bootstrap candidate");
    assert!(database
        .circle_operation(&journal.operation_id)
        .await
        .unwrap()
        .is_none());
    assert!(database.active_store_publication().await.unwrap().is_none());
    let after = author_db
        .remote_object_for_test(blob.object().clone())
        .await
        .unwrap();
    let RemoteObjectRecord::SharedLiveSet(after) = after else {
        panic!("discard retains the accepted blob")
    };
    let OwnedObjectState::UploadedVerified { ownership: after } = after.state else {
        panic!("discard retains verified upload")
    };
    assert!(
        !after.pending.contains(&candidate),
        "the completed discard must release its bootstrap blob claim"
    );
    assert!(
        after.activated.is_superset(&before.activated),
        "discard preserves every prior accepted owner"
    );
    assert!(
        home.exact_creates().is_empty(),
        "discard does not recreate accepted or candidate objects"
    );
    storage
        .verify_blob_object(&blob)
        .await
        .expect("the accepted attachment survives bootstrap discard");
}

#[tokio::test]
async fn discard_rejects_a_signed_unaccepted_retirement_at_the_real_accepted_boundary() {
    check_bootstrap_discard(
        BootstrapOperation::MemberAddition,
        DiscardAuthority::UnacceptedRetirement,
    )
    .await;
}
