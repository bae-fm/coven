use super::{decode_membership_mutation, MembershipMutationPlan};
use crate::sync::test_helpers::{
    copy_payload_files, open_test_db, test_cloud_home, test_migrations, test_store_dir,
    test_synced_tables, InterceptedStorage, ProtocolRead, StorageInterceptor, TestCustody,
    TestStore,
};
use coven_database::{Database, StoreDatabase};
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::{self, UserKeypair};
use coven_protocol::membership::{MemberRole, StoreAuthorityChange};
use coven_protocol::objects::{ExactObjectRef, PreparedExactObject, StorageError};
use coven_protocol::remote_object::ClosedRemoteObject;
use coven_protocol::store_commit::StorePublicationPayload;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug)]
enum AuthorityUploadFailure {
    Missing,
    Duplicate,
    Corrupt,
    Create,
    Readback,
}

struct FailAuthorityUpload {
    failure: AuthorityUploadFailure,
    target: OnceLock<ExactObjectRef>,
    armed: AtomicBool,
    reached: AtomicBool,
}

#[async_trait::async_trait]
impl StorageInterceptor for FailAuthorityUpload {
    async fn before_protocol_create(
        &self,
        prepared: &PreparedExactObject,
    ) -> Result<(), StorageError> {
        if self.armed.load(Ordering::SeqCst)
            && matches!(self.failure, AuthorityUploadFailure::Create)
            && prepared.reference()
                == self
                    .target
                    .get()
                    .expect("armed interceptor has an exact target")
        {
            self.reached.store(true, Ordering::SeqCst);
            return Err(StorageError::Storage(
                "interrupted membership authority create".into(),
            ));
        }
        Ok(())
    }

    async fn before_protocol_read(
        &self,
        _read: ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), StorageError> {
        if self.armed.load(Ordering::SeqCst)
            && matches!(self.failure, AuthorityUploadFailure::Readback)
            && self
                .target
                .get()
                .expect("armed interceptor has an exact target")
                .slot()
                .logical_key()
                == format!("{semantic_prefix}.json")
        {
            self.reached.store(true, Ordering::SeqCst);
            return Err(StorageError::Storage(
                "interrupted membership authority readback".into(),
            ));
        }
        Ok(())
    }
}

#[tokio::test]
async fn authority_payload_refusal_preserves_the_unpublished_membership_request() {
    for failure in [
        AuthorityUploadFailure::Missing,
        AuthorityUploadFailure::Duplicate,
        AuthorityUploadFailure::Corrupt,
    ] {
        interrupted_authority_upload(failure).await;
    }
}

#[tokio::test]
async fn authority_upload_interruption_retains_verified_progress_across_reopen() {
    for failure in [
        AuthorityUploadFailure::Create,
        AuthorityUploadFailure::Readback,
    ] {
        interrupted_authority_upload(failure).await;
    }
}

async fn interrupted_authority_upload(failure: AuthorityUploadFailure) {
    let directory = test_store_dir();
    let database = open_test_db(directory.clone());
    let owner = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "membership-authority-upload",
        owner.clone(),
        home.clone(),
    )
    .await
    .expect("create the actual Store");
    let encryption = EncryptionService::from_key([42; 32]);
    let removed = keys::public_key_hex(&UserKeypair::generate());
    let retained = keys::public_key_hex(&UserKeypair::generate());
    for member in [&removed, &retained] {
        store
            .admit_member(
                &database,
                directory.clone(),
                &owner,
                member,
                None,
                MemberRole::Member,
                &encryption,
                "Authority upload",
            )
            .await
            .expect("admit the removal fixture's members");
    }
    let durable = StoreDatabase::new(&database);
    let interceptor = Arc::new(FailAuthorityUpload {
        failure,
        target: OnceLock::new(),
        armed: AtomicBool::new(false),
        reached: AtomicBool::new(false),
    });
    let intercepted = Arc::new(InterceptedStorage::new(
        storage.clone(),
        interceptor.clone(),
    ));
    let operation = store
        .open_store_with_storage(durable.clone(), intercepted, directory.clone(), &owner)
        .await
        .expect("open the actual operation before staging its rotation");
    let mut writer = operation
        .authorize_writer()
        .await
        .expect("retain the writer that will upload the removal authority");
    let custody = TestCustody::default();
    home.fail_exact_create_before_call(1);
    let error = writer
        .remove_member(
            &removed,
            &encryption,
            &custody,
            storage.as_ref(),
            storage.as_ref(),
        )
        .await
        .expect_err("stage removal before its first authority upload");
    assert!(
        error
            .to_string()
            .contains("forced failure before exact create call 1"),
        "{error}"
    );
    let row = durable
        .outbound_membership_mutation()
        .await
        .unwrap()
        .expect("real removal journal");
    let (MembershipMutationPlan::Revoke(plan), _) =
        decode_membership_mutation(row.clone()).unwrap()
    else {
        panic!("the staged request must be a removal");
    };
    let publication = plan.candidate.prepared_membership_publication().unwrap();
    let StoreAuthorityChange::RemoveMember { sealed_keys, .. } = &publication.entry.change else {
        panic!("the signed entry must remove the requested member");
    };
    assert_eq!(
        sealed_keys.len(),
        2,
        "the removal seals the rotated keyring to both surviving members"
    );
    let selected = publication.entry_ref.object.clone();
    let mut remotes = plan.candidate_remote_objects().unwrap();
    let index = remotes
        .iter()
        .position(|remote| remote.object() == &selected)
        .expect("candidate owns target");
    match failure {
        AuthorityUploadFailure::Missing => {
            remotes.remove(index);
        }
        AuthorityUploadFailure::Duplicate => remotes.push(remotes[index].clone()),
        AuthorityUploadFailure::Corrupt => {
            let remote = &remotes[index];
            let mut payloads = remote.payload_bytes().clone();
            payloads.insert(
                selected.stored_hash(),
                b"corrupt retained authority bytes".to_vec(),
            );
            remotes[index] = ClosedRemoteObject::with_payloads(remote.record().clone(), payloads)
                .expect("payload keys remain exact while stored bytes are corrupt");
        }
        AuthorityUploadFailure::Create | AuthorityUploadFailure::Readback => {}
    }
    interceptor
        .target
        .set(selected.clone())
        .expect("set the staged exact target once");
    let before = database.database_image_for_test().await.unwrap();
    let reservation = durable.active_store_publication().await.unwrap();
    let access = home.access_requests();
    home.clear_exact_creates();
    interceptor.armed.store(true, Ordering::SeqCst);
    let error = writer
        .publish_membership_authority(&plan.candidate, &remotes)
        .await
        .expect_err("the actual authority upload must report the selected failure");
    let network_failure = matches!(
        failure,
        AuthorityUploadFailure::Create | AuthorityUploadFailure::Readback
    );
    if network_failure {
        assert!(
            interceptor.reached.load(Ordering::SeqCst),
            "the selected provider boundary was reached: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("interrupted membership authority"),
            "{error}"
        );
    } else {
        assert!(
            home.exact_creates().is_empty(),
            "invalid later input cannot upload earlier authority"
        );
        assert_eq!(
            database.database_image_for_test().await.unwrap(),
            before,
            "preflight refusal leaves every durable owner unchanged"
        );
    }
    let objects = database.remote_objects_for_test().await.unwrap();
    let record = objects
        .iter()
        .find(|record| record.object() == &selected)
        .expect("staged exact record");
    assert!(
        !record.records_verified_upload(),
        "{failure:?}: the interrupted entry upload records no verified upload"
    );
    assert_eq!(
        home.stored_exact_bytes(selected.slot()).is_some(),
        matches!(failure, AuthorityUploadFailure::Readback),
        "{failure:?}: physical entry object"
    );
    let retained_row = durable
        .outbound_membership_mutation()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained_row.intent_hash, row.intent_hash);
    assert_eq!(retained_row.plan_bytes, row.plan_bytes);
    assert_eq!(retained_row.progress_bytes, row.progress_bytes);
    assert_eq!(
        durable.active_store_publication().await.unwrap(),
        reservation
    );
    assert_eq!(
        home.access_requests(),
        access,
        "failed authority cannot revoke provider access"
    );
    drop(writer);
    drop(operation);

    let reopened_directory = test_store_dir();
    database
        .vacuum_into_for_test(reopened_directory.db_path().to_string_lossy().into_owned())
        .await
        .unwrap();
    copy_payload_files(&directory, &reopened_directory);
    let reopened = Database::open_synthetic_for_test(
        &reopened_directory.db_path(),
        reopened_directory.clone(),
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".into(),
        Arc::new(coven_foundation::clock::SystemClock),
        &test_migrations(),
    )
    .expect("reopen the real journal and payload files");
    let reopened_store = StoreDatabase::new(&reopened);
    let resumed_row = reopened_store
        .outbound_membership_mutation()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed_row.plan_bytes, row.plan_bytes);
    assert_eq!(resumed_row.progress_bytes, row.progress_bytes);
    assert_eq!(reopened.remote_objects_for_test().await.unwrap(), objects);
    store
        .remove_member(
            &reopened,
            reopened_directory.clone(),
            &owner,
            &removed,
            &encryption,
            &custody,
        )
        .await
        .expect("retry the exact retained removal");
    let accepted = reopened_store.store_publication_entries().await.unwrap();
    assert_eq!(
        accepted
            .iter()
            .filter(|entry| entry.value.payload
                == StorePublicationPayload::Commit(plan.candidate.reference.clone()))
            .count(),
        1,
        "one exact candidate completes the retained logical request"
    );
    assert!(reopened_store
        .outbound_membership_mutation()
        .await
        .unwrap()
        .is_none());
    assert!(reopened_store
        .active_store_publication()
        .await
        .unwrap()
        .is_none());
    let device = store
        .bind_device_in(&reopened, reopened_directory, &owner)
        .await
        .unwrap();
    let membership = device.membership_for_test().await.unwrap();
    assert!(!membership.is_member_now(&removed));
    assert!(membership.is_member_now(&retained));
}
