//! Device admission through the public four-transfer join client.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::clock::SystemClock;
use crate::database::DbError;
use crate::encryption::EncryptionService;
use crate::join_code::encode;
use crate::keys::UserKeypair;
use crate::storage::cloud::{no_progress, BlobBody, CloudHomeJoinInfo, ExactSlotStorage};
use crate::sync::hlc::Hlc;
use crate::sync::snapshot::create_snapshot;
use crate::sync::test_helpers::*;
use async_trait::async_trait;

/// A cancel receiver whose sender is dropped immediately: `borrow()` reads the
/// initial `false` forever, so the join/restore flows run to completion exactly
/// as an uncancelled caller's would.
fn never_cancelled() -> tokio::sync::watch::Receiver<bool> {
    tokio::sync::watch::channel(false).1
}
#[tokio::test]
async fn device_join_client_four_transfer_retries_and_process_restarts_preserve_exact_state() {
    tokio::spawn(run_device_join_client_four_transfer_retries_and_process_restarts())
        .await
        .expect("device join state-machine task");
}

async fn run_device_join_client_four_transfer_retries_and_process_restarts() {
    crate::keys::test_keyring::install();
    let store_id = "device-join-client-state-machine";
    let owner = UserKeypair::generate();
    let owner_db = open_test_db();
    let create_store_db = owner_db.clone();
    let create_store_owner = owner.clone();
    let store = tokio::spawn(async move {
        TestStore::create(&create_store_db, store_id, create_store_owner).await
    })
    .await
    .expect("join Owner Store creation task")
    .expect("create Owner Store");
    let join_request = crate::generate_join_request(None).expect("generate join request");
    let member_pubkey = crate::join_code::decode_join_request(&join_request)
        .expect("decode join request")
        .public_key;
    let invitation_home = GrantingCloudHome(store.home.as_ref().clone());
    let invite = crate::sync::membership_ops::invite_member(
        &store.storage,
        &invitation_home,
        &owner,
        &Hlc::new("owner-device".to_string()),
        &member_pubkey,
        None,
        crate::sync::membership::MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        store_id,
        "Device Join Client Store",
        &owner_db,
    )
    .await
    .expect("invite joiner identity");
    let membership = store
        .open_into(&owner_db)
        .await
        .expect("load membership including joiner");
    let tables = test_synced_tables();
    let snapshot_dir = tempfile::tempdir().expect("snapshot directory");
    let snapshot_path = snapshot_dir.path().to_path_buf();
    let snapshot_tables = tables.clone();
    let snapshot = owner_db
        .call(move |connection| {
            create_snapshot(connection, &snapshot_path, &snapshot_tables)
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await
        .expect("create join snapshot");
    let snapshot_coverage = crate::sync::store_commit::CommitFrontier(BTreeMap::new());
    crate::sync::test_helpers::publish_snapshot_fixture(
        &store.storage,
        &store.root,
        snapshot,
        snapshot_coverage.clone(),
        &owner,
        &membership,
        &owner_db,
    )
    .await
    .expect("publish join snapshot");
    publish_store_ack_fixture(&owner_db, &store.storage, snapshot_coverage, &owner)
        .await
        .expect("publish join snapshot acknowledgement");
    let authorization = membership;
    let invite_code = encode(&invite);
    let app = tempfile::tempdir().expect("join app directory");
    let layout = crate::store_dir::StoreLayout::new(app.path());
    let new_client = || {
        crate::DeviceJoinClient::new(
            &invite_code,
            &join_request,
            layout.clone(),
            tables.clone(),
            test_migrations(),
            None,
            crate::custody::KeyCustody::Keyring,
            crate::identity_custody::IdentityCustody::Keyring,
            None,
            None,
            Arc::new(SystemClock),
        )
        .expect("construct DeviceJoinClient")
        .with_test_bootstrap_home(store.home.clone())
    };
    let offer = Box::pin(crate::sync::device_join::begin_device_join(
        &owner_db,
        &store.storage,
        &authorization,
        &owner,
        &member_pubkey,
        store
            .protocol_root
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone(),
    ))
    .await
    .expect("begin join");

    let access_request = new_client()
        .prepare_provider_access_request(offer.clone())
        .await
        .expect("prepare provider access request");
    assert_eq!(
        new_client()
            .prepare_provider_access_request(offer)
            .await
            .expect("retry provider access request"),
        access_request,
    );
    assert_eq!(
        new_client()
            .resume_device_joins()
            .expect("enumerate pending join actions"),
        vec![crate::DeviceJoinAction::TransferProviderAccessRequest(
            access_request.clone(),
        )],
    );
    let approval = Box::pin(crate::sync::device_join::authorize_device_provider_access(
        &owner_db,
        &store.storage,
        Some(store.home.as_ref()),
        None,
        &authorization,
        &owner,
        access_request,
    ))
    .await
    .expect("authorize provider access");
    let registration_request = new_client()
        .prepare_registration_request(approval.clone())
        .await
        .expect("prepare registration request");
    assert_eq!(
        new_client()
            .prepare_registration_request(approval)
            .await
            .expect("retry registration request after process restart"),
        registration_request,
    );
    let provisional = Box::pin(
        crate::sync::device_join::accept_device_registration_request(
            &owner_db,
            &store.storage,
            &authorization,
            &owner,
            registration_request,
        ),
    )
    .await
    .expect("accept registration request");
    let provider_ready = Box::pin(crate::sync::device_join::publish_device_provider_challenge(
        &owner_db,
        &store.storage,
        Some(store.home.as_ref()),
        provisional,
    ))
    .await
    .expect("publish provider challenge");
    let readiness = Box::pin(new_client().bootstrap_pending_device(
        provider_ready.clone(),
        |_status| {},
        &never_cancelled(),
    ))
    .await
    .expect("bootstrap pending device");
    let readiness_retry = Box::pin(new_client().bootstrap_pending_device(
        provider_ready,
        |_status| {},
        &never_cancelled(),
    ))
    .await
    .expect("resume bootstrap after lost response");
    assert_eq!(readiness_retry, readiness);
    let completion = Box::pin(
        crate::sync::device_join::complete_device_provider_admission(
            &owner_db,
            Some(store.home.as_ref()),
            &owner,
            readiness,
        ),
    )
    .await
    .expect("complete provider admission");
    store.home.fail_exact_create_before_call(1);
    let interrupted = Box::pin(crate::sync::device_join::finalize_device_join(
        &owner_db,
        &store.storage,
        &authorization,
        &owner,
        completion.clone(),
    ))
    .await;
    assert!(
        interrupted.is_err(),
        "the outcome create interruption surfaces"
    );
    assert!(matches!(
        crate::sync::device_join::load_store_device_join_status(
            &owner_db,
            completion.readiness.proof.attempt.attempt_id,
            crate::DeviceJoinRole::Owner,
        )
        .await
        .expect("load interrupted owner finalization"),
        Some(crate::DeviceJoinStatus::AwaitingActivation { completion: durable })
            if durable == completion
    ));
    assert!(
        crate::sync::device_join::load_store_device_join_actions(&owner_db)
            .await
            .expect("enumerate Store join actions")
            .contains(&crate::DeviceJoinAction::ResumeOperation {
                attempt_id: completion.readiness.proof.attempt.attempt_id,
                role: crate::DeviceJoinRole::Owner,
            })
    );
    let activation = Box::pin(crate::sync::device_join::finalize_device_join(
        &owner_db,
        &store.storage,
        &authorization,
        &owner,
        completion,
    ))
    .await
    .expect("resume finalization from the durable completion");
    let activation_slot = activation.outcome_activation.object.slot();
    let activation_bytes = store
        .home
        .read_at(activation_slot)
        .await
        .expect("activation commit is stored");
    store
        .home
        .delete_at(activation_slot)
        .await
        .expect("remove activation commit");
    let interrupted_completion = new_client()
        .complete_device_join(activation.clone(), |_status| {})
        .await;
    assert!(
        interrupted_completion.is_err(),
        "a missing activation commit blocks completion"
    );
    assert!(matches!(
        new_client()
            .device_join_status(activation.outcome.attempt().attempt_id)
            .expect("load interrupted joiner completion"),
        Some(crate::DeviceJoinStatus::AwaitingCompletion { activation: durable })
            if durable == activation
    ));
    assert_eq!(
        new_client()
            .resume_device_joins()
            .expect("enumerate interrupted completion"),
        vec![crate::DeviceJoinAction::CompleteJoin(activation.clone())],
    );
    store
        .home
        .create_at(
            activation_slot,
            BlobBody::from_bytes(activation_bytes),
            &no_progress(),
        )
        .await
        .expect("restore activation commit after interruption");
    let config = new_client()
        .complete_device_join(activation.clone(), |_status| {})
        .await
        .expect("complete join after process restart");
    let retry = new_client()
        .complete_device_join(activation, |_status| {})
        .await
        .expect("retry completed join after lost response");
    assert_eq!(retry.device_id, config.device_id);
    assert!(config.store_dir.config_path().exists());
    assert!(new_client()
        .resume_device_joins()
        .expect("enumerate completed joins")
        .is_empty());
}

struct GrantingCloudHome(crate::InMemoryCloudHome);

#[async_trait]
impl crate::storage::cloud::CloudHome for GrantingCloudHome {
    async fn put_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), crate::storage::cloud::CloudHomeError> {
        self.0.put_object(key, data).await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'a>, crate::storage::cloud::CloudHomeError> {
        self.0.open_multipart(key, total_len).await
    }

    fn multipart_threshold(&self) -> u64 {
        self.0.multipart_threshold()
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, crate::storage::cloud::CloudHomeError> {
        self.0.read(key).await
    }

    async fn read_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, crate::storage::cloud::CloudHomeError> {
        self.0.read_range(key, start, end).await
    }

    async fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, crate::storage::cloud::CloudHomeError> {
        self.0.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), crate::storage::cloud::CloudHomeError> {
        self.0.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, crate::storage::cloud::CloudHomeError> {
        self.0.exists(key).await
    }

    async fn set_access(
        &self,
        desired: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, crate::storage::cloud::CloudHomeError>
    {
        match desired {
            crate::storage::cloud::CloudAccessState::Present { .. } => Ok(
                crate::storage::cloud::CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                    bucket: "test-bucket".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint: None,
                    access_key: "test-access-key".to_string(),
                    secret_key: "test-secret-key".to_string(),
                    key_prefix: None,
                }),
            ),
            absent => self.0.set_access(absent).await,
        }
    }
}
