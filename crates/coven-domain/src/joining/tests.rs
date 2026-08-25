//! Device admission through the public four-transfer join client.

use std::sync::Arc;

use coven_foundation::clock::SystemClock;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_replication::sync::test_helpers::*;
use coven_storage::cloud::{no_progress, ExactSlotStorage, ExactUpload};

/// A cancel receiver whose sender is dropped immediately: `borrow()` reads the
/// initial `false` forever, so the join/restore flows run to completion exactly
/// as an uncancelled caller's would.
fn never_cancelled() -> tokio::sync::watch::Receiver<bool> {
    tokio::sync::watch::channel(false).1
}

fn no_join_progress() -> coven_replication::sync::JoiningDeviceJoinProgressObserver {
    Arc::new(|_| {})
}
#[tokio::test]
async fn device_join_client_four_transfer_retries_and_process_restarts_preserve_exact_state() {
    tokio::spawn(run_device_join_client_four_transfer_retries_and_process_restarts())
        .await
        .expect("device join state-machine task");
}

async fn run_device_join_client_four_transfer_retries_and_process_restarts() {
    coven_keys::keys::test_keyring::install();
    let store_id = "device-join-client-state-machine";
    let owner = UserKeypair::generate();
    let owner_db_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let owner_db = open_test_db(owner_db_store_dir.clone());
    let owner_database = coven_database::StoreDatabase::from_database(owner_db.clone());
    let namespace_id = "device-join-client-shared-namespace";
    let home = test_cloud_home_with_binding(coven_protocol::ResolvedProviderBinding {
        store: coven_protocol::StoreProviderBinding::Dropbox {
            namespace_id: namespace_id.to_string(),
        },
        device: coven_protocol::ProviderDeviceBinding {
            principal: coven_protocol::ProviderPrincipalId::Dropbox {
                account_id: "owner-provider-account".to_string(),
            },
        },
    });
    let create_store_db = owner_db.clone();
    let create_store_db_store_dir = owner_db_store_dir.clone();
    let create_store_owner = owner.clone();
    let create_store_home = home.clone();
    let store = tokio::spawn(async move {
        TestStore::create(
            &create_store_db,
            create_store_db_store_dir,
            store_id,
            create_store_owner,
            create_store_home,
        )
        .await
    })
    .await
    .expect("join Owner Store creation task")
    .expect("create Owner Store");
    let joining_identity =
        coven_keys::keys::mint_pending_identity().expect("mint pending joining identity");
    let member_pubkey = coven_keys::keys::public_key_hex(&joining_identity);
    let joiner_home = Arc::new(home.as_ref().clone().with_provider_binding(
        coven_protocol::ResolvedProviderBinding {
            store: coven_protocol::StoreProviderBinding::Dropbox {
                namespace_id: namespace_id.to_string(),
            },
            device: coven_protocol::ProviderDeviceBinding {
                principal: coven_protocol::ProviderPrincipalId::Dropbox {
                    account_id: "joining-provider-account".to_string(),
                },
            },
        },
    ));
    let access_administrator = TestDropboxAccessAdministrator {
        namespace_id: namespace_id.to_string(),
    };
    let admission = store
        .admit_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &member_pubkey,
            None,
            coven_protocol::membership::MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Device Join Client Store",
        )
        .await
        .expect("admit joining identity");
    let owner_device = store
        .open_into(&owner_db, owner_db_store_dir.clone())
        .await
        .expect("load membership including joiner");
    let tables = test_synced_tables();
    let snapshot_dir = tempfile::tempdir().expect("snapshot directory");
    crate::test_snapshots::publish_owner_snapshot(
        &owner_device,
        &owner_database,
        store.root().clone(),
        snapshot_dir.path(),
    )
    .await;
    let owner_store = owner_device;
    let app = tempfile::tempdir().expect("join app directory");
    let layout = coven_foundation::store_dir::StoreLayout::new(app.path());
    let new_client = || {
        crate::joining::client::DeviceJoinClient::new(
            admission.clone(),
            member_pubkey.clone(),
            layout.clone(),
            tables.clone(),
            test_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            coven_keys::custody::KeyCustody::Keyring,
            coven_keys::identity_custody::IdentityCustody::Keyring,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            None,
            Arc::new(SystemClock),
        )
        .expect("construct DeviceJoinClient")
        .with_test_bootstrap_home(joiner_home.clone())
    };
    let offer = owner_store
        .begin_device_join(&member_pubkey)
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
        vec![
            coven_replication::sync::DeviceJoinAction::TransferProviderAccessRequest(
                access_request.clone(),
            )
        ],
    );
    let approval = owner_store
        .authorize_device_provider_access(access_request, Some(&access_administrator))
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
    let provisional = owner_store
        .accept_device_registration_request(registration_request)
        .await
        .expect("accept registration request");
    let provider_ready = owner_store
        .publish_device_provider_challenge(provisional)
        .await
        .expect("publish provider challenge");
    let progress = no_join_progress();
    let readiness = Box::pin(new_client().bootstrap_pending_device(
        provider_ready.clone(),
        &progress,
        &never_cancelled(),
    ))
    .await
    .expect("bootstrap pending device");
    let readiness_retry = Box::pin(new_client().bootstrap_pending_device(
        provider_ready,
        &progress,
        &never_cancelled(),
    ))
    .await
    .expect("resume bootstrap after lost response");
    assert_eq!(readiness_retry, readiness);
    let completion = owner_store
        .complete_device_provider_admission(readiness)
        .await
        .expect("complete provider admission");
    home.fail_exact_create_before_call(1);
    let interrupted = owner_store.finalize_device_join(completion.clone()).await;
    assert!(
        interrupted.is_err(),
        "the outcome create interruption surfaces"
    );
    assert!(matches!(
        owner_database
            .device_join_status(
                completion.attempt_id(),
                coven_replication::sync::DeviceJoinRole::Owner,
            )
        .await
        .expect("load interrupted owner finalization"),
        Some(coven_replication::sync::DeviceJoinStatus::AwaitingActivation { completion: durable })
            if durable == completion
    ));
    assert!(owner_database
        .device_join_actions()
        .await
        .expect("enumerate Store join actions")
        .contains(
            &coven_replication::sync::DeviceJoinAction::ResumeOperation {
                attempt_id: completion.attempt_id(),
                role: coven_replication::sync::DeviceJoinRole::Owner,
            }
        ));
    let activation = owner_store
        .finalize_device_join(completion)
        .await
        .expect("resume finalization from the durable completion");
    let activation_slot = activation.outcome_activation.object.slot();
    let activation_bytes = home
        .read_at(activation_slot)
        .await
        .expect("activation commit is stored");
    home.delete_at(activation_slot)
        .await
        .expect("remove activation commit");
    let interrupted_completion = new_client()
        .complete_device_join(activation.clone(), &progress)
        .await;
    assert!(
        interrupted_completion.is_err(),
        "a missing activation commit blocks completion"
    );
    assert!(matches!(
        new_client()
            .device_join_status(activation.attempt_id)
            .expect("load interrupted joiner completion"),
        Some(coven_replication::sync::DeviceJoinStatus::AwaitingCompletion { activation: durable })
            if durable == activation
    ));
    assert_eq!(
        new_client()
            .resume_device_joins()
            .expect("enumerate interrupted completion"),
        vec![coven_replication::sync::DeviceJoinAction::CompleteJoin(
            activation.clone()
        )],
    );
    let activation_object = coven_protocol::objects::ExactObjectRef::new(
        activation_slot.clone(),
        activation_bytes.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(&activation_bytes),
    );
    let activation_upload = ExactUpload::from_bytes(&activation_object, &activation_bytes)
        .expect("activation bytes match their exact reference");
    home.create_at(&activation_upload, &no_progress())
        .await
        .expect("restore activation commit after interruption");
    let config = new_client()
        .complete_device_join(activation.clone(), &progress)
        .await
        .expect("complete join after process restart");
    let retry = new_client()
        .complete_device_join(activation, &progress)
        .await
        .expect("retry completed join after lost response");
    assert_eq!(retry.device_id, config.device_id);
    assert!(layout.store_dir(store_id).config_path().exists());
    assert!(new_client()
        .resume_device_joins()
        .expect("enumerate completed joins")
        .is_empty());
}
