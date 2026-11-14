//! The device that shows a pairing's code: starting one, the status the
//! screen follows, and what approving, declining and closing it do (§13.2).

use super::*;
use coven_format::objects::{JoinRequest as AskRequest, Joiner as Asking};

/// The S3 key the pairing code carries, if it carries one.
fn carried_key(code: &str) -> Option<S3Credentials> {
    let code = coven_sync::read_pairing_code(code).unwrap();
    coven_storage::RestoreStorage::decode(code.storage.as_bytes()).unwrap().s3_key
}

fn same_key(actual: &S3Credentials, expected: &S3Credentials) {
    assert_eq!(actual.access_key_id, expected.access_key_id);
    assert_eq!(actual.secret_access_key.as_str(), expected.secret_access_key.as_str());
}

/// The new device writes its request to the pairing's one path, with fresh
/// keys, naming `device`.
async fn ask(owner: &Owner, pairing: &AddDevice, device: DeviceId) -> coven_crypto::MemberKeys {
    let code = coven_sync::read_pairing_code(&pairing.shared_code).unwrap();
    let keys = coven_crypto::MemberKeys::generate().unwrap();
    let request = AskRequest {
        invite: code.pairing,
        member: code.member,
        keys: keys.public_keys(),
        device,
        device_name: "Ana’s laptop".into(),
        joiner: Asking::OwnDevice,
    };
    let bytes =
        coven_sync::sealed_join_request(&code.secret, &code.request_path(), &request, &keys)
            .unwrap();
    owner.storage.create(&code.request_path(), &bytes).await.unwrap();
    keys
}

/// The request the pairing offers, once Ana's pass has read it.
async fn offered(owner: &Owner, pairing: &AddDevice) -> AddDeviceRequest {
    owner.pass_now().await;
    match owner.handle.add_device_status(pairing.id).await.unwrap() {
        Some(AddDeviceStatus::Waiting { request: Some(request) }) => request,
        status => panic!("a checked request was expected: {status:?}"),
    }
}

async fn status(owner: &Owner, pairing: &AddDevice) -> Option<AddDeviceStatus> {
    owner.handle.add_device_status(pairing.id).await.unwrap()
}

#[tokio::test]
async fn an_s3_pairings_qr_code_carries_the_key_this_device_uses_now() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let phone = Installation::new();
    let handle = phone.restore(&owner).await;
    let directory = phone.app.store_dir(&owner.directory.id());
    let pairing = handle.add_device().await.unwrap();
    same_key(&carried_key(&pairing.qr_code).unwrap(), &owner.s3_credentials().unwrap());
    // The code to share carries none, and the new device asks for it.
    assert!(carried_key(&pairing.shared_code).is_none());
    assert_ne!(pairing.qr_code, pairing.shared_code);
    let CodeInfo::Pairing(info) = decode_code_info(&pairing.qr_code).unwrap() else {
        panic!("a pairing code")
    };
    assert!(info.metadata.carries_s3_key);
    assert_eq!(info.expires_at, pairing.expires_at);
    let CodeInfo::Pairing(info) = decode_code_info(&pairing.shared_code).unwrap() else {
        panic!("a pairing code")
    };
    assert!(!info.metadata.carries_s3_key);
    // The key this device takes, and then another, is the key the next pairing carries.
    let replaced = S3Credentials {
        access_key_id: "replaced".into(),
        secret_access_key: SecretText::new("replaced-secret".into()),
    };
    handle
        .use_access_key(replaced.access_key_id.clone(), replaced.secret_access_key.clone())
        .await
        .unwrap();
    same_key(&carried_key(&handle.add_device().await.unwrap().qr_code).unwrap(), &replaced);
    let updated = S3Credentials {
        access_key_id: "updated".into(),
        secret_access_key: SecretText::new("updated-secret".into()),
    };
    handle
        .use_access_key(updated.access_key_id.clone(), updated.secret_access_key.clone())
        .await
        .unwrap();
    same_key(&carried_key(&handle.add_device().await.unwrap().qr_code).unwrap(), &updated);
    handle.close().await.unwrap();
    let handle = phone.handle(directory, &owner).await;
    same_key(&carried_key(&handle.add_device().await.unwrap().qr_code).unwrap(), &updated);
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn on_other_providers_the_two_codes_are_equal_and_carry_no_key() {
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive, CloudProvider::CloudKit] {
        let owner = Owner::new(provider, false).await;
        let pairing = owner.handle.add_device().await.unwrap();
        assert_eq!(pairing.qr_code, pairing.shared_code, "{provider:?}");
        assert!(carried_key(&pairing.qr_code).is_none(), "{provider:?}");
        let CodeInfo::Pairing(info) = decode_code_info(&pairing.qr_code).unwrap() else {
            panic!("a pairing code")
        };
        assert_eq!(info.metadata.cloud_provider, provider);
        assert!(!info.metadata.carries_s3_key);
        owner.close().await;
    }
}

#[tokio::test]
async fn a_member_who_is_no_admin_can_start_a_pairing() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let control = JoinControl::new();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, carol) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let carol_handle = result.unwrap().unwrap();
    let members = carol_handle.members().await.unwrap();
    let info = members.iter().find(|member| member.id == carol).unwrap();
    assert_eq!(info.role, MemberRole::Member);
    let pairing = carol_handle.add_device().await.unwrap();
    assert_eq!(coven_sync::read_pairing_code(&pairing.shared_code).unwrap().member, carol);
    carol_handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn a_removed_device_cannot_start_a_pairing() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let phone = Installation::new();
    let handle = phone.restore(&owner).await;
    handle.start_sync().await.unwrap();
    let (removal, _) = owner.handle.remove_device(handle.device_id()).await.unwrap();
    crate::tests::landed(&owner.handle, removal).await;
    handle.sync_now();
    tokio::time::timeout(
        coven_foundation::clock::HANG_GUARD,
        handle
            .test_sync_status()
            .wait_for(|status| matches!(status.state, SyncState::Removed { .. })),
    )
    .await
    .expect("the phone hears of its removal");
    let started = handle.add_device().await;
    assert!(
        matches!(
            started,
            Err(SyncError::Rejected(OperationRejection::NotAllowed)) | Err(SyncError::Removed(_))
        ),
        "{started:?}"
    );
    owner.close().await;
}

/// Approving a request the pairing doesn't show, or after expiry, fails; the
/// approved pairing can't be declined, and closing it still ends it.
#[tokio::test]
async fn approval_takes_the_request_shown_and_declining_an_approved_pairing_fails() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let pairing = owner.handle.add_device().await.unwrap();
    assert_eq!(status(&owner, &pairing).await, Some(AddDeviceStatus::Waiting { request: None }));
    let keys = ask(&owner, &pairing, DeviceId(70)).await;
    let request = offered(&owner, &pairing).await;
    assert_eq!(request.device_name, "Ana’s laptop");
    assert_eq!(
        request.confirmation,
        coven_crypto::ConfirmationNumber::of(
            coven_foundation::id_source::InviteId::from(pairing.id),
            &keys.public_keys()
        )
    );
    // A request the pairing doesn't hold is refused.
    let mut other = request.clone();
    other.pairing = AddDeviceId(uuid::Uuid::from_u128(1));
    assert!(matches!(
        owner.handle.approve_add_device(&other).await,
        Err(SyncError::InvitationChanged)
    ));
    owner.handle.approve_add_device(&request).await.unwrap();
    assert_eq!(
        status(&owner, &pairing).await,
        Some(AddDeviceStatus::Approved { request: request.clone() })
    );
    // Once approved, declining fails, and so does approving again.
    assert!(matches!(
        owner.handle.decline_add_device(&request).await,
        Err(SyncError::InvitationChanged)
    ));
    assert!(matches!(
        owner.handle.approve_add_device(&request).await,
        Err(SyncError::InvitationChanged)
    ));
    // A pass puts the handover in the request's place, which opens with the
    // request's key and holds this member's pair.
    owner.pass_now().await;
    let code = coven_sync::read_pairing_code(&pairing.shared_code).unwrap();
    let stored = owner.storage.read(&code.request_path()).await.unwrap();
    let (member, pair) = coven_format::handover::Handover::decode(&stored)
        .unwrap()
        .open(&code.request_path(), &keys)
        .unwrap();
    assert_eq!(member, owner.member.member());
    assert_eq!(pair.public_keys(), owner.member.keys().public_keys());
    // Closing the screen still ends it, and forgets it.
    owner.handle.end_add_device(pairing.id).await.unwrap();
    assert_eq!(status(&owner, &pairing).await, None);
    owner.pass_now().await;
    let marker = owner.storage.read(&code.request_path()).await.unwrap();
    assert!(coven_format::invite_end::InviteEnd::decode(&marker).is_ok());
    // Closing again, or a pairing never made, succeeds and changes nothing.
    owner.handle.end_add_device(pairing.id).await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn an_expired_pairing_refuses_approval_and_reports_how_it_ended() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let pairing = owner.handle.add_device().await.unwrap();
    ask(&owner, &pairing, DeviceId(70)).await;
    let request = offered(&owner, &pairing).await;
    owner.clock.set(pairing.expires_at);
    assert!(matches!(
        owner.handle.approve_add_device(&request).await,
        Err(SyncError::InvitationChanged)
    ));
    assert!(matches!(
        owner.handle.decline_add_device(&request).await,
        Err(SyncError::InvitationChanged)
    ));
    owner.pass_now().await;
    assert_eq!(status(&owner, &pairing).await, Some(AddDeviceStatus::Ended(AddDeviceEnd::Expired)));
    owner.close().await;
}

/// The status the screen follows moves forward only: waiting, with the
/// request, approved, ended and then forgotten.
#[tokio::test]
async fn the_live_status_moves_forward_and_ends_forgotten() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let pairing = owner.handle.add_device().await.unwrap();
    let mut live =
        coven_database::test_utils::Followed::new(owner.handle.follow_add_device(pairing.id));
    live.wait_for(|status| matches!(status, Some(AddDeviceStatus::Waiting { request: None })))
        .await;
    ask(&owner, &pairing, DeviceId(70)).await;
    let request = offered(&owner, &pairing).await;
    live.wait_for(|status| matches!(status, Some(AddDeviceStatus::Waiting { request: Some(_) })))
        .await;
    owner.handle.decline_add_device(&request).await.unwrap();
    live.wait_for(|status| matches!(status, Some(AddDeviceStatus::Ended(AddDeviceEnd::Declined))))
        .await;
    owner.pass_now().await;
    owner.handle.end_add_device(pairing.id).await.unwrap();
    live.wait_for(|status| status.is_none()).await;
    owner.close().await;
}
