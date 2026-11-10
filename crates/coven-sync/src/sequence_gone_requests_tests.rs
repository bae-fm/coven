use crate::sequence_sync::sequence_invites::tests::carol_for;
use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::{admin::StoreChange, device_gone::DeviceGone};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPath, ObjectPrefix, Storage, StorageFailure};

/// A joining device for `invite`'s code, with its request prepared and not
/// yet sent.
async fn s3_joiner(group: &Group, invite: &Invite, name: &str) -> JoiningDevice {
    let code = read_invite_code(invite.shown_code()).unwrap();
    let (temp, directory) = group.directory();
    let request = JoiningIdentity::prepare(
        &directory,
        &code,
        directory.settings().unwrap().device_id,
        name,
        carol_for(&code),
    )
    .unwrap();
    let device = group.open_with(temp, request.identity(), None, group.storage.for_device()).await;
    JoiningDevice { device, request }
}

/// What storage holds at `path`, if anything.
async fn stored(group: &Group, path: &ObjectPath) -> Option<Vec<u8>> {
    match group.storage.read(path).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.failure() == StorageFailure::NotFound => None,
        Err(error) => panic!("reading {}: {error}", path.as_str()),
    }
}

/// The paths under a device's join requests' folder.
async fn folder(group: &Group, device: DeviceId) -> Vec<ObjectPath> {
    let prefix = ObjectPrefix::device_join_requests(group.store, device);
    group.storage.list(&prefix).await.unwrap().into_iter().map(|object| object.path).collect()
}

/// Household is on S3. Ana's phone has invited Carol, whose request waits,
/// and Dan, who hasn't sent his yet; it ended Eve's invite, whose marker
/// holds her request's path. Ben removes Ana's phone. No pass of Ana's phone
/// will ever answer Carol, and on S3 nothing shared is taken back, so
/// Carol's request would wait forever. Ben's phone applies the removal: it
/// writes the phone's gone marker, then deletes everything else in its
/// requests' folder. Carol's phone reads her missing request as a decline,
/// and Dan's request, sent later, finds the marker and is never stored
/// (§13.2).
#[tokio::test]
async fn an_admin_ends_the_requests_of_a_device_gone_for_good() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let carols = s3_invite(&ana, "Carol").await;
    let dans = s3_invite(&ana, "Dan").await;
    let eves = s3_invite(&ana, "Eve").await;
    ana.sync.cancel_invite(eves.id).await.unwrap();
    let mut carol = s3_joiner(&group, &carols, "Carol's phone").await;
    assert!(carol.device.sync.submit_join(&carol.request).await.unwrap());
    let mut dan = s3_joiner(&group, &dans, "Dan's phone").await;
    let phone = ana.sync.device;
    assert_eq!(folder(&group, phone).await.len(), 2, "Carol's request and Eve's marker");

    ben.sync.pass().await.unwrap();
    ben.sync.administer(StoreChange::RemoveDevice { device: phone }).await.unwrap();
    ben.sync.pass().await.unwrap();
    let gone = ObjectPath::device_gone(group.store, phone);
    assert_eq!(folder(&group, phone).await, std::slice::from_ref(&gone));
    DeviceGone::decode(&stored(&group, &gone).await.unwrap()).unwrap();
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Declined
    );
    assert!(!dan.device.sync.submit_join(&dan.request).await.unwrap(), "the marker turns it away");
    assert_eq!(stored(&group, dan.request.path()).await, None);
    assert_eq!(
        dan.device.sync.join_outcome(&mut dan.request).await.unwrap(),
        JoinOutcome::Declined
    );
    // Ben's phone is done with Ana's: a later pass doesn't list the folder again.
    let prefix = ObjectPrefix::device_join_requests(group.store, phone);
    let listings = || {
        ben.storage
            .request_trace()
            .iter()
            .filter(|request| {
                matches!(request,
                coven_storage::test_utils::MemoryRequest::List(listed) if *listed == prefix)
            })
            .count()
    };
    assert_eq!(listings(), 1);
    ben.sync.pass().await.unwrap();
    assert_eq!(listings(), 1);
    for device in [&ana, &ben, &carol.device, &dan.device] {
        device.database().close().await.unwrap();
    }
}

/// Carol's request reaches storage after Ben's phone listed Ana's phone's
/// requests: listings lag (§20.4), or her phone checked for the gone
/// marker just before Ben's phone wrote it. No device will list that folder
/// again, so Carol's phone, finding the marker while her request waits,
/// deletes the request itself, and her join ends as declined (§13.2).
#[tokio::test]
async fn a_joiner_that_finds_the_gone_marker_deletes_its_waiting_request() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let carols = s3_invite(&ana, "Carol").await;
    let mut carol = s3_joiner(&group, &carols, "Carol's phone").await;
    assert!(carol.device.sync.submit_join(&carol.request).await.unwrap());
    let path = carol.request.path().clone();
    let request = stored(&group, &path).await.unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.administer(StoreChange::RemoveDevice { device: ana.sync.device }).await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(stored(&group, &path).await, None);
    // The request Ben's listing didn't show.
    group.storage.plant(&path, &request).await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(stored(&group, &path).await, Some(request));
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Declined
    );
    assert_eq!(stored(&group, &path).await, None);
    for device in [&ana, &ben, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// Ben, an admin, removes Ana's phone while Carol's request waits. Dan is a
/// member, not an admin: his phone applies the removal and leaves Ana's
/// phone's requests alone. Ana's laptop registers after the removal, so it
/// never applied it, and leaves them too. Ben's phone, an admin's that
/// applied it, ends them (§13.2).
#[tokio::test]
async fn only_an_admins_device_that_applied_the_stop_ends_the_requests() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let dan = group.device(4).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync
        .administer(admission(
            &dan,
            coven_format::admin::MemberRole::Member,
            access("dan"),
            "Dan's phone",
        ))
        .await
        .unwrap();
    dan.sync.pass().await.unwrap();
    let carols = s3_invite(&ana, "Carol").await;
    let carol = s3_joiner(&group, &carols, "Carol's phone").await;
    assert!(carol.device.sync.submit_join(&carol.request).await.unwrap());
    let phone = ana.sync.device;
    ben.sync.pass().await.unwrap();
    ben.sync.administer(StoreChange::RemoveDevice { device: phone }).await.unwrap();
    let laptop = group.laptop(&ana).await;
    let waiting = vec![carol.request.path().clone()];
    for device in [&dan, &laptop] {
        device.sync.pass().await.unwrap();
        assert_eq!(folder(&group, phone).await, waiting);
    }
    ben.sync.pass().await.unwrap();
    assert_eq!(folder(&group, phone).await, [ObjectPath::device_gone(group.store, phone)]);
    for device in [&ana, &ben, &dan, &laptop, &carol.device] {
        device.database().close().await.unwrap();
    }
}
