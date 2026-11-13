use crate::sequence_sync::tests::*;
use crate::*;
use coven_database::operation_kind;
use coven_database::DbError;
use coven_format::{admin::MemberRole, MemberAccess};
use coven_foundation::clock::Clock;
use coven_foundation::files::StoreDirAccess;
use coven_storage::{test_utils::MemoryStorage, InviteStorage, ObjectPath, Storage};

/// The checked requests among the device's open invites.
pub(super) async fn requests(device: &Device) -> Vec<JoinRequest> {
    listed(device).await.into_iter().filter_map(|invite| invite.shown_request().cloned()).collect()
}

pub(crate) struct JoiningDevice {
    pub(crate) device: Device,
    pub(crate) request: JoiningIdentity,
}

/// Who joins with `code`: Carol, typing her name, when it adds a new person;
/// a recovery otherwise.
pub(crate) fn carol_for(code: &coven_format::codes::InviteCode) -> Joiner {
    match code.kind {
        InviteKind::NewPerson => Joiner::NewPerson { name: "Carol".into() },
        InviteKind::Recovery => Joiner::Recovery,
    }
}

/// The provider's invitation an account invite's `code` holds.
pub(crate) fn accepted_invitation(
    code: &coven_format::codes::InviteCode,
) -> coven_storage::StorageInvitation {
    let InviteStorage::Account(invitation) =
        InviteStorage::decode(code.storage.as_bytes()).unwrap()
    else {
        panic!("an account invite");
    };
    invitation
}

/// A joining device for an account invite's `code`: its request prepared
/// and the provider's share accepted, the request not yet sent.
pub(crate) async fn joiner(group: &Group, code: &str, name: &str) -> JoiningDevice {
    let code = read_invite_code(code).unwrap();
    let invitation = accepted_invitation(&code);
    let (temp, directory) = group.directory();
    let request = JoiningIdentity::prepare(
        &directory,
        &code,
        directory.settings().unwrap().device_id,
        name,
        carol_for(&code),
    )
    .unwrap();
    let storage = MemoryStorage::for_recipient(&group.storage, "carol@example.org").unwrap();
    let device = group.open_with(temp, request.identity(), None, storage).await;
    device.sync.accept_join(&invitation).await.unwrap();
    JoiningDevice { device, request }
}

/// A joining device that has sent its request to an open invite.
pub(crate) async fn join(group: &Group, code: &str, name: &str) -> JoiningDevice {
    let joiner = joiner(group, code, name).await;
    assert!(joiner.device.sync.submit_join(&joiner.request).await.unwrap(), "the invite is open");
    joiner
}

/// Where `invite`'s join request goes: under the device that made it, as its
/// code names it (§13.2).
pub(crate) fn request_path(invite: &Invite) -> ObjectPath {
    read_invite_code(invite.shown_code()).unwrap().request_path()
}

/// Whether no join request is at `path`: nothing, or the marker an ended
/// invite leaves in the request's place (§13.2).
pub(crate) async fn no_request(storage: &MemoryStorage, path: &ObjectPath) -> bool {
    match storage.read(path).await {
        Ok(found) => coven_format::invite_end::InviteEnd::decode(&found).is_ok(),
        Err(error) if error.failure() == coven_storage::StorageFailure::NotFound => true,
        Err(error) => panic!("reading the request: {error}"),
    }
}

/// The device's open invites, as `coven_invites` lists them.
pub(crate) async fn listed(device: &Device) -> Vec<Invite> {
    device.sync.database.read(Invite::list).await.unwrap()
}

/// Ana's invite for Carol, a new member, through Carol's provider account,
/// spelled as a person may type it: the invite keeps it normalized.
pub(crate) async fn carol_invite(ana: &Device, lifetime: InviteLifetime) -> Invite {
    ana.sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member },
            InviteAccess::ProviderAccount { email: "Carol@example.org".into() },
            lifetime,
        )
        .await
        .unwrap()
}

/// Ana's invite for Carol, open for a day (`carol_invite`).
pub(crate) async fn invite(ana: &Device) -> Invite {
    carol_invite(ana, InviteLifetime::SevenDays).await
}

/// Ana's invite for `member`'s replacement pair, through Carol's
/// provider account.
pub(crate) async fn key_replacement_invite(
    ana: &Device,
    member: coven_foundation::id_source::MemberId,
) -> Invite {
    ana.sync
        .create_invite(
            InviteTarget::ReplaceMemberKeys { member },
            InviteAccess::ProviderAccount { email: "carol@example.org".into() },
            InviteLifetime::SevenDays,
        )
        .await
        .unwrap()
}

/// Ana's invite for `name`, a new member, through an S3 access key
/// named for them.
pub(crate) async fn s3_invite(ana: &Device, name: &str) -> Invite {
    ana.sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member },
            InviteAccess::S3AccessKey {
                access_key_id: format!("{}-key", name.to_lowercase()),
                secret_access_key: coven_crypto::SecretText::new("secret".into()),
            },
            InviteLifetime::SevenDays,
        )
        .await
        .unwrap()
}

/// Cancelling an invite whose request waits deletes the request, which
/// Carol's phone sees as a decline (§13.2).
#[tokio::test]
async fn cancelling_an_invite_with_a_waiting_request_ends_the_join_as_declined() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let mut carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(requests(&ana).await.len(), 1);
    ana.sync.cancel_invite(invite.id).await.unwrap();
    assert!(listed(&ana).await.is_empty());
    assert!(no_request(&group.storage, &request_path(&invite)).await);
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Declined
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana declines Carol's request in her app's write. The write ends the
/// invite, so it leaves her list at once, with no storage work: the request
/// stays, and Carol's phone still waits, until the pass the commit asks for
/// deletes it (§13.2).
#[tokio::test]
async fn a_decline_ends_the_invite_in_its_write_and_a_pass_deletes_the_request() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let mut carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let request = requests(&ana).await.remove(0);
    let steps = ana.sync.operation_steps();
    ana.sync
        .database
        .write_as(move |sql| steps.decline_join_request(&sql, &request))
        .await
        .unwrap();
    assert!(listed(&ana).await.is_empty());
    let path = request_path(&invite);
    assert!(group.storage.status(&path).await.unwrap().is_some());
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Waiting
    );
    ana.sync.pass().await.unwrap();
    assert!(no_request(&group.storage, &path).await);
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Declined
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_declined_request_takes_back_the_share_and_the_joiner_sees_the_decline() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let mut carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    ana.sync.decline_join_request(&checked[0]).await.unwrap();
    ana.sync.pass().await.unwrap();
    let access =
        ana.sync.database.provider_access("carol@example.org".into()).await.unwrap().unwrap();
    assert!(!access.intended);
    assert_eq!(access.granted, Some(false));
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    // The revoked share makes the request unreadable, which is not absence:
    // the provider's permission failure reaches the app (§13.2).
    let outcome = carol.device.sync.join_outcome(&mut carol.request).await;
    assert!(
        matches!(&outcome, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::PermissionDenied),
        "{outcome:?}"
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// The lifetime fixes the expiry when the invite is made, and the code
/// carries the person's name to the joining device.
#[tokio::test]
async fn each_lifetime_expires_on_the_inviting_device() {
    use std::time::Duration;
    for (lifetime, seconds) in
        [(InviteLifetime::SevenDays, 604_800), (InviteLifetime::ThirtyDays, 2_592_000)]
    {
        let group = Group::new();
        let ana = group.device(1).await;
        group.create(&ana).await;
        let made = coven_foundation::clock::Clock::now(group.clock.as_ref());
        let invite = ana
            .sync
            .create_invite(
                InviteTarget::NewMember { role: MemberRole::Member },
                InviteAccess::S3AccessKey {
                    access_key_id: "dan-key".into(),
                    secret_access_key: coven_crypto::SecretText::new("dan-secret".into()),
                },
                lifetime,
            )
            .await
            .unwrap();
        assert_eq!(invite.expires_at, made + Duration::from_secs(seconds));
        assert_eq!(invite.kind, InviteKind::NewPerson);
        let CodeInfo::Invite(InviteInfo { kind, inviter, .. }) =
            decode_code_info(invite.shown_code()).unwrap()
        else {
            panic!("an invite code");
        };
        assert_eq!(kind, InviteKind::NewPerson);
        // The code names whom the joining person tells and waits for.
        assert_eq!(inviter, "Ana");
        group.clock.set(invite.expires_at - Duration::from_secs(1));
        ana.sync.pass().await.unwrap();
        let waiting = |operations: Vec<coven_database::OperationRecord>| {
            operations
                .iter()
                .filter(|record| !crate::sequence_sync::sequence_retention::is_live_body(record))
                .count()
        };
        assert_eq!(waiting(ana.database().operations().await.unwrap()), 1);
        group.clock.set(invite.expires_at);
        ana.sync.pass().await.unwrap();
        assert_eq!(waiting(ana.database().operations().await.unwrap()), 0);
        ana.database().close().await.unwrap();
    }
}

/// Opening ends each invite whose lifetime ended while the store was
/// closed, before any app reads the invites, and leaves one still open
/// (§13.2).
#[tokio::test]
async fn opening_writes_the_expiry_of_an_invite_that_ended_while_closed() {
    use std::time::Duration;
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let opened = |at| InviteExpiry::opened(&ana.sync.database, at);
    opened(invite.expires_at - Duration::from_secs(1)).await.unwrap();
    assert_eq!(listed(&ana).await, std::slice::from_ref(&invite));
    opened(invite.expires_at).await.unwrap();
    assert!(listed(&ana).await.is_empty());
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn an_invite_expires_seven_days_later_on_the_inviting_device() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    invite(&ana).await;
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(604_800));
    ana.sync.pass().await.unwrap();
    // The pass deletes the request, and the next takes back the share.
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    let access =
        ana.sync.database.provider_access("carol@example.org".into()).await.unwrap().unwrap();
    assert_eq!((access.intended, access.granted), (false, Some(false)));
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn a_replacement_invite_rebinds_an_existing_member_on_a_new_device() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let carol_id = coven_crypto::MemberIdentity::new(
        coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(91)),
        coven_format::test_utils::keys_from_seed(3),
    );
    ana.sync
        .administer(coven_format::admin::StoreChange::AdmitMember {
            member: carol_id.member(),
            keys: carol_id.keys().public_keys(),
            role: MemberRole::Member,
            access: MemberAccess::ProviderAccount("carol@example.org".into()),
            device: coven_foundation::id_source::DeviceId(81),
            device_name: "Carol's old phone".into(),
        })
        .await
        .unwrap();
    let invite = key_replacement_invite(&ana, carol_id.member()).await;
    assert_eq!(invite.member, carol_id.member());
    let mut carol = join(&group, invite.shown_code(), "Carol's new phone").await;
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    ana.sync.approve_join_request(&checked[0]).await.unwrap();
    let state = ana.sync.database.store_state().await.unwrap();
    assert_eq!(
        state.members[&carol_id.member()].keys,
        carol.request.identity().keys().public_keys()
    );
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    carol.device.sync.pass().await.unwrap();
    let state = carol.device.sync.database.store_state().await.unwrap();
    assert_eq!(state.devices[&carol.device.sync.device].member, carol_id.member());
    assert!(
        ana.sync
            .database
            .provider_access("carol@example.org".into())
            .await
            .unwrap()
            .unwrap()
            .intended
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_explicit_retry_reads_a_request_before_its_backoff_is_due() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    ana.sync.pass().await.unwrap();
    let operation = ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "invite")
        .unwrap()
        .id;
    let pending =
        ana.sync.database.operation_pending::<PendingRecord>(operation).await.unwrap().unwrap();
    assert!(matches!(pending.reason, PendingReason::Missing { .. }));
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.storage.request_count() - count, 2, "the request poll backs off");
    assert!(requests(&ana).await.is_empty());
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert_eq!(requests(&ana).await.len(), 1);
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana invites the account that holds the store. The provider refuses the
/// grant, which is the invite's pending record under its operation, beside
/// the invite listed without a code; cancelling it ends the invite and takes
/// back the account's intention (§13.2, §14.2).
#[tokio::test]
async fn a_refused_grant_is_the_invites_pending_record_until_cancelled() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let result = ana
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member },
            InviteAccess::ProviderAccount { email: "owner@example.test".into() },
            crate::InviteLifetime::SevenDays,
        )
        .await;
    let Err(SyncError::Pending(pending)) = result else { panic!("{:?}", result.err()) };
    assert!(
        matches!(&pending.reason, PendingReason::Storage { failure, .. } if *failure == coven_storage::StorageFailure::InviteeIsStoreOwner),
        "{pending:?}"
    );
    let open = listed(&ana).await;
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].stage, InviteStage::Granting);
    assert!(
        matches!(pending.subject, PendingSubject::Operation { id, .. } if id == open[0].operation)
    );
    ana.sync.cancel_invite(open[0].id).await.unwrap();
    assert!(listed(&ana).await.is_empty());
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "invite"));
    let access =
        ana.sync.database.provider_access("owner@example.test".into()).await.unwrap().unwrap();
    assert!(!access.intended);
    ana.sync.database.close().await.unwrap();
}

/// Ben makes Ana a member while her phone is behind. Her write makes an
/// invite for Carol, checked against the slots her phone had applied; the
/// pass that catches up refuses it before granting anything. The refusal is
/// the invite's pending record, beside the invite listed without a code,
/// until her app cancels it (§11.1, §13.2).
#[tokio::test]
async fn an_invite_its_maker_can_no_longer_make_is_refused_before_any_grant() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    ben.sync
        .administer(coven_format::admin::StoreChange::ChangeRole {
            member: ana.identity.member(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    let result = ana
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member },
            InviteAccess::ProviderAccount { email: "carol@example.org".into() },
            crate::InviteLifetime::SevenDays,
        )
        .await;
    let Err(SyncError::Pending(pending)) = result else { panic!("{:?}", result.err()) };
    assert_eq!(pending.reason, PendingReason::Rejected(OperationRejection::NotAllowed));
    let open = listed(&ana).await;
    assert_eq!((open.len(), &open[0].stage), (1, &InviteStage::Granting));
    assert!(
        matches!(pending.subject, PendingSubject::Operation { id, .. } if id == open[0].operation)
    );
    // Nothing was shared with Carol's account.
    assert!(ana.sync.database.provider_access("carol@example.org".into()).await.unwrap().is_none());
    // The refusal never retries on its own: later passes leave it.
    ana.sync.pass().await.unwrap();
    assert_eq!(listed(&ana).await.len(), 1);
    ana.sync.cancel_invite(open[0].id).await.unwrap();
    assert!(listed(&ana).await.is_empty());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// The app's live query on `coven_invites` follows each invite in the
/// writes that change it, in order and without a call: shown with the code
/// it showed, then with its checked request once a pass reads it, then gone
/// once declined.
#[tokio::test]
async fn a_live_query_follows_each_open_invite_in_order() {
    use coven_format::admin::StoreChange;
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut invites = Invite::follow(&ana.sync.database);
    assert!(invites.next().await.is_empty());
    let invite = invite(&ana).await;
    let shown = invites.next().await;
    assert_eq!(shown.len(), 1);
    assert_eq!(
        (shown[0].id, shown[0].shown_code(), &shown[0].shown_request().cloned()),
        (invite.id, invite.shown_code(), &None)
    );
    assert_eq!(shown[0].kind, InviteKind::NewPerson);
    assert_eq!(shown[0], invite);
    // A rename and the slots after it leave the code the joiner scanned as
    // it was: its saved request is bound to those exact bytes.
    ana.sync.administer(StoreChange::RenameStore { name: "Family".into() }).await.unwrap();
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let requested = invites.next().await;
    assert_eq!(requested[0].shown_code(), invite.shown_code());
    let request = requested[0].shown_request().cloned().clone().unwrap();
    assert_eq!(request.device_name, "Carol's phone");
    ana.sync.decline_join_request(&request).await.unwrap();
    assert!(invites.next().await.is_empty());
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Approving, declining or cancelling once the invite's lifetime has ended
/// fails and writes nothing: ending the invite is its expiry's write, which
/// the next pass makes here, as the handle's expiry wake would. The pass
/// after it takes back the share (§13.2).
#[tokio::test]
async fn a_decision_after_the_lifetime_fails_and_leaves_the_expiry() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let request = requests(&ana).await.remove(0);
    group.clock.set(invite.expires_at);
    for result in [
        ana.sync.approve_join_request(&request).await,
        ana.sync.decline_join_request(&request).await,
        ana.sync.cancel_invite(invite.id).await,
    ] {
        assert!(matches!(result, Err(SyncError::InvitationChanged)), "{result:?}");
    }
    assert_eq!(listed(&ana).await.len(), 1, "a refused decision writes nothing");
    ana.sync.pass().await.unwrap();
    assert!(listed(&ana).await.is_empty());
    ana.sync.pass().await.unwrap();
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    let access =
        ana.sync.database.provider_access("carol@example.org".into()).await.unwrap().unwrap();
    assert_eq!((access.intended, access.granted), (false, Some(false)));
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana cancels an invite while a pass is looking for its request. The pass
/// read the invite before her write ended it, and finds no request: that
/// finding belongs to a step the invite has left, so it records no wait over
/// the cancel, and the next pass ends the invite (§13.2, §11.4).
#[tokio::test]
async fn a_pass_under_way_records_nothing_over_a_cancel() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let operation = ana.database().first_operation("invite").await.unwrap().unwrap().id;
    let (observed, read) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    ana.storage.hold_next_read(request_path(&invite), observed, held).await;
    let cancel = async {
        read.await.unwrap();
        let steps = ana.sync.operation_steps();
        let id = invite.id;
        ana.sync.database.write_as(move |sql| steps.cancel_invite(&sql, id)).await.unwrap();
        resume.send(()).unwrap();
    };
    let (passed, ()) = tokio::join!(ana.sync.pass(), cancel);
    passed.unwrap();
    assert!(ana.database().operation_pending::<PendingRecord>(operation).await.unwrap().is_none());
    assert!(ana
        .database()
        .retry_wait(coven_database::RetrySubject::Operation(operation))
        .await
        .unwrap()
        .is_none());
    ana.sync.pass().await.unwrap();
    assert!(ana.database().operation(operation).await.unwrap().is_none());
    ana.database().close().await.unwrap();
}

/// A request waiting for the app's decision does not keep an expired invite
/// open: the next pass takes its grant back (§13.2).
#[tokio::test]
async fn an_invite_waiting_for_a_decision_still_expires() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(requests(&ana).await.len(), 1);
    group.clock.set(invite.expires_at);
    ana.sync.pass().await.unwrap();
    assert!(requests(&ana).await.is_empty());
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "invite"));
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// An invite's request read that got no reply waits its backoff; a pass
/// before then leaves it. The network report ends that wait, keeping
/// its failure count, and the next pass reads the request again (§6.7).
#[tokio::test]
async fn a_network_report_ends_an_offline_invite_wait() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let request = request_path(&invite);
    ana.storage
        .set_faults(coven_storage::test_utils::Faults {
            read_failures: [(request.clone(), coven_storage::StorageFailure::Network)].into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    ana.sync.pass().await.unwrap();
    let operation = ana
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "invite")
        .unwrap()
        .id;
    let reason = || async {
        ana.database().operation_pending::<PendingRecord>(operation).await.unwrap().unwrap().reason
    };
    assert!(reason().await.offline(), "{:?}", reason().await);
    let subject = coven_database::RetrySubject::Operation(operation);
    let wait = ana.database().retry_wait(subject).await.unwrap().unwrap();
    ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    ana.sync.pass().await.unwrap();
    assert!(reason().await.offline(), "a pass before the wait is due leaves it");
    ana.sync.waits.report_network();
    // The upload worker's report leaves the operation to the connection.
    ana.sync.waits.end_reported_waits(crate::NetworkOwner::Uploads).await.unwrap();
    assert_eq!(ana.database().retry_wait(subject).await.unwrap(), Some(wait));
    assert!(ana.sync.waits.end_reported_waits(crate::NetworkOwner::Connection).await.unwrap());
    let ended = ana.database().retry_wait(subject).await.unwrap().unwrap();
    assert_eq!((ended.failures, ended.wait), (wait.failures, std::time::Duration::ZERO));
    ana.sync.pass().await.unwrap();
    assert!(
        matches!(reason().await, PendingReason::Missing { .. }),
        "the report let the invite read its request again"
    );
    ana.database().close().await.unwrap();
}

/// Ana gives Dan her own S3 key in his invite. Coven records the one key id
/// as both members' access, so removing Dan leaves it Ana's: the app sees
/// she still uses it, and the key can't be deleted until she replaces it
/// (§14.3).
#[tokio::test]
async fn an_invite_may_carry_a_key_another_member_uses() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = ana
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member },
            InviteAccess::S3AccessKey {
                access_key_id: "ana".into(),
                secret_access_key: coven_crypto::SecretText::new("secret".into()),
            },
            InviteLifetime::SevenDays,
        )
        .await
        .unwrap();
    assert_eq!(invite.access, access("ana"));
    let code = read_invite_code(invite.shown_code()).unwrap();
    let (temp, directory) = group.directory();
    let mut request = JoiningIdentity::prepare(
        &directory,
        &code,
        directory.settings().unwrap().device_id,
        "Dan's phone",
        carol_for(&code),
    )
    .unwrap();
    let dan = group.open_with(temp, request.identity(), None, group.storage.for_device()).await;
    assert!(dan.sync.submit_join(&request).await.unwrap());
    ana.sync.pass().await.unwrap();
    ana.sync.approve_join_request(&requests(&ana).await.remove(0)).await.unwrap();
    assert_eq!(dan.sync.join_outcome(&mut request).await.unwrap(), JoinOutcome::Admitted);
    let members = ana.database().store_state().await.unwrap().members;
    assert_eq!(members[&invite.member].access, Some(access("ana")));
    assert_eq!(members[&ana.identity.member()].access, Some(access("ana")));
    let removal = coven_format::admin::StoreChange::RemoveMember { member: invite.member };
    ana.sync.administer(removal).await.unwrap();
    let members = ana.database().store_state().await.unwrap().members;
    assert!(members[&invite.member].removed);
    let ana_member = &members[&ana.identity.member()];
    assert_eq!((ana_member.removed, &ana_member.access), (false, &Some(access("ana"))));
    for device in [&ana, &dan] {
        device.database().close().await.unwrap();
    }
}

/// Cancelling an S3 invite takes back nothing at the provider and records
/// nothing about its key: the person deletes it in the console (§14.3).
#[tokio::test]
async fn a_cancelled_s3_invite_leaves_no_work_behind() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = ana
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member },
            InviteAccess::S3AccessKey {
                access_key_id: "dan-key".into(),
                secret_access_key: coven_crypto::SecretText::new("dan-secret".into()),
            },
            crate::InviteLifetime::SevenDays,
        )
        .await
        .unwrap();
    // The invite names its key so the app can tell the person which to delete.
    assert_eq!(invite.access, MemberAccess::S3AccessKey { access_key_id: "dan-key".into() });
    ana.sync.cancel_invite(invite.id).await.unwrap();
    assert!(ana.database().operations().await.unwrap().is_empty());
    assert!(ana.database().pending::<PendingRecord>().await.unwrap().is_empty());
    ana.database().close().await.unwrap();
}

/// Ana sends Carol an invite, then Carol comes over and Ana shows her
/// another. The second invite finds Carol's account already shared by the
/// first, and the share stays while either is open (§13.2, §14.2).
#[tokio::test]
async fn a_second_invite_to_an_account_already_shared_is_made() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let sent = invite(&ana).await;
    let shown = carol_invite(&ana, InviteLifetime::SevenDays).await;
    let open: Vec<_> = listed(&ana).await.into_iter().map(|invite| invite.id).collect();
    assert_eq!(open, [sent.id, shown.id]);
    // Ending the first invite leaves the account shared for the one still open.
    ana.sync.cancel_invite(sent.id).await.unwrap();
    let access =
        ana.sync.database.provider_access("carol@example.org".into()).await.unwrap().unwrap();
    assert_eq!((access.intended, access.granted), (true, Some(true)));
    let mut carol = join(&group, shown.shown_code(), "Carol's phone").await;
    // The cancel's pass found no request yet and waits its backoff.
    group.clock.set(group.clock.now() + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    ana.sync.approve_join_request(&requests(&ana).await.remove(0)).await.unwrap();
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben removes Ana's device while two of her invites are open: one waits for
/// its request, and Carol's approval is under way. Neither can ever finish on
/// a device stopped for good, so the transaction that applies the removal
/// ends both with the device: nothing of hers stays listed, running or
/// pending (§13.2, §14.1).
#[tokio::test]
async fn a_stop_for_good_ends_this_devices_invites() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let approving = invite(&ana).await;
    let waiting = ana
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member },
            InviteAccess::ProviderAccount { email: "dan@example.org".into() },
            InviteLifetime::SevenDays,
        )
        .await
        .unwrap();
    let carol = join(&group, approving.shown_code(), "Carol's phone").await;
    // Dan's invite made a pass that found no request yet, which waits its backoff.
    group.clock.set(group.clock.now() + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let request = requests(&ana).await.remove(0);
    let steps = ana.sync.operation_steps();
    let binding = ana
        .database()
        .write_as(move |sql| steps.approve_join_request(&sql, &request))
        .await
        .unwrap();
    assert_eq!(listed(&ana).await.len(), 2);
    ben.sync.pass().await.unwrap();
    ben.sync
        .administer(coven_format::admin::StoreChange::RemoveDevice { device: ana.sync.device })
        .await
        .unwrap();
    assert!(matches!(ana.sync.pass().await, Err(SyncError::Removed)));
    assert!(listed(&ana).await.is_empty(), "{:?}", listed(&ana).await);
    let operations = ana.database().operations().await.unwrap();
    assert!(
        operations.iter().all(|record| record.kind != operation_kind::INVITE
            && record.started_by != operation_kind::INVITE),
        "{operations:?}"
    );
    let outcome = ana
        .database()
        .read(move |sql| {
            Ok::<_, DbError>(sql.query_row(
                "SELECT outcome FROM coven_operations WHERE operation=?1",
                [binding.0],
                |row| row.get::<_, Option<OperationOutcome>>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(outcome, Some(OperationOutcome::Stopped(DeviceStop::Removed)));
    for invite in [&approving, &waiting] {
        assert!(ana
            .database()
            .operation_pending::<PendingRecord>(invite.operation)
            .await
            .unwrap()
            .is_none());
    }
    for device in [&ana, &ben, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Carol scans an invite on S3 but sends her request only after Ana has
/// cancelled it and her pass has ended it. Nothing is shared to take back on
/// S3, so her request could be stored, and wait forever for a device that
/// no longer looks for it. The ended invite's marker holds the path instead:
/// her phone's send finds it, and her join ends as declined (§13.2).
#[tokio::test]
async fn a_request_sent_after_the_invite_ended_finds_its_marker() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = s3_invite(&ana, "Carol").await;
    let code = read_invite_code(invite.shown_code()).unwrap();
    let (temp, directory) = group.directory();
    let mut request = JoiningIdentity::prepare(
        &directory,
        &code,
        directory.settings().unwrap().device_id,
        "Carol's phone",
        carol_for(&code),
    )
    .unwrap();
    let carol = group.open_with(temp, request.identity(), None, group.storage.for_device()).await;
    ana.sync.cancel_invite(invite.id).await.unwrap();
    assert!(ana.database().operations().await.unwrap().is_empty());
    assert!(!carol.sync.submit_join(&request).await.unwrap(), "the marker turns the request away");
    assert_eq!(carol.sync.join_outcome(&mut request).await.unwrap(), JoinOutcome::Declined);
    for device in [&ana, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

/// The marker stays until no joining device can send a first request: a
/// week, the retention grace, past the invite's expiry, for a joining
/// device whose clock lags Ana's. The first pass after then deletes it
/// (§13.2).
#[tokio::test]
async fn an_ended_invites_marker_goes_after_the_grace() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = s3_invite(&ana, "Carol").await;
    ana.sync.cancel_invite(invite.id).await.unwrap();
    let path = request_path(&invite);
    let marker = group.storage.read(&path).await.unwrap();
    let kept_until = invite.expires_at + crate::sequence_sync::sequence_retention::GRACE;
    assert_eq!(
        coven_format::invite_end::InviteEnd::decode(&marker).unwrap().kept_until,
        kept_until.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
    );
    group.clock.set(kept_until - std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_some());
    group.clock.set(kept_until);
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_none());
    assert!(ana.database().due_object_removals(u64::MAX).await.unwrap().is_empty());
    ana.database().close().await.unwrap();
}

/// A removed member can't be brought back by replacing their keys: the
/// recovery invite's write refuses it, and journals nothing (§13.3).
#[tokio::test]
async fn a_recovery_invite_for_a_removed_member_is_refused() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    let journaled = ana.sync.database.operations().await.unwrap().len();
    let result = ana
        .sync
        .create_invite(
            InviteTarget::ReplaceMemberKeys { member: ben.identity.member() },
            InviteAccess::ProviderAccount { email: "ben-again@example.org".into() },
            InviteLifetime::SevenDays,
        )
        .await;
    assert!(
        matches!(
            &result,
            Err(SyncError::Rejected(OperationRejection::TargetGone(
                crate::EntryTarget::Member(gone)
            ))) if *gone == ben.identity.member()
        ),
        "{:?}",
        result.err()
    );
    assert!(listed(&ana).await.is_empty());
    assert_eq!(ana.sync.database.operations().await.unwrap().len(), journaled);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// An ended invite's marker is kept a grace past the expiry, and a time its
/// seconds can't hold is refused as one the clock can't represent (§13.2).
#[test]
fn a_marker_kept_past_the_last_second_is_refused() {
    let grace = crate::sequence_sync::sequence_retention::GRACE.as_secs();
    assert_eq!(super::kept_until(u64::MAX - grace).unwrap(), u64::MAX);
    let refused = super::kept_until(u64::MAX - grace + 1);
    assert!(
        matches!(&refused, Err(SyncError::Storage(error))
            if error.failure() == coven_storage::StorageFailure::InvalidConfiguration),
        "{refused:?}"
    );
}
