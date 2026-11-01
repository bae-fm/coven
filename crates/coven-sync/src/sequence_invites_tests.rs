use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::{admin::MemberRole, MemberAccess};
use coven_foundation::clock::Clock;
use coven_storage::{ObjectPath, Storage};

/// The checked requests among the device's open invites.
pub(super) async fn requests(device: &Device) -> Vec<JoinRequest> {
    listed(device).await.into_iter().filter_map(|invite| invite.shown_request().cloned()).collect()
}

#[tokio::test]
async fn an_approved_request_binds_its_pair_after_copies_and_the_joiner_registers() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    insert(&ana.sync.database, "a", "before the invite").await;
    ana.sync.pass().await.unwrap();
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    assert_eq!(carol.device.sync.join_outcome(&carol.request).await.unwrap(), JoinOutcome::Waiting);
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    assert_eq!(checked.len(), 1);
    assert_eq!(checked[0].member, invite.member);
    assert_eq!(checked[0].device_name, "Carol's phone");
    assert_eq!(checked[0].provider_account_email.as_deref(), Some("carol@example.org"));
    // Ana's number names the keys Carol's phone made for its request.
    assert_eq!(
        checked[0].confirmation,
        coven_crypto::ConfirmationNumber::of(
            invite.id,
            &carol.request.identity().keys().public_keys()
        )
    );
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.storage.request_count() - count, 2, "a checked request waits without reads");
    ana.sync.approve_join_request(&checked[0]).await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::join_request(group.store, invite.id))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    carol.device.sync.register_device("Carol's phone".into(), None).await.unwrap();
    assert_eq!(rows(&carol.device.sync.database).await, rows(&ana.sync.database).await);
    insert(&carol.device.sync.database, "c", "from Carol").await;
    carol.device.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    assert!(requests(&ana).await.is_empty());
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

/// Cancelling a sent invite whose request waits deletes the request, which
/// Carol's phone sees as a decline (§13.2).
#[tokio::test]
async fn cancelling_an_invite_with_a_waiting_request_ends_the_join_as_declined() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(requests(&ana).await.len(), 1);
    ana.sync.cancel_invite(invite.id).await.unwrap();
    assert!(listed(&ana).await.is_empty());
    assert!(group
        .storage
        .status(&ObjectPath::join_request(group.store, invite.id))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let request = requests(&ana).await.remove(0);
    let (now, waits, state) = (group.clock.now(), ana.sync.waits.clone(), ana.sync.state.clone());
    ana.sync
        .database
        .write(move |sql| {
            crate::decline_join_request(&sql, now, waits, state, &request)
                .map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))
        })
        .await
        .unwrap();
    assert!(listed(&ana).await.is_empty());
    let path = ObjectPath::join_request(group.store, invite.id);
    assert!(group.storage.status(&path).await.unwrap().is_some());
    assert_eq!(carol.device.sync.join_outcome(&carol.request).await.unwrap(), JoinOutcome::Waiting);
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_none());
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
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
    let outcome = carol.device.sync.join_outcome(&carol.request).await;
    assert!(
        matches!(&outcome, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::PermissionDenied),
        "{outcome:?}"
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// The delivery fixes the expiry when the invite is made, and the code
/// carries the person's name and the delivery to the joining device.
#[tokio::test]
async fn each_delivery_expires_after_its_lifetime_on_the_inviting_device() {
    use std::time::Duration;
    for (delivery, lifetime) in [
        (InviteDelivery::InPerson, 3_600),
        (InviteDelivery::Sent { lifetime: InviteLifetime::Hour }, 3_600),
        (InviteDelivery::Sent { lifetime: InviteLifetime::Day }, 86_400),
        (InviteDelivery::Sent { lifetime: InviteLifetime::Week }, 604_800),
    ] {
        let group = Group::new();
        let ana = group.device(1).await;
        group.create(&ana).await;
        let made = coven_foundation::clock::Clock::now(group.clock.as_ref());
        let invite = ana
            .sync
            .create_invite(
                InviteTarget::NewMember { role: MemberRole::Member, name: "Dan".into() },
                InviteAccess::S3AccessKey {
                    access_key_id: "dan-key".into(),
                    secret_access_key: coven_crypto::SecretText::new("dan-secret".into()),
                },
                delivery,
            )
            .await
            .unwrap();
        assert_eq!(invite.expires_at, made + Duration::from_secs(lifetime));
        assert_eq!(invite.invitee.as_deref(), Some("Dan"));
        assert_eq!(invite.delivery, delivery.kind());
        let CodeInfo::Invite(InviteInfo { invitee, delivery: carried, .. }) =
            decode_code_info(invite.shown_code()).unwrap()
        else {
            panic!("an invite code");
        };
        assert_eq!((invitee.as_deref(), carried), (Some("Dan"), delivery.kind()));
        group.clock.set(invite.expires_at - Duration::from_secs(1));
        ana.sync.pass().await.unwrap();
        assert_eq!(ana.database().operations().await.unwrap().len(), 1);
        group.clock.set(invite.expires_at);
        ana.sync.pass().await.unwrap();
        assert!(ana.database().operations().await.unwrap().is_empty());
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = invite(&ana).await;
    let opened = |at| InviteExpiry::opened(&ana.sync.database, at);
    opened(invite.expires_at - Duration::from_secs(1)).await.unwrap();
    assert_eq!(listed(&ana).await, std::slice::from_ref(&invite));
    opened(invite.expires_at).await.unwrap();
    assert!(listed(&ana).await.is_empty());
    ana.sync.database.close().await.unwrap();
}

/// A new person's invite needs a name the format can carry, checked before
/// any provider change.
#[tokio::test]
async fn an_invite_for_a_new_person_refuses_an_empty_name() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let count = ana.storage.request_count();
    let result = ana
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member, name: String::new() },
            InviteAccess::ProviderAccount { email: "dan@example.org".into() },
            InviteDelivery::InPerson,
        )
        .await;
    assert!(matches!(result, Err(SyncError::Format(_))), "{:?}", result.err());
    assert_eq!(ana.storage.request_count(), count);
    assert!(ana.database().operations().await.unwrap().is_empty());
    ana.database().close().await.unwrap();
}

#[tokio::test]
async fn an_invite_expires_a_day_later_on_the_inviting_device() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    invite(&ana).await;
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(86_400));
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let carol_id = coven_crypto::MemberIdentity::new(
        coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(91)),
        coven_format::test_utils::keys_from_seed(3),
    );
    ana.sync
        .administer(coven_format::admin::StoreChange::AddMember {
            member: carol_id.member(),
            keys: carol_id.keys().public_keys(),
            role: MemberRole::Member,
            access: MemberAccess::ProviderAccount("carol@example.org".into()),
        })
        .await
        .unwrap();
    let invite = ana
        .sync
        .create_invite(
            InviteTarget::ReplaceMemberKeys { member: carol_id.member() },
            InviteAccess::ProviderAccount { email: "carol@example.org".into() },
            crate::InviteDelivery::Sent { lifetime: crate::InviteLifetime::Day },
        )
        .await
        .unwrap();
    assert_eq!(invite.member, carol_id.member());
    let carol = join(&group, invite.shown_code(), "Carol's new phone").await;
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    ana.sync.approve_join_request(&checked[0]).await.unwrap();
    let state = ana.sync.database.store_state().await.unwrap();
    assert_eq!(
        state.members[&carol_id.member()].keys,
        carol.request.identity().keys().public_keys()
    );
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    carol.device.sync.register_device("Carol's new phone".into(), None).await.unwrap();
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let result = ana
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::ProviderAccount { email: "owner@example.test".into() },
            crate::InviteDelivery::Sent { lifetime: crate::InviteLifetime::Day },
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
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
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::ProviderAccount { email: "carol@example.org".into() },
            crate::InviteDelivery::Sent { lifetime: crate::InviteLifetime::Day },
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let mut invites = Invite::follow(&ana.sync.database);
    assert!(invites.next().await.is_empty());
    let invite = invite(&ana).await;
    let shown = invites.next().await;
    assert_eq!(shown.len(), 1);
    assert_eq!(
        (shown[0].id, shown[0].shown_code(), &shown[0].shown_request().cloned()),
        (invite.id, invite.shown_code(), &None)
    );
    assert_eq!(shown[0].invitee.as_deref(), Some("Carol"));
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = invite(&ana).await;
    let operation = ana.database().first_operation("invite").await.unwrap().unwrap().id;
    let (observed, read) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    ana.storage
        .hold_next_read(ObjectPath::join_request(group.store, invite.id), observed, held)
        .await;
    let cancel = async {
        read.await.unwrap();
        let (now, waits, state) =
            (group.clock.now(), ana.sync.waits.clone(), ana.sync.state.clone());
        let id = invite.id;
        ana.sync
            .database
            .write(move |sql| {
                crate::cancel_invite(&sql, now, waits, state, id)
                    .map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))
            })
            .await
            .unwrap();
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
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
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
/// before then leaves it. The app's network report ends that wait, keeping
/// its failure count, and the next pass reads the request again (§6.7).
#[tokio::test]
async fn a_network_report_ends_an_offline_invite_wait() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = invite(&ana).await;
    let request = ObjectPath::join_request(group.store, invite.id);
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
    ana.sync.waits.network_changed();
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
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::S3AccessKey {
                access_key_id: "dan-key".into(),
                secret_access_key: coven_crypto::SecretText::new("dan-secret".into()),
            },
            crate::InviteDelivery::Sent { lifetime: crate::InviteLifetime::Day },
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

/// Ana sends Carol an invite, then Carol comes over and Ana shows her one in
/// person. The second invite finds Carol's account already shared by the
/// first, and the share stays while either is open (§13.2, §14.2).
#[tokio::test]
async fn a_second_invite_to_an_account_already_shared_is_made() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let sent = invite(&ana).await;
    let in_person = ana
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::ProviderAccount { email: "carol@example.org".into() },
            InviteDelivery::InPerson,
        )
        .await
        .unwrap();
    let open: Vec<_> = listed(&ana).await.into_iter().map(|invite| invite.id).collect();
    assert_eq!(open, [sent.id, in_person.id]);
    // Ending the sent invite leaves the account shared for the one still open.
    ana.sync.cancel_invite(sent.id).await.unwrap();
    let access =
        ana.sync.database.provider_access("carol@example.org".into()).await.unwrap().unwrap();
    assert_eq!((access.intended, access.granted), (true, Some(true)));
    let carol = join(&group, in_person.shown_code(), "Carol's phone").await;
    // The cancel's pass found no request yet and waits its backoff.
    group.clock.set(group.clock.now() + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}
