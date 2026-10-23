use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::{admin::MemberRole, MemberAccess};
use coven_storage::{ObjectPath, Storage};

/// The checked requests among the device's open invites.
pub(super) async fn requests(device: &Device) -> Vec<JoinRequest> {
    device
        .sync
        .invites()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|invite| invite.request)
        .collect()
}

#[tokio::test]
async fn an_approved_request_binds_its_pair_after_copies_and_the_joiner_registers() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    insert(&ana.sync.database, "a", "before the invite").await;
    ana.sync.pass().await.unwrap();
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Waiting
    );
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    assert_eq!(checked.len(), 1);
    assert_eq!(checked[0].member, invite.member);
    assert_eq!(checked[0].device_name, "Carol's phone");
    assert_eq!(
        checked[0].provider_account_email.as_deref(),
        Some("carol@example.org")
    );
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
    assert_eq!(
        ana.storage.request_count() - count,
        2,
        "a checked request waits without reads"
    );
    ana.sync.approve_join_request(&checked[0]).await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::join_request(group.store, invite.id))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Admitted
    );
    carol
        .device
        .sync
        .register_device("Carol's phone".into(), None)
        .await
        .unwrap();
    assert_eq!(
        rows(&carol.device.sync.database).await,
        rows(&ana.sync.database).await
    );
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
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(requests(&ana).await.len(), 1);
    ana.sync.cancel_invite(invite.id).await.unwrap();
    assert!(ana.sync.invites().await.unwrap().is_empty());
    assert!(group
        .storage
        .status(&ObjectPath::join_request(group.store, invite.id))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
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
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    ana.sync.decline_join_request(&checked[0]).await.unwrap();
    ana.sync.pass().await.unwrap();
    let access = ana
        .sync
        .database
        .provider_access("carol@example.org".into())
        .await
        .unwrap()
        .unwrap();
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
        (
            InviteDelivery::Sent {
                lifetime: InviteLifetime::Hour,
            },
            3_600,
        ),
        (
            InviteDelivery::Sent {
                lifetime: InviteLifetime::Day,
            },
            86_400,
        ),
        (
            InviteDelivery::Sent {
                lifetime: InviteLifetime::Week,
            },
            604_800,
        ),
    ] {
        let group = Group::new();
        let ana = group.device(1).await;
        group.create(&ana).await;
        let made = coven_foundation::clock::Clock::now(group.clock.as_ref());
        let invite = ana
            .sync
            .create_invite(
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                    name: "Dan".into(),
                },
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
        let CodeInfo::Invite {
            invitee,
            delivery: carried,
            ..
        } = decode_code_info(&invite.code).unwrap()
        else {
            panic!("an invite code");
        };
        assert_eq!(
            (invitee.as_deref(), carried),
            (Some("Dan"), delivery.kind())
        );
        group.clock.set(invite.expires_at - Duration::from_secs(1));
        ana.sync.pass().await.unwrap();
        assert_eq!(ana.database().operations().await.unwrap().len(), 1);
        group.clock.set(invite.expires_at);
        ana.sync.pass().await.unwrap();
        assert!(ana.database().operations().await.unwrap().is_empty());
        ana.database().close().await.unwrap();
    }
}

/// A new person's invite needs a name the format can carry, checked before
/// any provider change.
#[tokio::test]
async fn an_invite_for_a_new_person_refuses_an_empty_name() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let count = ana.storage.request_count();
    let result = ana
        .sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: String::new(),
            },
            InviteAccess::ProviderAccount {
                email: "dan@example.org".into(),
            },
            InviteDelivery::InPerson,
        )
        .await;
    assert!(
        matches!(result, Err(SyncError::Format(_))),
        "{:?}",
        result.err()
    );
    assert_eq!(ana.storage.request_count(), count);
    assert!(ana.database().operations().await.unwrap().is_empty());
    ana.database().close().await.unwrap();
}

#[tokio::test]
async fn an_invite_expires_a_day_later_on_the_inviting_device() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    invite(&ana).await;
    group
        .clock
        .set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(86_400));
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    let access = ana
        .sync
        .database
        .provider_access("carol@example.org".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((access.intended, access.granted), (false, Some(false)));
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn a_replacement_invite_rebinds_an_existing_member_on_a_new_device() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
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
            InviteTarget::ReplaceMemberKeys {
                member: carol_id.member(),
            },
            InviteAccess::ProviderAccount {
                email: "carol@example.org".into(),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    assert_eq!(invite.member, carol_id.member());
    let carol = join(&group, &invite.code, "Carol's new phone").await;
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    ana.sync.approve_join_request(&checked[0]).await.unwrap();
    let state = ana.sync.database.store_state().await.unwrap();
    assert_eq!(
        state.members[&carol_id.member()].keys,
        carol.request.identity().keys().public_keys()
    );
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Admitted
    );
    carol
        .device
        .sync
        .register_device("Carol's new phone".into(), None)
        .await
        .unwrap();
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
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
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
    let pending = ana
        .sync
        .database
        .operation_pending::<PendingRecord>(operation)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(pending.reason, PendingReason::Missing { .. }));
    let carol = join(&group, &invite.code, "Carol's phone").await;
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.storage.request_count() - count,
        2,
        "the request poll backs off"
    );
    assert!(requests(&ana).await.is_empty());
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert_eq!(requests(&ana).await.len(), 1);
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// A grant the provider refuses answers the call at once with that refusal,
/// and the invite takes back what it intended (T296).
#[tokio::test]
async fn a_refused_grant_answers_the_invite_call_and_settles_the_invite() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let result = ana
        .sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::ProviderAccount {
                email: "owner@example.test".into(),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await;
    assert!(
        matches!(&result, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::InviteeIsStoreOwner),
        "{:?}",
        result.err()
    );
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "invite"));
    let access = ana
        .sync
        .database
        .provider_access("owner@example.test".into())
        .await
        .unwrap()
        .unwrap();
    assert!(!access.intended);
    ana.sync.database.close().await.unwrap();
}

/// The owner's open invites reach the app's subscription without a call:
/// each as soon as it is shown, with the code it showed, and its checked
/// request once a pass reads it.
#[tokio::test]
async fn a_pass_publishes_open_invites_and_their_requests_to_the_subscription() {
    use coven_format::admin::StoreChange;
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invites = ana.sync.state.subscribe_invites();
    assert!(invites.borrow().is_empty());
    let invite = invite(&ana).await;
    let listed = invites.borrow().clone();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        (listed[0].id, &listed[0].code, &listed[0].request),
        (invite.id, &invite.code, &None)
    );
    assert_eq!(listed[0].invitee.as_deref(), Some("Carol"));
    // A rename and the slots after it leave the code the joiner scanned as
    // it was: its saved request is bound to those exact bytes.
    ana.sync
        .administer(StoreChange::RenameStore {
            name: "Family".into(),
        })
        .await
        .unwrap();
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let shown = invites.borrow().clone();
    assert_eq!(shown, ana.sync.invites().await.unwrap());
    assert_eq!(shown[0].code, invite.code);
    let request = shown[0].request.clone().unwrap();
    assert_eq!(request.device_name, "Carol's phone");
    ana.sync.decline_join_request(&request).await.unwrap();
    assert!(invites.borrow().is_empty());
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// A request waiting for the app's decision does not keep an expired invite
/// open: the next pass takes its grant back (§13.2).
#[tokio::test]
async fn an_invite_waiting_for_a_decision_still_expires() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
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
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
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
        ana.database()
            .operation_pending::<PendingRecord>(operation)
            .await
            .unwrap()
            .unwrap()
            .reason
    };
    assert!(reason().await.offline(), "{:?}", reason().await);
    let subject = coven_database::RetrySubject::Operation(operation);
    let wait = ana.database().retry_wait(subject).await.unwrap().unwrap();
    ana.storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    ana.sync.pass().await.unwrap();
    assert!(
        reason().await.offline(),
        "a pass before the wait is due leaves it"
    );
    ana.sync.waits.network_changed();
    // The upload worker's report leaves the operation to the connection.
    ana.sync
        .waits
        .end_reported_waits(crate::NetworkOwner::Uploads)
        .await
        .unwrap();
    assert_eq!(
        ana.database().retry_wait(subject).await.unwrap(),
        Some(wait)
    );
    assert!(ana
        .sync
        .waits
        .end_reported_waits(crate::NetworkOwner::Connection)
        .await
        .unwrap());
    let ended = ana.database().retry_wait(subject).await.unwrap().unwrap();
    assert_eq!(
        (ended.failures, ended.wait),
        (wait.failures, std::time::Duration::ZERO)
    );
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
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::S3AccessKey {
                access_key_id: "dan-key".into(),
                secret_access_key: coven_crypto::SecretText::new("dan-secret".into()),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    // The invite names its key so the app can tell the person which to delete.
    assert_eq!(
        invite.access,
        MemberAccess::S3AccessKey {
            access_key_id: "dan-key".into()
        }
    );
    ana.sync.cancel_invite(invite.id).await.unwrap();
    assert!(ana.database().operations().await.unwrap().is_empty());
    assert!(ana
        .database()
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .is_empty());
    ana.database().close().await.unwrap();
}

/// Ana sends Carol an invite, then Carol comes over and Ana shows her one in
/// person. The second invite finds Carol's account already shared by the
/// first, and the share stays while either is open (§13.2, §14.2).
#[tokio::test]
async fn a_second_invite_to_an_account_already_shared_is_made() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let sent = invite(&ana).await;
    let in_person = ana
        .sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::ProviderAccount {
                email: "carol@example.org".into(),
            },
            InviteDelivery::InPerson,
        )
        .await
        .unwrap();
    let open: Vec<_> = ana
        .sync
        .invites()
        .await
        .unwrap()
        .into_iter()
        .map(|invite| invite.id)
        .collect();
    assert_eq!(open, [sent.id, in_person.id]);
    // Ending the sent invite leaves the account shared for the one still open.
    ana.sync.cancel_invite(sent.id).await.unwrap();
    let access = ana
        .sync
        .database
        .provider_access("carol@example.org".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((access.intended, access.granted), (true, Some(true)));
    let carol = join(&group, &in_person.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}
