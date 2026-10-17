use super::*;
use coven_foundation::clock::Clock;
use coven_storage::test_utils::MemoryRequest;

const ACCOUNT: &str = "cat@example.com";

/// Only queued commands wake the worker; wall time still controls invite expiry.
struct CommandClock(Arc<FixedClock>);

impl Clock for CommandClock {
    fn now(&self) -> std::time::SystemTime {
        self.0.now()
    }
    fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
    fn sleep(
        &self,
        _duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn expiring_an_invitation_completes_a_retry_of_its_obsolete_grant() {
    let [mut a, _, _] = accounts(dropbox()).await;
    let invite = begin(
        &mut a,
        Command::Invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::ProviderAccount {
                email: "owner@example.com".into(),
            },
        ),
    )
    .await;
    let access =
        a.db.provider_access("owner@example.com".into())
            .await
            .unwrap()
            .unwrap()
            .request
            .unwrap()
            .operation;
    let Data::Invite(work) = Data::read(&a.operation(invite).await).unwrap() else {
        panic!()
    };
    a.clock.set(work.expires_at);
    a.sync.clock = Arc::new(CommandClock(a.clock.clone()));
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    tokio::time::timeout(
        Duration::from_secs(5),
        operations.retry_pending_operation(access),
    )
    .await
    .expect("retiring access work must answer its waiter")
    .unwrap();
    assert!(a.db.operation(access).await.unwrap().is_none());
    assert!(pending(&operations, &a.db).await.is_empty());
    operations.close().await.unwrap();
}

async fn account_operation(d: &Device) -> OperationId {
    d.db.provider_access(ACCOUNT.into())
        .await
        .unwrap()
        .unwrap()
        .request
        .unwrap()
        .operation
}

#[tokio::test]
async fn removal_answers_while_the_provider_request_is_held() {
    let storage = dropbox();
    let [a, _, c] = accounts(storage.clone()).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_access(ACCOUNT, false, started, held)
        .await;
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    tokio::time::timeout(
        Duration::from_secs(5),
        operations.remove_member(&c.member.member()),
    )
    .await
    .unwrap()
    .unwrap();
    observed.await.unwrap();
    assert!(
        a.db.local_store_log()
            .await
            .unwrap()
            .log
            .replay
            .state
            .members[&c.member.member()]
            .removed
    );
    assert!(a
        .db
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|r| r.kind != "remove-member"));
    assert!(MemoryStorage::for_recipient(&storage, ACCOUNT)
        .unwrap()
        .list(&ObjectPrefix::all(store(1)))
        .await
        .is_ok());
    assert!(matches!(
        a.db.pending::<PendingRecord>().await.unwrap()[0].reason,
        PendingReason::ProviderPending {
            action: ProviderAccessAction::Revoke,
            ..
        }
    ));
    resume.send(()).unwrap();
    assert!(pending(&operations, &a.db).await.is_empty());
    operations.close().await.unwrap();
}

#[tokio::test]
async fn restart_repeats_unconfirmed_access_and_keeps_the_same_pending_subject() {
    for checkpoint in [0, 1, 2, 3] {
        let storage = dropbox();
        let [mut a, _, c] = accounts(storage.clone()).await;
        publish(&mut a, Command::RemoveMember(c.member.member())).await;
        let id = account_operation(&a).await;
        if checkpoint > 0 {
            assert!(matches!(
                step(&mut a, id).await.unwrap(),
                Progress::Advanced
            ));
        }
        if checkpoint == 2 {
            // The provider committed, but the reply never reached the journal.
            storage.revoke_access(ACCOUNT).await.unwrap();
        }
        if checkpoint == 3 {
            storage
                .set_retained_access(
                    ACCOUNT,
                    vec![coven_storage::RetainedAccess {
                        provider_id: "inherited".into(),
                        reason: coven_storage::RetainedAccessReason::Inherited,
                    }],
                )
                .await;
            assert!(matches!(
                step(&mut a, id).await.unwrap(),
                Progress::Waiting(PendingReason::AccessRemains { .. })
            ));
        }
        a.restart(storage.clone()).await;
        assert_eq!(account_operation(&a).await, id);
        if checkpoint == 3 {
            let before =
                a.db.operation_pending::<PendingRecord>(id)
                    .await
                    .unwrap()
                    .unwrap();
            a.sync().await;
            assert_eq!(
                a.db.operation_pending::<PendingRecord>(id)
                    .await
                    .unwrap()
                    .unwrap(),
                before
            );
            storage.set_retained_access(ACCOUNT, Vec::new()).await;
            begin(&mut a, Command::Retry(id)).await;
            assert!(matches!(
                a.db.operation_pending::<PendingRecord>(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .reason,
                PendingReason::ProviderPending {
                    action: ProviderAccessAction::Revoke,
                    ..
                }
            ));
        }
        drive_access(&mut a).await;
        assert!(a
            .db
            .provider_access(ACCOUNT.into())
            .await
            .unwrap()
            .unwrap()
            .request
            .is_none());
        assert!(
            matches!(MemoryStorage::for_recipient(&storage,ACCOUNT).unwrap().list(&ObjectPrefix::all(store(1))).await,
            Err(error) if error.failure()==StorageFailure::PermissionDenied)
        );
    }
}

#[tokio::test]
async fn a_cancelled_unsent_grant_never_reaches_the_provider() {
    let storage = dropbox();
    let [mut a, _, _] = accounts(storage.clone()).await;
    let id = begin(
        &mut a,
        Command::Invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::ProviderAccount {
                email: "unsent@example.com".into(),
            },
        ),
    )
    .await;
    let record = a.db.operation(id).await.unwrap().unwrap();
    let Data::Invite(work) = Data::read(&record).unwrap() else {
        panic!()
    };
    let before = storage
        .request_trace()
        .iter()
        .filter(|r| matches!(r, MemoryRequest::Other("grant_access" | "revoke_access")))
        .count();
    begin(&mut a, Command::Cancel(work.id)).await;
    finish(&mut a, id).await;
    assert!(a.db.operations().await.unwrap().is_empty());
    assert_eq!(
        storage
            .request_trace()
            .iter()
            .filter(|r| matches!(r, MemoryRequest::Other("grant_access" | "revoke_access")))
            .count(),
        before
    );
}

async fn adopt_account(a: &mut Device, target: &MemberIdentity, intended: bool) {
    let change = if intended {
        StoreChange::AddMember {
            member: target.member(),
            keys: crate::effects::tests::addition_keys(&a.log().await, target),
            role: MemberRole::Member,
            access: coven_format::MemberAccess::ProviderAccount(ACCOUNT.into()),
        }
    } else {
        StoreChange::RemoveMember {
            member: target.member(),
        }
    };
    // Authoring requires a completed catch-up; a reopened device starts closed.
    if !a.send.can_send() {
        a.catch_up().await;
    }
    a.sync.make_and_upload_entry(change).await.unwrap();
}

async fn phase(a: &Device, storage: &MemoryStorage) -> serde_json::Value {
    let state = a.db.provider_access(ACCOUNT.into()).await.unwrap().unwrap();
    let (phase, target, reason) = if let Some(request) = state.request {
        let row = a.db.operation(request.operation).await.unwrap().unwrap();
        let phase = match row.last_step {
            0 => "ready",
            1 => "sending",
            2 => "failed",
            _ => panic!(),
        };
        let pending =
            a.db.operation_pending::<PendingRecord>(row.id)
                .await
                .unwrap()
                .unwrap();
        let reason = match pending.reason {
            PendingReason::ProviderPending {
                action: ProviderAccessAction::Grant,
                ..
            } => "grant",
            PendingReason::ProviderPending {
                action: ProviderAccessAction::Revoke,
                ..
            } => "revoke",
            PendingReason::AccessRemains { .. } => "retained",
            other => panic!("{other:?}"),
        };
        (
            phase,
            serde_json::json!(request.target.unwrap_or(state.intended)),
            reason,
        )
    } else {
        ("idle", serde_json::Value::Null, "none")
    };
    // The account reaches the store once it mounts what the provider shares.
    let actual = match joined(storage, ACCOUNT).await {
        Ok(recipient) => {
            recipient.list(&ObjectPrefix::all(store(1))).await.unwrap();
            true
        }
        Err(error) if error.failure() == StorageFailure::PermissionDenied => false,
        Err(error) => panic!("{error}"),
    };
    serde_json::json!({"intended":state.intended,"actual":actual,"phase":phase,"target":target,"reason":reason})
}

async fn access_history(initial: bool, events: &[&str]) -> Vec<serde_json::Value> {
    let storage = dropbox();
    let [mut a, _, c] = accounts(storage.clone()).await;
    if !initial {
        adopt_account(&mut a, &c.member, false).await;
        drive_access(&mut a).await;
    }
    let mut result = vec![phase(&a, &storage).await];
    for event in events {
        match *event {
            "grant" | "revoke" => adopt_account(&mut a, &c.member, *event == "grant").await,
            "start" => {
                let id = account_operation(&a).await;
                assert!(matches!(
                    step(&mut a, id).await.unwrap(),
                    Progress::Advanced
                ));
            }
            "success" | "retained" => {
                let shares = if *event == "retained" {
                    vec![coven_storage::RetainedAccess {
                        provider_id: "parent".into(),
                        reason: coven_storage::RetainedAccessReason::Inherited,
                    }]
                } else {
                    Vec::new()
                };
                storage.set_retained_access(ACCOUNT, shares).await;
                let id = account_operation(&a).await;
                let progress = step(&mut a, id).await.unwrap();
                assert!(matches!(
                    progress,
                    Progress::Advanced
                        | Progress::Finished(Output::Unit)
                        | Progress::Waiting(PendingReason::AccessRemains { .. })
                ));
            }
            "retry" => {
                let id = account_operation(&a).await;
                begin(&mut a, Command::Retry(id)).await;
            }
            _ => unreachable!(),
        }
        a.restart(storage.clone()).await;
        result.push(phase(&a, &storage).await);
    }
    result
}

fn histories() -> Vec<(bool, Vec<&'static str>)> {
    let mut cases = Vec::new();
    for initial in [false, true] {
        let first = if initial { "revoke" } else { "grant" };
        let opposite = if initial { "grant" } else { "revoke" };
        cases.push((initial, vec![first, "start", "success"]));
        cases.push((initial, vec![first, opposite]));
        cases.push((
            initial,
            vec![first, "start", opposite, "success", "start", "success"],
        ));
        cases.push((initial, vec![first, "start", opposite, first, "success"]));
    }
    cases.push((
        true,
        vec!["revoke", "start", "retained", "retry", "start", "success"],
    ));
    cases
}

#[tokio::test]
async fn obsolete_access_requests_queue_the_opposite_before_clearing_pending() {
    for (initial, events) in histories() {
        let states = access_history(initial, &events).await;
        let last = states.last().unwrap();
        assert_eq!(last["phase"], "idle");
        assert_eq!(last["actual"], last["intended"]);
        assert_eq!(last["reason"], "none");
    }
}

#[tokio::test]
#[ignore = "requires COVEN_ACCESS_LEAN; scripts/check.sh supplies it"]
async fn lean_differential_access_phases() {
    let mut inputs = Vec::new();
    let mut rust = Vec::new();
    for (initial, events) in histories() {
        inputs.push(serde_json::json!({"initial":initial,"events":events}));
        rust.push(access_history(initial, &events).await);
    }
    let output = crate::tests::run_lean("COVEN_ACCESS_LEAN", &serde_json::json!(inputs));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lean: Vec<Vec<serde_json::Value>> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(lean, rust);
}

#[tokio::test]
async fn a_lost_reply_followed_by_reversal_reconfirms_actual_provider_access() {
    let storage = dropbox();
    let [mut a, _, c] = accounts(storage.clone()).await;
    adopt_account(&mut a, &c.member, false).await;
    let id = account_operation(&a).await;
    step(&mut a, id).await.unwrap();
    storage.revoke_access(ACCOUNT).await.unwrap();
    let record = a.db.operation(id).await.unwrap().unwrap();
    a.sync
        .record_operation_error(&record, &SyncError::Storage(StorageFailure::Network.into()))
        .await
        .unwrap();
    a.restart(storage.clone()).await;
    adopt_account(&mut a, &c.member, true).await;
    assert_eq!(account_operation(&a).await, id);
    let state = a.db.provider_access(ACCOUNT.into()).await.unwrap().unwrap();
    assert_eq!(state.granted, None);
    assert!(state.intended);
    drive_access(&mut a).await;
    assert_eq!(phase(&a, &storage).await["actual"], true);
    assert!(a
        .db
        .operation_pending::<PendingRecord>(id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn unidentified_retained_permissions_do_not_confirm_a_returning_accounts_grant() {
    let storage = dropbox();
    let [mut a, _, c] = accounts(storage.clone()).await;
    adopt_account(&mut a, &c.member, false).await;
    let id = account_operation(&a).await;
    step(&mut a, id).await.unwrap();
    // A provider can remove the direct grant and retain an unidentified
    // permission whose recipient may be someone else.
    storage.revoke_access(ACCOUNT).await.unwrap();
    storage
        .set_retained_access(
            ACCOUNT,
            vec![coven_storage::RetainedAccess {
                provider_id: "unidentified".into(),
                reason: coven_storage::RetainedAccessReason::UnidentifiedAccount,
            }],
        )
        .await;
    assert!(matches!(
        step(&mut a, id).await.unwrap(),
        Progress::Waiting(PendingReason::AccessRemains { .. })
    ));
    adopt_account(&mut a, &c.member, true).await;
    assert!(a
        .db
        .provider_access(ACCOUNT.into())
        .await
        .unwrap()
        .unwrap()
        .request
        .is_some());
    drive_access(&mut a).await;
    assert_eq!(phase(&a, &storage).await["actual"], true);
}

#[tokio::test]
async fn starting_revocation_invalidates_cached_invitation_tokens() {
    let storage = dropbox();
    let [mut a, _, c] = accounts(storage).await;
    let id = begin(
        &mut a,
        Command::Invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::ProviderAccount {
                email: ACCOUNT.into(),
            },
        ),
    )
    .await;
    let Output::Invite(invite) = finish(&mut a, id).await else {
        panic!()
    };
    begin(&mut a, Command::Cancel(invite.id)).await;
    finish(&mut a, id).await;
    assert!(a
        .db
        .provider_access(ACCOUNT.into())
        .await
        .unwrap()
        .unwrap()
        .invitation
        .is_some());
    adopt_account(&mut a, &c.member, false).await;
    let id = account_operation(&a).await;
    step(&mut a, id).await.unwrap();
    assert!(a
        .db
        .provider_access(ACCOUNT.into())
        .await
        .unwrap()
        .unwrap()
        .invitation
        .is_none());
}

#[tokio::test]
async fn discarding_an_invalid_s3_account_invite_leaves_no_provider_request() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    assert!(
        matches!(operations.create_invite(InviteTarget::NewMember { role: MemberRole::Member },
        InviteAccess::ProviderAccount {email:ACCOUNT.into()}).await,
        Err(SyncError::Storage(error)) if error.failure()==StorageFailure::InvalidConfiguration)
    );
    pending(&operations, &a.db).await;
    let invite =
        a.db.operations()
            .await
            .unwrap()
            .into_iter()
            .find(|row| matches!(Data::read(row).unwrap(), Data::Invite(_)))
            .unwrap()
            .id;
    operations.discard_pending_operation(invite).await.unwrap();
    assert!(pending(&operations, &a.db).await.is_empty());
    assert!(a.db.operations().await.unwrap().is_empty());
    assert!(storage
        .request_trace()
        .iter()
        .all(|r| !matches!(r, MemoryRequest::Other("grant_access" | "revoke_access"))));
    operations.close().await.unwrap();
}

/// The refused grant fails its provider request; the invite reports that
/// refusal when its own wait for the request next comes due.
#[tokio::test]
async fn inviting_the_store_owners_account_fails_the_invite_with_that_refusal() {
    let [a, _, _] = accounts(dropbox()).await;
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    let invite = operations.create_invite(
        InviteTarget::NewMember {
            role: MemberRole::Member,
        },
        InviteAccess::ProviderAccount {
            email: "Owner@example.test".into(),
        },
    );
    tokio::pin!(invite);
    let mut minutes = 0;
    let refused = loop {
        tokio::select! {
            result = &mut invite => break result,
            () = tokio::time::sleep(Duration::from_millis(20)) => {
                minutes += 1;
                assert!(minutes < 60, "the invite never answered");
                a.clock.set(a.clock.now() + Duration::from_secs(60));
            }
        }
    };
    let Err(refused) = refused else {
        panic!("the store owner's account was invited");
    };
    assert!(
        matches!(&refused, SyncError::Pending(pending) if matches!(pending.reason,
        PendingReason::Storage { failure: StorageFailure::InviteeIsStoreOwner, .. })),
        "{refused:?}"
    );
    operations.close().await.unwrap();
}

#[tokio::test]
async fn an_obsolete_held_revoke_is_followed_by_a_grant_before_completion() {
    let storage = dropbox();
    let [a, mut b, c] = accounts(storage.clone()).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_access(ACCOUNT, false, started, held)
        .await;
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    operations.remove_member(&c.member.member()).await.unwrap();
    observed.await.unwrap();
    b.sync().await;
    adopt_account(&mut b, &c.member, true).await;
    resume.send(()).unwrap();
    assert!(pending(&operations, &a.db).await.is_empty());
    assert!(joined(&storage, ACCOUNT)
        .await
        .unwrap()
        .list(&ObjectPrefix::all(store(1)))
        .await
        .is_ok());
    let requests: Vec<_> = storage
        .request_trace()
        .into_iter()
        .filter(|r| matches!(r, MemoryRequest::Other("revoke_access" | "grant_access")))
        .collect();
    assert_eq!(
        &requests[requests.len() - 2..],
        &[
            MemoryRequest::Other("revoke_access"),
            MemoryRequest::Other("grant_access")
        ]
    );
    operations.close().await.unwrap();
}

#[tokio::test]
async fn retained_provider_access_does_not_prevent_an_independent_sync_pass() {
    let storage = dropbox();
    let [a, _, c] = accounts(storage.clone()).await;
    storage
        .set_retained_access(
            ACCOUNT,
            vec![coven_storage::RetainedAccess {
                provider_id: "parent".into(),
                reason: coven_storage::RetainedAccessReason::Inherited,
            }],
        )
        .await;
    let device = a.device().await;
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    operations.remove_member(&c.member.member()).await.unwrap();
    let conditions = pending(&operations, &a.db).await;
    assert_eq!(conditions.len(), 1);
    assert!(matches!(
        conditions[0].reason,
        PendingReason::AccessRemains { .. }
    ));
    operations.sync().await.unwrap();
    assert_eq!(a.db.pending::<PendingRecord>().await.unwrap(), conditions);
    assert!(storage
        .status(&ObjectPath::positions(store(1), device))
        .await
        .unwrap()
        .is_some());
    assert!(matches!(
        operations
            .discard_pending_operation(pending_id(&conditions[0]))
            .await,
        Err(SyncError::NotPending(_))
    ));
    operations.close().await.unwrap();
}

#[tokio::test]
async fn shared_accounts_follow_the_union_of_active_members() {
    let storage = dropbox();
    let [mut a, b, mut c] = accounts(storage.clone()).await;
    c.sync
        .make_and_upload_entry(StoreChange::SetAccess {
            member: c.member.member(),
            access: coven_format::MemberAccess::ProviderAccount("BEN@example.com".into()),
        })
        .await
        .unwrap();
    a.sync().await;
    drive_access(&mut a).await;
    assert!(
        matches!(MemoryStorage::for_recipient(&storage,ACCOUNT).unwrap().list(&ObjectPrefix::all(store(1))).await,
        Err(error) if error.failure()==StorageFailure::PermissionDenied)
    );
    publish(&mut a, Command::RemoveMember(b.member.member())).await;
    assert!(a
        .db
        .provider_access("ben@example.com".into())
        .await
        .unwrap()
        .unwrap()
        .request
        .is_none());
    assert!(a.db.pending::<PendingRecord>().await.unwrap().is_empty());
    assert!(MemoryStorage::for_recipient(&storage, "ben@example.com")
        .unwrap()
        .list(&ObjectPrefix::all(store(1)))
        .await
        .is_ok());
    publish(&mut a, Command::RemoveMember(c.member.member())).await;
    drive_access(&mut a).await;
    assert!(
        matches!(MemoryStorage::for_recipient(&storage,"ben@example.com").unwrap().list(&ObjectPrefix::all(store(1))).await,
        Err(error) if error.failure()==StorageFailure::PermissionDenied)
    );
}

#[tokio::test]
async fn removing_self_returns_only_after_its_own_entry_is_kept() {
    let storage = storage();
    let mut a = device(storage, 1, member(1), store(1)).await;
    a.create(key(1)).await;
    a.add(&member(2), MemberRole::Admin).await;
    let own = a.member.member();
    publish(&mut a, Command::RemoveMember(own)).await;
    let log = a.log().await;
    assert!(log.replay.state.members[&own].removed);
    assert_eq!(
        log.replay.entries[&log.entries.last().unwrap().entry.position],
        EntryOutcome::Kept
    );
}
