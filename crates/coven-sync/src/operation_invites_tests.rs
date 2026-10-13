use super::*;
use coven_crypto::ObjectHasher;
use coven_format::{
    codes::InviteCode,
    objects::JoinRequest as WireRequest,
    sealed_single::{SingleChunkObject, SingleChunkPrefix},
    Object,
};
use coven_foundation::clock::Clock;

async fn invite(d: &mut Device, account: &str) -> (OperationId, Invite) {
    let id = begin(
        d,
        Command::Invite(
            MemberRole::Member,
            InviteAccess::ProviderAccount {
                email: account.into(),
            },
        ),
    )
    .await;
    let Output::Invite(invite) = finish(d, id).await else {
        panic!()
    };
    (id, invite)
}
async fn request(storage: &dyn Storage, invite: &Invite, keys: &MemberKeys, name: &str) {
    let code = InviteCode::from_text(&invite.code).unwrap();
    let path = ObjectPath::join_request(code.store, invite.id);
    let plain = Object::JoinRequest(WireRequest {
        invite: invite.id,
        keys: public(keys),
        device_name: name.into(),
    })
    .encode()
    .unwrap();
    let prefix = SingleChunkPrefix::JoinRequest;
    let chunk = code
        .secret
        .join_request_key()
        .seal_object_chunk(path.as_str(), &prefix.encode().unwrap(), 0, 0, &plain)
        .unwrap();
    let mut hash = ObjectHasher::new();
    hash.update(&prefix.encode_chunk(&chunk).unwrap());
    let signature = keys.sign_object(path.as_str(), &hash.finish());
    let bytes = SingleChunkObject::JoinRequest {
        chunk: &chunk,
        signature,
    }
    .encode()
    .unwrap();
    storage.create(&path, &bytes).await.unwrap();
}

#[tokio::test]
async fn approve_seals_store_history_and_removes_the_request() {
    let storage = google();
    let [mut a, b, _c] = accounts(storage.clone()).await;
    let id = begin(&mut a, Command::RemoveMember(b.member.member_id())).await;
    finish(&mut a, id).await;
    a.sync.test_rotate(Audience::Store, key(2)).await;
    let expected: Vec<_> = a.custody.read().unwrap().unwrap().store_key_ids().collect();
    assert_eq!(expected.len(), 2);
    let (operation, invite) = invite(&mut a, "join@example.com").await;
    let code = InviteCode::from_text(&invite.code).unwrap();
    assert_eq!(code.initial_key, key(1));
    assert_eq!(code.inviting_writer, a.device().await);
    assert_eq!(
        code.key_copy_number,
        a.db.published_key_copy_number().await.unwrap()
    );
    assert!(code.key_copy_number > 0);
    let joining = MemoryStorage::for_recipient(&storage, "join@example.com").unwrap();
    let keys = member(9);
    request(&joining, &invite, &keys, "New phone").await;
    assert!(matches!(
        step(&mut a, operation).await.unwrap(),
        Progress::Advanced
    ));
    let requests = a.sync.current_join_requests().await.unwrap();
    assert_eq!(
        requests[0].provider_account_email.as_deref(),
        Some("join@example.com")
    );
    assert_eq!(
        begin(&mut a, Command::Approve(requests[0].clone())).await,
        operation
    );
    assert!(matches!(finish(&mut a, operation).await, Output::Unit));
    assert!(a.sync.current_join_requests().await.unwrap().is_empty());
    assert!(matches!(
        storage.read(&ObjectPath::join_request(store(1), invite.id)).await,
        Err(error) if error.failure() == StorageFailure::NotFound
    ));
    for key in expected {
        let path = copy_path(&joining, Audience::Store, key, &keys.member_id()).await;
        let (writer, number) = path.key_copy_position().unwrap();
        assert_eq!(writer, code.inviting_writer);
        assert!(number.get() > code.key_copy_number);
        assert_eq!(
            coven_format::sealed_key::SealedKey::decode(&joining.read(&path).await.unwrap())
                .unwrap()
                .open_store_key(&keys, path.as_str())
                .unwrap()
                .id(),
            key
        );
    }
    let state = a.log().await.replay.state;
    assert_eq!(
        state.members[&keys.member_id()].access,
        coven_format::MemberAccess::ProviderAccount("join@example.com".into())
    );
    assert!(!state.devices.values().any(|d| d.member == keys.member_id())); // Joining device adds itself later.
}

#[tokio::test]
async fn invite_cursor_excludes_reserved_copies_until_the_invites_catch_up_publishes_them() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(9)).await;
    a.sync.test_rotate(Audience::Store, key(2)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    b.sync().await;
    b.sync.load_new_device("Second phone").await.unwrap();
    assert_eq!(a.db.published_key_copy_number().await.unwrap(), 2);
    assert_eq!(b.db.published_key_copy_number().await.unwrap(), 0);
    for (d, expected) in [(&mut a, 2), (&mut b, 0)] {
        let fixed_key = d
            .custody
            .read()
            .unwrap()
            .unwrap()
            .store_key(key(9))
            .unwrap()
            .clone();
        let recipient = d.member.clone();
        let reserved =
            d.db.prepare_key_upload(move |path| {
                keys::store_copy(
                    path,
                    &fixed_key,
                    &recipient.member_id(),
                    &recipient.sealing_public_key(),
                )
            })
            .await
            .unwrap();
        assert_eq!(reserved.number.get(), expected + 1);
        // Reserved bytes are not a publication.
        assert_eq!(d.db.published_key_copy_number().await.unwrap(), expected);
        d.restart(storage.clone()).await;
        let operation = begin(
            d,
            Command::Invite(
                MemberRole::Member,
                InviteAccess::S3AccessKey {
                    access_key_id: "new-key".into(),
                    secret_access_key: coven_crypto::SecretText::new("secret".into()),
                },
            ),
        )
        .await;
        let Output::Invite(invite) = finish(d, operation).await else {
            panic!()
        };
        let code = InviteCode::from_text(&invite.code).unwrap();
        assert_eq!(code.initial_key, key(9));
        assert_eq!(code.inviting_writer, d.device().await);
        // The invite's catch-up publishes the reserved copy before the code is made.
        assert_eq!(code.key_copy_number, reserved.number.get());
        assert!(d.db.key_uploads().await.unwrap().is_empty());
        assert_eq!(
            d.db.published_key_copy_number().await.unwrap(),
            reserved.number.get()
        );
    }
}

#[tokio::test]
async fn decline_cancel_and_expiry_take_access_back() {
    for settle in ["decline", "cancel", "expiry"] {
        let storage = google();
        let [mut a, _b, _c] = accounts(storage.clone()).await;
        let (operation, invite) = invite(&mut a, "join@example.com").await;
        let joining = MemoryStorage::for_recipient(&storage, "join@example.com").unwrap();
        assert!(joining
            .list(&ObjectPrefix::store_logs(store(1)))
            .await
            .is_ok());
        request(&joining, &invite, &member(9), "New phone").await;
        step(&mut a, operation).await.unwrap();
        match settle {
            "decline" => {
                let request = a.sync.current_join_requests().await.unwrap().remove(0);
                begin(&mut a, Command::Decline(request)).await;
            }
            "cancel" => {
                begin(&mut a, Command::Cancel(invite.id)).await;
            }
            "expiry" => {
                a.clock.set(invite.expires_at);
                assert!(matches!(
                    step(&mut a, operation).await.unwrap(),
                    Progress::Reply(Err(SyncError::InvitationChanged))
                ));
            }
            _ => unreachable!(),
        }
        finish(&mut a, operation).await;
        assert!(
            matches!(joining.list(&ObjectPrefix::store_logs(store(1))).await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
        );
        assert!(matches!(
            storage.read(&ObjectPath::join_request(store(1), invite.id)).await,
            Err(error) if error.failure() == StorageFailure::NotFound
        ));
        assert!(!a
            .log()
            .await
            .replay
            .state
            .members
            .contains_key(&member(9).member_id()));
    }
}

#[tokio::test]
async fn another_open_invite_or_member_preserves_shared_account_access() {
    let storage = google();
    let [mut a, _b, _c] = accounts(storage.clone()).await;
    let (first, invite1) = invite(&mut a, "join@example.com").await;
    let (second, invite2) = invite(&mut a, "JOIN@example.com").await;
    begin(&mut a, Command::Cancel(invite1.id)).await;
    finish(&mut a, first).await;
    let joining = MemoryStorage::for_recipient(&storage, "join@example.com").unwrap();
    assert!(joining
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .is_ok());
    begin(&mut a, Command::Cancel(invite2.id)).await;
    finish(&mut a, second).await;
    assert!(
        matches!(joining.list(&ObjectPrefix::store_logs(store(1))).await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
    );
    let (operation, invite) = invite(&mut a, "ben@example.com").await;
    begin(&mut a, Command::Cancel(invite.id)).await;
    finish(&mut a, operation).await;
    assert!(MemoryStorage::for_recipient(&storage, "ben@example.com")
        .unwrap()
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .is_ok());
}

#[tokio::test]
async fn s3_invitation_carries_its_key_and_retains_the_manual_deletion_notice() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let id = begin(
        &mut a,
        Command::Invite(
            MemberRole::Member,
            InviteAccess::S3AccessKey {
                access_key_id: "new-key".into(),
                secret_access_key: coven_crypto::SecretText::new("secret".into()),
            },
        ),
    )
    .await;
    let Output::Invite(invite) = finish(&mut a, id).await else {
        panic!()
    };
    let code = InviteCode::from_text(&invite.code).unwrap();
    let coven_storage::InviteStorage::S3 { credentials, .. } =
        coven_storage::InviteStorage::decode(code.storage.as_bytes()).unwrap()
    else {
        panic!()
    };
    assert_eq!(credentials.secret_access_key.as_str(), "secret");
    begin(&mut a, Command::Cancel(invite.id)).await;
    finish(&mut a, id).await;
    a.restart(storage).await;
    assert_eq!(
        a.db.access_keys_to_delete().await.unwrap(),
        [AccessKeyToDelete {
            access_key_id: "new-key".into(),
            member: None
        }]
    );
}

#[tokio::test]
async fn replacement_request_cannot_be_approved_from_a_stale_prompt() {
    let storage = google();
    let [mut a, _b, _c] = accounts(storage.clone()).await;
    let (operation, invite) = invite(&mut a, "join@example.com").await;
    let joining = MemoryStorage::for_recipient(&storage, "join@example.com").unwrap();
    request(&joining, &invite, &member(9), "Phone").await;
    step(&mut a, operation).await.unwrap();
    let previous = a.sync.current_join_requests().await.unwrap().remove(0);
    storage
        .delete(&ObjectPath::join_request(store(1), invite.id))
        .await
        .unwrap();
    request(&joining, &invite, &member(8), "Phone").await;
    step(&mut a, operation).await.unwrap();
    assert!(matches!(
        a.sync
            .begin_operation_call(Command::Approve(previous))
            .await,
        Err(SyncError::InvitationChanged)
    ));
    begin(&mut a, Command::Cancel(invite.id)).await;
    finish(&mut a, operation).await;
}

pub(super) struct RetryClock {
    pub(super) time: Arc<FixedClock>,
    pub(super) sleeps: tokio::sync::watch::Sender<usize>,
}

impl Clock for RetryClock {
    fn now(&self) -> std::time::SystemTime {
        self.time.now()
    }
    fn sleep(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let sleep = self.time.sleep(duration);
        self.sleeps.send_modify(|count| *count += 1);
        sleep
    }
}

#[tokio::test]
async fn failed_join_request_still_expires_and_live_requests_are_delivered() {
    let storage = google();
    let [mut a, _b, _c] = accounts(storage.clone()).await;
    let (sleeps, mut sleeping) = tokio::sync::watch::channel(0);
    let clock = Arc::new(RetryClock {
        time: a.clock.clone(),
        sleeps,
    });
    a.sync.clock = clock.clone();
    let files = file_owner(&a);
    let operations = { operation_owner(a.sync, files) };
    let mut joins = operations.subscribe_join_requests();
    tokio::time::timeout(
        Duration::from_secs(5),
        sleeping.wait_for(|count| *count == 2),
    )
    .await
    .expect("operation worker did not schedule its injected retry")
    .unwrap();
    let invite = operations
        .create_invite(
            MemberRole::Member,
            InviteAccess::ProviderAccount {
                email: "join@example.com".into(),
            },
        )
        .await
        .unwrap();
    let joining = MemoryStorage::for_recipient(&storage, "join@example.com").unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_millis(1500));
    operations.get_members().await.unwrap();
    assert_eq!(
        *sleeping.borrow(),
        2,
        "a command must not postpone the retry"
    );
    request(&joining, &invite, &member(9), "Phone").await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    tokio::time::timeout(Duration::from_secs(10), joins.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(joins.borrow()[0].member, member(9).member_id());
    storage
        .delete(&ObjectPath::join_request(store(1), invite.id))
        .await
        .unwrap();
    storage
        .create(&ObjectPath::join_request(store(1), invite.id), &[0])
        .await
        .unwrap();
    assert_eq!(pending(&operations, &a.db).await.len(), 1);
    a.clock.set(invite.expires_at);
    assert!(pending(&operations, &a.db).await.is_empty());
    assert!(
        matches!(joining.list(&ObjectPrefix::store_logs(store(1))).await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
    );
    assert!(matches!(
        storage.read(&ObjectPath::join_request(store(1), invite.id)).await,
        Err(error) if error.failure() == StorageFailure::NotFound
    ));
    operations.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_keeps_retained_provider_grants_visible_until_acknowledged() {
    let storage = google();
    let [a, _b, _c] = accounts(storage.clone()).await;
    let files = file_owner(&a);
    let operations = { operation_owner(a.sync, files) };
    let invite = operations
        .create_invite(
            MemberRole::Member,
            InviteAccess::ProviderAccount {
                email: "join@example.com".into(),
            },
        )
        .await
        .unwrap();
    storage
        .set_retained_access(
            "join@example.com",
            vec![coven_storage::RetainedAccess {
                provider_id: "parent-share".into(),
                reason: coven_storage::RetainedAccessReason::Inherited,
            }],
        )
        .await;
    assert!(matches!(
        operations.cancel_invite(&invite.id).await,
        Err(SyncError::AccessRemains(_))
    ));
    let conditions = pending(&operations, &a.db).await;
    assert_eq!(conditions.len(), 1);
    storage
        .set_retained_access("join@example.com", Vec::new())
        .await;
    operations
        .retry_pending_operation(pending_id(&conditions[0]))
        .await
        .unwrap();
    assert!(a.db.operations().await.unwrap().is_empty());
    operations.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_invite_is_retained_and_expiry_finishes_on_reconnect() {
    let storage = google();
    let [a, _b, _c] = accounts(storage.clone()).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_listing(ObjectPrefix::store_logs(store(1)), started, held)
        .await;
    let files = file_owner(&a);
    let operations = { operation_owner(a.sync, files) };
    let mut pending = Box::pin(operations.create_invite(
        MemberRole::Member,
        InviteAccess::ProviderAccount {
            email: "join@example.com".into(),
        },
    ));
    std::future::poll_fn(|cx| {
        use std::future::Future;
        assert!(pending.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    observed.await.unwrap();
    // Let the preflight finish, then interrupt the journaled grant's catch-up.
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume_grant, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_listing(ObjectPrefix::store_logs(store(1)), started, held)
        .await;
    resume.send(()).unwrap();
    observed.await.unwrap();
    assert_eq!(a.db.operations().await.unwrap().len(), 1);
    storage.set_online(false);
    resume_grant.send(()).unwrap();
    operations.get_members().await.unwrap();
    assert!(matches!(
        pending.await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network
    ));
    assert_eq!(
        a.db.pending::<PendingRecord>().await.unwrap()[0].reason,
        PendingReason::Storage {
            provider: coven_storage::CloudProvider::GoogleDrive,
            failure: StorageFailure::Network,
        }
    );
    a.clock.set(UNIX_EPOCH + Duration::from_secs(86402));
    storage.set_online(true);
    operations.set_storage(Some(storage.clone())).await.unwrap();
    assert!(a.db.operations().await.unwrap().is_empty());
    operations.close().await.unwrap();
}
