use super::*;
use coven_database::{Migration, RowIdentity, SyncedTable};

async fn rows(d: &mut Device) {
    d.reopen(d.storage.clone(), vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")],
        vec![Migration::sql(1, "notes", "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, audience TEXT NOT NULL)")]).await;
}
async fn write(d: &Device, circle: CircleId, n: u128) {
    d.db.write(move |sql| {
        sql.execute(
            "INSERT INTO notes VALUES(?1,?2,?3)",
            (
                Uuid::from_u128(0x00000000000070008000000000000000 | n).to_string(),
                n.to_string(),
                circle.to_string(),
            ),
        )?;
        Ok(())
    })
    .await
    .unwrap();
}
async fn count(d: &Device) -> i64 {
    d.db.read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?))
        .await
        .unwrap()
}
async fn transfer(a: &Device, b: &Device) {
    let records = a.db.test_queued_writes().await.unwrap();
    a.writes().upload_writes().await.unwrap();
    for record in records {
        assert!(!matches!(
            b.db.apply_downloaded::<crate::PendingRecord>(
                record.into(),
                coven_database::test_utils::published(coven_format::value::StorageTime::UNIX_EPOCH)
            )
            .await
            .unwrap(),
            coven_database::ApplyOutcome::Waiting(_)
        ));
    }
}

#[tokio::test]
async fn circle_deletion_publishes_only_an_entry_and_does_not_wait_for_a_row_upload() {
    let storage = dropbox();
    let [mut a, mut b, _c] = accounts(storage).await;
    rows(&mut a).await;
    rows(&mut b).await;
    let id = begin(&mut a, Command::CreateCircle("Circle".into())).await;
    let Output::CircleId(circle) = finish(&mut a, id).await else {
        panic!()
    };
    let id = begin(&mut a, Command::AddCircleMember(circle, b.member.member())).await;
    finish(&mut a, id).await;
    b.sync().await;
    write(&a, circle, 1).await;
    transfer(&a, &b).await;
    assert_eq!(count(&b).await, 1);
    let deletion = begin(&mut a, Command::DeleteCircle(circle)).await;
    assert_eq!(count(&a).await, 1);
    assert!(!a
        .log()
        .await
        .replay
        .state
        .circle_deletions
        .contains_key(&circle));
    assert_eq!(a.db.operations().await.unwrap().len(), 1);
    assert_eq!(a.db.operations().await.unwrap()[0].last_step, 0);
    assert!(a.db.test_queued_writes().await.unwrap().is_empty());
    assert!(matches!(
        step(&mut a, deletion).await.unwrap(),
        Progress::Advanced
    ));
    finish(&mut a, deletion).await;
    assert_eq!(count(&a).await, 0);
    assert_eq!(count(&b).await, 1);
    assert!(a.db.test_queued_writes().await.unwrap().is_empty());
    b.sync().await;
    assert!(b
        .log()
        .await
        .replay
        .state
        .circle_deletions
        .contains_key(&circle));
}

#[tokio::test]
async fn dropping_circle_deletion_restores_existing_and_arriving_rows() {
    let [mut a, mut b, _c] = accounts(dropbox()).await;
    rows(&mut a).await;
    rows(&mut b).await;
    let create = begin(&mut a, Command::CreateCircle("Gifts".into())).await;
    let Output::CircleId(circle) = finish(&mut a, create).await else {
        panic!("created circle")
    };
    let add = begin(&mut a, Command::AddCircleMember(circle, b.member.member())).await;
    finish(&mut a, add).await;
    b.sync().await;
    write(&a, circle, 1).await;
    let first = a.db.test_queued_writes().await.unwrap()[0].header.position;
    transfer(&a, &b).await;

    // Ana fixes a concurrent store removal before Ben publishes his deletion.
    queue_removal(&mut a, Command::RemoveMember(b.member.member())).await;
    let deletion = begin(&mut b, Command::DeleteCircle(circle)).await;
    finish(&mut b, deletion).await;
    assert_eq!(count(&b).await, 0);
    assert!(b.db.test_queued_writes().await.unwrap().is_empty());

    // Ana has not read the deletion; Ben receives her second row while hidden.
    write(&a, circle, 2).await;
    let second = a.db.test_queued_writes().await.unwrap()[0].header.position;
    transfer(&a, &b).await;
    assert_eq!(count(&b).await, 0);
    let entry = b.log().await.replay.state.circle_deletions[&circle];
    let losses = b.db.lost_values().await.unwrap();
    assert_eq!(losses.len(), 2);
    for (loss, setter) in losses.iter().zip([first, second]) {
        assert_eq!(
            loss.replaced_by,
            coven_database::Replacement::Rules(vec![coven_database::RemovalRule::DeletedCircle {
                entry
            }])
        );
        let coven_database::Lost::Row(cells) = &loss.lost else {
            panic!("circle deletion retains each row")
        };
        assert!(cells.iter().all(|cell| cell.set_by == setter));
    }

    a.sync().await;
    drive_access(&mut a).await;
    assert_eq!(count(&a).await, 2);
    assert!(a.db.lost_values().await.unwrap().is_empty());
    assert!(a.db.test_queued_writes().await.unwrap().is_empty());
    rows(&mut a).await;
    assert_eq!(count(&a).await, 2);
}

#[tokio::test]
async fn circle_deletion_survives_an_earlier_concurrent_circle_removal() {
    let storage = dropbox();
    let [mut a, mut b, _c] = accounts(storage).await;
    rows(&mut a).await;
    rows(&mut b).await;
    let id = begin(&mut a, Command::CreateCircle("Circle".into())).await;
    let Output::CircleId(circle) = finish(&mut a, id).await else {
        panic!()
    };
    let id = begin(&mut a, Command::AddCircleMember(circle, b.member.member())).await;
    finish(&mut a, id).await;
    b.sync().await;
    write(&a, circle, 1).await;
    transfer(&a, &b).await;
    // Ben knows another row that Ana has not read.
    write(&b, circle, 2).await;
    // Ben fixes his earlier removal before Ana publishes her deletion.
    queue_removal(
        &mut b,
        Command::RemoveCircleMember(circle, a.member.member()),
    )
    .await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let deletion = begin(&mut a, Command::DeleteCircle(circle)).await;
    transfer(&a, &b).await;
    assert!(matches!(
        step(&mut a, deletion).await.unwrap(),
        Progress::Advanced
    ));
    let delete_entry = a.db.local_store_log().await.unwrap().upload.unwrap().entry;
    finish(&mut a, deletion).await;
    transfer(&b, &a).await;
    assert_eq!(count(&a).await, 0); // replay's deleted-circle rule hides Ben's row
    b.sync().await;
    a.sync().await;
    assert!(matches!(
        a.log().await.replay.entries[&delete_entry.position],
        coven_database::EntryOutcome::Kept
    ));
    assert_eq!(count(&a).await, 0);
    assert_eq!(count(&b).await, 0);
}

#[tokio::test]
async fn concurrent_circle_removal_keeps_deletion_entry_only() {
    let storage = dropbox();
    let [mut a, mut b, c] = accounts(storage).await;
    rows(&mut a).await;
    rows(&mut b).await;
    let id = begin(&mut a, Command::CreateCircle("Circle".into())).await;
    let Output::CircleId(circle) = finish(&mut a, id).await else {
        panic!()
    };
    for member in [&b.member, &c.member] {
        let id = begin(&mut a, Command::AddCircleMember(circle, member.member())).await;
        finish(&mut a, id).await;
    }
    b.sync().await;
    write(&a, circle, 1).await;
    transfer(&a, &b).await;
    write(&b, circle, 2).await;
    queue_removal(
        &mut b,
        Command::RemoveCircleMember(circle, c.member.member()),
    )
    .await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let deletion = begin(&mut a, Command::DeleteCircle(circle)).await;
    transfer(&a, &b).await;
    step(&mut a, deletion).await.unwrap(); // fix deletion against the original members
    b.sync().await;
    // Carol's copy exposes the circle key; Ben's pass replaces it before the
    // first attempt of his earlier write (§11).
    b.sync
        .rotate_if_exposed(coven_merge::Audience::Circle(circle), &b.member.member())
        .await
        .unwrap();
    transfer(&b, &a).await;
    assert_eq!(count(&a).await, 2);
    finish(&mut a, deletion).await;
    assert_eq!(count(&a).await, 0);
    assert!(a.db.test_queued_writes().await.unwrap().is_empty());
    b.sync().await;
    assert_eq!(count(&b).await, 0);
}

#[tokio::test]
async fn resumed_removal_lists_original_and_replaced_access() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Member).await;
    a.add(&c.member, MemberRole::Admin).await;
    b.sync().await;
    c.sync().await;
    b.sync.load_new_device("Ben", None).await.unwrap();
    c.sync.load_new_device("Carol", None).await.unwrap();
    a.sync().await;
    queue_removal(&mut a, Command::RemoveMember(b.member.member())).await;
    b.sync
        .make_and_upload_entry(StoreChange::SetAccess {
            member: b.member.member(),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "replacement-key".into(),
                principal: coven_format::S3Principal::Console,
            },
        })
        .await
        .unwrap();
    c.sync().await;
    publish(&mut c, Command::RemoveMember(b.member.member())).await;
    a.sync().await;
    // Both admins list both: the replaced key from its recording, the
    // replacement since the removal (§13).
    for device in [&a, &c] {
        assert_eq!(
            access_keys(&device.db).await,
            ["fixture-access-key", "replacement-key"].map(|key| console_key(key, Some(&b.member)))
        );
    }
}

#[tokio::test]
async fn late_access_survives_restart_and_cannot_revive_confirmed_keys() {
    for creator in [false, true] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(2), store(1)).await;
        a.create(key(1)).await;
        a.add(&b.member, MemberRole::Admin).await;
        b.sync().await;
        b.sync.load_new_device("Ben", None).await.unwrap();
        a.sync().await;
        let (target, observer) = if creator {
            (&mut a, &mut b)
        } else {
            (&mut b, &mut a)
        };
        queue_removal(observer, Command::RemoveMember(target.member.member())).await;
        let replacement = StoreChange::SetAccess {
            member: target.member.member(),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "late-key".into(),
                principal: coven_format::S3Principal::Console,
            },
        };
        let entry = target
            .sync
            .make_and_upload_entry(replacement.clone())
            .await
            .unwrap();
        let path = object::path(store(1), entry);
        let bytes = storage.read(&path).await.unwrap();
        storage.delete(&path).await.unwrap();
        observer.sync().await;
        assert_eq!(
            access_keys(&observer.db).await,
            [console_key("fixture-access-key", Some(&target.member))]
        );
        observer
            .db
            .clear_access_key("fixture-access-key".into())
            .await
            .unwrap();
        observer.restart(storage.clone()).await;
        storage.create(&path, &bytes).await.unwrap();
        observer.sync().await;
        assert!(matches!(
            observer.log().await.replay.entries[&entry],
            coven_database::EntryOutcome::Dropped(_)
        ));
        // Applying the late entry records its key with the replay; the removal
        // already finished and no other operation is needed.
        let expected = [console_key("late-key", Some(&target.member))];
        assert_eq!(access_keys(&observer.db).await, expected);
        observer.restart(storage.clone()).await;
        observer.sync().await;
        assert_eq!(access_keys(&observer.db).await, expected);
        observer
            .db
            .clear_access_key("late-key".into())
            .await
            .unwrap();
        // This device has not read its removal and repeats its recorded key.
        target
            .sync
            .make_and_upload_entry(replacement)
            .await
            .unwrap();
        observer.sync().await;
        assert!(access_keys(&observer.db).await.is_empty());
        assert!(observer.db.operations().await.unwrap().is_empty());
        observer.restart(storage.clone()).await;
        observer.sync().await;
        assert!(access_keys(&observer.db).await.is_empty());
    }
}

#[tokio::test]
async fn removal_revokes_accounts_in_dropped_access_updates() {
    let storage = dropbox();
    let [mut a, mut b, _c] = accounts(storage.clone()).await;
    queue_removal(&mut a, Command::RemoveMember(b.member.member())).await;
    storage
        .grant_access("replacement@example.com")
        .await
        .unwrap();
    let replacement = b
        .sync
        .make_and_upload_entry(StoreChange::SetAccess {
            member: b.member.member(),
            access: coven_format::MemberAccess::ProviderAccount("replacement@example.com".into()),
        })
        .await
        .unwrap();
    a.sync().await;
    drive_access(&mut a).await;
    assert!(matches!(
        a.log().await.replay.entries[&replacement],
        coven_database::EntryOutcome::Dropped(_)
    ));
    for account in ["ben@example.com", "replacement@example.com"] {
        let recipient = MemoryStorage::for_recipient(&storage, account).unwrap();
        assert!(matches!(recipient.list(&ObjectPrefix::all(store(1))).await,
            Err(error) if error.failure() == StorageFailure::PermissionDenied));
    }
    assert!(MemoryStorage::for_recipient(&storage, "cat@example.com")
        .unwrap()
        .list(&ObjectPrefix::all(store(1)))
        .await
        .is_ok());
}

/// An addition never changes an existing member's access, so the account in a
/// dropped addition comes from a concurrent addition of the same new id
/// (`Keyed.Additions.additions_conflict_iff_different`). Removing that member
/// still takes it back (§13).
#[tokio::test]
async fn removal_revokes_the_account_in_a_dropped_concurrent_addition() {
    let storage = dropbox();
    let [mut a, mut b, _c] = accounts(storage.clone()).await;
    let dan = member(4);
    let add = |email: &str| StoreChange::AddMember {
        member: dan.member(),
        keys: public(&dan),
        role: MemberRole::Member,
        access: coven_format::MemberAccess::ProviderAccount(email.into()),
    };
    for account in ["dan@example.com", "addition@example.com"] {
        storage.grant_access(account).await.unwrap();
    }
    // Neither admin has read the other's addition.
    let first = a
        .sync
        .make_and_upload_entry(add("dan@example.com"))
        .await
        .unwrap();
    let second = b
        .sync
        .make_and_upload_entry(add("addition@example.com"))
        .await
        .unwrap();
    a.sync().await;
    let replay = a.log().await.replay;
    let dropped = [first, second]
        .iter()
        .filter(|entry| {
            matches!(
                replay.entries[entry],
                coven_database::EntryOutcome::Dropped(_)
            )
        })
        .count();
    assert_eq!(dropped, 1);
    queue_removal(&mut a, Command::RemoveMember(dan.member())).await;
    a.sync().await;
    drive_access(&mut a).await;
    for account in ["dan@example.com", "addition@example.com"] {
        let recipient = MemoryStorage::for_recipient(&storage, account).unwrap();
        assert!(matches!(recipient.list(&ObjectPrefix::all(store(1))).await,
            Err(error) if error.failure() == StorageFailure::PermissionDenied));
    }
    for account in ["ben@example.com", "cat@example.com"] {
        assert!(MemoryStorage::for_recipient(&storage, account)
            .unwrap()
            .list(&ObjectPrefix::all(store(1)))
            .await
            .is_ok());
    }
}

#[tokio::test]
async fn a_retained_grant_on_old_access_stays_pending_after_removal() {
    let storage = dropbox();
    let [mut a, mut b, _c] = accounts(storage.clone()).await;
    storage
        .grant_access("replacement@example.com")
        .await
        .unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::SetAccess {
            member: b.member.member(),
            access: coven_format::MemberAccess::ProviderAccount("replacement@example.com".into()),
        })
        .await
        .unwrap();
    a.sync().await;
    let shares = vec![coven_storage::RetainedAccess {
        provider_id: "inherited-old-account".into(),
        reason: coven_storage::RetainedAccessReason::Inherited,
    }];
    storage
        .set_retained_access("ben@example.com", shares.clone())
        .await;
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    operations.remove_member(&b.member.member()).await.unwrap();
    let conditions = pending(&operations, &a.db).await;
    assert!(
        matches!(MemoryStorage::for_recipient(&storage, "replacement@example.com")
            .unwrap()
            .list(&ObjectPrefix::all(store(1)))
            .await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
    );
    assert_eq!(conditions.len(), 1);
    assert!(
        matches!(&conditions[0].reason, PendingReason::AccessRemains { account, shares: found } if account == "ben@example.com" && *found == shares)
    );
    storage
        .set_retained_access("ben@example.com", Vec::new())
        .await;
    operations
        .retry_pending_operation(pending_id(&conditions[0]))
        .await
        .unwrap();
    assert!(
        matches!(MemoryStorage::for_recipient(&storage, "ben@example.com")
        .unwrap()
        .list(&ObjectPrefix::all(store(1)))
        .await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
    );
    operations.close().await.unwrap();
}

#[tokio::test]
async fn grants_retained_on_two_accounts_are_each_reported_and_retried_alone() {
    let storage = dropbox();
    let [mut a, mut b, _c] = accounts(storage.clone()).await;
    storage
        .grant_access("replacement@example.com")
        .await
        .unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::SetAccess {
            member: b.member.member(),
            access: coven_format::MemberAccess::ProviderAccount("replacement@example.com".into()),
        })
        .await
        .unwrap();
    a.sync().await;
    let retained = |account: &str| {
        vec![coven_storage::RetainedAccess {
            provider_id: format!("parent-of-{account}"),
            reason: coven_storage::RetainedAccessReason::Inherited,
        }]
    };
    let accounts = ["ben@example.com", "replacement@example.com"];
    for account in accounts {
        storage
            .set_retained_access(account, retained(account))
            .await;
    }
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    operations.remove_member(&b.member.member()).await.unwrap();
    // Each account is its own provider access record; the removal is done.
    let conditions = pending(&operations, &a.db).await;
    assert_eq!(
        conditions
            .iter()
            .map(|record| (pending_kind(record), record.reason.clone()))
            .collect::<Vec<_>>(),
        accounts.map(|account| (
            OperationKind::RevokeAccess,
            PendingReason::AccessRemains {
                account: account.into(),
                shares: retained(account),
            }
        ))
    );
    let ids = conditions.iter().map(pending_id).collect::<Vec<_>>();
    assert_eq!(
        a.db.operations()
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>(),
        ids
    );
    assert!(matches!(
        operations.discard_pending_operation(ids[0]).await,
        Err(SyncError::NotPending(id)) if id == ids[0]
    ));
    for (index, account) in accounts.into_iter().enumerate() {
        assert!(matches!(
            operations.retry_pending_operation(ids[index]).await,
            Err(SyncError::Pending(found)) if *found == conditions[index]
        ));
        storage.set_retained_access(account, Vec::new()).await;
        operations
            .retry_pending_operation(ids[index])
            .await
            .unwrap();
        assert_eq!(pending(&operations, &a.db).await, conditions[index + 1..]);
        assert!(matches!(MemoryStorage::for_recipient(&storage, account)
            .unwrap()
            .list(&ObjectPrefix::all(store(1)))
            .await, Err(error) if error.failure() == StorageFailure::PermissionDenied));
    }
    assert!(a.db.operations().await.unwrap().is_empty());
    operations.close().await.unwrap();
}

#[tokio::test]
async fn another_invites_revocation_cannot_revive_a_confirmed_key() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let mut invites = Vec::new();
    for _ in 0..2 {
        let operation = begin(
            &mut a,
            Command::Invite(
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                },
                InviteAccess::S3AccessKey {
                    access_key_id: "invited-key".into(),
                    secret_access_key: coven_crypto::SecretText::new("secret".into()),
                },
            ),
        )
        .await;
        let Output::Invite(invite) = finish(&mut a, operation).await else {
            panic!("invite result")
        };
        invites.push(invite);
    }
    let first = begin(&mut a, Command::Cancel(invites[0].id)).await;
    finish(&mut a, first).await;
    assert_eq!(access_keys(&a.db).await, [console_key("invited-key", None)]);
    a.db.clear_access_key("invited-key".into()).await.unwrap();
    a.restart(storage.clone()).await;
    let second = begin(&mut a, Command::Cancel(invites[1].id)).await;
    finish(&mut a, second).await;
    assert!(access_keys(&a.db).await.is_empty());
}
