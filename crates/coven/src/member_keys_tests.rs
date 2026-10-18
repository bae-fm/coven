use super::*;

/// Carol joins (the request she is approved by names her exact pair), then
/// replaces her own keys through the public call: her id, role and device
/// stay, members show the new pair live, her restore code carries it and the
/// old code no longer restores. Ana, an admin, then publishes a pair Carol
/// prepared elsewhere: Carol's installation reports the replacement, and a
/// code holding the prepared pair restores her on a new device.
#[tokio::test]
async fn member_keys_are_replaced_through_the_public_calls() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let first = Installation::new();
    let (_, cancel) = watch::channel(false);
    let joining = first.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let requested = std::sync::Mutex::new(None);
    let (result, carol) = join_and_approve(&owner.handle, joining, |request| {
        assert_eq!(request.member, invite.member);
        *requested.lock().unwrap() = Some(request.keys);
        async {}
    })
    .await;
    let handle = result.unwrap().unwrap();
    let old_code = handle.restore_code().await.unwrap();
    let old = coven_sync::read_restore_code(&old_code).unwrap();
    assert_eq!(old.member, carol);
    assert_eq!(
        decode_code_info(&old_code).unwrap().metadata().member,
        carol
    );
    assert_eq!(
        decode_code_info(&invite.code).unwrap().metadata().member,
        carol
    );
    assert_eq!(
        Some(old.member_keys.public_keys()),
        *requested.lock().unwrap()
    );
    let mut members = handle.subscribe_members();
    let initial = members.next().await.unwrap();
    let mine = |list: &[MemberInfo]| list.iter().find(|m| m.id == carol).unwrap().clone();
    assert_eq!(mine(&initial).keys, old.member_keys.public_keys());
    // The keychain syncs Carol's code to her other Apple devices.
    let synced = || {
        let bytes = StoreKeychain::new(first.keychain.clone(), owner.directory.id())
            .synced_restore_code()
            .unwrap()
            .unwrap();
        RestoreCode::from_bytes(bytes.as_bytes())
            .unwrap()
            .member_keys
            .public_keys()
    };
    assert_eq!(synced(), old.member_keys.public_keys());
    let returned = handle.replace_member_keys().await.unwrap();
    assert_eq!(returned.as_str(), handle.restore_code().await.unwrap());
    let code = coven_sync::read_restore_code(returned.as_str()).unwrap();
    assert_eq!(code.member, carol);
    let replaced = code.member_keys.public_keys();
    assert_ne!(replaced, old.member_keys.public_keys());
    assert_eq!(synced(), replaced);
    let current = loop {
        let list = members.next().await.unwrap();
        if mine(&list).keys == replaced {
            break mine(&list);
        }
    };
    let before = mine(&initial);
    assert_eq!(
        (current.role, current.devices.clone()),
        (before.role, before.devices.clone())
    );
    assert!(handle
        .pending()
        .await
        .unwrap()
        .iter()
        .all(|record| !matches!(record.subject, PendingSubject::Operation { .. })));
    handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('carol',x'01')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    owner.sync().await;
    let seen = owner.handle.members().await.unwrap();
    assert_eq!(mine(&seen).keys, replaced);
    let carol_key = || S3Credentials {
        access_key_id: "invited-key".into(),
        secret_access_key: SecretText::new("invited-secret".into()),
    };
    // The old code cannot bind its pair back.
    let stale = Installation::new();
    let restored = restore_from_code(
        stale.builder(&owner, owner.storage.clone()),
        &old_code,
        Some(carol_key()),
        "Carol's old code",
        |_| {},
        &cancel,
    )
    .await;
    assert!(
        matches!(&restored, Err(BootstrapError::Sync(SyncError::MemberKeysReplaced { member })) if *member == carol),
        "{:?}",
        restored.as_ref().err()
    );
    stale.absent(owner.directory.id()).await;
    // Ana publishes a pair Carol prepared; no private key reaches Ana.
    let prepared = MemberKeys::generate().unwrap();
    owner
        .handle
        .replace_member_keys_for(&carol, prepared.public_keys())
        .await
        .unwrap();
    assert!(matches!(
        owner
            .handle
            .replace_member_keys_for(&carol, prepared.public_keys())
            .await,
        Err(SyncError::Rejected(OperationRejection::KeysAlreadyUsed))
    ));
    let stopped = handle.circles().create("Garden").await;
    assert!(
        matches!(&stopped, Err(SyncError::MemberKeysReplaced { member }) if *member == carol),
        "{stopped:?}"
    );
    assert!(matches!(
        handle.restore_code().await,
        Err(SyncError::MemberKeysReplaced { member }) if member == carol
    ));
    handle.close().await.unwrap();
    // A code holding the prepared pair restores Carol on a new device.
    let fresh = RestoreCode {
        member_keys: prepared,
        ..code
    };
    let next = Installation::new();
    let restored = restore_from_code(
        next.builder(&owner, owner.storage.clone()),
        &fresh.to_text().unwrap(),
        Some(carol_key()),
        "Carol's new phone",
        |_| {},
        &cancel,
    )
    .await
    .unwrap();
    let list = restored.members().await.unwrap();
    let info = mine(&list);
    assert!(info.is_self);
    assert_eq!(info.keys, fresh.member_keys.public_keys());
    assert_eq!(
        info.devices
            .iter()
            .filter(|d| d.state == DeviceState::Active)
            .count(),
        2
    );
    restored.close().await.unwrap();
    owner.close().await;
}

/// An owned row's owner is a member id, which a key replacement keeps (§9.6, §10).
mod owner_identity {
    use crate::tests::Network;
    use crate::*;

    const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)";

    async fn owned_notes() -> Network {
        Network::with_schema(
            CloudProvider::S3,
            vec![SyncedTable::new("notes", RowIdentity::SharedKey).owner_only()],
            vec![Migration::addition(1, "notes", NOTES)],
        )
        .await
    }

    async fn own_id(handle: &CovenHandle) -> MemberId {
        handle
            .members()
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.is_self)
            .unwrap()
            .id
    }

    async fn title(handle: &CovenHandle) -> String {
        handle
            .read(|sql| {
                Ok(sql.query_row("SELECT title FROM notes WHERE id='carol'", [], |r| r.get(0))?)
            })
            .await
            .unwrap()
    }

    async fn rename(handle: &CovenHandle, title: &'static str) -> CovenResult<()> {
        handle
            .write(move |sql| {
                sql.execute("UPDATE notes SET title=?1 WHERE id='carol'", [title])?;
                Ok(())
            })
            .await
    }

    /// The refusal of a direct edit names the row's owner.
    fn refused_naming(result: CovenResult<()>, owner: MemberId) {
        match result {
            Err(CovenError::Database(DbError::NotOwner {
                table,
                owner: named,
                ..
            })) => {
                assert_eq!((table.as_str(), named), ("notes", owner));
            }
            other => panic!("expected NotOwner, got {other:?}"),
        }
    }

    /// Carol creates a note, then loses her only installation. Ana's recovery
    /// invite binds fresh keys to Carol's id on a new device. The note stays
    /// Carol's: her new device edits it, Ana's edits are refused naming the same
    /// member before and after, and Ana's device applies Carol's edit.
    #[tokio::test]
    async fn owned_rows_keep_their_member_across_key_replacement() {
        let mut network = owned_notes().await;
        network
            .join_as(
                1,
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                },
            )
            .await;
        let carol = own_id(&network.devices[1].handle).await;
        network.devices[1]
            .handle
            .write(|sql| {
                sql.execute("INSERT INTO notes VALUES('carol','first')", [])?;
                Ok(())
            })
            .await
            .unwrap();
        network.sync(1).await;
        network.sync(0).await;
        let ana = &network.devices[0].handle;
        assert_eq!(title(ana).await, "first");
        refused_naming(rename(ana, "ana's").await, carol);
        let before = ana
            .members()
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.id == carol)
            .unwrap()
            .keys;
        network
            .join_as(2, InviteTarget::ReplaceMemberKeys { member: carol })
            .await;
        let replaced = &network.devices[2].handle;
        assert_eq!(own_id(replaced).await, carol);
        network.sync(0).await;
        let ana = &network.devices[0].handle;
        let after = ana
            .members()
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.id == carol)
            .unwrap()
            .keys;
        assert_ne!(after, before);
        assert_eq!(title(replaced).await, "first");
        rename(replaced, "still hers").await.unwrap();
        network.sync(2).await;
        network.sync(0).await;
        let ana = &network.devices[0].handle;
        assert_eq!(title(ana).await, "still hers");
        refused_naming(rename(ana, "ana's").await, carol);
        network.close().await;
    }

    /// Ana's device snapshots the store holding Carol's note, and every slot
    /// it covers is deleted. Dan joins later and has only that snapshot: it
    /// carries Carol's creation with the evidence of her write, which Ana
    /// cannot sign for her, so Dan's device knows the note is Carol's and
    /// refuses Dan's edit naming her, as Ana's does (§9.6, §16.1, D7).
    #[tokio::test]
    async fn a_snapshot_carries_another_members_ownership_to_a_new_device() {
        use coven_storage::Storage;
        let mut network = owned_notes().await;
        network
            .join_as(
                1,
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                },
            )
            .await;
        let carol = own_id(&network.devices[1].handle).await;
        network.devices[1]
            .handle
            .write(|sql| {
                sql.execute("INSERT INTO notes VALUES('carol','first')", [])?;
                Ok(())
            })
            .await
            .unwrap();
        network.sync(1).await;
        network.sync(0).await;
        // Ana's large note makes her device publish a snapshot; a month
        // later the slots it covers are gone.
        let large = "x".repeat(1024 * 1024 + 1);
        network.devices[0]
            .handle
            .write(move |sql| {
                sql.execute("INSERT INTO notes VALUES('ana',?1)", [large])?;
                Ok(())
            })
            .await
            .unwrap();
        network.sync(0).await;
        network
            .clock
            .set(network.clock.now() + std::time::Duration::from_secs(31 * 24 * 3600));
        network.sync(1).await;
        network.sync(0).await;
        let store = network.devices[0].store;
        let storage = &network.devices[0].storage;
        // Aged history makes each device snapshot what it reads (§16.2).
        assert!(!crate::tests::snapshots(storage, store).await.is_empty());
        // Retention moved the floor past the slots Dan never reads.
        assert!(storage
            .status(&ObjectPath::slot(store, 1.try_into().unwrap()))
            .await
            .unwrap()
            .is_none());
        network
            .join_as(
                2,
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                },
            )
            .await;
        let dan = &network.devices[2].handle;
        assert_eq!(title(dan).await, "first");
        refused_naming(rename(dan, "dan's").await, carol);
        refused_naming(rename(&network.devices[0].handle, "ana's").await, carol);
        network.close().await;
    }
}

#[tokio::test]
async fn concurrent_replacements_return_their_own_codes() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::join!(
            owner.handle.replace_member_keys(),
            owner.handle.replace_member_keys()
        )
    })
    .await
    .unwrap();
    let first = first.unwrap();
    let second = second.unwrap();
    let a = coven_sync::read_restore_code(first.as_str()).unwrap();
    let b = coven_sync::read_restore_code(second.as_str()).unwrap();
    assert_eq!(a.member, b.member);
    assert_ne!(a.member_keys.public_keys(), b.member_keys.public_keys());
    let current = owner.handle.restore_code().await.unwrap();
    assert!(current == first.as_str() || current == second.as_str());
    owner.close().await;
}
