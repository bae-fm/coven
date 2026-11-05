use crate::sequence_sync::tests::*;
use crate::*;
use coven_database::DbError;
use coven_format::admin::{MemberRole, StoreChange};
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::id_source::{IdSource, KeyId};
use coven_merge::Audience;
use coven_storage::{test_utils::Faults, ObjectPath, Storage, StorageFailure};

#[tokio::test]
async fn the_provider_owner_cannot_be_removed_or_claim_a_removal() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    ana.sync
        .administer(StoreChange::AddMember {
            member: ben.identity.member(),
            keys: ben.identity.keys().public_keys(),
            role: MemberRole::Admin,
            access: coven_format::MemberAccess::ProviderAccount("ben@example.org".into()),
        })
        .await
        .unwrap();
    ben.sync
        .administer(StoreChange::AddDevice { name: "Ben".into(), replaces: None })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let before = ana.sync.database.applied_slot().await.unwrap();
    for initiator in [&ana, &ben] {
        let counter = initiator.sync.database.claim_counter().await.unwrap();
        let result = initiator
            .sync
            .administer(StoreChange::RemoveMember { member: ana.identity.member() })
            .await;
        assert!(matches!(result, Err(SyncError::StoreOwner)), "{result:?}");
        assert_eq!(initiator.sync.database.applied_slot().await.unwrap(), before);
        assert_eq!(initiator.sync.database.claim_counter().await.unwrap(), counter);
        assert!(initiator.sync.database.current_claim().await.unwrap().is_none());
        assert!(initiator.sync.database.operations().await.unwrap().is_empty());
    }
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, (before + 1).try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn abandoned_preparation_deletion_preserves_reader_progress() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    ben.sync.pass().await.unwrap();
    let before = carol.sync.database.applied_slot().await.unwrap();
    let (upload_started, upload_observed) = tokio::sync::oneshot::channel();
    let (upload_resume, upload_held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::all(group.store),
            upload_started,
            upload_held,
        )
        .await;
    let prepare = ana.sync.reset(coven_merge::Audience::Store);
    let remove = async {
        upload_observed.await.unwrap();
        let (announce_started, announce_observed) = tokio::sync::oneshot::channel();
        let (announce_resume, announce_held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(
                coven_storage::ObjectPrefix::all(group.store),
                announce_started,
                announce_held,
            )
            .await;
        upload_resume.send(()).unwrap();
        announce_observed.await.unwrap();
        let (activate_started, activate_observed) = tokio::sync::oneshot::channel();
        let (activate_resume, activate_held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(
                coven_storage::ObjectPrefix::all(group.store),
                activate_started,
                activate_held,
            )
            .await;
        announce_resume.send(()).unwrap();
        activate_observed.await.unwrap();
        ben.sync
            .administer(StoreChange::RemoveMember { member: ana.identity.member() })
            .await
            .unwrap();
        activate_resume.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(HANG_GUARD, async { tokio::join!(prepare, remove) })
        .await
        .expect("publication controls must finish");
    assert!(
        matches!(result, Err(SyncError::Rejected(OperationRejection::NotAllowed))),
        "{result:?}"
    );
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    let session = ben.sync.session.lock().await;
    let (announced, announcement) = session.history.announcements.last_key_value().unwrap();
    assert!(announced.0 > before);
    let announced = *announced;
    let path = crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_none(), "undo must delete its artifact");
    assert!(group.storage.status(&ObjectPath::floor(group.store)).await.unwrap().is_none());
    drop(session);
    let result = carol.sync.pass().await;
    assert!(
        result.is_ok(),
        "honest undo must preserve Carol's ability to reach the head: {result:?}"
    );
    assert!(carol.sync.database.applied_slot().await.unwrap() > announced.0);
    assert!(
        matches!(&*carol.sync.state.subscribe().borrow(),SequenceStatus::Synced {through} if *through > announced.0)
    );
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
    carol.sync.database.close().await.unwrap();
}

/// The person deletes S3 keys in the provider's console: replacing a
/// member's key and removing the member record nothing about the keys they
/// leave behind, on the author's device or any other (§14.3).
#[tokio::test]
async fn replaced_and_removed_s3_keys_leave_no_work_behind() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    ana.sync
        .administer(StoreChange::SetAccess {
            member: ben.identity.member(),
            access: access("ben-new"),
        })
        .await
        .unwrap();
    ana.sync.administer(StoreChange::RemoveMember { member: ben.identity.member() }).await.unwrap();
    carol.sync.pass().await.unwrap();
    for device in [&ana, &carol] {
        assert!(device.sync.database.operations().await.unwrap().is_empty());
        assert!(device.sync.database.pending::<PendingRecord>().await.unwrap().is_empty());
    }
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn provider_access_stays_until_the_last_member_using_the_account_is_removed() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.storage.grant_access("shared@example.org").await.unwrap();
    for member in [&ben, &carol] {
        ana.sync
            .administer(StoreChange::AddMember {
                member: member.identity.member(),
                keys: member.identity.keys().public_keys(),
                role: MemberRole::Member,
                access: coven_format::MemberAccess::ProviderAccount("shared@example.org".into()),
            })
            .await
            .unwrap();
    }
    ana.sync.administer(StoreChange::RemoveMember { member: ben.identity.member() }).await.unwrap();
    assert!(ana
        .sync
        .database
        .provider_access("shared@example.org".into())
        .await
        .unwrap()
        .unwrap()
        .request
        .is_none());
    ana.sync
        .administer(StoreChange::RemoveMember { member: carol.identity.member() })
        .await
        .unwrap();
    let before =
        ana.sync.database.provider_access("shared@example.org".into()).await.unwrap().unwrap();
    assert!(!before.intended);
    assert!(before.request.is_some());
    ana.sync.pass().await.unwrap();
    let after =
        ana.sync.database.provider_access("shared@example.org".into()).await.unwrap().unwrap();
    assert_eq!(after.granted, Some(false));
    assert!(after.request.is_none());
    assert_eq!(
        ana.storage
            .request_trace()
            .iter()
            .filter(|request| matches!(
                request,
                coven_storage::test_utils::MemoryRequest::Other("revoke_access")
            ))
            .count(),
        1
    );
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn creating_a_circle_does_not_require_a_snapshot_of_nonexistent_history() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let circle = coven_foundation::id_source::CircleId(group.ids.new_id());
    let key = coven_crypto::CircleKey::from_bytes(circle, KeyId(group.ids.new_id()), [9; 32]);
    let mut ring = ana.sync.keys.read().unwrap().unwrap();
    let key_id = key.id();
    let hash = key.hash();
    ring.insert_circle_key(key).unwrap();
    ana.sync.keys.persist(&ring).unwrap();
    ana.sync
        .administer(StoreChange::CreateCircle {
            circle,
            name: "Gifts".into(),
            key: key_id,
            key_hash: hash,
            breaking_version: 0,
        })
        .await
        .unwrap();
    assert!(ana
        .sync
        .database
        .store_state()
        .await
        .unwrap()
        .reads_circle(circle, &ana.identity.member()));
    assert!(ana.sync.database.reloading_audiences().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
}

async fn offline_circle_write(compare: bool) {
    use serde_json::json;
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    pin(ben.database(), "00000000-0000-4000-8000-000000000001", circle, "before removal").await;
    let originals = ben.database().waiting_snapshot_headers().await.unwrap();
    let original = originals
        .iter()
        .find(|header| {
            header.parts.iter().any(|part| part.audience == coven_merge::Audience::Circle(circle))
        })
        .unwrap()
        .clone();
    ana.sync
        .administer(StoreChange::RemoveCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    let removal = ana.database().applied_slot().await.unwrap();
    assert!(original.header.read_through < removal);
    let mut after_read = original.clone();
    after_read.header.read_through = removal;
    let (before_allowed, after_allowed) = {
        let session = ana.sync.session.lock().await;
        (
            session.history.write_signer(&original).is_ok(),
            session.history.write_signer(&after_read).is_ok(),
        )
    };
    assert!(before_allowed);
    assert!(!after_allowed);
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        pins(ana.database()).await,
        vec![("00000000-0000-4000-8000-000000000001".into(), "before removal".into())]
    );
    if compare {
        // Prefix zero projects the shared state immediately before removal.
        let part = |read_through| {
            json!({"audience":1,"version":1,"read_through":read_through,
            "unique_claims":[],"owner_creations":[],"writes":[1],"rows":[]})
        };
        let input = json!({"kind":"apply","member_count":3,
            "initial":{"name":"sequence tests","members":[{"member":0,"role":"admin","key_gen":0},
                {"member":2,"role":"member","key_gen":0}],
                "devices":[{"device":0,"member":0},{"device":2,"member":2}]},
            "audiences":[{"audience":1,"version":1,"writers":[0,2]}],
            "slots":[{"kind":"admin","device":0,"claim":1,"digest":"removal",
                "entry":{"kind":"circleMember","audience":1,"member":2,"allowed":false}},
                {"kind":"writes","device":2,"claim":1,"digest":"before","key_gen":0,"parts":[part(0)]},
                {"kind":"writes","device":2,"claim":2,"digest":"after","key_gen":0,"parts":[part(1)]}]});
        let model = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);
        let refused = model["audiences"][0]["refused"].as_array().unwrap();
        assert_eq!(
            (!refused.contains(&json!(2)), !refused.contains(&json!(3))),
            (before_allowed, after_allowed)
        );
    }
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}

#[tokio::test]
async fn circle_write_uses_authored_permission_after_removal() {
    offline_circle_write(false).await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_authored_circle_permission_matches_lean() {
    offline_circle_write(true).await;
}

// An installation that stops for good, removed, replaced or holding keys
// its member no longer has, ends every operation its app's writes journaled
// in the transaction that applies the stop (§12.3, §14.1).

/// Journal an admin change in a write of the device's own, as the app does.
async fn journal(device: &Device, change: StoreChange) -> coven_database::OperationId {
    let steps = device.sync.operation_steps();
    device
        .database()
        .write_as(move |sql| steps.administer(&sql, change))
        .await
        .unwrap()
        .expect("the change journals")
}

/// The operation's outcome, as `coven_operations` lists it.
async fn outcome(
    device: &Device,
    operation: coven_database::OperationId,
) -> Option<OperationOutcome> {
    device
        .database()
        .read(move |sql| {
            Ok::<_, DbError>(sql.query_row(
                "SELECT outcome FROM coven_operations WHERE operation=?1",
                [operation.0],
                |row| row.get::<_, Option<OperationOutcome>>(0),
            )?)
        })
        .await
        .unwrap()
}

/// The outcome of an operation a stop for good ended: nothing refused it.
fn stopped(stop: DeviceStop) -> Option<OperationOutcome> {
    Some(OperationOutcome::Stopped(stop))
}

/// Ana and Ben, an admin, each with a device; Ben's has applied his
/// admission.
async fn household() -> (Group, Device, Device) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    (group, ana, ben)
}

async fn close(devices: &[&Device]) {
    for device in devices {
        device.database().close().await.unwrap();
    }
}

/// Ben removes his own device, and the provider stores the removal but loses
/// the reply, as a crash mid-claim would. His next pass reads the removal
/// before settling the claim; applying it ends the removal landed at its
/// slot, and ends his other journaled operation stopped by the removal, in
/// the same transaction (§14.1).
#[tokio::test]
async fn applying_this_devices_removal_ends_its_operations_whatever_its_claim_says() {
    let (_group, ana, ben) = household().await;
    let removal = journal(&ben, StoreChange::RemoveDevice { device: ben.sync.device }).await;
    let rename = journal(&ben, StoreChange::RenameStore { name: "Ben's".into() }).await;
    ben.storage
        .set_faults(coven_storage::test_utils::Faults {
            refuse_after_storing: 1,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    assert!(matches!(ben.sync.pass().await, Err(SyncError::Storage(_))));
    let claim = ben.database().current_claim().await.unwrap().unwrap();
    assert_eq!(claim.work, coven_database::ClaimWork::Operation(removal));
    assert_eq!(claim.settlement, coven_database::ClaimSettlement::Unknown);
    let slot = claim.attempts.last().unwrap().slot_number().unwrap();
    ben.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    assert!(matches!(ben.sync.pass().await, Err(SyncError::Removed)));
    assert_eq!(outcome(&ben, removal).await, Some(OperationOutcome::Landed(slot)));
    assert_eq!(outcome(&ben, rename).await, stopped(DeviceStop::Removed));
    assert!(ben.database().operations().await.unwrap().is_empty());
    assert!(ben.database().current_claim().await.unwrap().is_none());
    close(&[&ana, &ben]).await;
}

/// Ben journals a rename with his device offline, and Ana removes the
/// device. Retention deletes the removal's slot before Ben reads it, so the
/// removal reaches him in the floor's snapshot; loading it ends his
/// journaled rename, stopped, in that transaction (§14.1).
#[tokio::test]
async fn a_removal_loaded_from_the_floor_ends_this_devices_operations() {
    let (group, ana, ben) = household().await;
    let rename = journal(&ben, StoreChange::RenameStore { name: "Ben's".into() }).await;
    ana.sync.administer(StoreChange::RemoveDevice { device: ben.sync.device }).await.unwrap();
    let removal = ana.sync.database.applied_slot().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    let path = ObjectPath::slot(group.store, removal.try_into().unwrap());
    assert!(group.storage.status(&path).await.unwrap().is_none());
    assert!(matches!(ben.sync.pass().await, Err(SyncError::Removed)));
    assert_eq!(outcome(&ben, rename).await, stopped(DeviceStop::Removed));
    assert!(ben.database().operations().await.unwrap().is_empty());
    close(&[&ana, &ben]).await;
}

/// Ben journals a rename on his phone, and a replacement installation of
/// his registers in the phone's place (§19.3). Applying that registration
/// stops the phone for good, and ends the rename, stopped by the replacement.
#[tokio::test]
async fn a_replaced_device_ends_its_operations() {
    let (group, ana, ben) = household().await;
    let rename = journal(&ben, StoreChange::RenameStore { name: "Ben's".into() }).await;
    let replacement = group.newcomer(ben.identity.clone(), ben.storage.for_device()).await;
    replacement.sync.unlock().await.unwrap();
    replacement.sync.register_device("Replacement".into(), Some(ben.sync.device)).await.unwrap();
    assert!(matches!(ben.sync.pass().await, Err(SyncError::DeviceIdInUse(_))));
    assert_eq!(outcome(&ben, rename).await, stopped(DeviceStop::Replaced));
    assert!(ben.database().operations().await.unwrap().is_empty());
    close(&[&ana, &ben, &replacement]).await;
}

/// Ben journals a rename, and Ana replaces Ben's keys with a pair his phone
/// doesn't hold (§12.3). Applying the replacement stops the phone for good,
/// and ends the rename, stopped by the replacement of its keys.
#[tokio::test]
async fn a_device_whose_member_keys_were_replaced_ends_its_operations() {
    let (_group, ana, ben) = household().await;
    let rename = journal(&ben, StoreChange::RenameStore { name: "Ben's".into() }).await;
    let elsewhere = coven_format::test_utils::keys_from_seed(12).public_keys();
    ana.sync.replace_member_keys_for(ben.identity.member(), elsewhere).await.unwrap();
    let pass = ben.sync.pass().await;
    assert!(matches!(pass, Err(SyncError::MemberKeysReplaced { .. })), "{pass:?}");
    assert_eq!(outcome(&ben, rename).await, stopped(DeviceStop::MemberKeysReplaced));
    assert!(ben.database().operations().await.unwrap().is_empty());
    close(&[&ana, &ben]).await;
}

/// Ben replaces his own keys while a rename of his is journaled. The
/// binding names the candidate his phone recorded before keeping it, so the
/// phone goes on, and the rename lands.
#[tokio::test]
async fn replacing_ones_own_keys_ends_no_operation() {
    let (_group, ana, ben) = household().await;
    let rename = journal(&ben, StoreChange::RenameStore { name: "Ben's".into() }).await;
    ben.sync.replace_member_keys().await.unwrap();
    assert_eq!(outcome(&ben, rename).await, None);
    ben.sync.pass().await.unwrap();
    assert!(matches!(outcome(&ben, rename).await, Some(OperationOutcome::Landed(_))));
    close(&[&ana, &ben]).await;
}

/// Ana's phone is restored from a backup, so two copies share its device
/// id, and the copy has an invite open and a rename journaled. The original
/// claims a slot; the copy reads it, a slot signed by its own id that it never
/// attempted, and stops for good (§19.3). No slot applies that stop, yet it
/// ends the copy's operations and invites as one would: none would ever run.
#[tokio::test]
async fn a_copy_stopped_by_history_ends_its_operations_and_invites() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    s3_invite(&ana, "Carol").await;
    let (ana, copy) = group.copy(ana).await;
    let rename = journal(&copy, StoreChange::RenameStore { name: "Copied".into() }).await;
    insert(&ana.sync.database, "a", "from the original").await;
    ana.sync.pass().await.unwrap();
    assert!(matches!(copy.sync.pass().await, Err(SyncError::DeviceIdInUse(_))));
    assert_eq!(outcome(&copy, rename).await, stopped(DeviceStop::Copied));
    assert!(copy.database().read(crate::Invite::list).await.unwrap().is_empty());
    assert!(copy
        .database()
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "invite"));
    close(&[&ana, &copy]).await;
}

/// A rejected operation whose undo fails keeps both causes in the call's
/// error and the pending list, stays in the journal, and finishes its undo
/// on the retry (§11.3).
#[tokio::test]
async fn a_failed_undo_keeps_both_causes_in_the_journal_and_retries() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let (upload_started, upload_observed) = tokio::sync::oneshot::channel();
    let (upload_resume, upload_held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::all(group.store),
            upload_started,
            upload_held,
        )
        .await;
    let prepare = ben.sync.reset(coven_merge::Audience::Store);
    let remove = async {
        upload_observed.await.unwrap();
        let body = ben
            .storage
            .request_trace()
            .into_iter()
            .find_map(|request| match request {
                coven_storage::test_utils::MemoryRequest::Create(path)
                    if path.snapshot_id().is_some() =>
                {
                    Some(path)
                }
                _ => None,
            })
            .expect("the snapshot body is being created");
        ben.storage
            .set_faults(coven_storage::test_utils::Faults {
                delete_failures: [(body.clone(), coven_storage::StorageFailure::Network)].into(),
                ..coven_storage::test_utils::Faults::none()
            })
            .await;
        ana.sync
            .administer(StoreChange::ChangeRole {
                member: ben.identity.member(),
                role: MemberRole::Member,
            })
            .await
            .unwrap();
        upload_resume.send(()).unwrap();
        body
    };
    let (result, body) = tokio::time::timeout(HANG_GUARD, async { tokio::join!(prepare, remove) })
        .await
        .expect("publication controls must finish");
    match &result {
        Err(SyncError::Cleanup { operation, cleanup }) => {
            assert!(matches!(**operation, SyncError::Rejected(OperationRejection::NotAllowed)));
            assert!(matches!(**cleanup, SyncError::Storage(_)), "{cleanup:?}");
        }
        other => panic!("both causes are kept: {other:?}"),
    }
    assert_eq!(ben.sync.database.operations().await.unwrap().len(), 1);
    let pending = ben.sync.database.pending::<PendingRecord>().await.unwrap();
    assert!(
        pending.iter().any(|record| matches!(record.reason, PendingReason::Cleanup { .. })),
        "{pending:?}"
    );

    ben.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(10));
    ben.sync.pass().await.unwrap();
    assert!(ben.sync.database.operations().await.unwrap().is_empty());
    assert!(group.storage.status(&body).await.unwrap().is_none());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's ordinary store snapshot, announced after Ana's reset preparation,
/// covers more than it; activating the stale preparation would be refused,
/// so Ana's operation recaptures and activates a body covering Ben's (§16.3).
#[tokio::test]
async fn a_preparation_covered_by_a_later_announcement_is_recaptured() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let prepared = ana.sync.database.applied_slot().await.unwrap();
    let activation = ObjectPath::slot(group.store, (prepared + 2).try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(activation, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    assert!(ana.sync.reset(Audience::Store).await.is_err());
    group.storage.set_faults(Faults::none()).await;
    let ordinary = ben.sync.snapshot(Audience::Store).await.unwrap();
    let operation = ana.sync.database.operations().await.unwrap()[0].id;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    let history = ana.sync.session.lock().await.history.clone();
    let covered = history.announcements[&ordinary].covers;
    let boundary = history.boundaries.last().unwrap();
    let activated = history
        .announcements
        .values()
        .find(|announcement| &announcement.snapshot == boundary.boundary.snapshot())
        .unwrap();
    assert!(activated.covers >= covered);
    assert!(boundary.boundary.entry().0 > ordinary.0);
    ben.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        ben.sync.database.applied_slot().await.unwrap()
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A breaking migration journals its raise with the schema; passes publish
/// it for every audience it captured, and a member that migrated later
/// takes the published raise instead of raising again (§18.3).
#[tokio::test]
async fn a_breaking_migration_raises_its_captured_audiences_through_passes() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the raise").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .any(|record| record.kind == "schema-raise"));
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    for device in [&ana, &ben] {
        let state = device.sync.database.store_state().await.unwrap();
        assert_eq!(state.schema[&Audience::Store].number, 2);
        assert!(device.sync.database.raise_captures().await.unwrap().is_empty());
        assert!(device
            .sync
            .database
            .operations()
            .await
            .unwrap()
            .iter()
            .all(|record| record.kind != "schema-raise"));
    }
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana is in Gifts and the store; Ben only in the store. Ben, migrating first,
/// raises the store alone, since he can't read Gifts. Ana, migrating later,
/// raises Gifts with a snapshot of her own, and every device ends with both
/// audiences at version 2 (§18.3).
#[tokio::test]
async fn a_raise_covers_every_readable_audience_and_a_member_raises_a_circle_others_cannot_read() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ana.sync.create_circle("Gifts".into()).await.unwrap();
    insert(&ana.sync.database, "a", "before the raise").await;
    pin(ana.database(), "00000000-0000-4000-8000-000000000001", gifts, "gift").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);

    for _ in 0..2 {
        ben.sync.pass().await.unwrap();
    }
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.schema[&Audience::Store].number, 2);
    assert!(
        !state.schema.contains_key(&Audience::Circle(gifts))
            || state.schema[&Audience::Circle(gifts)].number < 2,
        "Ben, who cannot read Gifts, raised only the store"
    );

    for _ in 0..2 {
        ana.sync.pass().await.unwrap();
    }
    for _ in 0..2 {
        ben.sync.pass().await.unwrap();
    }
    for device in [&ana, &ben] {
        let state = device.sync.database.store_state().await.unwrap();
        assert_eq!(state.schema[&Audience::Store].number, 2);
        assert_eq!(state.schema[&Audience::Circle(gifts)].number, 2);
    }
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A write made after a breaking migration is a new-version write: it waits
/// in the queue behind the migration marker until a raise to its version
/// lands, then goes out (§18.5).
#[tokio::test]
async fn writes_after_a_breaking_migration_wait_in_the_queue_until_the_raise_lands() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "before", "old version").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);
    insert(&ana.sync.database, "after", "new version").await;

    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(rows(&ben.sync.database).await, [("before".into(), "old version".into())]);
    let waiting = ana.sync.database.waiting_snapshot_headers().await.unwrap();
    assert_eq!(waiting.len(), 1, "the new-version write stays queued");

    ana.sync.raise_schema(Audience::Store, 2).await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(
        rows(&ben.sync.database).await,
        [("after".into(), "new version".into()), ("before".into(), "old version".into())]
    );
    assert!(ana.sync.database.waiting_snapshot_headers().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Two members migrate; Ana's raise lands while Ben's raise of the same
/// version is under way. Ben's operation adopts the established version and
/// claims nothing (§18.3).
#[tokio::test]
async fn a_raise_whose_version_is_already_established_is_satisfied() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the raise").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);
    // Each sends its migration marker; Ben has not read Ana's raise.
    for device in [&ana, &ben] {
        device.sync.pass().await.unwrap();
    }
    ana.sync.raise_schema(Audience::Store, 2).await.unwrap();
    let before = ben.sync.database.applied_slot().await.unwrap();
    ben.sync.raise_schema(Audience::Store, 2).await.unwrap();
    assert!(ben.sync.database.operations().await.unwrap().is_empty());
    assert!(ben.sync.database.pending::<crate::PendingRecord>().await.unwrap().is_empty());
    ana.sync.pass().await.unwrap();
    // Ben read Ana's slots and published none of his own.
    assert_eq!(
        ben.sync.database.applied_slot().await.unwrap(),
        ana.sync.database.applied_slot().await.unwrap()
    );
    assert!(ben.sync.database.applied_slot().await.unwrap() > before);
    for device in [&ana, &ben] {
        let state = device.sync.database.store_state().await.unwrap();
        assert_eq!(state.schema[&Audience::Store].number, 2);
    }
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben announces the body of a version 2 raise but his claim fails. Ana's
/// raise of the same version lands first. When Ben's operation checks again it
/// finds the version taken, deletes its announced body and publishes no entry;
/// the announcement stays in history and supplies no snapshot to anyone
/// (§18.8).
#[tokio::test]
async fn a_losing_raise_deletes_its_announced_body_and_publishes_no_entry() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the raise").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);
    for device in [&ana, &ben] {
        device.sync.pass().await.unwrap();
    }

    // Ben's claim at the slot after his announcement fails.
    let prepared = ben.sync.database.applied_slot().await.unwrap();
    let activation = ObjectPath::slot(group.store, (prepared + 2).try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(activation, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    let traced = ben.storage.request_trace().len();
    assert!(ben.sync.raise_schema(Audience::Store, 2).await.is_err());
    group.storage.set_faults(Faults::none()).await;
    let body = ben.storage.request_trace()[traced..]
        .iter()
        .find_map(|request| match request {
            coven_storage::test_utils::MemoryRequest::Create(path)
                if path.snapshot_id().is_some() =>
            {
                Some(path.clone())
            }
            _ => None,
        })
        .expect("Ben announced a body");
    let announced = body.snapshot_id().unwrap();

    ana.sync.raise_schema(Audience::Store, 2).await.unwrap();
    let operation = ben.sync.database.operations().await.unwrap()[0].id;
    let traced = ben.storage.request_trace().len();
    ben.sync.retry_pending_operation(operation).await.unwrap();
    let requests = ben.storage.request_trace()[traced..].to_vec();
    assert!(ben.sync.database.operations().await.unwrap().is_empty());
    assert!(
        requests.contains(&coven_storage::test_utils::MemoryRequest::Delete(body)),
        "Ben deletes his body: {requests:?}"
    );
    assert!(
        !requests.iter().any(|request| matches!(
            request,
            coven_storage::test_utils::MemoryRequest::Create(path) if path.slot_number().is_some()
        )),
        "Ben publishes no slot: {requests:?}"
    );
    let history = ben.sync.session.lock().await.history.clone();
    assert!(history.announcements.values().any(|announcement| announcement.snapshot == announced));
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.schema[&Audience::Store].number, 2);
    assert_ne!(state.schema[&Audience::Store].snapshot, announced);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// The app drops the future of a key rotation after its first key copy's
/// creation is under way. The journal row stays, the operation finishes
/// without the waiter, and the rotation lands once (§11.4).
#[tokio::test]
async fn dropping_an_operations_call_keeps_its_journal_and_the_rotation_lands_once() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let introductions = ana.sync.session.lock().await.history.introductions.len();
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    tokio::select! {
        result = ana.sync.rotate_key(coven_merge::Audience::Store) => {
            panic!("the held creation keeps the rotation running: {result:?}")
        }
        held = observed => held.unwrap(),
    }
    let journal = ana.sync.database.operations().await.unwrap();
    assert_eq!(journal.len(), 1, "the dropped call leaves its journal row");
    resume.send(()).unwrap();
    tokio::time::timeout(HANG_GUARD, ana.sync.pass()).await.unwrap().unwrap();
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    assert_eq!(
        ana.sync.session.lock().await.history.introductions.len(),
        introductions + 1,
        "the rotation lands once"
    );
    ben.sync.pass().await.unwrap();
    close(&[&ana, &ben]).await;
}
