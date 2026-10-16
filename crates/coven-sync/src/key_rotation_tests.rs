//! First attempts use only unexposed keys; an exposed key is replaced (§11, C12).

use super::*;
use crate::store_log_sync::tests::Device;
use crate::store_log_sync::tests::{device, find_copy, key, member, storage, store};
use coven_database::EntryOutcome;
use coven_format::store_log::MemberRole;

/// A fresh complete observation, as a new pass would make.
pub(crate) async fn observed(device: &Device) -> Vec<keys::ObservedCopy> {
    let reading = keys::read_copies(
        &device.db,
        &crate::pass_reads::PassReads::default(),
        std::num::NonZeroUsize::MIN,
        device.storage.as_ref(),
        store(1),
    )
    .await
    .unwrap();
    assert!(reading.unreadable.is_empty(), "{:?}", reading.unreadable);
    reading.copies
}

async fn selected(device: &Device) -> Result<KeyId, SyncError> {
    let ring = device.custody.read().unwrap().unwrap();
    crate::write_seal::first_attempt(
        &device.log().await,
        &observed(device).await,
        &ring,
        &device.member.member(),
        &Audience::Store,
    )
}

#[tokio::test]
async fn a_forged_copy_to_an_outsider_exposes_the_key_and_costs_one_rotation() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let dan = member(4);
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben", None).await.unwrap();
    ana.sync().await;
    assert_eq!(selected(&ana).await.unwrap(), key(1));
    // Ben publishes a box naming the store key for Dan, who is not a member.
    // Nobody but Dan can open it, so it counts whatever it holds.
    let (dan_id, dan_sealing) = (dan.member(), dan.keys().public_keys().sealing);
    ben.db
        .prepare_key_upload(move |path| {
            keys::store_copy(
                path,
                &coven_crypto::StoreKey::generate(key(1)).unwrap(),
                &dan_id,
                &dan_sealing,
            )
        })
        .await
        .unwrap();
    object::upload_keys(store(1), Some(ben.storage.as_ref()), &ben.db, &ben.send)
        .await
        .unwrap();
    assert!(matches!(
        selected(&ana).await,
        Err(SyncError::KeyUnavailable(id)) if id == key(1)
    ));
    // Phase 2 publishes one rotation; the fresh key reached only members.
    ana.sync.sync_store_log().await.unwrap();
    ana.sync.rotate_exposed_keys().await.unwrap();
    finish_rotation(&mut ana).await;
    let fresh = selected(&ana).await.unwrap();
    assert_ne!(fresh, key(1));
    let log = ana.log().await;
    let rotation = log.entries.last().unwrap();
    assert!(matches!(
        &rotation.entry.change,
        StoreChange::RotateKey { audience: Audience::Store, key, .. } if *key == fresh
    ));
    assert_eq!(
        log.replay.entries[&rotation.entry.position],
        EntryOutcome::Kept
    );
    for (who, has) in [(&ana.member, true), (&ben.member, true), (&dan, false)] {
        assert_eq!(
            find_copy(storage.as_ref(), Audience::Store, fresh, &who.member())
                .await
                .is_some(),
            has
        );
    }
    // Another pass keeps the replacement: the old copies stay listed but no
    // longer require a rotation.
    ana.sync.rotate_exposed_keys().await.unwrap();
    finish_rotation(&mut ana).await;
    assert_eq!(ana.log().await.entries.len(), log.entries.len());
    // Old keys still decrypt: Ben reads entries sealed with either key.
    ben.sync().await;
    assert_eq!(ben.log().await, ana.log().await);
    let ring = ben.custody.read().unwrap().unwrap();
    assert!(ring.store_key(key(1)).is_ok() && ring.store_key(fresh).is_ok());
    assert_eq!(selected(&ben).await.unwrap(), fresh);
    let mut restored = device(storage.clone(), 3, member(2), store(1)).await;
    restored.sync().await;
    assert_eq!(restored.log().await, ana.log().await);
}

/// The provider corrupts a key copy this device published before any of its
/// scans read it. Undecodable bytes at an own number are damage, never another
/// copy's object (`OwnCopyDamage.unreadable_is_damage`): the first phase is
/// incomplete as on any unreadable copy, a catch-up that must be complete
/// names the path, and the device keeps its id and its unsent write. A reset
/// would discard the write and leave the phase incomplete all the same
/// (`damage_resets_uncopied`, `reset_never_completes`).
#[tokio::test]
async fn an_undecodable_own_copy_waits_as_damage() {
    use crate::device_log_sync::tests::{group, sql, STORE_ID};
    let storage = crate::device_log_sync::tests::storage();
    let ana = group(storage.clone(), 1).await.remove(0);
    let ring = ana.custody.read().unwrap().unwrap();
    let key = ring.store_key(key(1)).unwrap().clone();
    let (id, sealing) = (ana.member.member(), ana.member.keys().public_keys().sealing);
    let copy = ana
        .db
        .prepare_key_upload(move |path| {
            crate::store_log_keys::store_copy(path, &key, &id, &sealing)
        })
        .await
        .unwrap();
    crate::store_log_object::upload_keys(STORE_ID, Some(storage.as_ref()), &ana.db, &ana.send)
        .await
        .unwrap();
    let path = copy.path(STORE_ID);
    storage.delete(&path).await.unwrap();
    storage.create(&path, b"not a sealed key").await.unwrap();
    sql(&ana.db, "INSERT INTO notes VALUES('one','title','body')").await;
    assert!(matches!(
        ana.sync.scan_store_log_once(true).await,
        Err(SyncError::Damaged(damage)) if damage.path == path.as_str()
    ));
    assert!(!ana.sync.scan_store_log_once(false).await.unwrap());
    assert_eq!(copy_damage(&ana.db).await, std::slice::from_ref(&path));
    assert_eq!(ana.send.stopped(), None);
    assert_eq!(ana.directory.device_reset().unwrap(), None);
    let attempted = ana
        .db
        .read_oldest_upload(|upload| Ok::<_, coven_database::DbError>(upload.keys.is_some()))
        .await
        .unwrap();
    assert_eq!(attempted, Some(false));
}

/// The paths of this device's damaged-copy records, each a parse refusal.
async fn copy_damage(db: &coven_database::Database) -> Vec<coven_storage::ObjectPath> {
    db.pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|record| match record.subject {
            crate::PendingSubject::KeyCopyPath { path } => {
                assert!(matches!(record.reason, crate::PendingReason::Refused(_)));
                Some(path)
            }
            _ => None,
        })
        .collect()
}

/// Each completed copy reading decides this device's damaged-copy records as
/// a whole (§19.1): an undecodable copy gets one, naming its path, and a
/// recorded path that now reads, or that the reading no longer finds, loses
/// its record. The app sees why the first phase waits.
#[tokio::test]
async fn each_copy_reading_decides_the_damaged_copy_records() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben", None).await.unwrap();
    let own = ana.db.local_store_log().await.unwrap().device;
    let readable = find_copy(
        storage.as_ref(),
        Audience::Store,
        key(1),
        &ana.member.member(),
    )
    .await
    .expect("stored copy")
    .path;
    let absent = coven_storage::ObjectPath::key_copy(
        store(1),
        coven_foundation::id_source::DeviceId(9),
        1.try_into().unwrap(),
    );
    let stale = |path: &coven_storage::ObjectPath| crate::PendingRecord {
        subject: crate::PendingSubject::KeyCopyPath { path: path.clone() },
        reason: crate::PendingReason::Refused(crate::Refusal::Parse { cause: None }),
        reported_by: own,
    };
    ana.db
        .replace_local_pending(
            |record: &crate::PendingRecord| {
                matches!(record.subject, crate::PendingSubject::KeyCopyPath { .. })
            },
            vec![stale(&readable), stale(&absent)],
        )
        .await
        .unwrap();
    assert_eq!(copy_damage(&ana.db).await.len(), 2);
    // Ben's log, not Ana's: her own number beyond the reservations would
    // reset as storage ahead.
    let writer = ben.db.local_store_log().await.unwrap().device;
    let bad = coven_storage::ObjectPath::key_copy(store(1), writer, 1.try_into().unwrap());
    storage.create(&bad, b"not a sealed key").await.unwrap();
    assert!(!ana.sync.scan_store_log_once(false).await.unwrap());
    assert_eq!(copy_damage(&ana.db).await, [bad]);
}

#[tokio::test]
async fn an_incomplete_copy_observation_blocks_first_attempts() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben", None).await.unwrap();
    ana.sync().await;
    // An undecodable copy in another known writer's log leaves an observation
    // incomplete. It cannot renew the earlier completion once that expires.
    // (In this device's own log it is the same damage; see
    // `an_undecodable_own_copy_waits_as_damage`.)
    let writer = ben.db.local_store_log().await.unwrap().device;
    let bad = coven_storage::ObjectPath::key_copy(store(1), writer, 1.try_into().unwrap());
    storage.create(&bad, b"not a sealed key").await.unwrap();
    let now = coven_foundation::clock::Clock::now(ana.clock.as_ref());
    ana.clock.set(now + std::time::Duration::from_secs(300));
    // A catch-up that must be complete fails on that copy; a pass's scan is
    // incomplete and supplies no sending authority.
    assert!(matches!(
        ana.sync.scan_store_log_once(true).await,
        Err(SyncError::Damaged(damage)) if damage.path == bad.as_str()
    ));
    assert!(!ana.sync.scan_store_log_once(false).await.unwrap());
    assert_eq!(copy_damage(&ana.db).await, std::slice::from_ref(&bad));
    assert!(matches!(
        ana.send.observation(),
        Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::CatchUpRequired
    ));
    let entries = ana.log().await.entries.len();
    ana.sync.rotate_exposed_keys().await.unwrap();
    finish_rotation(&mut ana).await;
    assert_eq!(
        ana.log().await.entries.len(),
        entries,
        "no rotation without an observation"
    );
}

#[tokio::test]
async fn rotation_is_journaled_before_any_copy_or_entry_publication() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: member(2).member(),
        })
        .await
        .unwrap();
    let before = storage
        .list(&coven_storage::ObjectPrefix::all(store(1)))
        .await
        .unwrap();
    ana.sync.rotate_exposed_keys().await.unwrap();
    let operations: Vec<_> = ana
        .db
        .operations()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.kind == "rotate-key")
        .collect();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].kind, "rotate-key");
    assert_eq!(operations[0].last_step, 0);
    assert_eq!(
        storage
            .list(&coven_storage::ObjectPrefix::all(store(1)))
            .await
            .unwrap(),
        before
    );
    assert!(selected(&ana).await.is_err());
    ana.sync.rotate_exposed_keys().await.unwrap();
    assert_eq!(
        ana.db
            .operations()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.kind == "rotate-key")
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        operations.iter().map(|r| r.id).collect::<Vec<_>>()
    );
}

async fn exposed_store() -> Device {
    let mut ana = device(storage(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: member(2).member(),
        })
        .await
        .unwrap();
    ana
}

pub(crate) async fn finish_rotation(device: &mut Device) {
    for row in device.db.operations().await.unwrap() {
        if row.kind != "rotate-key" {
            continue;
        }
        loop {
            match device.step(row.id).await.unwrap() {
                crate::operations::Progress::Advanced => (),
                crate::operations::Progress::Finished(crate::operations::Output::Unit) => break,
                _ => panic!("rotation must advance"),
            }
        }
    }
}

#[tokio::test]
async fn every_rotation_step_resumes_with_fixed_copies_and_entry() {
    for crash_step in 0..6 {
        let mut ana = exposed_store().await;
        ana.sync.rotate_exposed_keys().await.unwrap();
        let id = ana
            .db
            .operations()
            .await
            .unwrap()
            .iter()
            .find(|r| r.kind == "rotate-key")
            .unwrap()
            .id;
        for _ in 0..if crash_step == 4 {
            3
        } else if crash_step == 5 {
            4
        } else {
            crash_step
        } {
            assert!(matches!(
                ana.step(id).await.unwrap(),
                crate::operations::Progress::Advanced
            ));
        }
        if crash_step == 4 {
            ana.sync().await;
        }
        let row = ana.operation(id).await;
        let local = ana.db.local_store_log().await.unwrap();
        let fixed = local.upload.as_ref().map(|upload| {
            (
                upload.clone(),
                object::seal_upload(
                    store(1),
                    &local.log,
                    upload,
                    ana.custody.read().unwrap().as_ref(),
                    &ana.member,
                )
                .unwrap(),
            )
        });
        if crash_step < 4 {
            assert!(selected(&ana).await.is_err());
        }
        let provider = ana.storage.clone();
        ana.restart(provider.clone()).await;
        assert_same_operation(&ana.operation(id).await, &row);
        assert_eq!(ana.db.local_store_log().await.unwrap().upload, local.upload);
        finish_rotation(&mut ana).await;
        let fresh = selected(&ana).await.unwrap();
        assert_ne!(fresh, key(1));
        if let Some((fixed, bytes)) = fixed {
            assert_eq!(
                provider
                    .read(&object::path(store(1), fixed.entry.position))
                    .await
                    .unwrap(),
                bytes
            );
            for copy in fixed.sealing.keys {
                assert_eq!(
                    provider.read(&copy.path(store(1))).await.unwrap(),
                    copy.bytes
                );
            }
        }
        assert!(ana
            .db
            .operations()
            .await
            .unwrap()
            .iter()
            .all(|row| row.kind != "rotate-key"));
    }
}

#[tokio::test]
#[ignore = "requires COVEN_ROTATION_LEAN"]
async fn lean_differential_rotation_phases() {
    use coven_foundation::id_source::IdSource;
    struct RotationIds(std::sync::atomic::AtomicU64);
    impl IdSource for RotationIds {
        fn new_id(&self) -> uuid::Uuid {
            uuid::Uuid::from_u128(u128::from(
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ))
        }
    }
    use serde_json::json;
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for circle in [false, true] {
        for crash_step in 0..7 {
            let ids = std::sync::Arc::new(RotationIds(std::sync::atomic::AtomicU64::new(100)));
            let storage = storage();
            let mut ana =
                Device::new(storage.clone(), 1, member(1), store(1), vec![], vec![], ids).await;
            ana.create(key(1)).await;
            ana.add(&member(2), MemberRole::Member).await;
            let audience = if circle {
                let circle = crate::store_log_sync::tests::circle(1);
                let key_hash = ana.sync.test_key(Audience::Circle(circle), key(2));
                ana.sync
                    .make_and_upload_entry(StoreChange::CreateCircle {
                        circle,
                        name: "Private".into(),
                        key: key(2),
                        key_hash,
                    })
                    .await
                    .unwrap();
                ana.sync
                    .make_and_upload_entry(StoreChange::AddCircleMember {
                        circle,
                        member: member(2).member(),
                    })
                    .await
                    .unwrap();
                ana.sync
                    .make_and_upload_entry(StoreChange::RemoveCircleMember {
                        circle,
                        member: member(2).member(),
                    })
                    .await
                    .unwrap();
                Audience::Circle(circle)
            } else {
                ana.sync
                    .make_and_upload_entry(StoreChange::RemoveMember {
                        member: member(2).member(),
                    })
                    .await
                    .unwrap();
                Audience::Store
            };
            let before = ana.log().await;
            let copies = observed(&ana).await;
            ana.sync.rotate_exposed_keys().await.unwrap();
            let id = ana
                .db
                .operations()
                .await
                .unwrap()
                .iter()
                .find(|r| r.kind == "rotate-key")
                .unwrap()
                .id;
            let mut trace = Vec::new();
            for stage in 0..7 {
                if crash_step == stage {
                    ana.restart(storage.clone()).await;
                }
                let row = ana.db.operation(id).await.unwrap();
                let selected = match crate::write_seal::first_attempt(
                    &ana.log().await,
                    &observed(&ana).await,
                    &ana.custody.read().unwrap().unwrap(),
                    &ana.member.member(),
                    &audience,
                ) {
                    Ok(key) => Some(key),
                    Err(SyncError::KeyUnavailable(_)) => None,
                    Err(error) => panic!("unexpected selection failure: {error:?}"),
                };
                trace.push(json!({"step":row.as_ref().map(|r|r.last_step),
                    "selected":selected.map(crate::tests::key_number)}));
                if stage == 3 {
                    ana.sync().await;
                } else if row.is_some() {
                    ana.step(id).await.unwrap();
                }
            }
            let after = ana.log().await;
            let h = crate::replay::tests::History {
                entries: after.entries.iter().map(|e| e.entry.clone()).collect(),
            };
            let past: Vec<_> = (0..before.entries.len()).collect();
            let mut input = crate::tests::input(&h, &vec![0; h.entries.len()], &[past]);
            input["rotation"] = json!(h.entries.len() - 1);
            input["self"] = json!(0);
            input["device"] = json!(1);
            input["audience"] = json!(crate::tests::audience_number(&audience));
            input["held"] = json!(before
                .entries
                .iter()
                .enumerate()
                .filter_map(|(i, e)| keys::introduced(&e.entry.change).map(|_| i))
                .collect::<Vec<_>>());
            input["copies"] = json!(copies
                .iter()
                .map(|copy| {
                    let index = before
                        .entries
                        .iter()
                        .position(|e| {
                            keys::introduced(&e.entry.change)
                                == Some((copy.prefix.audience.clone(), copy.prefix.key))
                        })
                        .unwrap();
                    let member = (1..=2)
                        .find(|&n| member(n).member() == copy.prefix.member)
                        .unwrap()
                        - 1;
                    [index, usize::from(member)]
                })
                .collect::<Vec<_>>());
            inputs.push(input);
            outputs.push(json!(trace));
        }
    }
    let output = crate::tests::run_lean("COVEN_ROTATION_LEAN", &json!(inputs));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual, json!(outputs));
}

#[tokio::test]
async fn ordinary_members_can_rotate_concurrently_and_both_keys_remain_readable() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ana.add(&member(3), MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben", None).await.unwrap();
    ana.sync().await;
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: member(3).member(),
        })
        .await
        .unwrap();
    ben.sync().await;
    let mut keys = Vec::new();
    for device in [&mut ana, &mut ben] {
        device.sync.rotate_exposed_keys().await.unwrap();
        let id = device
            .db
            .operations()
            .await
            .unwrap()
            .iter()
            .find(|r| r.kind == "rotate-key")
            .unwrap()
            .id;
        device.step(id).await.unwrap();
        let fixed = device.db.local_store_log().await.unwrap().upload.unwrap();
        keys.push(keys::introduced(&fixed.entry.change).unwrap().1);
        assert!(selected(device).await.is_err());
    }
    assert_ne!(keys[0], keys[1]);
    finish_rotation(&mut ana).await;
    finish_rotation(&mut ben).await;
    ana.sync().await;
    for device in [&ana, &ben] {
        let ring = device.custody.read().unwrap().unwrap();
        assert!(keys.iter().all(|id| ring.store_key(*id).is_ok()));
        assert!(keys.contains(&selected(device).await.unwrap()));
        let log = device.log().await;
        assert!(log
            .entries
            .iter()
            .filter(|e| matches!(e.entry.change, StoreChange::RotateKey { .. }))
            .all(|e| log.replay.entries[&e.entry.position] == EntryOutcome::Kept));
    }
}

#[tokio::test]
async fn dropped_rotation_restarts_with_new_material_only_after_revalidation() {
    use coven_foundation::clock::FixedClock;
    use coven_storage::test_utils::MemoryStorage;
    use std::{
        sync::Arc,
        time::{Duration, UNIX_EPOCH},
    };
    for authority_lost in [false, true] {
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
        let storage = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .build()
                .unwrap(),
        );
        let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
        let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
        ana.create(key(1)).await;
        ana.add(&ben.member, MemberRole::Member).await;
        ana.add(&member(3), MemberRole::Member).await;
        ben.sync().await;
        ben.sync.load_new_device("Ben", None).await.unwrap();
        let circle = crate::store_log_sync::tests::circle(1);
        let key_hash = ben.sync.test_key(Audience::Circle(circle), key(2));
        ben.sync
            .make_and_upload_entry(StoreChange::CreateCircle {
                circle,
                name: "Gifts".into(),
                key: key(2),
                key_hash,
            })
            .await
            .unwrap();
        for who in [member(1), member(3)] {
            ben.sync
                .make_and_upload_entry(StoreChange::AddCircleMember {
                    circle,
                    member: who.member(),
                })
                .await
                .unwrap();
        }
        ben.sync
            .make_and_upload_entry(StoreChange::RemoveCircleMember {
                circle,
                member: member(3).member(),
            })
            .await
            .unwrap();
        ana.sync().await;
        ben.sync.rotate_exposed_keys().await.unwrap();
        let id = ben.db.operations().await.unwrap()[0].id;
        ben.step(id).await.unwrap();
        let fixed = ben.db.local_store_log().await.unwrap().upload.unwrap();
        let (_, old) = keys::introduced(&fixed.entry.change).unwrap();
        ben.step(id).await.unwrap();
        clock.set(UNIX_EPOCH + Duration::from_secs(86400));
        let witness = if authority_lost {
            StoreChange::RemoveCircleMember {
                circle,
                member: ben.member.member(),
            }
        } else {
            let member = ana.member.member();
            StoreChange::SetAccess {
                access: ana.log().await.replay.state.members[&member].access.clone(),
                member,
            }
        };
        ana.sync.make_and_upload_entry(witness).await.unwrap();
        clock.set(UNIX_EPOCH + Duration::from_secs(32 * 86400));
        // Ben's catch-up is as old as the storage time since: he reads the
        // witness before his retry lands.
        ben.clock.set(UNIX_EPOCH + Duration::from_secs(32 * 86400));
        ben.step(id).await.unwrap();
        ben.step(id).await.unwrap();
        assert_eq!(
            ben.log().await.replay.entries[&fixed.entry.position],
            EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
        );
        assert_eq!(ben.operation(id).await.last_step, 0);
        ben.restart(storage.clone()).await;
        let before = ben
            .custody
            .read()
            .unwrap()
            .unwrap()
            .circle_key_ids(circle)
            .collect::<Vec<_>>();
        let retry = ben.step(id).await;
        if authority_lost {
            // Ben left the circle: its rotation is the remaining members',
            // so his finishes with no new material or entry (§11).
            retry.unwrap();
            ben.step(id).await.unwrap();
            assert!(ben
                .db
                .operations()
                .await
                .unwrap()
                .iter()
                .all(|r| r.id != id));
            assert!(ben.db.local_store_log().await.unwrap().upload.is_none());
            assert_eq!(
                ben.custody
                    .read()
                    .unwrap()
                    .unwrap()
                    .circle_key_ids(circle)
                    .collect::<Vec<_>>(),
                before
            );
        } else {
            retry.unwrap();
            let next = ben.db.local_store_log().await.unwrap().upload.unwrap();
            assert_eq!(next.entry.position.number, fixed.entry.position.number + 1);
            assert_ne!(keys::introduced(&next.entry.change).unwrap().1, old);
            finish_rotation(&mut ben).await;
            assert!(ben
                .custody
                .read()
                .unwrap()
                .unwrap()
                .circle_key(circle, old)
                .is_ok());
        }
        assert!(storage
            .read(&object::path(store(1), fixed.entry.position))
            .await
            .is_ok());
    }
}

#[tokio::test]
async fn lost_copy_or_entry_reply_keeps_the_same_rotation_attempt() {
    use coven_storage::test_utils::Faults;
    for failing_step in [1, 2] {
        let mut ana = exposed_store().await;
        ana.sync.rotate_exposed_keys().await.unwrap();
        let id = ana
            .db
            .operations()
            .await
            .unwrap()
            .iter()
            .find(|r| r.kind == "rotate-key")
            .unwrap()
            .id;
        for _ in 0..failing_step {
            ana.step(id).await.unwrap();
        }
        let fixed = ana.db.local_store_log().await.unwrap().upload.unwrap();
        let row = ana.operation(id).await;
        ana.storage
            .set_faults(Faults {
                lose_completion_reply: true,
                ..Faults::none()
            })
            .await;
        assert!(matches!(ana.step(id).await, Err(SyncError::Storage(_))));
        assert_same_operation(&ana.operation(id).await, &row);
        assert!(selected(&ana).await.is_err());
        ana.restart(ana.storage.clone()).await;
        finish_rotation(&mut ana).await;
        assert_eq!(
            selected(&ana).await.unwrap(),
            keys::introduced(&fixed.entry.change).unwrap().1
        );
        assert_eq!(
            ana.log()
                .await
                .entries
                .iter()
                .filter(|e| matches!(e.entry.change, StoreChange::RotateKey { .. }))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn an_exposed_replacement_is_rotated_again_without_reusing_its_material() {
    let mut ana = exposed_store().await;
    ana.sync.rotate_exposed_keys().await.unwrap();
    finish_rotation(&mut ana).await;
    let exposed = selected(&ana).await.unwrap();
    let held = ana
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(exposed)
        .unwrap()
        .clone();
    let dan = member(4);
    ana.db
        .prepare_key_upload(move |path| {
            keys::store_copy(
                path,
                &held,
                &dan.member(),
                &dan.keys().public_keys().sealing,
            )
        })
        .await
        .unwrap();
    object::upload_keys(store(1), Some(ana.storage.as_ref()), &ana.db, &ana.send)
        .await
        .unwrap();
    ana.sync.sync_store_log().await.unwrap();
    assert!(selected(&ana).await.is_err());
    ana.sync.rotate_exposed_keys().await.unwrap();
    finish_rotation(&mut ana).await;
    assert_ne!(selected(&ana).await.unwrap(), exposed);
    assert!(ana
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(exposed)
        .is_ok());
}

#[tokio::test]
async fn only_a_remaining_circle_member_schedules_its_rotation() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let mut carol = device(storage.clone(), 3, member(3), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ana.add(&carol.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben", None).await.unwrap();
    let circle = crate::store_log_sync::tests::circle(1);
    let key_hash = ben.sync.test_key(Audience::Circle(circle), key(2));
    ben.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle,
            name: "Private".into(),
            key: key(2),
            key_hash,
        })
        .await
        .unwrap();
    ben.sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle,
            member: carol.member.member(),
        })
        .await
        .unwrap();
    carol.sync().await;
    carol.sync.load_new_device("Carol", None).await.unwrap();
    ana.sync().await;
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: ben.member.member(),
        })
        .await
        .unwrap();
    ana.sync.rotate_exposed_keys().await.unwrap();
    finish_rotation(&mut ana).await;
    assert_eq!(ana.newest(Audience::Circle(circle)).await, key(2));
    carol.sync().await;
    carol.sync.rotate_exposed_keys().await.unwrap();
    finish_rotation(&mut carol).await;
    let fresh = carol.newest(Audience::Circle(circle)).await;
    assert_ne!(fresh, key(2));
    ana.sync().await;
    assert!(ana
        .custody
        .read()
        .unwrap()
        .unwrap()
        .circle_key(circle, fresh)
        .is_err());
    assert!(carol
        .custody
        .read()
        .unwrap()
        .unwrap()
        .circle_key(circle, fresh)
        .is_ok());
    assert!(find_copy(
        storage.as_ref(),
        Audience::Circle(circle),
        fresh,
        &ana.member.member()
    )
    .await
    .is_none());
}

fn assert_same_operation(a: &coven_database::OperationRecord, b: &coven_database::OperationRecord) {
    assert_eq!(
        (a.id, &a.kind, a.last_step, &a.data, &a.started_by),
        (b.id, &b.kind, b.last_step, &b.data, &b.started_by)
    );
}
