use crate::sequence_sync::tests::*;
use crate::*;
use coven_database::DbError;
use coven_format::admin::StoreChange;
use coven_foundation::clock::Clock;
use coven_storage::{test_utils::Faults, ObjectPath, Storage};

#[tokio::test]
async fn a_lost_create_reply_settles_exact_bytes_without_a_second_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "one").await;
    ana.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    assert!(matches!(ana.sync.pass().await, Err(SyncError::Storage(_))));
    assert!(ana.sync.database.current_claim().await.unwrap().is_some());
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), 3);
    assert!(ana.sync.database.current_claim().await.unwrap().is_none());
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 4.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    ana.sync.database.close().await.unwrap();
}

/// Queued writes leave the queue only once their claim is settled and the
/// state that consumed them is committed: a claim whose reply is lost leaves
/// them queued, and settling it applies them once (§6.3).
#[tokio::test]
async fn queued_writes_leave_the_queue_only_once_their_claim_settles() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync.pass().await.unwrap();
    insert(&ana.sync.database, "a", "one").await;
    insert(&ana.sync.database, "b", "two").await;
    let queued = ana.sync.database.test_queued_writes().await.unwrap().len();
    assert!(queued >= 2);
    ana.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    assert!(matches!(ana.sync.pass().await, Err(SyncError::Storage(_))));
    assert_eq!(ana.sync.database.test_queued_writes().await.unwrap().len(), queued);

    ana.sync.pass().await.unwrap();
    assert!(ana.sync.database.test_queued_writes().await.unwrap().is_empty());
    ben.sync.pass().await.unwrap();
    let expected = [("a".into(), "one".into()), ("b".into(), "two".into())];
    assert_eq!(rows(&ana.sync.database).await, expected);
    assert_eq!(rows(&ben.sync.database).await, expected);
    assert!(ana.sync.database.lost_values().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn administrative_contention_reuses_the_claim_number() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let counter = ana.sync.database.claim_counter().await.unwrap();
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    ana.hold_key("ana-new").await;
    ben.hold_key("ben-new").await;
    let first = ana.sync.administer(StoreChange::SetAccess {
        member: ana.identity.member(),
        access: access("ana-new"),
    });
    let second = async {
        observed.await.unwrap();
        ben.sync
            .administer(StoreChange::SetAccess {
                member: ben.identity.member(),
                access: access("ben-new"),
            })
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(first, second);
    result.unwrap();
    assert_eq!(ana.sync.database.claim_counter().await.unwrap(), counter + 1);
    ben.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        ben.sync.database.applied_slot().await.unwrap()
    );
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn a_stale_create_below_the_floor_is_removed_and_reclaimed_above_it() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "ana", "waiting").await;
    let stale = ana.sync.database.applied_slot().await.unwrap() + 1;
    let path = ObjectPath::slot(group.store, stale.try_into().unwrap());
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let retention = async {
        observed.await.unwrap();
        insert(&ben.sync.database, "ben", "first claimant").await;
        ben.sync.pass().await.unwrap();
        ben.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
        group.clock.set(
            std::time::UNIX_EPOCH
                + crate::sequence_sync::sequence_retention::GRACE
                + std::time::Duration::from_secs(1),
        );
        ben.sync.pass().await.unwrap();
        assert!(group.storage.status(&path).await.unwrap().is_none());
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(ana.sync.pass(), retention);
    result.unwrap();
    assert!(
        group.storage.status(&path).await.unwrap().is_none(),
        "a settled stale attempt must not leave recreated bytes below the floor"
    );
    ben.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

/// Ana's writes and Ben's snapshot both go for the slot after the last one
/// either has read. Ben's snapshot lands its slots, and his retention writes
/// a floor whose `at` is past every slot Ana has read. Ana's create then finds
/// the slot taken. Settling it, she reads the slots through the floor's `at`
/// before she checks the floor, accepts it, applies Ben's slots and claims the
/// next one (§5.1, §16.4).
#[tokio::test]
async fn a_lost_claim_accepts_a_floor_past_the_slots_it_has_read() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let contested = ana.sync.database.applied_slot().await.unwrap() + 1;
    assert_eq!(ben.sync.database.applied_slot().await.unwrap() + 1, contested);
    insert(&ana.sync.database, "ana", "waiting").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let retention = async {
        observed.await.unwrap();
        ben.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
        group.clock.set(
            std::time::UNIX_EPOCH
                + crate::sequence_sync::sequence_retention::GRACE
                + std::time::Duration::from_secs(1),
        );
        ben.sync.pass().await.unwrap();
        let floor = crate::floor_object::FloorObject::read(
            ben.database(),
            &group.storage,
            &ObjectPath::floor(group.store),
            None,
            &mut group.ring.clone(),
            &ben.identity,
        )
        .await
        .unwrap()
        .expect("retention wrote a floor");
        // The floor keeps the contested slot, and its `at` is past every
        // slot Ana has read.
        assert!(
            floor.header.first.get() <= contested && floor.header.at >= contested,
            "{:?}",
            floor.header
        );
        resume.send(()).unwrap();
        floor.header.at
    };
    let (result, at) = tokio::join!(ana.sync.pass(), retention);
    result.unwrap();
    // Her writes land in the slot after the floor's.
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), at + 1);
    assert!(ana.sync.database.current_claim().await.unwrap().is_none());
    ben.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    assert_eq!(rows(&ben.sync.database).await, [("ana".into(), "waiting".into())]);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn contending_writes_slots_apply_the_winner_and_claim_the_next_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let contested = ana.sync.database.applied_slot().await.unwrap() + 1;
    let counter = ana.sync.database.claim_counter().await.unwrap();
    insert(&ana.sync.database, "ana", "second").await;
    insert(&ben.sync.database, "ben", "first").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let ben_first = async {
        observed.await.unwrap();
        ben.sync.pass().await.unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(ana.sync.pass(), ben_first);
    result.unwrap();
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), contested + 1);
    assert_eq!(
        ana.sync.database.claim_counter().await.unwrap(),
        counter + 1,
        "contention keeps the claim number"
    );
    assert!(ana.sync.database.current_claim().await.unwrap().is_none());
    ben.sync.pass().await.unwrap();
    let rows_ben = rows(&ben.sync.database).await;
    assert_eq!(rows(&ana.sync.database).await, rows_ben);
    assert_eq!(rows_ben, [("ana".into(), "second".into()), ("ben".into(), "first".into())]);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn equal_claim_numbers_from_two_copies_at_one_slot_are_not_contention() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let (ana, copy) = group.copy(ana).await;
    insert(&ana.sync.database, "a", "original").await;
    insert(&copy.sync.database, "b", "copy").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let copy_first = async {
        observed.await.unwrap();
        copy.sync.pass().await.unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(ana.sync.pass(), copy_first);
    assert!(matches!(result, Err(SyncError::DeviceIdInUse(_))), "{result:?}");
    assert!(matches!(
        &*ana.sync.state.subscribe().borrow(),
        SequenceStatus::Stopped(SyncFailure::DeviceIdInUse(_))
    ));
    for device in [&ana, &copy] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn another_devices_slot_1_makes_setup_report_that_the_store_exists() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let first = group.create_result(&ana);
    let second = async {
        observed.await.unwrap();
        group.create(&ben).await;
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(first, second);
    assert!(
        matches!(&result, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::StoreExists),
        "{result:?}"
    );
    // Ana's laptop keeps the store it created, unsynced.
    assert_eq!(
        ana.sync.database.store_state().await.unwrap().members.keys().copied().collect::<Vec<_>>(),
        [ana.identity.member()]
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

mod differential {
    use crate::sequence_sync::tests::*;
    use crate::{slot_object, SyncError};
    use coven_database::{ClaimAttempt, ClaimSettlement, ClaimWork};
    use coven_format::{
        admin::StoreChange,
        sealed_slot::{SlotKind, SlotObjectPrefix},
        sequence::{SlotFrame, SlotHeader},
    };
    use coven_storage::{ObjectPath, Storage};
    use serde_json::json;
    use std::io::Cursor;

    async fn compare(prior: usize, current: usize, offset: u64) {
        let group = Group::new();
        let ana = group.device(1).await;
        group.create(&ana).await;
        for i in 0..prior {
            ana.hold_key(&format!("prior-{i}")).await;
            ana.sync
                .administer(StoreChange::SetAccess {
                    member: ana.identity.member(),
                    access: access(&format!("prior-{i}")),
                })
                .await
                .unwrap();
        }
        let after = ana.sync.database.applied_slot().await.unwrap();
        let number = ana.sync.database.claim_counter().await.unwrap() + 1;
        let path = ObjectPath::slot(group.store, (after + 1).try_into().unwrap());
        let seal = |n, name: &str| {
            slot_object::seal_bounded(
                &path,
                &SlotHeader {
                    device: ana.sync.device,
                    claim: n,
                    author: ana.identity.member(),
                    after,
                },
                &SlotFrame::Admin(StoreChange::SetAccess {
                    member: ana.identity.member(),
                    access: access(name),
                }),
                SlotObjectPrefix {
                    format: coven_format::FormatVersion::CURRENT,
                    key: group.ring.store_key_ids().next().unwrap(),
                    kind: SlotKind::Admin,
                },
                &group.ring,
                &ana.identity,
            )
            .unwrap()
        };
        let own = seal(number, "own");
        let mut hash = coven_crypto::ObjectHasher::new();
        hash.update(&own);
        let digest = format!("{:?}", hash.finish().as_bytes());
        let journal = own.clone();
        let attempted = path.clone();
        ana.sync
            .database
            .begin_claim(ClaimWork::Writes(0), move |n| {
                assert_eq!(n, number);
                Ok::<_, SyncError>(ClaimAttempt { path: attempted, bytes: Cursor::new(journal) })
            })
            .await
            .unwrap();
        let incoming_number = match current {
            3 => number + offset,
            4 => number - 1,
            _ => number,
        };
        let current_json = match current {
            0 => serde_json::Value::Null,
            1 => {
                group.storage.create(&path, &own).await.unwrap();
                json!({"device":ana.sync.device.0,"number":number,"bytes":"own"})
            }
            2..=4 => {
                group.storage.create(&path, &seal(incoming_number, "copy")).await.unwrap();
                json!({"device":ana.sync.device.0,"number":incoming_number,"bytes":"different"})
            }
            _ => unreachable!(),
        };
        let input = json!({"kind":"settle","recorded":{"device":ana.sync.device.0,"number":number,"slot":after+1,"bytes":"own","digest":digest},
            "current":current_json,"floor":{"first_kept":1,"last_claims":[]}});
        let result = {
            let mut session = ana.sync.session.lock().await;
            ana.sync.settle_open(&mut session, &mut group.ring.clone(), &ana.identity).await
        };
        let verdict = match result {
            Ok(Some(ClaimSettlement::Landed)) => "landed",
            Ok(Some(ClaimSettlement::Retry)) => "retry",
            Err(SyncError::DeviceIdInUse(_)) => "copy",
            other => panic!("unexpected settlement {other:?}: {input}"),
        };
        assert_eq!(
            json!(verdict),
            crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
            "{input}"
        );
        ana.sync.database.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
    async fn sequence_kept_settlement_matches_lean() {
        for prior in 0..3 {
            for current in 0..5 {
                compare(prior, current, 1).await;
            }
        }
        let mut rng = 0xc01d_u64;
        for _ in 0..16 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            compare((rng % 6) as usize, ((rng >> 8) % 5) as usize, 1 + ((rng >> 16) % 100)).await;
        }
        eprintln!("sequence settle: 15 exhaustive + 16 seeded authenticated kept-slot cases");
    }
}

/// Ana's library holds `notes` notes, and she writes one more while Ben's
/// edit arrives. How many rows applying Ben's slot kept aside or put back.
async fn rows_rebuilt_beside_an_unsent_write(notes: usize) -> usize {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync
        .database
        .write(move |sql| {
            for note in 0..notes {
                sql.execute("INSERT INTO notes VALUES(?1,'library')", [format!("note {note:03}")])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    insert(&ana.sync.database, "unsent", "Ana's").await;
    ben.sync
        .database
        .write(|sql| {
            sql.execute("UPDATE notes SET title='edited' WHERE id='note 001'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ana.work.take();
    ana.sync.pass().await.unwrap();
    let rebuilt = ana.work.take().published_rows;
    let titles = rows(&ana.sync.database).await;
    assert!(titles.contains(&("note 001".into(), "edited".into())));
    assert!(titles.contains(&("unsent".into(), "Ana's".into())));
    assert_eq!(titles.len(), notes + 1);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
    rebuilt
}

/// Applying a slot beside an unsent write keeps aside and puts back only the
/// rows the slot and the write reach, so it costs the same in a library of
/// 200 notes as in one of 2 (§6.2).
#[tokio::test]
async fn a_slot_applied_beside_an_unsent_write_rebuilds_only_the_rows_they_reach() {
    assert_eq!(
        rows_rebuilt_beside_an_unsent_write(200).await,
        rows_rebuilt_beside_an_unsent_write(2).await
    );
}

/// Ana's library holds `notes` notes. She writes one more, and her app writes
/// the next while the pass that sends the first waits on its slot's create,
/// as an import's next write lands during the pass sending the one before.
/// How many rows applying her own slot kept aside or put back.
async fn rows_rebuilt_as_the_next_write_lands(notes: usize) -> usize {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    ana.sync
        .database
        .write(move |sql| {
            for note in 0..notes {
                sql.execute("INSERT INTO notes VALUES(?1,'library')", [format!("note {note:04}")])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    insert(&ana.sync.database, "sent", "first").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    ana.work.take();
    let next = async {
        observed.await.unwrap();
        insert(&ana.sync.database, "next", "second").await;
        resume.send(()).unwrap();
    };
    let (sent, ()) = tokio::join!(ana.sync.pass(), next);
    sent.unwrap();
    let rebuilt = ana.work.take().published_rows;
    let titles = rows(&ana.sync.database).await;
    assert!(titles.contains(&("sent".into(), "first".into())));
    assert!(titles.contains(&("next".into(), "second".into())));
    assert_eq!(titles.len(), notes + 2);
    ana.sync.database.close().await.unwrap();
    rebuilt
}

/// A device applying its own slot while its next write waits keeps aside
/// only that write's rows, so an import's pass costs the same in a library of
/// 2,000 notes as in one of 2. Keeping every row aside instead made each such
/// slot rebuild the whole store, and the slower pass made the next write land
/// during the next pass too: the import stalled (§6.2).
#[tokio::test]
async fn a_slot_applied_as_the_next_write_lands_rebuilds_only_the_rows_it_reaches() {
    assert_eq!(
        rows_rebuilt_as_the_next_write_lands(2_000).await,
        rows_rebuilt_as_the_next_write_lands(2).await
    );
}

/// A device of `group` whose notes each belong to a folder, removed with it.
async fn folders_device(group: &Group, seed: u8) -> Device {
    use coven_database::{DatabaseBuilder, Migration, RowIdentity, SyncedTable};
    use coven_foundation::id_source::{IdSource, MemberId};
    let (temp, directory) = group.directory();
    let database = DatabaseBuilder::new(directory)
        .synced_tables(vec![
            SyncedTable::new("folders", RowIdentity::SharedKey),
            SyncedTable::new("notes", RowIdentity::SharedKey),
        ])
        .migrations(vec![Migration::addition(
            1,
            "folders",
            "CREATE TABLE folders(id TEXT NOT NULL PRIMARY KEY);
             CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,
               folder TEXT NOT NULL REFERENCES folders(id) ON DELETE CASCADE)",
        )])
        .open()
        .await
        .unwrap();
    let identity = coven_crypto::MemberIdentity::new(
        MemberId(group.ids.new_id()),
        coven_format::test_utils::keys_from_seed(seed),
    );
    group
        .attach(temp, identity, Some(group.ring.clone()), group.storage.for_device(), database)
        .await
}

async fn write(db: &coven_database::Database, statement: &'static str) {
    db.write(move |sql| {
        sql.execute(statement, [])?;
        Ok(())
    })
    .await
    .unwrap();
}

/// What a device's database holds: its notes, and its lost values.
async fn folders_state(db: &coven_database::Database) -> (Vec<(String, String)>, String) {
    let notes = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query("SELECT id,title FROM notes ORDER BY id", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?)
        })
        .await
        .unwrap();
    let mut lost: Vec<String> =
        db.lost_values().await.unwrap().iter().map(|lost| format!("{lost:?}")).collect();
    lost.sort();
    (notes, lost.join("\n"))
}

/// Ana deletes the "drafts" folder and hasn't sent it yet when Ben's note
/// in that folder arrives: with her delete over it, his note goes with the
/// folder. Then Ben's edit of the note arrives, her delete still unsent. The
/// removal changed only the note's app row, so the note's records are the
/// slots' when his edit applies, and once her delete goes out both devices
/// hold the same notes and lost values (§6.2).
#[tokio::test]
async fn a_slot_changes_a_row_an_unsent_removal_reached() {
    let group = Group::new();
    let ana = folders_device(&group, 1).await;
    let ben = folders_device(&group, 2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    write(&ana.sync.database, "INSERT INTO folders VALUES('drafts')").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    write(&ana.sync.database, "DELETE FROM folders WHERE id='drafts'").await;
    write(&ben.sync.database, "INSERT INTO notes VALUES('plan','first','drafts')").await;
    ben.sync.pass().await.unwrap();
    // Ana applies each of Ben's slots while her delete waits: sending it
    // fails.
    let refuse_next_slot = || async {
        let next = ben.sync.database.applied_slot().await.unwrap() + 1;
        let mut faults = coven_storage::test_utils::Faults::none();
        faults.create_failures.insert(
            ObjectPath::slot(group.store, next.try_into().unwrap()),
            (coven_storage::StorageFailure::PermissionDenied, 403),
        );
        ana.storage.set_faults(faults).await;
    };
    refuse_next_slot().await;
    assert!(ana.sync.pass().await.is_err());
    ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    write(&ben.sync.database, "UPDATE notes SET title='second' WHERE id='plan'").await;
    ben.sync.pass().await.unwrap();
    refuse_next_slot().await;
    assert!(ana.sync.pass().await.is_err());
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        ben.sync.database.applied_slot().await.unwrap()
    );
    ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    assert!(folders_state(&ana.sync.database).await.0.is_empty());
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(folders_state(&ana.sync.database).await, folders_state(&ben.sync.database).await);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A slot the provider refuses to publish for its size, a copy past the
/// profile's single-copy limit, fails the pass naming the limit; the writes
/// stay queued with their bytes and go out once storage takes the slot
/// (§20.6).
#[tokio::test]
async fn a_slot_past_the_providers_copy_limit_waits_naming_copy_too_large() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let next = ana.database().applied_slot().await.unwrap() + 1;
    let refused = ObjectPath::slot(group.store, next.try_into().unwrap());
    ana.storage
        .set_faults(Faults {
            create_failures: [(
                refused.clone(),
                (coven_storage::StorageFailure::CopyTooLarge { size: 9, limit: 5 }, 400),
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    insert(ana.database(), "big", "queued note").await;

    let error = ana.sync.pass().await.unwrap_err();
    assert!(
        matches!(&error, SyncError::Storage(source)
            if source.failure() == (coven_storage::StorageFailure::CopyTooLarge { size: 9, limit: 5 })),
        "{error:?}"
    );
    assert_eq!(ana.database().waiting_snapshot_headers().await.unwrap().len(), 1);
    assert!(ana.storage.status(&refused).await.unwrap().is_none());

    ana.storage.set_faults(Faults::none()).await;
    ana.sync.pass().await.unwrap();
    assert!(ana.database().waiting_snapshot_headers().await.unwrap().is_empty());
    assert!(ana.storage.status(&refused).await.unwrap().is_some());
    ana.database().close().await.unwrap();
}

/// A writes slot above the provider's single-request limit uploads in parts
/// and becomes visible only when complete: while a later part is held, the
/// slot's path has no object and another device finds no new slot (§6.3).
#[tokio::test]
async fn a_large_writes_slot_is_visible_only_once_complete() {
    let mut group = Group::new();
    group.storage = coven_storage::test_utils::MemoryStorage::builder()
        .clock(group.clock.clone())
        .transfer_limits(256, 128)
        .build()
        .unwrap();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let slot = ana.database().applied_slot().await.unwrap() + 1;
    let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
    let title: &'static str = Box::leak("x".repeat(1000).into_boxed_str());
    insert(ana.database(), "big", title).await;

    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    group.storage.gate_parts(gate.clone()).await;
    let traced = ana.storage.request_trace().len();
    let mut requests = ana.storage.subscribe_requests();
    let sending = tokio::spawn({
        let sync = ana.sync.clone();
        async move { sync.pass().await }
    });
    let part = coven_storage::test_utils::MemoryRequest::Other("upload_part");
    let parts = || ana.storage.request_trace()[traced..].iter().filter(|r| **r == part).count();
    // The first part is stored; the second is held with its bytes taken.
    for held in 1..=2 {
        // Each request is traced before it is counted, so the trace holds
        // every request the count has heard of.
        let arrived = async {
            while parts() < held {
                requests.changed().await.unwrap();
            }
        };
        coven_foundation::clock::within(arrived).await.expect("a part reaches storage");
        assert_eq!(parts(), held, "the slot is sent in parts");
        if held == 1 {
            gate.add_permits(1);
        }
    }
    assert!(group.storage.status(&path).await.unwrap().is_none());
    ben.sync.pass().await.unwrap();
    assert_eq!(ben.database().applied_slot().await.unwrap(), slot - 1);
    assert!(rows(ben.database()).await.is_empty());

    gate.add_permits(64);
    sending.await.unwrap().unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_some());
    ben.sync.pass().await.unwrap();
    assert_eq!(rows(ben.database()).await, [("big".into(), title.into())]);
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Ana's large note goes up in parts. Its claim records the plan of its
/// upload at the slot, and then a provider cooldown, which another request
/// met meanwhile, refuses the first part before anything is sent: the plan
/// stays unmarked. Once the cooldown has passed, the next pass sends the
/// claim from its plan at the slot, which no device owns (§17.9, Appendix
/// E.10).
#[tokio::test]
async fn a_claim_refused_before_its_first_part_goes_on_from_its_plan() {
    use coven_storage::test_utils::MemoryRequest;
    let mut group = Group::new();
    group.storage = coven_storage::test_utils::MemoryStorage::builder()
        .clock(group.clock.clone())
        .transfer_limits(256, 128)
        .build()
        .unwrap();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let slot = ana.database().applied_slot().await.unwrap() + 1;
    let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
    let title: &'static str = Box::leak("x".repeat(1000).into_boxed_str());
    insert(ana.database(), "big", title).await;

    let (started, planning) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group.storage.hold_next_request_of(MemoryRequest::Other("begin_upload"), started, held).await;
    let sending = tokio::spawn({
        let sync = ana.sync.clone();
        async move { sync.pass().await }
    });
    coven_foundation::clock::within(planning).await.expect("the claim plans").unwrap();
    ana.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: coven_storage::StorageFailure::RateLimited,
            retry_after: Some(60),
            ..Faults::none()
        })
        .await;
    ana.storage.status(&path).await.unwrap_err();
    resume.send(()).unwrap();
    let refused = sending.await.unwrap().unwrap_err();
    assert!(
        matches!(&refused, SyncError::Storage(error)
            if error.failure() == coven_storage::StorageFailure::RateLimited),
        "{refused:?}"
    );
    assert!(group.storage.unfinished_uploads().await.is_empty(), "nothing started");

    group.clock.set(group.clock.now() + std::time::Duration::from_secs(60));
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_some());
    assert_eq!(ana.database().applied_slot().await.unwrap(), slot);
    assert_eq!(rows(ana.database()).await, [("big".into(), title.into())]);
    ana.database().close().await.unwrap();
}

/// The first device applies slot 1 when it creates the store, so its writes
/// before storage was set up read through it and are checked against slot 1's
/// record of the store's creation; a write reading nothing is refused for
/// every device (§7.1).
#[tokio::test]
async fn the_first_devices_writes_before_storage_read_through_the_creation() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    crate::creation::tests::decide_for(&group, &ana).await.unwrap();
    insert(ana.database(), "offline", "before storage").await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(rows(ben.database()).await, [("offline".into(), "before storage".into())]);
    assert_eq!(rows(ana.database()).await, rows(ben.database()).await);

    let history = ana.sync.session.lock().await.history.clone();
    let first = history.added(ana.sync.device).expect("slot 1 registers the first device");
    assert_eq!(first, 1);
    let (member, _) = history.owner(ana.sync.device, first).expect("it writes once it read slot 1");
    assert_eq!(member, ana.identity.member());
    assert!(matches!(history.owner(ana.sync.device, 0), Err(Refusal::NotAuthorized)));
    assert!(matches!(history.owner(ben.sync.device, 0), Err(Refusal::NotAuthorized)));
    // Once the device has been added, reading slot 1 is enough for its own.
    assert!(history.owner(ben.sync.device, history.added(ben.sync.device).unwrap()).is_ok());
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Ana and Ben share the Gifts circle.
async fn gifts() -> (Group, Device, Device, coven_foundation::id_source::CircleId) {
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
    (group, ana, ben, circle)
}

/// The slots `device` has tried to create.
fn slot_creates(device: &Device) -> usize {
    device
        .storage
        .request_trace()
        .into_iter()
        .filter(|request| {
            matches!(request, coven_storage::test_utils::MemoryRequest::Create(path)
                if path.slot_number().is_some())
        })
        .count()
}

/// How many slots Ben's rotation of Gifts creates: its key copies, then its entry.
async fn rotation_creates() -> usize {
    let (_group, ana, ben, circle) = gifts().await;
    let before = slot_creates(&ben);
    ben.sync.rotate_key(coven_merge::Audience::Circle(circle)).await.unwrap();
    let creates = slot_creates(&ben) - before;
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
    creates
}

/// Hold each of Ben's rotation creations in turn. `at_entry` runs while the
/// last, the entry's own, is held.
async fn rotating(
    group: &Group,
    ben: &Device,
    circle: coven_foundation::id_source::CircleId,
    creates: usize,
    at_entry: impl AsyncFnOnce(),
) -> Result<(), SyncError> {
    let arm = || async {
        let (started, observed) = tokio::sync::oneshot::channel();
        let (resume, held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
            .await;
        (observed, resume)
    };
    let mut current = Some(arm().await);
    let rotation = ben.sync.rotate_key(coven_merge::Audience::Circle(circle));
    let steering = async {
        let mut at_entry = Some(at_entry);
        for step in 0..creates {
            let (observed, resume) = current.take().unwrap();
            observed.await.unwrap();
            if step + 1 < creates {
                current = Some(arm().await);
            } else {
                at_entry.take().unwrap()().await;
            }
            resume.send(()).unwrap();
        }
    };
    let (result, ()) = tokio::join!(rotation, steering);
    result
}

/// Ben rotates the Gifts key. His key copies land, and his entry's slot is
/// created with its reply lost. After a restart his first pass settles that
/// slot, finishes the rotation, and claims no second entry (§11.1, §11.4).
#[tokio::test]
async fn a_rotation_whose_reply_was_lost_settles_its_slot_before_finishing() {
    let creates = rotation_creates().await;
    let (group, ana, ben, circle) = gifts().await;
    let introductions = ben.sync.session.lock().await.history.introductions.len();
    let result = rotating(&group, &ben, circle, creates, async || {
        ben.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    })
    .await;
    assert!(matches!(result, Err(SyncError::Storage(_))), "{result:?}");
    assert_eq!(ben.sync.database.operations().await.unwrap().len(), 1);

    let ben = group.reopen(ben).await;
    group.clock.set(group.clock.now() + std::time::Duration::from_secs(600));
    ben.sync.pass().await.unwrap();
    let left = ben.sync.database.operations().await.unwrap();
    assert!(left.is_empty(), "{left:?}");
    assert!(ben.sync.database.current_claim().await.unwrap().is_none());
    let session = ben.sync.session.lock().await;
    assert_eq!(session.history.introductions.len(), introductions + 1, "the rotation lands once");
    let slot = ben.sync.database.applied_slot().await.unwrap();
    drop(session);
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, (slot + 1).try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), slot);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's entry is beaten to its slot by Ana's removal of him from Gifts. His
/// call applies it, undoes what is left and fails; the copies he claimed stay
/// in history, and the key they carry counts as exposed (§11.3, §12.2).
#[tokio::test]
async fn a_rotation_beaten_to_its_slot_by_a_removal_is_rejected_and_its_copies_stay_exposed() {
    let creates = rotation_creates().await;
    let (group, ana, ben, circle) = gifts().await;
    let audience = coven_merge::Audience::Circle(circle);
    let keys_before: Vec<_> =
        ben.sync.keys.read().unwrap().unwrap().circle_key_ids(circle).collect();
    let copies = ben.sync.session.lock().await.history.copies.len();
    let result = rotating(&group, &ben, circle, creates, async || {
        ana.sync.remove_circle_member(circle, ben.identity.member()).await.unwrap();
    })
    .await;
    assert!(
        matches!(result, Err(SyncError::Rejected(OperationRejection::NotAllowed))),
        "{result:?}"
    );
    assert!(ben.sync.database.operations().await.unwrap().is_empty());
    let ring = ben.sync.keys.read().unwrap().unwrap();
    let rotated: Vec<_> =
        ring.circle_key_ids(circle).filter(|key| !keys_before.contains(key)).collect();
    assert_eq!(rotated.len(), 1, "the new key stays in custody");
    let session = ben.sync.session.lock().await;
    assert!(session.history.copies.len() > copies, "the claimed copies stay in history");
    assert!(session.history.exposed(&audience, rotated[0]));
    drop(session);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
