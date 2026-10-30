use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::StoreChange;
use coven_storage::{test_utils::Faults, ObjectPath, Storage};

#[tokio::test]
async fn a_lost_create_reply_settles_exact_bytes_without_a_second_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "one").await;
    ana.storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
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
    assert_eq!(
        ana.sync.database.claim_counter().await.unwrap(),
        counter + 1
    );
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
        ben.sync
            .snapshot(coven_merge::Audience::Store)
            .await
            .unwrap();
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
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
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
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        contested + 1
    );
    assert_eq!(
        ana.sync.database.claim_counter().await.unwrap(),
        counter + 1,
        "contention keeps the claim number"
    );
    assert!(ana.sync.database.current_claim().await.unwrap().is_none());
    ben.sync.pass().await.unwrap();
    let rows_ben = rows(&ben.sync.database).await;
    assert_eq!(rows(&ana.sync.database).await, rows_ben);
    assert_eq!(
        rows_ben,
        [
            ("ana".into(), "second".into()),
            ("ben".into(), "first".into())
        ]
    );
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
    assert!(
        matches!(result, Err(SyncError::DeviceIdInUse(_))),
        "{result:?}"
    );
    assert!(matches!(
        &*ana.sync.state.subscribe().borrow(),
        SequenceStatus::Stopped(SyncFailure::DeviceIdInUse(_))
    ));
    for device in [&ana, &copy] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_competing_first_setup_reports_that_the_store_exists() {
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
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync
            .database
            .store_state()
            .await
            .unwrap()
            .members
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [ben.identity.member()]
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
                Ok(ClaimAttempt {
                    path: attempted,
                    bytes: Cursor::new(journal),
                })
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
                group
                    .storage
                    .create(&path, &seal(incoming_number, "copy"))
                    .await
                    .unwrap();
                json!({"device":ana.sync.device.0,"number":incoming_number,"bytes":"different"})
            }
            _ => unreachable!(),
        };
        let input = json!({"kind":"settle","recorded":{"device":ana.sync.device.0,"number":number,"slot":after+1,"bytes":"own","digest":digest},
            "current":current_json,"floor":{"first_kept":1,"last_claims":[]}});
        let result = {
            let mut session = ana.sync.session.lock().await;
            ana.sync
                .settle_open(&mut session, &mut group.ring.clone(), &ana.identity)
                .await
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
            compare(
                (rng % 6) as usize,
                ((rng >> 8) % 5) as usize,
                1 + ((rng >> 16) % 100),
            )
            .await;
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
                sql.execute(
                    "INSERT INTO notes VALUES(?1,'library')",
                    [format!("note {note:03}")],
                )?;
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
                sql.execute(
                    "INSERT INTO notes VALUES(?1,'library')",
                    [format!("note {note:04}")],
                )?;
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
        .attach(
            temp,
            identity,
            Some(group.ring.clone()),
            group.storage.for_device(),
            database,
        )
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
            Ok(
                sql.query("SELECT id,title FROM notes ORDER BY id", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?,
            )
        })
        .await
        .unwrap();
    let mut lost: Vec<String> = db
        .lost_values()
        .await
        .unwrap()
        .iter()
        .map(|lost| format!("{lost:?}"))
        .collect();
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
    write(
        &ben.sync.database,
        "INSERT INTO notes VALUES('plan','first','drafts')",
    )
    .await;
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
    ana.storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    write(
        &ben.sync.database,
        "UPDATE notes SET title='second' WHERE id='plan'",
    )
    .await;
    ben.sync.pass().await.unwrap();
    refuse_next_slot().await;
    assert!(ana.sync.pass().await.is_err());
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        ben.sync.database.applied_slot().await.unwrap()
    );
    ana.storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    assert!(folders_state(&ana.sync.database).await.0.is_empty());
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        folders_state(&ana.sync.database).await,
        folders_state(&ben.sync.database).await
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
