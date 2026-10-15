//! Old-key windows (§10, D5; `OldKeyObjects`): each unread kept replacement
//! of a write's owner's keys that installs another signing key bounds the
//! write to thirty storage days, judged and rebuilt like device retirements.

use crate::entry_effects::tests::fate;
use crate::pending::tests::Record;
use crate::store_log::tests::{checked, keys, member};
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::write_landing::tests::{at, dropped_reports, retire, DAY};
use crate::{DeviceState, EntryId, EntryOutcome, Published, StoreDevice};
use coven_format::store_log::{StoreChange, StoreLogEntry};
use coven_format::value::EntryPositions;
use coven_foundation::id_source::{DeviceId, SequentialIds};

/// Member 2's pair in the fixtures, bound when [`retire`] first adds them.
fn old() -> coven_crypto::MemberPublicKeys {
    keys(2)
}

/// A kept replacement of member 2's keys with `seed`'s pair, stored at
/// `seconds`, as replay supplies it.
async fn replace_keys(target: &crate::Database, number: u64, seed: u8, seconds: u64) -> EntryId {
    let log = target.store_log().await.unwrap();
    let id = EntryId {
        device: DeviceId(98),
        number,
    };
    let mut replay = log.replay;
    let fresh = keys(seed);
    replay.state.members.get_mut(&member(2)).unwrap().keys = fresh;
    replay.state.bound_keys.insert(fresh);
    let time = log.entries.iter().map(|e| e.entry.timestamp).max();
    let entry = StoreLogEntry {
        position: id,
        timestamp: coven_merge::Timestamp::new(
            time.map_or(1, |t| t.milliseconds() + 1),
            0,
            id.device,
        )
        .unwrap(),
        author: member(2),
        had_read: EntryPositions(Vec::new()),
        change: StoreChange::ReplaceMemberKeys {
            member: member(2),
            keys: fresh,
        },
    };
    replay.entries.insert(id, EntryOutcome::Kept);
    let mut checked = checked(entry);
    checked.stored_at = at(seconds);
    target
        .apply_store_log::<Record>(checked, replay, Vec::new())
        .await
        .unwrap();
    id
}

/// Register `device` to member 2, added by an unrelated kept entry that
/// retires nothing the device writes.
async fn register(target: &crate::Database, device: DeviceId) {
    retire(target, DeviceId(5000), 1, "device", 0).await;
    let mut replay = target.store_log().await.unwrap().replay;
    replay.state.devices.insert(
        device,
        StoreDevice {
            member: member(2),
            name: "writer".into(),
            state: DeviceState::Active,
        },
    );
    fate_with(target, replay).await;
}

/// A source and a target store; the target receives the source's write.
struct Pair {
    _stores: [TestStore; 2],
    target: crate::Database,
}

/// A pair whose write device belongs to member 2.
async fn registered() -> (Pair, coven_format::write::WriteRecord) {
    let ids = SequentialIds::new();
    let stores = [TestStore::with_ids(&ids), TestStore::with_ids(&ids)];
    let source = stores[0].schema(notes(), NOTES).await.unwrap();
    let target = stores[1].schema(notes(), NOTES).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let write = records(&source).remove(0);
    register(&target, write.header.position.device).await;
    (
        Pair {
            _stores: stores,
            target,
        },
        write,
    )
}

/// Commit `replay` over the applied entries unchanged.
async fn fate_with(target: &crate::Database, replay: crate::StoreLogReplay) {
    let log = target.store_log().await.unwrap();
    target
        .apply_store_log_batch::<Record>(
            log,
            vec![],
            replay,
            vec![],
            vec![],
            Vec::new(),
            Vec::new(),
        )
        .await
        .unwrap();
}

fn signed(seconds: u64, signer: coven_crypto::MemberPublicKeys) -> Published {
    Published {
        stored_at: at(seconds),
        signer: signer.signing,
    }
}

/// An unread kept replacement admits an old-key write through exactly thirty
/// storage days after it; one second more consumes it without effects or
/// losses, reported as landed too late (`boundary_inclusive`).
#[tokio::test]
async fn an_unread_replacement_bounds_old_key_writes_to_thirty_days() {
    for (offset, late) in [(0, false), (1, true)] {
        let (p, write) = registered().await;
        let position = write.header.position;
        replace_keys(&p.target, 1, 9, DAY).await;
        p.target
            .apply_downloaded::<Record>(write.into(), signed(31 * DAY + offset, old()))
            .await
            .unwrap();
        assert_eq!(count(&p.target, "notes"), i64::from(!late));
        assert!(p.target.lost_values().await.unwrap().is_empty());
        assert!(p
            .target
            .sync_state(vec![])
            .await
            .unwrap()
            .positions
            .covers(position));
        assert_eq!(dropped_reports(&p.target).len(), usize::from(late));
    }
}

/// A replacement the write read retires no key current in its view, and a
/// write under the key a kept replacement installs is not an old-key write
/// (`OldKeyObjects.retires`, `equal_payload_is_current`).
#[tokio::test]
async fn read_replacements_and_the_installed_key_retire_nothing() {
    let (p, mut write) = registered().await;
    let read = replace_keys(&p.target, 1, 9, DAY).await;
    write.header.store_log_read = EntryPositions(vec![read]);
    p.target
        .apply_downloaded::<Record>(write.into(), signed(40 * DAY, keys(9)))
        .await
        .unwrap();
    assert_eq!(count(&p.target, "notes"), 1);
    let (p, write) = registered().await;
    replace_keys(&p.target, 1, 9, DAY).await;
    p.target
        .apply_downloaded::<Record>(write.into(), signed(40 * DAY, keys(9)))
        .await
        .unwrap();
    assert_eq!(count(&p.target, "notes"), 1);
    assert!(dropped_reports(&p.target).is_empty());
}

/// A write signed with a losing replacement's key is retired by the kept
/// winner's different key (`losing_binding_is_retired`).
#[tokio::test]
async fn the_winner_retires_a_losing_replacements_key() {
    let (p, write) = registered().await;
    replace_keys(&p.target, 1, 9, DAY).await;
    p.target
        .apply_downloaded::<Record>(write.into(), signed(31 * DAY + 1, keys(7)))
        .await
        .unwrap();
    assert_eq!(count(&p.target, "notes"), 0);
    assert_eq!(dropped_reports(&p.target).len(), 1);
}

/// Dropping the deciding replacement rebuilds the audience with the write;
/// keeping it again excludes it, with its report following each verdict.
/// Unlike a device retirement, a replacement drains no device log.
#[tokio::test]
async fn replay_recomputes_old_key_verdicts_and_drains_nothing() {
    let (p, write) = registered().await;
    let device = write.header.position.device;
    let replacement = replace_keys(&p.target, 1, 9, DAY).await;
    p.target
        .apply_downloaded::<Record>(write.into(), signed(40 * DAY, old()))
        .await
        .unwrap();
    assert_eq!(count(&p.target, "notes"), 0);
    fate(&p.target, replacement, false).await.unwrap();
    assert_eq!(count(&p.target, "notes"), 1);
    assert!(dropped_reports(&p.target).is_empty());
    fate(&p.target, replacement, true).await.unwrap();
    assert_eq!(count(&p.target, "notes"), 0);
    assert_eq!(dropped_reports(&p.target).len(), 1);
    let retiring = p.target.inspect_writer(|sql| {
        sql.query_row(
            "SELECT retires_device IS NULL AND retires_member IS NULL FROM _coven_store_log
             WHERE device=?1 AND number=?2",
            (
                replacement.device.0.to_be_bytes().as_slice(),
                replacement.number.to_be_bytes().as_slice(),
            ),
            |r| r.get::<_, bool>(0),
        )
        .unwrap()
    });
    assert!(retiring, "a key replacement retires no device id");
    assert_eq!(
        p.target.store_log().await.unwrap().replay.state.devices[&device].state,
        DeviceState::Active
    );
}

/// The late verdict is final once its deciding replacement is final
/// (`replacement_window_final`): inputs and the verdict are released.
#[tokio::test]
async fn a_final_replacement_settles_the_old_key_verdict() {
    let (p, write) = registered().await;
    replace_keys(&p.target, 1, 9, DAY).await;
    p.target
        .apply_downloaded::<Record>(write.into(), signed(40 * DAY, old()))
        .await
        .unwrap();
    let landings = |db: &crate::Database| {
        db.inspect_writer(|sql| {
            sql.query_row("SELECT count(*) FROM _coven_write_landings", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
        })
    };
    assert_eq!(landings(&p.target), 1);
    p.target
        .advance_store_log_horizon(p.target.store_log().await.unwrap(), at(DAY), at(DAY))
        .await
        .unwrap();
    assert_eq!(
        landings(&p.target),
        1,
        "the replacement at the horizon is not final"
    );
    p.target
        .advance_store_log_horizon(
            p.target.store_log().await.unwrap(),
            at(DAY + 1),
            at(DAY + 1),
        )
        .await
        .unwrap();
    assert_eq!(landings(&p.target), 0);
    assert_eq!(count(&p.target, "notes"), 0);
    assert_eq!(dropped_reports(&p.target).len(), 1);
}

/// B17's dependent histories with an old-key window instead of a device
/// retirement: Ana's creation lands one second after her replaced key's
/// deadline. Ben's note edit and Carol's delete built on it become frozen
/// losses naming it, Ben's playlist rename applies, both receive orders
/// agree, and dropping the replacement restores them (§8, §10, D7).
#[tokio::test]
async fn changes_built_on_an_old_key_write_are_frozen_and_unrelated_ones_apply() {
    use crate::Replacement;
    let ids = SequentialIds::new();
    let stores: Vec<_> = (0..5).map(|_| TestStore::with_ids(&ids)).collect();
    let mut dbs = Vec::new();
    for store in &stores {
        dbs.push(store.schema(notes(), NOTES).await.unwrap());
    }
    let [ana, ben, carol, first, second] = &dbs[..] else {
        unreachable!()
    };
    sql(ana, "INSERT INTO notes VALUES('note','draft','body')")
        .await
        .unwrap();
    let creation = records(ana).remove(0);
    sql(ben, "INSERT INTO notes VALUES('playlist','Mix','')")
        .await
        .unwrap();
    for reader in [ben, carol] {
        reader.receive(creation.clone().into()).await.unwrap();
    }
    sql(
        ben,
        "UPDATE notes SET title='edited' WHERE id='note'; UPDATE notes SET title='Road trip' WHERE id='playlist'",
    )
    .await
    .unwrap();
    sql(carol, "DELETE FROM notes WHERE id='note'")
        .await
        .unwrap();
    let ben_writes = records(ben);
    let delete = records(carol).remove(0);
    let rows = |db: &crate::Database| {
        db.inspect_writer(|sql| {
            sql.query("SELECT id,title FROM notes ORDER BY id", [], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .unwrap()
        })
    };
    let mut results = Vec::new();
    for (target, delete_first) in [(first, false), (second, true)] {
        register(target, creation.header.position.device).await;
        let replacement = replace_keys(target, 1, 9, 0).await;
        target
            .apply_downloaded::<Record>(creation.clone().into(), signed(30 * DAY + 1, old()))
            .await
            .unwrap();
        target.receive(ben_writes[0].clone().into()).await.unwrap();
        if delete_first {
            target.receive(delete.clone().into()).await.unwrap();
            target.receive(ben_writes[1].clone().into()).await.unwrap();
        } else {
            target.receive(ben_writes[1].clone().into()).await.unwrap();
            target.receive(delete.clone().into()).await.unwrap();
        }
        assert_eq!(rows(target), [("playlist".into(), "Road trip".into())]);
        let mut causes: Vec<_> = target
            .lost_values()
            .await
            .unwrap()
            .iter()
            .map(|loss| match &loss.replaced_by {
                Replacement::DataDependency { write, sources } => {
                    (*write, sources.iter().map(|s| s.write).collect::<Vec<_>>())
                }
                other => panic!("{other:?}"),
            })
            .collect();
        causes.sort_by_key(|(write, _)| format!("{write:?}"));
        let created = creation.header.position;
        let mut expected = vec![
            (ben_writes[1].header.position, vec![created, created]),
            (delete.header.position, vec![created; 4]),
        ];
        expected.sort_by_key(|(write, _)| format!("{write:?}"));
        assert_eq!(causes, expected);
        results.push((rows(target), causes));
        fate(target, replacement, false).await.unwrap();
        let losses = target.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        assert_eq!(
            losses[0].replaced_by,
            Replacement::Write(delete.header.position)
        );
        assert_eq!(rows(target).len(), 1);
    }
    assert_eq!(results[0], results[1]);
}
