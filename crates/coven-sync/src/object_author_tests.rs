//! Recorded-view signers under member-key replacement (§10, D9). Sending
//! needs the member's current credentials and the object's own recorded
//! signer to be the held key, while recorded views keep old signatures
//! checkable. The keyed differential compares replacements and old-key
//! write windows with `Keyed.resolve`, `authorized`, `conflict`/`prefer` and
//! `OldKeyObjects`.

use std::sync::Arc;

use coven_crypto::{MemberIdentity, MemberKeys};
use coven_format::store_log::{MemberRole, StoreChange};
use coven_foundation::id_source::DeviceId;
use coven_storage::Storage;

use crate::store_log_sync::tests::{device, key, member, public, storage, store, Device};
use crate::SyncError;

#[path = "../tests/fixtures/member_key_history.rs"]
pub(crate) mod member_key_history;
use member_key_history::{self as keyed, DAY};

use crate::entry_signature::tests::{Numbers, STORE};
use coven_database::{EntryOutcome, StoreLog};
use coven_format::value::EntryId;
use std::collections::{BTreeMap, BTreeSet};

/// Ben's id with a fresh private pair no entry has bound.
fn fresh_ben() -> MemberIdentity {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([0x42; 64]);
    MemberIdentity::new(
        member(2).member(),
        MemberKeys::from_secret_bytes(&bytes).unwrap(),
    )
}

/// Ana (admin, device 1) and Ben (member, device 2), both caught up.
async fn household() -> (
    Arc<coven_storage::test_utils::MemoryStorage>,
    Device,
    Device,
) {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    ben.sync().await;
    ben.sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "Ben's laptop".into(),
            replaces: None,
        })
        .await
        .unwrap();
    ana.sync().await;
    (storage, ana, ben)
}

fn replaced(error: &SyncError) -> bool {
    matches!(error, SyncError::MemberKeysReplaced { member: id } if *id == member(2).member())
}

/// An admin replaces Ben's keys; his installation, still holding the old
/// pair, keeps its device id but can author and post nothing more.
#[tokio::test]
async fn an_installation_holding_replaced_keys_stops_sending() {
    let (_storage, mut ana, mut ben) = household().await;
    ana.sync
        .make_and_upload_entry(StoreChange::ReplaceMemberKeys {
            member: member(2).member(),
            keys: public(&fresh_ben()),
        })
        .await
        .unwrap();
    let state = ana.log().await.replay.state;
    assert_eq!(
        state.members[&member(2).member()].keys,
        public(&fresh_ben())
    );
    assert!(state.devices[&DeviceId(2)].active());
    let caught_up = ben.sync.sync_store_log().await;
    assert!(caught_up.as_ref().is_err_and(replaced), "{caught_up:?}");
    let entry = ben
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(3),
            name: "Ben's phone".into(),
            replaces: None,
        })
        .await;
    assert!(entry.as_ref().is_err_and(replaced), "{entry:?}");
    let posted = ben.writes.post_positions().await;
    assert!(posted.as_ref().is_err_and(replaced), "{posted:?}");
    // The device id stays Ben's and active: replacement retires no device.
    assert!(ben.log().await.replay.state.devices[&DeviceId(2)].active());
}

/// An entry fixed before the replacement verifies only with the old key its
/// recorded past binds. Once custody holds the new pair, retrying it would
/// re-sign it with a key that past never bound, so it is never sent (§10).
#[tokio::test]
async fn newer_custody_never_re_signs_an_entry_fixed_under_old_keys() {
    let (storage, mut ana, mut ben) = household().await;
    let next = ben
        .log()
        .await
        .positions()
        .0
        .iter()
        .find(|id| id.device == DeviceId(2))
        .unwrap()
        .number
        + 1;
    ben.interrupt_entry(next).await;
    assert!(ben
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(3),
            name: "Ben's phone".into(),
            replaces: None,
        })
        .await
        .is_err());
    ben.restore_storage();
    ana.sync
        .make_and_upload_entry(StoreChange::ReplaceMemberKeys {
            member: member(2).member(),
            keys: public(&fresh_ben()),
        })
        .await
        .unwrap();
    ben.identity.persist(&fresh_ben()).unwrap();
    ben.member = fresh_ben();
    let result = ben.sync.sync_store_log().await;
    assert!(result.as_ref().is_err_and(replaced), "{result:?}");
    let path = crate::store_log_object::path(
        store(1),
        coven_format::value::EntryId {
            device: DeviceId(2),
            number: next,
        },
    );
    assert!(
        storage.read(&path).await.is_err(),
        "the old entry was never sent"
    );
    assert!(ben.db.local_store_log().await.unwrap().upload.is_some());
}

/// Positions sealed for Ben's device with `positions` as their signed store
/// log, signed by `signer`.
fn ben_positions(
    ana: &Device,
    positions: coven_format::value::EntryPositions,
    signer: &MemberIdentity,
) -> (coven_storage::ObjectPath, Vec<u8>) {
    use coven_format::{objects::PostedPositions, sealed_single::SingleChunkPrefix, Object};
    let path = coven_storage::ObjectPath::positions(store(1), DeviceId(2));
    let prefix = SingleChunkPrefix::PostedPositions(key(1));
    let ring = ana.custody.read().unwrap().unwrap();
    let plain = Object::PostedPositions(PostedPositions {
        device: DeviceId(2),
        writes: coven_format::value::WritePositions(Vec::new()),
        store_log: positions,
        schema_version: 0,
        fingerprints: Vec::new(),
        pending: Vec::new(),
    })
    .encode()
    .unwrap();
    let sealed = ring
        .store_key(key(1))
        .unwrap()
        .derive()
        .seal_object_chunk(path.as_str(), &prefix.encode().unwrap(), 0, 0, &plain)
        .unwrap();
    let mut bytes = prefix.encode_chunk(&sealed).unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend_from_slice(
        signer
            .keys()
            .sign_object(path.as_str(), &hash.finish())
            .as_bytes(),
    );
    (path, bytes)
}

/// Positions keep their historical signature: the old key verifies a post
/// whose signed view predates the replacement, and no longer verifies one
/// whose view includes it (D9). Replacement adds them no landing deadline.
#[tokio::test]
async fn positions_verify_with_the_key_their_signed_view_binds() {
    let (_storage, mut ana, _ben) = household().await;
    let before = ana.log().await.positions();
    ana.sync
        .make_and_upload_entry(StoreChange::ReplaceMemberKeys {
            member: member(2).member(),
            keys: public(&fresh_ben()),
        })
        .await
        .unwrap();
    let log = ana.log().await;
    let ring = ana.custody.read().unwrap();
    let open = |positions, signer: &MemberIdentity| {
        let (path, bytes) = ben_positions(&ana, positions, signer);
        crate::posted_positions::open(&bytes, &path, ring.as_ref(), &log).unwrap()
    };
    assert!(open(before.clone(), &member(2)).is_ok());
    assert!(matches!(
        open(log.positions(), &member(2)),
        Err(crate::PendingReason::InvalidPositions(
            crate::Refusal::Signature { .. }
        ))
    ));
    assert!(open(log.positions(), &fresh_ben()).is_ok());
    assert!(matches!(
        open(before, &fresh_ben()),
        Err(crate::PendingReason::InvalidPositions(
            crate::Refusal::Signature { .. }
        ))
    ));
}

#[tokio::test]
#[ignore = "requires COVEN_MEMBER_KEY_LEAN, supplied by scripts/check.sh"]
async fn lean_differential_member_key_replacements() {
    let keyed::Run {
        cases,
        inputs,
        mut numbers,
        results,
    } = keyed::run();
    // Outcomes the generated cases must reach.
    let mut seen = BTreeMap::<&str, usize>::new();
    for (((case, input), result), numbers) in
        cases.iter().zip(&inputs).zip(&results).zip(&mut numbers)
    {
        let h = &case.history;
        let position = |i: usize| h.entries[i].position;
        let received = keyed::receive(case, &case.received);
        let again = keyed::receive(case, &keyed::reversed(&case.received));
        assert_eq!(
            received.log.replay, again.log.replay,
            "arrival order of {input}"
        );
        assert_eq!(received.refused, again.refused, "arrival order of {input}");
        let replay = &received.log.replay;
        // Signatures: Rust refuses exactly a signer the recorded view does not
        // bind to its known author.
        for &i in &case.received {
            let signature = &result["signatures"][i];
            let refused =
                signature["known"].as_bool().unwrap() && !signature["current"].as_bool().unwrap();
            assert_eq!(
                received.refused.contains(&i),
                refused,
                "entry {i} of {input}"
            );
            *seen.entry("stale signature").or_default() += usize::from(refused);
        }
        let kept: BTreeSet<usize> = (0..h.entries.len())
            .filter(|&i| replay.entries.get(&position(i)) == Some(&EntryOutcome::Kept))
            .collect();
        let lean: BTreeSet<usize> = serde_json::from_value(result["kept"].clone()).unwrap();
        assert_eq!(kept, lean, "kept entries of {input}");
        let dropped: BTreeSet<usize> = case
            .received
            .iter()
            .copied()
            .filter(|i| {
                received.refused.contains(i)
                    || matches!(
                        replay.entries.get(&position(*i)),
                        Some(EntryOutcome::Dropped(_))
                    )
            })
            .collect();
        let lean: BTreeSet<usize> = serde_json::from_value(result["dropped"].clone()).unwrap();
        assert_eq!(dropped, lean, "dropped entries of {input}");
        for (i, entry) in h.entries.iter().enumerate() {
            if let (StoreChange::ReplaceMemberKeys { .. }, Some(outcome)) =
                (&entry.change, replay.entries.get(&position(i)))
            {
                *seen
                    .entry(match outcome {
                        EntryOutcome::Kept => "replacement kept",
                        EntryOutcome::Dropped(coven_database::DropReason::BeatenBy(_)) => {
                            "replacement beaten"
                        }
                        EntryOutcome::Dropped(coven_database::DropReason::KeysAlreadyUsed) => {
                            "keys reused"
                        }
                        EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate) => {
                            "replacement late"
                        }
                        EntryOutcome::Dropped(_) => "replacement dropped otherwise",
                    })
                    .or_default() += 1;
            }
        }
        // Current pairs by stable id and every pair ever bound.
        let keys: BTreeSet<[usize; 3]> = replay
            .state
            .members
            .iter()
            .map(|(id, member)| {
                [
                    Numbers::member(id) as usize,
                    numbers.signing(member.keys.signing),
                    numbers.sealing(member.keys.sealing),
                ]
            })
            .collect();
        let lean: BTreeSet<[usize; 3]> = serde_json::from_value(result["keys"].clone()).unwrap();
        assert_eq!(keys, lean, "member keys of {input}");
        let used: BTreeSet<[usize; 2]> = replay
            .state
            .bound_keys
            .iter()
            .map(|keys| [numbers.signing(keys.signing), numbers.sealing(keys.sealing)])
            .collect();
        let lean: BTreeSet<[usize; 2]> = serde_json::from_value(result["used"].clone()).unwrap();
        assert_eq!(used, lean, "bound pairs of {input}");
        // Authority in each recorded view, for every entry Rust checked.
        let checks: BTreeMap<EntryId, coven_database::StoreLogCheck> = received
            .log
            .entries
            .iter()
            .map(|applied| (applied.entry.position, applied.check.clone()))
            .collect();
        for &i in &case.received {
            if let Some(check) = checks.get(&position(i)) {
                assert_eq!(
                    Some(check.authorized()),
                    result["authorized"][i].as_bool(),
                    "authority of entry {i} in {input}"
                );
            }
        }
        // Conflicting pairs and the preference between them.
        let mut conflicts = BTreeSet::new();
        for a in 0..h.entries.len() {
            for b in a + 1..h.entries.len() {
                let (Some(va), Some(vb)) = (checks.get(&position(a)), checks.get(&position(b)))
                else {
                    continue;
                };
                let (ea, eb) = (&h.entries[a], &h.entries[b]);
                if crate::conflicts::conflict(ea, va, eb, vb) {
                    conflicts.insert([
                        a,
                        b,
                        usize::from(crate::conflicts::before(ea, va, eb, vb)),
                        usize::from(crate::conflicts::before(eb, vb, ea, va)),
                    ]);
                }
            }
        }
        let lean: BTreeSet<[usize; 4]> =
            serde_json::from_value(result["conflicts"].clone()).unwrap();
        let checked: BTreeSet<[usize; 4]> = lean
            .into_iter()
            .filter(|[a, b, ..]| {
                checks.contains_key(&position(*a)) && checks.contains_key(&position(*b))
            })
            .collect();
        assert_eq!(conflicts, checked, "conflicts of {input}");
        *seen.entry("conflicting pair").or_default() += conflicts.len();
        if case.writes.is_empty() {
            continue;
        }
        // Old-key writes against the real authority and window verdicts.
        let temporary = tempfile::tempdir().unwrap();
        let directory = coven_foundation::files::StoreLayout::new(temporary.path().to_owned())
            .create_store_dir(
                STORE,
                "Home",
                &coven_foundation::id_source::SequentialIds::new(),
            )
            .unwrap();
        let database = crate::tests::open(directory).await;
        database
            .apply_store_log_batch::<crate::PendingRecord>(
                StoreLog::default(),
                received.log.entries.clone(),
                replay.clone(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .unwrap();
        if case.horizon > 0 {
            let at = coven_format::value::StorageTime::since_epoch(std::time::Duration::from_secs(
                case.horizon * DAY,
            ));
            database
                .advance_store_log_horizon(database.store_log().await.unwrap(), at, at)
                .await
                .unwrap();
        }
        let log = database.store_log().await.unwrap();
        let mut replays = crate::replay_cache::ReplayCache::new(&log);
        for (number, (write, judged)) in case
            .writes
            .iter()
            .zip(result["writes"].as_array().unwrap())
            .enumerate()
        {
            let header = coven_format::write::WriteHeader {
                position: coven_merge::WriteId {
                    device: DeviceId(11),
                    number: number as u64 + 1,
                },
                timestamp: coven_merge::Timestamp::new(10_000 + number as u64, 0, DeviceId(11))
                    .unwrap(),
                had_read: coven_format::value::WritePositions(Vec::new()),
                store_log_read: keyed::frontier(h, &write.read),
                schema_version: 0,
                disposition: coven_format::write::WriteDisposition::Apply,
            };
            let missing = write
                .read
                .iter()
                .any(|i| !log.replay.entries.contains_key(&position(*i)));
            if missing {
                continue;
            }
            let signer = write.signer.keys().public_keys().signing;
            let authorized = crate::write_object::authority(&log, &mut replays, &header)
                .is_ok_and(|recorded| recorded == signer);
            let (late, fixed) = database
                .test_write_landing(
                    header,
                    coven_database::Published {
                        stored_at: coven_format::value::StorageTime::since_epoch(
                            std::time::Duration::from_secs(write.stored * DAY),
                        ),
                        signer,
                    },
                )
                .await
                .unwrap();
            let current =
                crate::write_seal::check_member(&log, &write.signer, DeviceId(11)).is_ok();
            assert_eq!(
                Some(authorized),
                judged["authorized"].as_bool(),
                "write {number} of {input}"
            );
            assert_eq!(
                Some(!late),
                judged["timely"].as_bool(),
                "write {number} of {input}"
            );
            assert_eq!(
                Some(authorized && !late),
                judged["counted"].as_bool(),
                "write {number} of {input}"
            );
            assert_eq!(
                Some(current),
                judged["current"].as_bool(),
                "write {number} of {input}"
            );
            assert_eq!(
                Some(fixed),
                judged["fixed"].as_bool(),
                "write {number} of {input}"
            );
            *seen
                .entry(match (authorized, late) {
                    (false, _) => "write unauthorized",
                    (true, false) => "write counted",
                    (true, true) => "write late",
                })
                .or_default() += 1;
            *seen
                .entry(if fixed {
                    "verdict final"
                } else {
                    "verdict open"
                })
                .or_default() += 1;
        }
        database.close().await.unwrap();
    }
    for outcome in [
        "stale signature",
        "replacement kept",
        "replacement beaten",
        "keys reused",
        "conflicting pair",
        "write counted",
        "write late",
        "write unauthorized",
        "verdict final",
        "verdict open",
    ] {
        assert!(
            seen.get(outcome).is_some_and(|&n| n > 0),
            "{outcome}: {seen:?}"
        );
    }
}
