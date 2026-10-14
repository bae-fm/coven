//! Key provenance comes from authorized received introductions (§11, C12).

use super::*;
use crate::replay::tests::{self as history, History};
use crate::store_log_keys as keys;
use crate::store_log_sync::tests::{circle, device, find_copy, key, member, storage, store};
use crate::SyncError;
use coven_database::{DropReason, EntryOutcome, StoreLogCheck, StoreLogSealing};
use coven_format::store_log::MemberRole;
use std::time::{Duration, UNIX_EPOCH};

/// The applied log of a history whose entries landed on the given days.
fn landed_log(h: &History, days: &[u64]) -> StoreLog {
    let publications: BTreeMap<_, _> = h
        .entries
        .iter()
        .zip(days)
        .map(|(e, day)| (e.position, UNIX_EPOCH + Duration::from_secs(day * 86_400)))
        .collect();
    let mut log = StoreLog::default();
    for entry in &h.entries {
        let (checked, replay) = crate::replay_entry(&log, entry.clone(), &publications);
        log.entries.push(checked);
        log.entries.sort_by_key(|applied| applied.entry.timestamp);
        log.replay = replay;
    }
    log
}

fn rotation(audience: Audience, id: u64, hash: u8) -> StoreChange {
    StoreChange::RotateKey {
        audience,
        key: history::key(id),
        key_hash: [hash; 32],
    }
}

fn outcome(log: &StoreLog, h: &History, index: usize) -> EntryOutcome {
    log.replay.entries[&h.entries[index].position].clone()
}

#[test]
fn authorized_dropped_introductions_stay_readable() {
    // Ben's rotation missed Ana's addition of Carol and landed 31 days later:
    // replay excludes it permanently, but his recorded view authorized it.
    let mut late = History::new();
    late.all(0, 0, history::add(1, MemberRole::Admin));
    late.all(1, 1, history::device(1));
    late.all(0, 0, history::add(2, MemberRole::Member));
    late.push(1, 1, &[0, 1, 2], rotation(Audience::Store, 50, 5));
    let log = landed_log(&late, &[0, 0, 0, 0, 31]);
    assert_eq!(
        outcome(&log, &late, 4),
        EntryOutcome::Dropped(DropReason::LandedTooLate)
    );
    assert_eq!(
        log.entries.last().unwrap().check,
        StoreLogCheck::LandedTooLate { authorized: true }
    );
    assert_eq!(
        needed(&log).get(&(Audience::Store, history::key(50))),
        Some(&[5; 32])
    );
    assert_eq!(newest(&log, &Audience::Store), Some(history::key(50)));

    // Ben makes Notes while Ana removes him from the store; the removal wins,
    // yet Ben's introduction was authorized in his view and stays readable.
    let mut beaten = History::new();
    beaten.all(0, 0, history::add(1, MemberRole::Member));
    beaten.all(1, 1, history::device(1));
    beaten.push(0, 0, &[0, 1, 2], history::remove(1));
    beaten.push(1, 1, &[0, 1, 2], history::make(3, "Notes"));
    let log = landed_log(&beaten, &[0; 5]);
    assert!(matches!(
        outcome(&log, &beaten, 4),
        EntryOutcome::Dropped(_)
    ));
    let notes = Audience::Circle(history::circle(3));
    assert!(needed(&log).contains_key(&(notes.clone(), history::key(13))));
    assert_eq!(newest(&log, &notes), Some(history::key(13)));
}

#[test]
fn unauthorized_introductions_supply_no_key() {
    // Carol belongs to the store but not to Gifts; her rotation of Gifts and a
    // late rotation she had no authority for introduce nothing.
    let mut h = History::new();
    h.all(0, 0, history::add(2, MemberRole::Member));
    h.all(0, 0, history::make(0, "Gifts"));
    h.all(2, 2, history::device(2));
    let gifts = Audience::Circle(history::circle(0));
    h.all(2, 2, rotation(gifts.clone(), 60, 6));
    h.push(2, 2, &[0, 1, 2, 3], rotation(gifts.clone(), 61, 6));
    h.push(0, 0, &[0, 1, 2, 3, 4], history::add(3, MemberRole::Member));
    let log = landed_log(&h, &[0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(
        outcome(&log, &h, 4),
        EntryOutcome::Dropped(DropReason::NotAllowed)
    );
    let accepted_keys = needed(&log);
    for id in [60, 61] {
        assert!(!accepted_keys.contains_key(&(gifts.clone(), history::key(id))));
        assert!(!introductions(&log)
            .iter()
            .any(|i| i.key == history::key(id)));
    }
    assert_eq!(newest(&log, &gifts), Some(history::key(10)));
    let late = landed_log(&h, &[0, 0, 0, 0, 0, 40, 0]);
    let check = &late
        .entries
        .iter()
        .find(|e| e.entry.position == h.entries[5].position)
        .unwrap()
        .check;
    assert_eq!(*check, StoreLogCheck::LandedTooLate { authorized: false });
    assert!(!needed(&late).contains_key(&(gifts, history::key(61))));
}

#[test]
fn conflicting_commitments_accept_neither() {
    // Ana and Ben concurrently name the same store key id with different
    // material. Neither commitment is accepted; equal ones may repeat.
    for same in [false, true] {
        let mut h = History::new();
        h.all(0, 0, history::add(1, MemberRole::Admin));
        h.all(1, 1, history::device(1));
        h.push(0, 0, &[0, 1, 2], rotation(Audience::Store, 70, 7));
        h.push(
            1,
            1,
            &[0, 1, 2],
            rotation(Audience::Store, 70, if same { 7 } else { 8 }),
        );
        let log = landed_log(&h, &[0; 5]);
        let introductions = introductions(&log);
        assert_eq!(
            introductions
                .iter()
                .filter(|i| i.key == history::key(70))
                .count(),
            2
        );
        assert_eq!(
            hash_for(&introductions, &Audience::Store, history::key(70)),
            same.then_some([7; 32])
        );
        assert_eq!(
            newest(&log, &Audience::Store),
            Some(history::key(if same { 70 } else { 1 }))
        );
        // The same id in another audience is a different key.
        assert_eq!(
            hash_for(
                &introductions,
                &Audience::Circle(history::circle(70)),
                history::key(70)
            ),
            None
        );
    }
}

#[tokio::test]
async fn conflicting_commitments_reject_acquisition() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let mut erin = device(storage.clone(), 3, member(3), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Admin).await;
    ana.add(&erin.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben").await.unwrap();
    ana.sync().await;
    erin.sync().await;
    // Ana's rotation is fixed before Ben's is published, and lands after it.
    ana.interrupt_entry(4).await;
    let ana_hash = ana.sync.test_key(Audience::Store, key(3));
    assert!(ana
        .sync
        .make_and_upload_entry(StoreChange::RotateKey {
            audience: Audience::Store,
            key: key(3),
            key_hash: ana_hash,
        })
        .await
        .is_err());
    ben.sync.test_rotate(Audience::Store, key(3)).await;
    ana.restore_storage();
    ana.sync().await;
    erin.sync().await;
    let log = erin.log().await;
    let introductions = introductions(&log);
    assert_eq!(
        introductions.iter().filter(|i| i.key == key(3)).count(),
        2,
        "both rotations were received and authorized"
    );
    assert_eq!(hash_for(&introductions, &Audience::Store, key(3)), None);
    // Copies to Erin exist from both suppliers, yet neither is accepted.
    assert!(find_copy(
        storage.as_ref(),
        Audience::Store,
        key(3),
        &erin.member.member_id()
    )
    .await
    .is_some());
    let ring = erin.custody.read().unwrap();
    assert!(!keys::holds(ring.as_ref(), &Audience::Store, key(3)));
    assert_eq!(erin.newest(Audience::Store).await, key(1));
    // Ana holds her own material but cannot select it either.
    let ring = ana.custody.read().unwrap().unwrap();
    assert!(keys::holds(Some(&ring), &Audience::Store, key(3)));
    assert_eq!(
        crate::write_seal::current_store(&ana.log().await, &ring).unwrap(),
        key(1)
    );
}

#[tokio::test]
async fn an_unauthorized_rotation_and_its_copy_supply_no_key() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben").await.unwrap();
    ana.sync().await;
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Gifts".into(),
            key: key(2),
            key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    ben.sync().await;
    // Ben is outside Gifts. Bypassing sync's validation, his database fixes a
    // rotation of Gifts and a sealed copy of its key addressed to Ana.
    let forged = coven_crypto::CircleKey::generate(circle(1), key(9)).unwrap();
    let hash = forged.hash();
    let ana_id = ana.member.member_id();
    let ana_sealing = ana.member.sealing_public_key();
    ben.db
        .prepare_store_log(
            ben.member.member_id(),
            StoreChange::RotateKey {
                audience: Audience::Circle(circle(1)),
                key: key(9),
                key_hash: hash,
            },
            move |_, _, mut copies| {
                copies.push(|path| keys::circle_copy(path, &forged, &ana_id, &ana_sealing))?;
                Ok::<_, SyncError>(StoreLogSealing {
                    key: key(1),
                    keys: copies.into_uploads(),
                })
            },
        )
        .await
        .unwrap();
    ben.sync().await;
    ana.sync().await;
    let log = ana.log().await;
    let rotation = log.entries.last().unwrap();
    assert_eq!(rotation.entry.author, ben.member.member_id());
    assert_eq!(rotation.check, StoreLogCheck::NotAllowed);
    assert!(find_copy(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(9),
        &ana.member.member_id()
    )
    .await
    .is_some());
    let ring = ana.custody.read().unwrap();
    assert!(!keys::holds(
        ring.as_ref(),
        &Audience::Circle(circle(1)),
        key(9)
    ));
    assert_eq!(ana.newest(Audience::Circle(circle(1))).await, key(2));
}

/// Rust's C12 projection: received authorized introductions by history index,
/// and the agreed commitment of each identity in order of first introduction.
fn introduction_projection(h: &History, log: &StoreLog) -> serde_json::Value {
    use crate::tests::{audience_number, key_number};
    let index = |id| h.entries.iter().position(|e| e.position == id).unwrap();
    let introductions = introductions(log);
    let mut identities = Vec::new();
    for i in &introductions {
        if !identities.contains(&(i.audience.clone(), i.key)) {
            identities.push((i.audience.clone(), i.key));
        }
    }
    serde_json::json!({
        "introductions": introductions
            .iter()
            .map(|i| serde_json::json!([index(i.entry), audience_number(&i.audience), key_number(i.key), i.key_hash]))
            .collect::<Vec<_>>(),
        "hashes": identities
            .iter()
            .map(|(audience, key)| serde_json::json!([
                audience_number(audience),
                key_number(*key),
                hash_for(&introductions, audience, *key),
            ]))
            .collect::<Vec<_>>(),
    })
}

fn introduction_histories() -> Vec<(History, Vec<u64>)> {
    let mut cases = Vec::new();
    let mut late = History::new();
    late.all(0, 0, history::add(1, MemberRole::Admin));
    late.all(1, 1, history::device(1));
    late.all(0, 0, history::add(2, MemberRole::Member));
    late.push(1, 1, &[0, 1, 2], rotation(Audience::Store, 50, 5));
    cases.push((late, vec![0, 0, 0, 0, 31]));
    let mut beaten = History::new();
    beaten.all(0, 0, history::add(1, MemberRole::Member));
    beaten.all(1, 1, history::device(1));
    beaten.push(0, 0, &[0, 1, 2], history::remove(1));
    beaten.push(1, 1, &[0, 1, 2], history::make(3, "Notes"));
    cases.push((beaten, vec![0; 5]));
    let gifts = Audience::Circle(history::circle(0));
    for days in [vec![0; 7], vec![0, 0, 0, 0, 0, 40, 0]] {
        let mut outsider = History::new();
        outsider.all(0, 0, history::add(2, MemberRole::Member));
        outsider.all(0, 0, history::make(0, "Gifts"));
        outsider.all(2, 2, history::device(2));
        outsider.all(2, 2, rotation(gifts.clone(), 60, 6));
        outsider.push(2, 2, &[0, 1, 2, 3], rotation(gifts.clone(), 61, 6));
        outsider.push(0, 0, &[0, 1, 2, 3, 4], history::add(3, MemberRole::Member));
        cases.push((outsider, days));
    }
    for second in [7, 8] {
        let mut conflict = History::new();
        conflict.all(0, 0, history::add(1, MemberRole::Admin));
        conflict.all(1, 1, history::device(1));
        conflict.push(0, 0, &[0, 1, 2], rotation(Audience::Store, 70, 7));
        conflict.push(1, 1, &[0, 1, 2], rotation(Audience::Store, 70, second));
        conflict.all(0, 0, rotation(gifts.clone(), 70, 9));
        conflict.all(0, 0, history::make(0, "Gifts"));
        conflict.all(0, 0, rotation(gifts.clone(), 70, 9));
        cases.push((conflict, vec![0; 8]));
    }
    for seed in 1..=48 {
        let h = crate::tests::Generator(seed).history();
        let stored = (0..h.entries.len() as u64).map(|i| i / 3).collect();
        cases.push((h, stored));
    }
    cases
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_introductions() {
    let cases = introduction_histories();
    let inputs: Vec<_> = cases
        .iter()
        .map(|(h, stored)| {
            let mut input = crate::tests::input(
                h,
                stored,
                &crate::tests::received_prefixes(&crate::tests::orders(h)),
            );
            input["introductions"] = serde_json::json!(true);
            input
        })
        .collect();
    let output = crate::tests::run_lean(
        "COVEN_STORELOG_LEAN",
        &serde_json::json!({"histories": inputs}),
    );
    assert!(
        output.status.success(),
        "Lean runner failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Vec<Vec<serde_json::Value>> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual.len(), cases.len());
    for ((h, stored), results) in cases.iter().zip(actual) {
        let publications = crate::tests::publication_times(h, stored);
        let orders = crate::tests::orders(h);
        assert_eq!(results.len(), orders.len() * (h.entries.len() + 1));
        for (order, results) in orders.iter().zip(results.chunks_exact(h.entries.len() + 1)) {
            let mut log = StoreLog::default();
            assert_eq!(results[0], introduction_projection(h, &log));
            for (n, &i) in order.iter().enumerate() {
                let (checked, replay) =
                    crate::replay_entry(&log, h.entries[i].clone(), &publications);
                log.entries.push(checked);
                log.entries.sort_by_key(|applied| applied.entry.timestamp);
                log.replay = replay;
                assert_eq!(
                    results[n + 1],
                    introduction_projection(h, &log),
                    "order {order:?}, prefix {}",
                    n + 1
                );
            }
        }
    }
}

/// Copies of every introduced identity (and one never introduced), addressed to
/// `me` or another member, opening to nothing, the committed bytes, other bytes
/// or another identity. The runner's hash is the identity on bytes.
fn copies(h: &History, me: u8) -> Vec<(KeyCopyPrefix, Option<Opened>)> {
    let log = landed_log(h, &vec![0; h.entries.len()]);
    let introductions = introductions(&log);
    let mut identities: Vec<_> = introductions
        .iter()
        .map(|i| (i.audience.clone(), i.key))
        .collect();
    identities.push((Audience::Store, history::key(999)));
    identities.dedup();
    let mut copies = Vec::new();
    for (audience, key) in identities {
        let mut hashes: Vec<_> = introductions
            .iter()
            .filter(|i| i.audience == audience && i.key == key)
            .map(|i| i.key_hash)
            .collect();
        hashes.push([0xee; 32]);
        for recipient in [me, me + 1] {
            let prefix = KeyCopyPrefix {
                audience: audience.clone(),
                key,
                member: history::member(recipient),
            };
            copies.push((prefix.clone(), None));
            for hash in &hashes {
                copies.push((
                    prefix.clone(),
                    Some(Opened {
                        audience: audience.clone(),
                        key,
                        hash: *hash,
                    }),
                ));
            }
            copies.push((
                prefix.clone(),
                Some(Opened {
                    audience: Audience::Circle(history::circle(5)),
                    key,
                    hash: hashes[0],
                }),
            ));
            copies.push((
                prefix,
                Some(Opened {
                    audience: audience.clone(),
                    key: history::key(998),
                    hash: hashes[0],
                }),
            ));
        }
    }
    copies
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_accepted_copies() {
    use crate::tests::{audience_number, key_number};
    let me = 1;
    let cases: Vec<_> = introduction_histories()
        .into_iter()
        .map(|(h, stored)| {
            let copies = copies(&h, me);
            (h, stored, copies)
        })
        .collect();
    let member_number =
        |id: &coven_crypto::MemberId| (0..8).find(|&i| history::member(i) == *id).unwrap() as u64;
    let key_json = |audience: &Audience, key: KeyId| serde_json::json!({"audience": audience_number(audience), "key": key_number(key)});
    let inputs: Vec<_> = cases
        .iter()
        .map(|(h, stored, copies)| {
            let mut input = crate::tests::input(
                h,
                stored,
                &crate::tests::received_prefixes(&crate::tests::orders(h)),
            );
            input["self"] = serde_json::json!(me);
            input["copies"] = copies
                .iter()
                .map(|(prefix, opened)| {
                    let mut copy = key_json(&prefix.audience, prefix.key);
                    copy["recipient"] = serde_json::json!(member_number(&prefix.member));
                    copy["opened"] = opened.as_ref().map_or(serde_json::Value::Null, |o| {
                        let mut payload = key_json(&o.audience, o.key);
                        payload["bytes"] = serde_json::json!(o.hash);
                        payload
                    });
                    copy
                })
                .collect();
            input
        })
        .collect();
    let output = crate::tests::run_lean(
        "COVEN_STORELOG_LEAN",
        &serde_json::json!({"histories": inputs}),
    );
    assert!(
        output.status.success(),
        "Lean runner failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Vec<Vec<serde_json::Value>> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual.len(), cases.len());
    for ((h, stored, copies), results) in cases.iter().zip(actual) {
        let publications = crate::tests::publication_times(h, stored);
        let orders = crate::tests::orders(h);
        let project = |log: &StoreLog| {
            let introductions = introductions(log);
            serde_json::Value::Array(
                copies
                    .iter()
                    .map(|(prefix, opened)| match opened {
                        Some(opened)
                            if accepts_copy(
                                &introductions,
                                prefix,
                                &history::member(me),
                                opened,
                            ) =>
                        {
                            serde_json::json!([
                                audience_number(&opened.audience),
                                key_number(opened.key),
                                opened.hash
                            ])
                        }
                        _ => serde_json::Value::Null,
                    })
                    .collect(),
            )
        };
        for (order, results) in orders.iter().zip(results.chunks_exact(h.entries.len() + 1)) {
            let mut log = StoreLog::default();
            assert_eq!(results[0], project(&log));
            for (n, &i) in order.iter().enumerate() {
                let (checked, replay) =
                    crate::replay_entry(&log, h.entries[i].clone(), &publications);
                log.entries.push(checked);
                log.entries.sort_by_key(|applied| applied.entry.timestamp);
                log.replay = replay;
                assert_eq!(
                    results[n + 1],
                    project(&log),
                    "order {order:?}, prefix {}",
                    n + 1
                );
            }
        }
    }
}
