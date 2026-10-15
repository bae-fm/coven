//! Recorded-view signers under member-key replacement (§10, D9). Sending
//! needs the member's current credentials and the object's own recorded
//! signer to be the held key, while recorded views keep old signatures
//! checkable.
//!
//! The differential compares member-key replacements and old-key writes
//! with the keyed model: real sealed entries through the production replay
//! batch, and the real write authority and window verdict, against
//! `Keyed.resolve`, `authorized`, `conflict`/`prefer` and `OldKeyObjects`
//! through `spec/proofs/storelog-data/MemberKeyRunner.lean`.
//!
//! Each history is a linear prefix (Ana creates, adds Ben and Carol, who
//! register devices 11 and 12), then concurrent leaves that read only the
//! prefix: Ben replacing his own keys, Ana and Carol replacing Ben's, with
//! fresh, identical or reused pairs, or Ana removing Ben. One follow-up by
//! Ben may read some of those leaves: a successive replacement, or an entry
//! signed with his current or his replaced key. Stored times tie, meet the
//! thirty-day deadline exactly or miss it. Writes from Ben's device read a
//! causally closed view and land around each replacement's deadline.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use coven_crypto::{MemberIdentity, MemberKeys, MemberPublicKeys};
use coven_database::{EntryOutcome, StoreLog};
use coven_format::{
    sealed_single::SingleChunkObject,
    store_log::{MemberRole, StoreChange},
    value::{EntryId, EntryPositions},
};
use coven_foundation::id_source::DeviceId;
use coven_storage::Storage;
use serde_json::{json, Value};

use crate::entry_signature::tests::{action, store_key, Numbers, STORE};
use crate::replay::tests::{self as history, History};
use crate::store_log_sync::tests::{device, key, member, public, storage, store, Device};
use crate::tests::{run_lean, Generator};
use crate::SyncError;

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

const BEN: u8 = 1;
const CAROL: u8 = 2;
const PREFIX: usize = 5;
const DAY: u64 = 86_400;

/// Member `n`'s original identity (`history::keys(n)`).
fn identity(n: u8) -> MemberIdentity {
    crate::store_log_sync::tests::member(n + 1)
}

/// A pair no fixture binds, for member `n`'s id.
fn fresh(n: u8, seed: u8) -> MemberIdentity {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([0x60 + seed; 64]);
    MemberIdentity::new(
        history::member(n),
        MemberKeys::from_secret_bytes(&bytes).unwrap(),
    )
}

fn rekey(n: u8, keys: MemberPublicKeys) -> StoreChange {
    StoreChange::ReplaceMemberKeys {
        member: history::member(n),
        keys,
    }
}

struct Write {
    read: Vec<usize>,
    signer: MemberIdentity,
    stored: u64,
}

struct Case {
    history: History,
    signers: Vec<MemberIdentity>,
    stored: Vec<u64>,
    attempted: Vec<u64>,
    received: Vec<usize>,
    writes: Vec<Write>,
    horizon: u64,
}

/// Every private pair a case can sign with, by public pair.
fn identities() -> Vec<MemberIdentity> {
    let mut all = vec![identity(0), identity(BEN), identity(CAROL)];
    all.extend((0..8).map(|seed| fresh(BEN, seed)));
    all
}

fn holder(keys: MemberPublicKeys) -> MemberIdentity {
    identities()
        .into_iter()
        .find(|identity| identity.keys().public_keys() == keys)
        .expect("a fixture pair")
}

/// Ben's key in the replay of exactly `read` (`Keyed` recorded view).
fn ben_key(h: &History, read: &[usize]) -> MemberPublicKeys {
    let entries: Vec<_> = read.iter().map(|&i| h.entries[i].clone()).collect();
    history::replay(&entries).state.members[&history::member(BEN)].keys
}

fn generate(seed: u64) -> Case {
    let mut g = Generator(seed);
    let mut h = History::new();
    let ben_role = if g.pick(2) == 0 {
        MemberRole::Member
    } else {
        MemberRole::Admin
    };
    h.all(0, 0, history::add(BEN, ben_role));
    h.all(BEN, 11, history::device(11));
    h.all(0, 0, history::add(CAROL, MemberRole::Admin));
    h.all(CAROL, 12, history::device(12));
    let mut signers: Vec<MemberIdentity> = [0, 0, BEN, 0, CAROL].map(identity).to_vec();
    let mut stored: Vec<u64> = (0..PREFIX as u64).map(|i| 2 * i + 2).collect();
    let mut attempted: Vec<u64> = (0..PREFIX as u64).map(|i| 2 * i + 1).collect();
    let writes_mode = g.pick(3) != 0;
    let prefix: Vec<usize> = (0..PREFIX).collect();
    let base = 2 * PREFIX as u64 + 1;
    let offsets = [1, 1, 4, 31, 32];
    // Concurrent leaves: one per device, each reading exactly the prefix.
    let mut pairs = Vec::new();
    let pair = |g: &mut Generator, pairs: &[MemberPublicKeys], seed: u8| match g.pick(6) {
        0 if !pairs.is_empty() => pairs[g.pick(pairs.len())],
        1 => history::keys(BEN),
        2 => history::keys(CAROL),
        _ => fresh(BEN, seed).keys().public_keys(),
    };
    let mut ben_leaf = None;
    if g.pick(4) != 0 {
        let keys = pair(&mut g, &pairs, 0);
        pairs.push(keys);
        ben_leaf = Some(h.entries.len());
        h.push(BEN, 11, &prefix, rekey(BEN, keys));
        signers.push(identity(BEN));
    }
    match g.pick(5) {
        0 => {}
        1 if !writes_mode => {
            h.push(0, 0, &prefix, history::remove(BEN));
            signers.push(identity(0));
        }
        2 => {
            let keys = fresh(CAROL, 7).keys().public_keys();
            h.push(0, 0, &prefix, rekey(CAROL, keys));
            signers.push(identity(0));
        }
        _ => {
            let keys = pair(&mut g, &pairs, 1);
            pairs.push(keys);
            h.push(0, 0, &prefix, rekey(BEN, keys));
            signers.push(identity(0));
        }
    }
    if g.pick(2) == 0 {
        let keys = pair(&mut g, &pairs, 2);
        pairs.push(keys);
        h.push(CAROL, 12, &prefix, rekey(BEN, keys));
        signers.push(identity(CAROL));
    }
    for _ in PREFIX..h.entries.len() {
        attempted.push(base);
        stored.push(base + offsets[g.pick(offsets.len())]);
    }
    let leaves = h.entries.len();
    // Ben's follow-up reads the leaves stored before its attempt, its own
    // leaf included.
    let mut times: Vec<u64> = stored[PREFIX..].to_vec();
    times.push(base + 40);
    let start = times[g.pick(times.len())] + 1;
    let read: Vec<usize> = (0..leaves).filter(|&i| stored[i] < start).collect();
    if g.pick(3) != 0 && ben_leaf.is_none_or(|own| read.contains(&own)) {
        let current = ben_key(&h, &read);
        let (change, signer) = match g.pick(3) {
            0 => (
                rekey(BEN, fresh(BEN, 5).keys().public_keys()),
                holder(current),
            ),
            1 => (history::device(13), holder(current)),
            // Signed with his original key, current or not in that view.
            _ => (history::device(13), identity(BEN)),
        };
        h.push(BEN, 11, &read, change);
        signers.push(signer);
        attempted.push(start);
        stored.push(start + [0, 30, 31][g.pick(3)]);
    }
    let n = h.entries.len();
    // A causally closed received set: everything, or without one leaf and
    // whatever read it.
    let received: Vec<usize> = if g.pick(3) == 0 && leaves > PREFIX {
        let missing = PREFIX + g.pick(leaves - PREFIX);
        (0..n)
            .filter(|&i| {
                i != missing && !crate::replay::had_read(&h.entries[i], &h.entries[missing])
            })
            .collect()
    } else {
        (0..n).collect()
    };
    let mut writes = Vec::new();
    if writes_mode {
        let replacements: Vec<u64> = (PREFIX..n)
            .filter(|&i| matches!(h.entries[i].change, StoreChange::ReplaceMemberKeys { .. }))
            .map(|i| stored[i])
            .collect();
        for _ in 0..4 {
            // A closed view: the prefix and leaves stored before some time,
            // with the follow-up when its past is inside.
            let cut = base + offsets[g.pick(offsets.len())] + g.pick(2) as u64;
            let mut read: Vec<usize> = (0..leaves)
                .filter(|&i| i < PREFIX || stored[i] < cut)
                .collect();
            if n > leaves
                && g.pick(2) == 0
                && (0..leaves).all(|i| {
                    !crate::replay::had_read(&h.entries[leaves], &h.entries[i]) || read.contains(&i)
                })
            {
                read.push(leaves);
            }
            let current = ben_key(&h, &read);
            let signer = if g.pick(4) == 0 {
                identity(BEN)
            } else {
                holder(current)
            };
            let anchor = replacements
                .get(g.pick(replacements.len().max(1)))
                .copied()
                .unwrap_or(base);
            writes.push(Write {
                read,
                signer,
                stored: anchor + [0, 29, 30, 31, 45][g.pick(5)],
            });
        }
    }
    let horizon = [0, base, base + 2, base + 33][g.pick(4)];
    Case {
        history: h,
        signers,
        stored,
        attempted,
        received,
        writes,
        horizon,
    }
}

/// Rust's results for one case, received in `order`.
struct Received {
    refused: BTreeSet<usize>,
    log: StoreLog,
}

fn receive(case: &Case, order: &[usize]) -> Received {
    let h = &case.history;
    let publications = crate::tests::publication_times(h, &case.stored);
    let key = store_key();
    let mut batch = crate::replay::ReplayBatch::new(&StoreLog::default());
    let mut refused = BTreeSet::new();
    for &i in order {
        let entry = &h.entries[i];
        let path = crate::store_log_object::path(STORE, entry.position);
        let bytes = crate::store_log_object::seal(
            STORE,
            entry,
            coven_format::FormatVersion::CURRENT,
            &key,
            &case.signers[i],
        )
        .unwrap();
        let envelope = SingleChunkObject::decode(&bytes).unwrap();
        let opened = crate::store_log_object::open(&envelope, &path, &key).unwrap();
        assert_eq!(&opened.entry, entry);
        match batch.push(&opened, &publications, STORE) {
            Ok(true) => {}
            Ok(false) => panic!("entry {i} is causally ready"),
            Err(crate::Refusal::Signature { .. }) => {
                refused.insert(i);
            }
            Err(failure) => panic!("entry {i}: {failure:?}"),
        }
    }
    let (mut entries, replay) = batch.finish().expect("received entries");
    entries.sort_by_key(|applied| applied.entry.timestamp);
    Received {
        refused,
        log: StoreLog {
            entries,
            replay,
            horizon: None,
        },
    }
}

/// The prefix, then the leaves in reverse, then the follow-up: another
/// causal arrival order of the same set.
fn reversed(received: &[usize]) -> Vec<usize> {
    let mut order: Vec<usize> = received.iter().copied().filter(|&i| i < PREFIX).collect();
    let mut rest: Vec<usize> = received.iter().copied().filter(|&i| i >= PREFIX).collect();
    let follow_up = rest.pop();
    rest.reverse();
    order.extend(rest);
    order.extend(follow_up);
    order
}

fn frontier(h: &History, read: &[usize]) -> EntryPositions {
    let mut ends = BTreeMap::<DeviceId, u64>::new();
    for &i in read {
        let id = h.entries[i].position;
        let end = ends.entry(id.device).or_default();
        *end = (*end).max(id.number);
    }
    EntryPositions(
        ends.into_iter()
            .map(|(device, number)| EntryId { device, number })
            .collect(),
    )
}

#[tokio::test]
#[ignore = "requires COVEN_MEMBER_KEY_LEAN, supplied by scripts/check.sh"]
async fn lean_differential_member_key_replacements() {
    let cases: Vec<Case> = (1..=60).map(|seed| generate(0x7265_6b65 + seed)).collect();
    let mut inputs = Vec::new();
    let mut numbers = Vec::new();
    for case in &cases {
        let mut n = Numbers::default();
        let h = &case.history;
        let entries: Vec<Value> = h
            .entries
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let past: Vec<usize> = (0..i)
                    .filter(|&j| crate::replay::had_read(entry, &h.entries[j]))
                    .collect();
                let signer = n.signing(case.signers[i].keys().public_keys().signing);
                let action = match &entry.change {
                    StoreChange::ReplaceMemberKeys { member, keys } => json!({"kind": 16,
                        "member": Numbers::member(member), "keys": n.keys(*keys)}),
                    _ => action(entry, &mut n),
                };
                json!({"author": Numbers::member(&entry.author), "signer": signer,
                    "device": entry.position.device.0, "past": past, "action": action,
                    "stored": case.stored[i], "attempted": case.attempted[i]})
            })
            .collect();
        let writes: Vec<Value> = case
            .writes
            .iter()
            .map(|w| {
                json!({"author": BEN, "device": 11, "read": w.read, "stored": w.stored,
                    "signer": n.signing(w.signer.keys().public_keys().signing)})
            })
            .collect();
        let mut input = json!({"entries": entries, "window": 30, "held": [], "copies": [],
            "self": 0, "audiences": [], "received": case.received, "writes": writes});
        if case.horizon > 0 {
            input["horizon"] = json!(case.horizon);
        }
        inputs.push(input);
        numbers.push(n);
    }
    let output = run_lean("COVEN_MEMBER_KEY_LEAN", &Value::Array(inputs.clone()));
    assert!(
        output.status.success(),
        "Lean runner failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let results: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(results.len(), cases.len());
    // Outcomes the generated cases must reach.
    let mut seen = BTreeMap::<&str, usize>::new();
    for (((case, input), result), numbers) in
        cases.iter().zip(&inputs).zip(&results).zip(&mut numbers)
    {
        let h = &case.history;
        let position = |i: usize| h.entries[i].position;
        let received = receive(case, &case.received);
        let again = receive(case, &reversed(&case.received));
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
                store_log_read: frontier(h, &write.read),
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
