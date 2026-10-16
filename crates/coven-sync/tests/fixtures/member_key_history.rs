//! Generated member-key replacement histories for the keyed differentials
//! (§9-§11, D9), compared through
//! `spec/proofs/storelog-data/MemberKeyRunner.lean`.
//!
//! Each history is a linear prefix (Ana creates, adds Ben and Carol, who
//! register devices 11 and 12), then concurrent leaves that read only the
//! prefix: Ben replacing his own keys, Ana and Carol replacing Ben's, with
//! fresh, identical or reused pairs, or Ana removing Ben. One follow-up by
//! Ben may read some of those leaves: a successive replacement, or an entry
//! signed with his current or his replaced key. Stored times tie, meet the
//! thirty-day deadline exactly or miss it. Writes from Ben's device read a
//! causally closed view and land around each replacement's deadline. Real
//! sealed copies of the store's creation key go to old, candidate, current
//! and outside credentials.

use std::collections::{BTreeMap, BTreeSet};

use coven_crypto::{MemberIdentity, MemberKeys, MemberPublicKeys};
use coven_database::StoreLog;
use coven_format::{
    sealed_single::SingleChunkObject,
    store_log::{MemberRole, StoreChange},
    value::{EntryId, EntryPositions},
};
use coven_foundation::id_source::DeviceId;
use serde_json::{json, Value};

use crate::entry_signature::tests::{action, store_key, Numbers, STORE};
use crate::replay::tests::{self as history, History};
use crate::tests::{run_lean, Generator};

pub(crate) const BEN: u8 = 1;
pub(crate) const CAROL: u8 = 2;
pub(crate) const PREFIX: usize = 5;
pub(crate) const DAY: u64 = 86_400;

/// Member `n`'s original identity (`history::keys(n)`).
pub(crate) fn identity(n: u8) -> MemberIdentity {
    crate::store_log_sync::tests::member(n + 1)
}

/// A pair no fixture binds, for member `n`'s id.
pub(crate) fn fresh(n: u8, seed: u8) -> MemberIdentity {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([0x60 + seed; 64]);
    MemberIdentity::new(
        history::member(n),
        MemberKeys::from_secret_bytes(&bytes).unwrap(),
    )
}

pub(crate) fn rekey(n: u8, keys: MemberPublicKeys) -> StoreChange {
    StoreChange::ReplaceMemberKeys {
        member: history::member(n),
        keys,
    }
}

pub(crate) struct Write {
    pub(crate) read: Vec<usize>,
    pub(crate) signer: MemberIdentity,
    pub(crate) stored: u64,
}

pub(crate) struct Case {
    pub(crate) history: History,
    pub(crate) signers: Vec<MemberIdentity>,
    pub(crate) stored: Vec<u64>,
    pub(crate) attempted: Vec<u64>,
    pub(crate) received: Vec<usize>,
    pub(crate) writes: Vec<Write>,
    pub(crate) horizon: u64,
    /// Whether custody holds the creation key, the only introduction.
    pub(crate) held: bool,
    /// Copies of the creation key: recipient id and exact sealing key.
    pub(crate) copies: Vec<(
        coven_foundation::id_source::MemberId,
        coven_crypto::SealingKey,
    )>,
    /// The member whose first attempts and sharing are judged.
    pub(crate) me: u8,
}

/// Every private pair a case can sign with, by public pair.
pub(crate) fn identities() -> Vec<MemberIdentity> {
    let mut all = vec![identity(0), identity(BEN), identity(CAROL)];
    all.extend((0..8).map(|seed| fresh(BEN, seed)));
    all
}

pub(crate) fn holder(keys: MemberPublicKeys) -> MemberIdentity {
    identities()
        .into_iter()
        .find(|identity| identity.keys().public_keys() == keys)
        .expect("a fixture pair")
}

/// Ben's key in the replay of exactly `read` (`Keyed` recorded view).
pub(crate) fn ben_key(h: &History, read: &[usize]) -> MemberPublicKeys {
    let entries: Vec<_> = read.iter().map(|&i| h.entries[i].clone()).collect();
    history::replay(&entries).state.members[&history::member(BEN)].keys
}

pub(crate) fn generate(seed: u64) -> Case {
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
        // A device addition registers its writer, device 13 (§10).
        let writer = match change {
            coven_format::store_log::StoreChange::AddDevice { .. } => 13,
            _ => 11,
        };
        h.push(BEN, writer, &read, change);
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
    // Copies to Ben's and Carol's original credentials, to the candidates
    // the leaves bind or lose with, to Ana, and to an outsider (member 4).
    let recipients = [
        identity(0),
        identity(BEN),
        identity(CAROL),
        identity(4),
        fresh(BEN, 0),
        fresh(BEN, 1),
        fresh(BEN, 2),
        fresh(BEN, 5),
        fresh(CAROL, 7),
    ];
    let copies = (0..g.pick(4))
        .map(|_| {
            let to = &recipients[g.pick(recipients.len())];
            (to.member(), to.keys().public_keys().sealing)
        })
        .collect();
    Case {
        history: h,
        signers,
        stored,
        attempted,
        received,
        writes,
        horizon,
        held: g.pick(5) != 0,
        copies,
        me: g.pick(3) as u8,
    }
}

/// The creation key's copy to `to`, sealed and decoded by the production codec.
pub(crate) fn copy_prefix(
    i: usize,
    to: &(
        coven_foundation::id_source::MemberId,
        coven_crypto::SealingKey,
    ),
) -> coven_format::sealed_key::KeyCopyPrefix {
    let path =
        coven_storage::ObjectPath::key_copy(STORE, DeviceId(7), (i as u64 + 1).try_into().unwrap());
    let bytes = crate::store_log_keys::store_copy(&path, &store_key(), &to.0, &to.1).unwrap();
    coven_format::sealed_key::SealedKey::decode(&bytes)
        .unwrap()
        .prefix()
        .clone()
}

/// Rust's results for one case, received in `order`.
pub(crate) struct Received {
    pub(crate) refused: BTreeSet<usize>,
    pub(crate) log: StoreLog,
}

pub(crate) fn receive(case: &Case, order: &[usize]) -> Received {
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
pub(crate) fn reversed(received: &[usize]) -> Vec<usize> {
    let mut order: Vec<usize> = received.iter().copied().filter(|&i| i < PREFIX).collect();
    let mut rest: Vec<usize> = received.iter().copied().filter(|&i| i >= PREFIX).collect();
    let follow_up = rest.pop();
    rest.reverse();
    order.extend(rest);
    order.extend(follow_up);
    order
}

pub(crate) fn frontier(h: &History, read: &[usize]) -> EntryPositions {
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

/// The generated cases, their runner inputs and numbering, and the Lean
/// model's results for each.
pub(crate) struct Run {
    pub(crate) cases: Vec<Case>,
    pub(crate) inputs: Vec<Value>,
    pub(crate) numbers: Vec<Numbers>,
    pub(crate) results: Vec<Value>,
}

/// Generate the cases and judge them with the Lean runner.
pub(crate) fn run() -> Run {
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
        let copies: Vec<Value> = case
            .copies
            .iter()
            .enumerate()
            .map(|(i, to)| {
                let prefix = copy_prefix(i, to);
                json!([
                    0,
                    Numbers::member(&prefix.member),
                    n.sealing(prefix.sealing)
                ])
            })
            .collect();
        let held: Vec<usize> = if case.held { vec![0] } else { Vec::new() };
        let mut input = json!({"entries": entries, "window": 30, "held": held,
            "copies": copies, "self": case.me, "audiences": [0],
            "shares": [[0, 0], [BEN, 0], [CAROL, 0]],
            "received": case.received, "writes": writes});
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
    Run {
        cases,
        inputs,
        numbers,
        results,
    }
}
