//! Entry signers in recorded views, and the keyed store-log adapter: real
//! sealed entries and key copies, decoded by the production codec, against
//! `Keyed` and `KeySelection.MemberKeys`
//! (`spec/proofs/storelog-data/MemberKeyRunner.lean`).
//!
//! Each history is a linear honest prefix followed by leaves nothing reads:
//! two concurrent additions of one new id, entries signed with a key their
//! author's recorded view never bound, and a self-registration by an id no
//! entry added. The prefix re-adds active members, repeating or changing
//! their keys, role or access (`Keyed.Additions`), returns removed members
//! with their old pair or a fresh one, and adds new ids borrowing another
//! member's key (`Keyed.freshKeys`). Rust refuses every signature its
//! recorded view does not bind, an unknown author's too, and never applies
//! it; the model drops it.
//! Because no entry reads a leaf, both replays keep the same prefix.

use std::collections::{BTreeMap, BTreeSet};

use coven_crypto::{MemberIdentity, MemberKeys, SealingKey, SigningKey, StoreKey};
use coven_database::{EntryOutcome, StoreLog};
use coven_format::{
    sealed_key::{KeyCopyPrefix, SealedKey},
    sealed_single::SingleChunkObject,
    store_log::{MemberRole, StoreChange, StoreLogEntry},
};
use coven_foundation::id_source::{DeviceId, KeyId, MemberId, StoreId};
use coven_merge::Audience;
use coven_storage::{ObjectPath, Storage};
use serde_json::{json, Value};

use crate::replay::tests::{self as history, History};
use crate::store_log_sync::tests as fixture;
use crate::tests::{audience_number, key_number, run_lean, Generator};
use crate::Refusal;

pub(crate) const STORE: StoreId = StoreId(uuid::Uuid::from_u128(1));
/// Members the generator may add; member 5 is never added.
const MEMBERS: u8 = 5;
/// The id the two concurrent addition leaves add.
const CONCURRENT: u8 = MEMBERS + 1;

/// Member `n`'s identity: `history::member(n)` with `history::keys(n)`.
fn identity(n: u8) -> MemberIdentity {
    crate::store_log_sync::tests::member(n + 1)
}

/// Member `n`'s id with a third private pair, bound only by a return.
fn returning(n: u8) -> MemberIdentity {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([n + 151; 64]);
    MemberIdentity::new(
        history::member(n),
        MemberKeys::from_secret_bytes(&bytes).unwrap(),
    )
}

/// Member `n`'s id with a private pair no entry ever binds to it.
fn unbound(n: u8) -> MemberIdentity {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([n + 101; 64]);
    MemberIdentity::new(
        history::member(n),
        MemberKeys::from_secret_bytes(&bytes).unwrap(),
    )
}

fn device(n: u8) -> u64 {
    10 + u64::from(n)
}

/// Creation registers member 0's device 0; others register their own.
fn device_of(author: u8) -> u64 {
    if author == 0 {
        0
    } else {
        device(author)
    }
}

pub(crate) fn store_key() -> StoreKey {
    StoreKey::from_bytes(history::key(1), [17; 32])
}

/// Small model numbers for ids and public keys, in first-seen order.
#[derive(Default)]
pub(crate) struct Numbers {
    signing: Vec<SigningKey>,
    sealing: Vec<SealingKey>,
}

impl Numbers {
    pub(crate) fn member(id: &MemberId) -> u64 {
        (0..=CONCURRENT)
            .find(|&n| history::member(n) == *id)
            .map(u64::from)
            .expect("generated member")
    }
    pub(crate) fn signing(&mut self, key: SigningKey) -> usize {
        self.signing
            .iter()
            .position(|k| *k == key)
            .unwrap_or_else(|| {
                self.signing.push(key);
                self.signing.len() - 1
            })
    }
    pub(crate) fn sealing(&mut self, key: SealingKey) -> usize {
        self.sealing
            .iter()
            .position(|k| *k == key)
            .unwrap_or_else(|| {
                self.sealing.push(key);
                self.sealing.len() - 1
            })
    }
    pub(crate) fn keys(&mut self, keys: coven_crypto::MemberPublicKeys) -> Value {
        json!({"signing": self.signing(keys.signing), "sealing": self.sealing(keys.sealing)})
    }
}

struct Case {
    history: History,
    /// The identity whose private pair signed each entry.
    signers: Vec<MemberIdentity>,
    /// The honest, linear prefix's length; the rest are leaves.
    honest: usize,
    /// Entries signed with their author's current key: the prefix and the
    /// concurrent additions.
    signed: usize,
    /// Additions of an id absent from their view: returns and borrowed keys.
    fresh: Vec<usize>,
    /// Introductions held in custody, by entry index.
    held: Vec<usize>,
    /// Copies of held keys: introduction index, recipient id and sealing key.
    copies: Vec<(usize, MemberId, SealingKey)>,
    me: u8,
}

fn generate(seed: u64) -> Case {
    let mut g = Generator(seed);
    let mut h = History::new();
    let mut signers = vec![identity(0)];
    let mut added = BTreeSet::from([0u8]);
    let mut registered = BTreeSet::from([0u8]);
    let mut removed = BTreeSet::new();
    let mut roles = BTreeMap::new();
    let mut circles = 0u64;
    let mut rotations = 0u64;
    let mut fresh = Vec::new();
    let push = |h: &mut History, signers: &mut Vec<MemberIdentity>, author: u8, change| {
        h.all(author, device_of(author), change);
        signers.push(identity(author));
    };
    for _ in 0..12 {
        let candidate = 1 + g.pick(usize::from(MEMBERS - 1)) as u8;
        let actors: Vec<u8> = registered.difference(&removed).copied().collect();
        let actor = actors[g.pick(actors.len())];
        match g.pick(9) {
            // Each id is added once, with its own fresh pair.
            0 if !added.contains(&candidate) => {
                let role = if g.pick(2) == 0 {
                    MemberRole::Admin
                } else {
                    MemberRole::Member
                };
                push(&mut h, &mut signers, 0, history::add(candidate, role));
                added.insert(candidate);
                roles.insert(candidate, role);
            }
            1 if added.contains(&candidate)
                && !registered.contains(&candidate)
                && !removed.contains(&candidate) =>
            {
                push(
                    &mut h,
                    &mut signers,
                    candidate,
                    history::device(device(candidate)),
                );
                registered.insert(candidate);
            }
            2 if added.contains(&candidate) && !removed.contains(&candidate) => {
                let role = if g.pick(2) == 0 {
                    MemberRole::Admin
                } else {
                    MemberRole::Member
                };
                push(&mut h, &mut signers, 0, history::role(candidate, role));
                roles.insert(candidate, role);
            }
            3 if added.contains(&candidate) && !removed.contains(&candidate) && g.pick(3) == 0 => {
                push(&mut h, &mut signers, 0, history::remove(candidate));
                removed.insert(candidate);
            }
            4 if circles < 2 => {
                push(
                    &mut h,
                    &mut signers,
                    actor,
                    history::make(circles, &format!("Circle {circles}")),
                );
                circles += 1;
            }
            5 if circles > 0 => {
                let circle = g.pick(circles as usize) as u64;
                push(
                    &mut h,
                    &mut signers,
                    actor,
                    history::join(circle, candidate),
                );
            }
            // A re-addition of an active member: repeated, or with another
            // role, access or pair, which only replacement may change.
            7 if added.contains(&candidate) && !removed.contains(&candidate) => {
                let role = roles[&candidate];
                let change = readd(candidate, role, g.pick(4));
                push(&mut h, &mut signers, 0, change);
            }
            // A new id borrows an added member's pair or one of its keys.
            8 if !added.contains(&candidate) => {
                let others: Vec<u8> = added.iter().copied().collect();
                let other = history::keys(others[g.pick(others.len())]);
                let own = history::keys(candidate);
                let keys = match g.pick(3) {
                    0 => other,
                    1 => coven_crypto::MemberPublicKeys {
                        signing: other.signing,
                        sealing: own.sealing,
                    },
                    _ => coven_crypto::MemberPublicKeys {
                        signing: own.signing,
                        sealing: other.sealing,
                    },
                };
                fresh.push(h.entries.len());
                push(&mut h, &mut signers, 0, with_keys(candidate, keys));
            }
            6 => {
                rotations += 1;
                push(
                    &mut h,
                    &mut signers,
                    actor,
                    crate::effects::tests::rotation(Audience::Store, 100 + rotations),
                );
            }
            _ => {}
        }
    }
    // Removed members return, with their old pair or a fresh one. A history
    // without a removal first removes an added member.
    if removed.is_empty() {
        if let Some(n) = added.iter().copied().find(|&n| n != 0) {
            push(&mut h, &mut signers, 0, history::remove(n));
            removed.insert(n);
        }
    }
    for &n in &removed {
        let keys = if g.pick(2) == 0 {
            history::keys(n)
        } else {
            returning(n).keys().public_keys()
        };
        fresh.push(h.entries.len());
        push(&mut h, &mut signers, 0, with_keys(n, keys));
    }
    // Member 0's two other devices, which add the same new id below.
    for other in [30, 31] {
        h.all(0, other, history::device(other));
        signers.push(identity(0));
    }
    let honest = h.entries.len();
    let all: Vec<usize> = (0..honest).collect();
    // Those devices each add the same new id without reading the other:
    // equal payloads combine, any difference conflicts.
    h.push(0, 30, &all, history::add(CONCURRENT, MemberRole::Member));
    signers.push(identity(0));
    h.push(
        0,
        31,
        &all,
        readd(CONCURRENT, MemberRole::Member, g.pick(4)),
    );
    signers.push(identity(0));
    let signed = h.entries.len();
    // A registered member's own id and device, signed with a key its view
    // never bound to that id; at most one leaf per device.
    for author in registered.iter().copied().filter(|a| !removed.contains(a)) {
        if g.pick(2) == 0 {
            h.push(
                author,
                device_of(author),
                &all,
                history::role(author, MemberRole::Member),
            );
            signers.push(unbound(author));
        }
    }
    // A self-registration by an id no entry added.
    h.push(
        MEMBERS,
        device(MEMBERS),
        &all,
        history::device(device(MEMBERS)),
    );
    signers.push(identity(MEMBERS));
    // Custody holds some introductions; copies go to current credentials,
    // to an unbound credential of the same id and to removed members.
    let introductions: Vec<usize> = (0..honest)
        .filter(|&e| crate::key_introductions::introduction(&h.entries[e].change).is_some())
        .collect();
    let held: Vec<usize> = introductions
        .iter()
        .copied()
        .filter(|_| g.pick(4) != 0)
        .collect();
    let mut copies = Vec::new();
    for &e in &held {
        for n in added.iter().copied() {
            match g.pick(5) {
                0 => {}
                1 => copies.push((
                    e,
                    history::member(n),
                    unbound(n).keys().public_keys().sealing,
                )),
                _ => copies.push((e, history::member(n), history::keys(n).sealing)),
            }
        }
    }
    let actors: Vec<u8> = added.difference(&removed).copied().collect();
    let me = actors[g.pick(actors.len())];
    Case {
        history: h,
        signers,
        honest,
        signed,
        fresh,
        held,
        copies,
        me,
    }
}

/// `history::add(n, Member)` with another public pair.
fn with_keys(n: u8, keys: coven_crypto::MemberPublicKeys) -> StoreChange {
    let StoreChange::AddMember {
        member,
        role,
        access,
        ..
    } = history::add(n, MemberRole::Member)
    else {
        unreachable!("history::add makes an addition")
    };
    StoreChange::AddMember {
        member,
        keys,
        role,
        access,
    }
}

/// An addition of `n`: 0 repeats `history::add`, 1 changes the role, 2 the
/// access and 3 the pair.
fn readd(n: u8, role: MemberRole, variant: usize) -> StoreChange {
    let StoreChange::AddMember {
        member,
        keys,
        role,
        access,
    } = history::add(n, role)
    else {
        unreachable!("history::add makes an addition")
    };
    let other = coven_format::MemberAccess::S3AccessKey {
        access_key_id: "other-access-key".into(),
        principal: coven_format::S3Principal::Console,
    };
    let (role, access, keys) = match variant {
        0 => (role, access, keys),
        1 if role == MemberRole::Admin => (MemberRole::Member, access, keys),
        1 => (MemberRole::Admin, access, keys),
        2 => (role, other, keys),
        _ => (role, access, unbound(n).keys().public_keys()),
    };
    StoreChange::AddMember {
        member,
        keys,
        role,
        access,
    }
}

pub(crate) fn action(entry: &StoreLogEntry, numbers: &mut Numbers) -> Value {
    let commitment =
        |key: KeyId, hash: &[u8; 32]| json!({"key": key_number(key), "key_hash": hash});
    match &entry.change {
        StoreChange::CreateStore {
            access,
            key,
            key_hash,
            admin,
            ..
        } => {
            let mut value = commitment(*key, key_hash);
            value["kind"] = json!(0);
            value["access"] = json!(serde_json::to_string(access).unwrap());
            value["keys"] = numbers.keys(*admin);
            value
        }
        StoreChange::AddMember {
            member,
            keys,
            role,
            access,
        } => json!({"kind": 1, "member": Numbers::member(member),
            "role": u64::from(*role == MemberRole::Member),
            "access": serde_json::to_string(access).unwrap(), "keys": numbers.keys(*keys)}),
        StoreChange::RemoveMember { member } => {
            json!({"kind": 2, "member": Numbers::member(member)})
        }
        StoreChange::ChangeRole { member, role } => json!({"kind": 3,
            "member": Numbers::member(member), "role": u64::from(*role == MemberRole::Member)}),
        StoreChange::AddDevice { .. } => {
            json!({"kind": 4, "member": Numbers::member(&entry.author)})
        }
        StoreChange::CreateCircle {
            circle,
            name,
            key,
            key_hash,
        } => {
            let mut value = commitment(*key, key_hash);
            value["kind"] = json!(6);
            value["circle"] = json!(u64::try_from(circle.0.as_u128()).unwrap());
            value["name"] = json!(name);
            value
        }
        StoreChange::AddCircleMember { circle, member } => json!({"kind": 9,
            "circle": u64::try_from(circle.0.as_u128()).unwrap(), "member": Numbers::member(member)}),
        StoreChange::RotateKey {
            audience,
            key,
            key_hash,
        } => {
            let mut value = commitment(*key, key_hash);
            value["kind"] = json!(15);
            value["audience"] = json!(audience_number(audience));
            value
        }
        change => unreachable!("the generator does not author {change:?}"),
    }
}

/// What Rust's production path did with one case.
struct Received {
    refused: BTreeSet<usize>,
    log: StoreLog,
    selected: Vec<Option<usize>>,
}

fn receive(case: &Case, stored: &[u64]) -> Received {
    let h = &case.history;
    let publications = crate::tests::publication_times(h, stored);
    let key = store_key();
    let mut batch = crate::replay::ReplayBatch::new(&StoreLog::default());
    let mut refused = BTreeSet::new();
    for (i, (entry, signer)) in h.entries.iter().zip(&case.signers).enumerate() {
        let path = crate::store_log_object::path(STORE, entry.position);
        let bytes = crate::store_log_object::seal(
            STORE,
            entry,
            coven_format::FormatVersion::CURRENT,
            &key,
            signer,
        )
        .unwrap();
        // The codec's decoded entry, never the generator's value.
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
    let log = StoreLog {
        entries,
        replay,
        horizon: None,
    };
    // Copies are real sealed boxes; selection reads their decoded prefixes.
    let introductions = crate::key_introductions::introductions(&log);
    let prefixes: Vec<KeyCopyPrefix> = case
        .copies
        .iter()
        .enumerate()
        .map(|(i, (e, member, sealing))| {
            let path = ObjectPath::key_copy(STORE, DeviceId(7), (i as u64 + 1).try_into().unwrap());
            let bytes = match crate::store_log_keys::introduced(&h.entries[*e].change).unwrap() {
                (Audience::Store, id) => crate::store_log_keys::store_copy(
                    &path,
                    &StoreKey::from_bytes(id, [17; 32]),
                    member,
                    sealing,
                ),
                (Audience::Circle(circle), id) => crate::store_log_keys::circle_copy(
                    &path,
                    &coven_crypto::CircleKey::from_bytes(circle, id, [17; 32]),
                    member,
                    sealing,
                ),
            }
            .unwrap();
            SealedKey::decode(&bytes).unwrap().prefix().clone()
        })
        .collect();
    let held: BTreeMap<(Audience, KeyId), [u8; 32]> = case
        .held
        .iter()
        .map(|&e| {
            let (audience, key, hash) =
                crate::key_introductions::introduction(&h.entries[e].change).unwrap();
            ((audience, key), hash)
        })
        .collect();
    let selected = audiences(case)
        .iter()
        .map(|audience| {
            crate::key_introductions::select(
                &introductions,
                &crate::member_credentials::Credentials::new(&log),
                &history::member(case.me),
                Some(&prefixes),
                |audience, key| held.get(&(audience.clone(), key)).copied(),
                audience,
            )
            .ok()
            .map(|key| {
                h.entries
                    .iter()
                    .position(|e| {
                        crate::store_log_keys::introduced(&e.change)
                            == Some((audience.clone(), key))
                    })
                    .unwrap()
            })
        })
        .collect();
    Received {
        refused,
        log,
        selected,
    }
}

fn audiences(case: &Case) -> Vec<Audience> {
    let mut audiences = vec![Audience::Store];
    audiences.extend(case.history.entries.iter().filter_map(|e| match &e.change {
        StoreChange::CreateCircle { circle, .. } => Some(Audience::Circle(*circle)),
        _ => None,
    }));
    audiences
}

/// Every honest entry reads all earlier ones; leaves read the honest prefix
/// and nothing reads them (`Keyed.metadataValid`).
fn times(case: &Case) -> (Vec<u64>, Vec<u64>) {
    let n = case.history.entries.len();
    let leaf = 2 * case.honest as u64 + 1;
    (0..n)
        .map(|i| {
            if i < case.honest {
                (2 * i as u64 + 2, 2 * i as u64 + 1)
            } else {
                (leaf + 1 + (i - case.honest) as u64, leaf)
            }
        })
        .unzip()
}

#[test]
#[ignore = "requires COVEN_MEMBER_KEY_LEAN, supplied by scripts/check.sh"]
fn lean_differential_member_keys() {
    let cases: Vec<Case> = (1..=40).map(|seed| generate(0x6b65_7973 + seed)).collect();
    let mut inputs = Vec::new();
    let mut numbers = Vec::new();
    for case in &cases {
        let mut n = Numbers::default();
        let (stored, attempted) = times(case);
        let entries: Vec<Value> = case
            .history
            .entries
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let past: Vec<usize> = (0..i)
                    .filter(|&j| crate::replay::had_read(entry, &case.history.entries[j]))
                    .collect();
                let signer = n.signing(case.signers[i].keys().public_keys().signing);
                json!({"author": Numbers::member(&entry.author), "signer": signer,
                    "device": entry.position.device.0, "past": past,
                    "action": action(entry, &mut n),
                    "stored": stored[i], "attempted": attempted[i]})
            })
            .collect();
        let copies: Vec<Value> = case
            .copies
            .iter()
            .map(|(e, member, sealing)| json!([e, Numbers::member(member), n.sealing(*sealing)]))
            .collect();
        inputs.push(json!({"entries": entries, "window": 30, "held": case.held,
            "copies": copies, "self": case.me,
            "audiences": audiences(case).iter().map(audience_number).collect::<Vec<_>>()}));
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
    let mut refusals = 0;
    let mut exposed = 0;
    let mut selections = [0; 2];
    // Re-additions kept and dropped, and concurrent additions combined and not.
    let mut additions = [0; 4];
    // Absent-id additions refused for a used key, and kept with a fresh pair.
    let mut fresh = [0; 2];
    for (((case, input), result), numbers) in
        cases.iter().zip(&inputs).zip(&results).zip(&mut numbers)
    {
        let (stored, _) = times(case);
        let received = receive(case, &stored);
        let replay = &received.log.replay;
        for (i, signature) in result["signatures"].as_array().unwrap().iter().enumerate() {
            let current = signature["current"].as_bool().unwrap();
            // Rust refuses exactly a signer the recorded view does not bind.
            assert_eq!(
                received.refused.contains(&i),
                !current,
                "entry {i} of {input}"
            );
            assert_eq!(i < case.signed, current, "entry {i} of {input}");
            refusals += usize::from(!current);
        }
        let position = |i: usize| case.history.entries[i].position;
        let rust: BTreeSet<usize> = (0..case.history.entries.len())
            .filter(|&i| replay.entries.get(&position(i)) == Some(&EntryOutcome::Kept))
            .collect();
        let lean: BTreeSet<usize> = serde_json::from_value(result["kept"].clone()).unwrap();
        assert_eq!(rust, lean, "kept entries of {input}");
        for (i, entry) in case.history.entries[..case.honest].iter().enumerate() {
            if let StoreChange::AddMember { member, .. } = &entry.change {
                if case.history.entries[..i].iter().any(|e| {
                    matches!(&e.change, StoreChange::AddMember { member: m, .. } if m == member)
                }) {
                    additions[usize::from(rust.contains(&i))] += 1;
                }
            }
        }
        additions[2 + usize::from(rust.contains(&(case.honest + 1)))] += 1;
        for &i in &case.fresh {
            match replay.entries.get(&position(i)) {
                Some(EntryOutcome::Kept) => fresh[1] += 1,
                Some(EntryOutcome::Dropped(coven_database::DropReason::KeysAlreadyUsed)) => {
                    fresh[0] += 1
                }
                _ => {}
            }
        }
        let rust: BTreeSet<usize> = (0..case.history.entries.len())
            .filter(|&i| {
                received.refused.contains(&i)
                    || matches!(
                        replay.entries.get(&position(i)),
                        Some(EntryOutcome::Dropped(_))
                    )
            })
            .collect();
        let lean: BTreeSet<usize> = serde_json::from_value(result["dropped"].clone()).unwrap();
        assert_eq!(rust, lean, "dropped entries of {input}");
        // Every installed pair, retained after removal, keyed by stable id.
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
        // Every pair kept entries bound, including replaced and removed ones.
        let used: BTreeSet<[usize; 2]> = replay
            .state
            .bound_keys
            .iter()
            .map(|keys| [numbers.signing(keys.signing), numbers.sealing(keys.sealing)])
            .collect();
        let lean: BTreeSet<[usize; 2]> = serde_json::from_value(result["used"].clone()).unwrap();
        assert_eq!(used, lean, "bound pairs of {input}");
        let members: BTreeSet<[u64; 2]> = replay
            .state
            .members
            .iter()
            .filter(|(_, member)| !member.removed)
            .map(|(id, member)| {
                [
                    Numbers::member(id),
                    u64::from(member.role == MemberRole::Member),
                ]
            })
            .collect();
        let lean: BTreeSet<[u64; 2]> = serde_json::from_value(result["members"].clone()).unwrap();
        assert_eq!(members, lean, "members of {input}");
        // A copy routes to a current credential exactly when the stable id's
        // current pair names its sealing key.
        for ((_, member, sealing), current) in case
            .copies
            .iter()
            .zip(result["current"].as_array().unwrap())
        {
            let rust = replay
                .state
                .members
                .get(member)
                .is_some_and(|m| m.keys.sealing == *sealing);
            assert_eq!(Some(rust), current.as_bool(), "copy routing of {input}");
            exposed += usize::from(!rust);
        }
        let lean: Vec<Option<usize>> = serde_json::from_value(result["selected"].clone()).unwrap();
        assert_eq!(received.selected, lean, "first-attempt keys of {input}");
        for selected in &received.selected {
            selections[usize::from(selected.is_some())] += 1;
        }
    }
    // The generated cases exercise every verdict.
    assert!(refusals > 0 && exposed > 0, "{refusals} {exposed}");
    assert!(selections.iter().all(|&n| n > 0), "{selections:?}");
    assert!(additions.iter().all(|&n| n > 0), "{additions:?}");
    assert!(fresh.iter().all(|&n| n > 0), "{fresh:?}");
}

#[tokio::test]
async fn creation_and_later_entries_use_the_same_authenticated_envelope() {
    let storage = fixture::storage();
    let mut a = fixture::device(storage.clone(), 1, fixture::member(1), fixture::store(1)).await;
    a.create(fixture::key(1)).await;
    a.add(&fixture::member(2), MemberRole::Member).await;
    let ring = a.custody.read().unwrap().unwrap();
    let key = ring.store_key(fixture::key(1)).unwrap();
    for applied in a.log().await.entries {
        let entry = applied.entry;
        let path = crate::store_log_object::path(fixture::store(1), entry.position);
        let bytes = storage.read(&path).await.unwrap();
        let envelope = SingleChunkObject::decode(&bytes).unwrap();
        assert_eq!(envelope.prefix().encode().unwrap().len(), 19);
        let log = a.log().await;
        let opened = crate::store_log_object::open(&envelope, &path, key).unwrap();
        assert_eq!(opened.entry, entry);
        // Each entry verifies with its author's key in its recorded view.
        let view = crate::replay::at(&log, &crate::replay::author_positions(&entry));
        opened.verify(&view.state).unwrap();
        assert_eq!(
            crate::store_log_object::seal(
                fixture::store(1),
                &entry,
                coven_format::FormatVersion::CURRENT,
                key,
                &a.member
            )
            .unwrap(),
            bytes
        );
        let other_path =
            ObjectPath::store_log(fixture::store(1), DeviceId(99), 1.try_into().unwrap());
        assert!(matches!(
            crate::store_log_object::open(&envelope, &other_path, key),
            Err(Refusal::Decryption { .. })
        ));
        let wrong_key = StoreKey::generate(key.id()).unwrap();
        assert!(matches!(
            crate::store_log_object::open(&envelope, &path, &wrong_key),
            Err(Refusal::Decryption { .. })
        ));
        let forged = crate::store_log_object::seal(
            fixture::store(1),
            &entry,
            coven_format::FormatVersion::CURRENT,
            key,
            &fixture::member(2),
        )
        .unwrap();
        assert!(matches!(
            crate::store_log_object::open(&SingleChunkObject::decode(&forged).unwrap(), &path, key)
                .unwrap()
                .verify(&view.state),
            Err(Refusal::Signature { .. })
        ));
        let mut changed = bytes.clone();
        changed[3] ^= 1;
        assert!(crate::store_log_object::open(
            &SingleChunkObject::decode(&changed).unwrap(),
            &path,
            key
        )
        .is_err());
        let last = changed.len() - 1;
        changed = bytes;
        changed[last] ^= 1;
        assert!(matches!(
            crate::store_log_object::open(
                &SingleChunkObject::decode(&changed).unwrap(),
                &path,
                key
            )
            .unwrap()
            .verify(&view.state),
            Err(Refusal::Signature { .. })
        ));
    }
}

/// T245: a self-registration by an id no entry added carries no signature
/// its recorded view can check, so it is refused rather than kept as a drop
/// that would count in lateness and finality.
#[test]
fn an_author_without_keys_in_its_view_is_a_signature_refusal() {
    let mut h = History::new();
    h.push(
        MEMBERS,
        device(MEMBERS),
        &[0],
        history::device(device(MEMBERS)),
    );
    let entry = h.entries.last().unwrap().clone();
    let path = crate::store_log_object::path(STORE, entry.position);
    let key = store_key();
    let bytes = crate::store_log_object::seal(
        STORE,
        &entry,
        coven_format::FormatVersion::CURRENT,
        &key,
        &identity(MEMBERS),
    )
    .unwrap();
    let envelope = SingleChunkObject::decode(&bytes).unwrap();
    let opened = crate::store_log_object::open(&envelope, &path, &key).unwrap();
    let view = crate::replay::tests::replay(&h.entries[..1]).state;
    assert!(matches!(
        opened.verify(&view),
        Err(Refusal::Signature { .. })
    ));
}
