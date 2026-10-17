//! Member-key replacement through the operation calls (§9, §10, §18):
//! this member's own, which pauses app writes, confirms the queue, keeps a
//! candidate in custody and installs it once the binding is kept, and a
//! pair an admin prepared elsewhere, which is only the entry.
//!
//! The differential below runs such call histories against the keyed model
//! (`spec/proofs/storelog-data/MemberKeyRunner.lean`). Ana (owner, admin),
//! Ben (admin) and Carol (a member, or an admin) start from one synced
//! prefix. Then each may reserve one concurrent call before any is
//! published: Carol replaces her own keys, Ana or Ben publish a pair prepared
//! for Carol (the same pair or different ones), or remove her. The reserved
//! entries are published in a generated order. Carol finally tries to author
//! a circle with whatever her installation holds. The real store log, its
//! signers and the stored key copies go through `Keyed.resolve` and
//! `KeySelection.MemberKeys`; ids, current and used pairs, roles, fates,
//! retired credentials, routes, exposed copies, sharing, first-attempt key
//! selection and whether each of Carol's held pairs may still send must agree.

use super::*;
use crate::entry_signature::tests::{action, Numbers};
use crate::tests::{run_lean, Generator};
use coven_crypto::{MemberIdentity, MemberKeys};
use coven_database::{DbError, DropReason, EntryOutcome, EntryTarget, StoreLog};
use coven_format::sealed_key::{KeyCopyPrefix, SealedKey};
use coven_format::store_log::{MemberPublicKeys, StoreChange};
use coven_foundation::clock::Clock;
use coven_foundation::id_source::MemberId;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn notes() -> Vec<coven_database::SyncedTable> {
    vec![coven_database::SyncedTable::new(
        "notes",
        coven_database::RowIdentity::SharedKey,
    )]
}

pub(super) fn schema() -> Vec<coven_database::Migration> {
    vec![crate::device_log_sync::tests::schema::initial()]
}

pub(super) async fn write_note(d: &Device, id: &'static str) -> Result<(), DbError> {
    d.db.write(move |sql| {
        sql.execute("INSERT INTO notes VALUES(?1,'title','body')", [id])?;
        Ok(())
    })
    .await
}

/// A pair no entry binds, for `member`.
pub(super) fn fresh(member: MemberId) -> MemberIdentity {
    MemberIdentity::new(member, MemberKeys::generate().unwrap())
}

/// The sealing keys of every stored copy of the store's key `id` to `to`.
async fn sealings(
    storage: &dyn Storage,
    id: KeyId,
    to: &MemberId,
) -> BTreeSet<coven_crypto::SealingKey> {
    let mut found = BTreeSet::new();
    for object in storage.list(&ObjectPrefix::keys(store(1))).await.unwrap() {
        let bytes = storage.read(&object.path).await.unwrap();
        let prefix = SealedKey::decode(&bytes).unwrap().prefix().clone();
        if prefix.audience == Audience::Store && prefix.key == id && prefix.member == *to {
            found.insert(prefix.sealing);
        }
    }
    found
}

/// Entries device `d` authored, in its applied log.
pub(super) async fn own_entries(d: &Device) -> u64 {
    let local = d.db.local_store_log().await.unwrap();
    local
        .log
        .entries
        .iter()
        .filter(|e| e.entry.position.device == local.device)
        .count() as u64
}

/// Step `id` until it replies or finishes, failing on any wait.
pub(super) async fn settle(d: &mut Device, id: OperationId) -> Result<Output, SyncError> {
    loop {
        match step(d, id).await? {
            Progress::Advanced => (),
            Progress::Finished(value) => return Ok(value),
            Progress::Reply(result) => return result,
            Progress::Waiting(reason) => panic!("unexpected wait: {reason:?}"),
        }
    }
}

/// Carol, a member, replaces her own keys while a write of hers is queued.
/// App writes pause at once; the operation waits for the queued write to be
/// stored, keeps a candidate, shares the store key with it, then publishes
/// the binding signed by her current pair. The kept binding installs the
/// candidate and keeps the old pair; her id, role, access, circles and device
/// stay, and writes resume under the new pair.
#[tokio::test]
async fn self_replacement_confirms_the_queue_and_installs_the_kept_pair() {
    let storage = dropbox();
    let [mut a, _b, mut c] = accounts_with(storage.clone(), notes, schema).await;
    let carol = c.member.member();
    let create = begin(&mut a, Command::CreateCircle("Garden".into())).await;
    let Output::CircleId(garden) = finish(&mut a, create).await else {
        panic!()
    };
    let add = begin(&mut a, Command::AddCircleMember(garden, carol)).await;
    finish(&mut a, add).await;
    c.sync().await;
    write_note(&c, "queued").await.unwrap();
    let before = c.log().await.replay.state;
    let operation = begin(&mut c, Command::ReplaceOwnKeys).await;
    assert!(matches!(
        write_note(&c, "paused").await,
        Err(DbError::MemberKeysChanging)
    ));
    // A second self-replacement cannot start behind the first.
    assert!(matches!(
        c.sync.begin_operation_call(Command::ReplaceOwnKeys).await,
        Err(SyncError::Database(DbError::MemberKeysChanging))
    ));
    let queued = c.db.unconfirmed_own_prefix(coven_merge::WriteId {
        device: c.device().await,
        number: u64::MAX,
    });
    let queued = queued.await.unwrap().unwrap();
    assert!(matches!(
        step(&mut c, operation).await.unwrap(),
        Progress::Waiting(PendingReason::Waits(Prerequisite::Object(path)))
            if path == crate::write_seal::path(store(1), queued)
    ));
    assert!(c.identity.read().unwrap().unwrap().candidate().is_none());
    c.writes.upload_writes().await.unwrap();
    assert!(matches!(
        step(&mut c, operation).await.unwrap(),
        Progress::Advanced
    ));
    // The candidate is kept before anything names it.
    let held = c.identity.read().unwrap().unwrap();
    let candidate = held.candidate().unwrap().public_keys();
    assert_eq!(held.keys().public_keys(), public(&c.member));
    assert_eq!(own_entries(&c).await, 1);
    assert!(
        matches!(settle(&mut c, operation).await, Ok(Output::MemberKeysReplaced(identity))
        if identity.keys().public_keys() == candidate)
    );
    // The kept binding installed the candidate; the old pair stays held.
    let installed = c.identity.read().unwrap().unwrap();
    assert_eq!(installed.keys().public_keys(), candidate);
    assert!(installed.candidate().is_none());
    assert!(installed.holds(&public(&c.member)));
    let log = c.log().await;
    let after = &log.replay.state;
    let (old, new) = (&before.members[&carol], &after.members[&carol]);
    assert_eq!(new.keys, candidate);
    assert_eq!((new.role, &new.access), (old.role, &old.access));
    assert!(after.reads_circle(garden, &carol));
    assert_eq!(before.devices, after.devices);
    // Every store key was shared with the candidate before the binding.
    let ring = a.custody.read().unwrap().unwrap();
    for id in ring.store_key_ids() {
        assert!(sealings(storage.as_ref(), id, &carol)
            .await
            .contains(&candidate.sealing));
    }
    assert!(c.db.operations().await.unwrap().is_empty());
    // Writes resume. Sending needs the installed pair; the old one, still
    // held for its copies, no longer admits a send (§10).
    write_note(&c, "after").await.unwrap();
    let device = c.device().await;
    crate::write_seal::check_member(&log, &installed, device).unwrap();
    assert!(matches!(
        crate::write_seal::check_member(&log, &c.member, device),
        Err(SyncError::MemberKeysReplaced { member }) if member == carol
    ));
    a.sync().await;
    assert_eq!(a.log().await.replay.state.members[&carol].keys, candidate);
}

/// Carol posts her positions, replaces her own keys, then commits a write
/// that is not yet uploaded, so her current past waits. A new pending record
/// still travels: its post names a store-log view in which her installed
/// pair signs, so Ana accepts it (§6, D9).
#[tokio::test]
async fn positions_held_across_self_replacement_stay_readable() {
    let storage = dropbox();
    let [mut a, _b, mut c] = accounts_with(storage.clone(), notes, schema).await;
    c.sync().await;
    assert!(c.writes.post_positions().await.unwrap());
    let operation = begin(&mut c, Command::ReplaceOwnKeys).await;
    assert!(matches!(
        settle(&mut c, operation).await,
        Ok(Output::MemberKeysReplaced(_))
    ));
    write_note(&c, "held").await.unwrap();
    c.sync().await;
    let device = c.device().await;
    c.db.record_pending(PendingRecord {
        subject: PendingSubject::Write(coven_merge::WriteId {
            device: a.device().await,
            number: 1,
        }),
        reason: PendingReason::Refused(Refusal::Signature { cause: None }),
        reported_by: device,
    })
    .await
    .unwrap();
    assert!(!c.writes.post_positions().await.unwrap());
    a.sync().await;
    let path = ObjectPath::positions(store(1), device);
    let opened = crate::posted_positions::open(
        &storage.read(&path).await.unwrap(),
        &path,
        a.custody.read().unwrap().as_ref(),
        &a.log().await,
    )
    .unwrap();
    let posted = opened.unwrap_or_else(|reason| panic!("{reason:?}"));
    assert_eq!(posted.store_log, c.log().await.positions());
    assert!(posted.fingerprints.is_empty());
    assert_eq!(posted.pending.len(), 1);
}

/// Carol's post fails after its attempt is signed, then she replaces her own
/// keys. The retry does not send the old pair's signature again (§10): it
/// signs a new attempt over the applied view with the installed pair.
#[tokio::test]
async fn a_positions_attempt_signed_before_self_replacement_is_signed_again() {
    let storage = dropbox();
    let [mut a, _b, mut c] = accounts_with(storage.clone(), notes, schema).await;
    c.sync().await;
    assert!(c.writes.post_positions().await.unwrap());
    let device = c.device().await;
    c.db.record_pending(PendingRecord {
        subject: PendingSubject::Write(coven_merge::WriteId {
            device: a.device().await,
            number: 1,
        }),
        reason: PendingReason::Refused(Refusal::Signature { cause: None }),
        reported_by: device,
    })
    .await
    .unwrap();
    {
        let _pass = c.sync.begin_pass(&mut c.writes);
        c.writes.observe_positions().await.unwrap();
        storage
            .set_faults(coven_storage::test_utils::Faults {
                fail_next: 1,
                ..coven_storage::test_utils::Faults::none()
            })
            .await;
        assert!(c.writes.post_positions().await.is_err());
    }
    let operation = begin(&mut c, Command::ReplaceOwnKeys).await;
    assert!(matches!(
        settle(&mut c, operation).await,
        Ok(Output::MemberKeysReplaced(_))
    ));
    c.sync().await;
    c.writes.post_positions().await.unwrap();
    a.sync().await;
    let path = ObjectPath::positions(store(1), device);
    let bytes = storage.read(&path).await.unwrap();
    let posted = crate::posted_positions::open(
        &bytes,
        &path,
        a.custody.read().unwrap().as_ref(),
        &a.log().await,
    )
    .unwrap()
    .unwrap_or_else(|reason| panic!("{reason:?}"));
    assert_eq!(posted.store_log, c.log().await.positions());
    let installed = c.identity.read().unwrap().unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    let signature: [u8; 64] = bytes[bytes.len() - 64..].try_into().unwrap();
    installed
        .keys()
        .public_keys()
        .signing
        .verify_object(
            path.as_str(),
            &hash.finish(),
            &coven_crypto::Signature::from_bytes(signature),
        )
        .unwrap();
}

/// Ben is an admin but not the store's owner. He publishes a pair Carol
/// prepared elsewhere: no provider access changes, and her held pair is
/// replaced. A self target, a non-admin author, a reused pair and an absent
/// member are refused before anything is reserved.
#[tokio::test]
async fn a_non_owner_admin_replaces_another_members_keys() {
    let storage = dropbox();
    let [mut a, mut b, mut c] = accounts_with(storage.clone(), notes, schema).await;
    let (ben, carol) = (b.member.member(), c.member.member());
    let before = b.log().await.replay.state;
    let refusals = [
        (
            &mut b,
            Command::ReplaceKeysFor(ben, public(&fresh(ben))),
            DropReason::NotAllowed,
        ),
        (
            &mut c,
            Command::ReplaceKeysFor(ben, public(&fresh(ben))),
            DropReason::NotAllowed,
        ),
    ];
    for (d, command, expected) in refusals {
        let result = d.sync.begin_operation_call(command).await;
        assert!(
            matches!(&result, Err(SyncError::Rejected(reason)) if *reason == expected),
            "{:?}",
            result.err()
        );
    }
    let unknown = MemberId(uuid::Uuid::from_u128(77));
    for (command, expected) in [
        (
            Command::ReplaceKeysFor(carol, public(&a.member)),
            DropReason::KeysAlreadyUsed,
        ),
        (
            Command::ReplaceKeysFor(unknown, public(&fresh(unknown))),
            DropReason::TargetGone(EntryTarget::Member(unknown)),
        ),
    ] {
        let result = b.sync.begin_operation_call(command).await;
        assert!(
            matches!(&result, Err(SyncError::Rejected(reason)) if *reason == expected),
            "{:?}",
            result.err()
        );
    }
    assert!(b.db.operations().await.unwrap().is_empty());
    let prepared = fresh(carol);
    let operation = begin(&mut b, Command::ReplaceKeysFor(carol, public(&prepared))).await;
    // Only the entry: no pause and no provider work.
    write_note(&b, "unpaused").await.unwrap();
    assert!(matches!(finish(&mut b, operation).await, Output::Unit));
    let after = b.log().await.replay.state;
    assert_eq!(after.members[&carol].keys, public(&prepared));
    assert_eq!(after.members[&carol].access, before.members[&carol].access);
    assert_eq!(after.devices, before.devices);
    assert!(b.db.operations().await.unwrap().is_empty());
    assert!(MemoryStorage::for_recipient(&storage, "cat@example.com")
        .unwrap()
        .list(&ObjectPrefix::all(store(1)))
        .await
        .is_ok());
    assert!(matches!(
        c.sync.sync_store_log().await,
        Err(SyncError::MemberKeysReplaced { member }) if member == carol
    ));
    a.sync().await;
    assert_eq!(
        a.log().await.replay.state.members[&carol].keys,
        public(&prepared)
    );
}

const ANA: usize = 0;
const BEN: usize = 1;
const CAROL: usize = 2;

/// One call reserved before any is published.
#[derive(Clone, Copy, Debug)]
enum Call {
    /// Carol replaces her own keys.
    Own,
    /// An admin publishes prepared pair `pair` for Carol.
    For(usize),
    /// An admin removes Carol.
    Remove,
}

struct Plan {
    carol_admin: bool,
    calls: Vec<(usize, Call)>,
    order: Vec<usize>,
    me: usize,
}

fn plan(seed: u64) -> Plan {
    let mut g = Generator(seed);
    let mut calls = Vec::new();
    if g.pick(3) != 0 {
        calls.push((CAROL, Call::Own));
    }
    for admin in [ANA, BEN] {
        match g.pick(4) {
            0 => {}
            1 => calls.push((admin, Call::Remove)),
            _ => calls.push((admin, Call::For(g.pick(2)))),
        }
    }
    let mut order: Vec<usize> = (0..calls.len()).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, g.pick(i + 1));
    }
    Plan {
        carol_admin: g.pick(2) == 0,
        calls,
        order,
        me: g.pick(3),
    }
}

/// Step without the harness's catch-up: a reserved entry is published by the
/// session that reserved it, as the operation worker does within its pass.
async fn direct(d: &mut Device, id: OperationId) -> Result<Progress, SyncError> {
    let row = d.operation(id).await;
    let data = crate::operation_data::Data::read(&row)?;
    d.sync.operation_step(&row, data).await
}

/// What Carol's installation holds after the calls: her original pair, her
/// candidate or installed pair, and the pairs prepared for her.
struct Held {
    original: MemberIdentity,
    custody: MemberIdentity,
    prepared: [MemberIdentity; 2],
}

/// Run one plan through the operation calls; returns Ana's final store log,
/// every stored key copy and what Carol holds.
async fn run(plan: &Plan) -> (StoreLog, Vec<KeyCopyPrefix>, Held, Vec<Device>) {
    let storage = dropbox();
    let [a, b, c] = accounts(storage.clone()).await;
    let carol = c.member.member();
    let prepared = [0, 1].map(|_| MemberIdentity::new(carol, MemberKeys::generate().unwrap()));
    let mut devices = vec![a, b, c];
    if plan.carol_admin {
        publish(
            &mut devices[ANA],
            Command::SetRole(carol, coven_format::store_log::MemberRole::Admin),
        )
        .await;
        for d in &mut devices {
            d.sync().await;
        }
    }
    // Reserve every call, each reading only the shared prefix, at distinct
    // times, then keep each session's catch-up fresh for publication.
    let mut reserved = Vec::new();
    for (n, (actor, call)) in plan.calls.iter().enumerate() {
        let d = &mut devices[*actor];
        d.clock
            .set(d.clock.now() + Duration::from_secs(1 + n as u64));
        let local = d.db.local_store_log().await.unwrap();
        let next = local
            .log
            .entries
            .iter()
            .filter(|e| e.entry.position.device == local.device)
            .count() as u64
            + 1;
        d.interrupt_entry(next).await;
        let command = match call {
            Call::Own => Command::ReplaceOwnKeys,
            Call::For(pair) => Command::ReplaceKeysFor(carol, public(&prepared[*pair])),
            Call::Remove => Command::RemoveMember(carol),
        };
        let operation = match d.sync.begin_operation_call(command).await {
            Ok(Begun::Operation(id)) => Some(id),
            Err(SyncError::Storage(_)) if matches!(call, Call::Remove) => None,
            other => panic!("{call:?}: {:?}", other.map(|_| ())),
        };
        if let Some(id) = operation {
            loop {
                match step(d, id).await {
                    Ok(Progress::Advanced) => (),
                    Err(SyncError::Storage(_)) => break,
                    other => panic!("{call:?}: {:?}", other.map(|_| ())),
                }
            }
        }
        reserved.push(operation);
    }
    for d in &mut devices {
        d.catch_up().await;
    }
    for &i in &plan.order {
        let (actor, call) = plan.calls[i];
        let d = &mut devices[actor];
        d.restore_storage();
        match reserved[i] {
            None => match d.sync.sync_store_log().await {
                Ok(()) | Err(SyncError::Removed) => {}
                Err(error) => panic!("{call:?}: {error:?}"),
            },
            Some(id) => loop {
                match direct(d, id).await {
                    Ok(Progress::Advanced) => (),
                    Ok(Progress::Finished(_) | Progress::Reply(_)) => break,
                    // An installation this history replaced cannot install.
                    Err(SyncError::MemberKeysReplaced { .. } | SyncError::Removed) => break,
                    other => panic!("{call:?}: {:?}", other.map(|_| ())),
                }
            },
        }
    }
    for actor in [ANA, BEN] {
        devices[actor].sync().await;
    }
    // Carol's follow-up: a circle, signed with the pair she holds active.
    let c = &mut devices[CAROL];
    let _ = c.sync.sync_store_log().await;
    let key = key(40);
    let key_hash = c.sync.test_key(Audience::Circle(circle(5)), key);
    let _ = c
        .sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(5),
            name: "Garden".into(),
            key,
            key_hash,
        })
        .await;
    devices[ANA].sync().await;
    let log = devices[ANA].log().await;
    let mut copies = Vec::new();
    for object in storage.list(&ObjectPrefix::keys(store(1))).await.unwrap() {
        let bytes = storage.read(&object.path).await.unwrap();
        copies.push(SealedKey::decode(&bytes).unwrap().prefix().clone());
    }
    let held = Held {
        original: devices[CAROL].member.clone(),
        custody: devices[CAROL].identity.read().unwrap().unwrap(),
        prepared,
    };
    (log, copies, held, devices)
}

/// The model input for `log`: entries in timestamp order with stored and
/// attempted times that reproduce exactly their recorded reads.
fn project(
    log: &StoreLog,
    copies: &[KeyCopyPrefix],
    held: &[usize],
    me: usize,
    numbers: &mut Numbers,
) -> Value {
    let entries: Vec<_> = log.entries.iter().map(|applied| &applied.entry).collect();
    let n = entries.len();
    let past: Vec<Vec<usize>> = (0..n)
        .map(|i| {
            (0..i)
                .filter(|&j| crate::replay::had_read(entries[i], entries[j]))
                .collect()
        })
        .collect();
    // Recorded pasts are nested here, so an entry is read exactly by the
    // entries with more reads than any non-reader of it.
    let attempted: Vec<usize> = past.iter().map(|p| 2 * p.len() + 1).collect();
    let stored: Vec<usize> = (0..n)
        .map(|j| {
            (0..n)
                .filter(|&i| past[i].contains(&j))
                .map(|i| 2 * past[i].len())
                .min()
                .unwrap_or(2 * n + 2)
        })
        .collect();
    for (e, (read, attempted)) in past.iter().zip(&attempted).enumerate() {
        for (a, stored) in stored.iter().enumerate() {
            assert_eq!(
                read.contains(&a),
                stored < attempted,
                "reads of entry {e} are not nested"
            );
        }
    }
    let projected: Vec<Value> = entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let signing = match &entry.change {
                StoreChange::CreateStore { admin, .. } => admin.signing,
                _ => crate::object_author::recorded_signer(
                    log,
                    &crate::replay::author_positions(entry),
                    &entry.author,
                )
                .expect("a kept author binding"),
            };
            let action = match &entry.change {
                StoreChange::ReplaceMemberKeys { member, keys } => json!({"kind": 16,
                    "member": Numbers::member(member), "keys": numbers.keys(*keys)}),
                _ => action(entry, numbers),
            };
            json!({"author": Numbers::member(&entry.author), "signer": numbers.signing(signing),
                "device": entry.position.device.0, "past": past[i], "action": action,
                "stored": stored[i], "attempted": attempted[i]})
        })
        .collect();
    let copies: Vec<Value> = copies
        .iter()
        .map(|copy| {
            json!([
                introduction(log, &copy.audience, copy.key),
                Numbers::member(&copy.member),
                numbers.sealing(copy.sealing)
            ])
        })
        .collect();
    json!({"entries": projected, "window": 1000, "held": held, "copies": copies,
        "self": me, "audiences": [0], "shares": [[0, 0], [1, 0], [2, 0]]})
}

/// The index of the entry introducing `audience`'s key `key`.
fn introduction(log: &StoreLog, audience: &Audience, key: KeyId) -> usize {
    log.entries
        .iter()
        .position(|applied| {
            crate::key_introductions::introduction(&applied.entry.change)
                .is_some_and(|(a, k, _)| &a == audience && k == key)
        })
        .expect("an introduced key")
}

fn set<T: serde::de::DeserializeOwned + Ord>(value: &Value) -> BTreeSet<T> {
    serde_json::from_value(value.clone()).unwrap()
}

#[tokio::test]
#[ignore = "requires COVEN_MEMBER_KEY_LEAN, supplied by scripts/check.sh"]
async fn lean_differential_member_key_calls() {
    let mut cases = Vec::new();
    let mut inputs = Vec::new();
    let mut numbers = Vec::new();
    for seed in 1..=30 {
        let plan = plan(0x6361_6c6c + seed);
        let (log, copies, held, devices) = run(&plan).await;
        let ring = devices[plan.me].custody.read().unwrap().unwrap();
        let custody: Vec<usize> = ring
            .store_key_ids()
            .map(|key| introduction(&log, &Audience::Store, key))
            .collect();
        let mut n = Numbers::default();
        inputs.push(project(&log, &copies, &custody, plan.me, &mut n));
        numbers.push(n);
        cases.push((plan, log, copies, held, devices));
    }
    let output = run_lean("COVEN_MEMBER_KEY_LEAN", &Value::Array(inputs.clone()));
    assert!(
        output.status.success(),
        "Lean runner failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let results: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let mut seen = BTreeMap::<&str, usize>::new();
    for (((plan, log, copies, held, devices), input), (result, numbers)) in cases
        .iter()
        .zip(&inputs)
        .zip(results.iter().zip(&mut numbers))
    {
        let context = format!("{:?} in {input}", plan.calls);
        let replay = &log.replay;
        let index = |position| {
            log.entries
                .iter()
                .position(|applied| applied.entry.position == position)
                .unwrap()
        };
        let fates = |kept: bool| -> BTreeSet<usize> {
            replay
                .entries
                .iter()
                .filter(|(_, outcome)| (**outcome == EntryOutcome::Kept) == kept)
                .map(|(position, _)| index(*position))
                .collect()
        };
        assert_eq!(fates(true), set(&result["kept"]), "kept {context}");
        assert_eq!(fates(false), set(&result["dropped"]), "dropped {context}");
        for (position, outcome) in &replay.entries {
            if matches!(
                log.entries[index(*position)].entry.change,
                StoreChange::ReplaceMemberKeys { .. }
            ) {
                *seen
                    .entry(match outcome {
                        EntryOutcome::Kept => "replacement kept",
                        EntryOutcome::Dropped(coven_database::DropReason::BeatenBy(_)) => {
                            "replacement beaten"
                        }
                        EntryOutcome::Dropped(_) => "replacement dropped otherwise",
                    })
                    .or_default() += 1;
            }
        }
        // Stable ids keep their roles; only Carol's pair can change.
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
        assert_eq!(keys, set(&result["keys"]), "member keys {context}");
        let used: BTreeSet<[usize; 2]> = replay
            .state
            .bound_keys
            .iter()
            .map(|keys| [numbers.signing(keys.signing), numbers.sealing(keys.sealing)])
            .collect();
        assert_eq!(used, set(&result["used"]), "bound pairs {context}");
        let members: BTreeSet<[usize; 2]> = replay
            .state
            .members
            .iter()
            .filter(|(_, member)| !member.removed)
            .map(|(id, member)| {
                [
                    Numbers::member(id) as usize,
                    usize::from(member.role == coven_format::store_log::MemberRole::Member),
                ]
            })
            .collect();
        assert_eq!(members, set(&result["members"]), "members {context}");
        // Replacement never touches a device id.
        let carol = held.original.member();
        let removed = replay.state.members[&carol].removed;
        assert!(replay
            .state
            .devices
            .values()
            .all(|device| device.active() || (removed && device.member == carol)));
        // Send admission for each pair Carol's installation could hold.
        let device = devices[CAROL].db.local_store_log().await.unwrap().device;
        let current: Vec<[usize; 3]> = serde_json::from_value(result["keys"].clone()).unwrap();
        let active: BTreeSet<u64> = set::<[u64; 2]>(&result["members"])
            .into_iter()
            .map(|[m, _]| m)
            .collect();
        let mut pairs: Vec<MemberIdentity> = vec![held.original.clone(), held.custody.clone()];
        pairs.extend(held.prepared.iter().cloned());
        for identity in &pairs {
            let keys: MemberPublicKeys = identity.keys().public_keys();
            let numbered = [
                Numbers::member(&carol) as usize,
                numbers.signing(keys.signing),
                numbers.sealing(keys.sealing),
            ];
            let model = active.contains(&Numbers::member(&carol)) && current.contains(&numbered);
            let rust = crate::write_seal::check_member(log, identity, device);
            assert_eq!(rust.is_ok(), model, "sending with {numbered:?} {context}");
            if !model {
                assert!(
                    matches!(
                        rust,
                        Err(SyncError::MemberKeysReplaced { .. } | SyncError::Removed)
                    ),
                    "{context}"
                );
            }
            *seen
                .entry(if model { "pair sends" } else { "pair stopped" })
                .or_default() += 1;
        }
        // Retired credentials, routes and exposed copies.
        let credentials = crate::member_credentials::Credentials::new(log);
        let retired: BTreeSet<usize> = credentials
            .retired
            .iter()
            .map(|sealing| numbers.sealing(*sealing))
            .collect();
        assert_eq!(retired, set(&result["retired"]), "retired {context}");
        let routes: BTreeSet<[usize; 2]> = replay
            .state
            .members
            .keys()
            .filter_map(|id| {
                let sealing = credentials.route(id)?;
                Some([Numbers::member(id) as usize, numbers.sealing(sealing)])
            })
            .collect();
        assert_eq!(routes, set(&result["routes"]), "routes {context}");
        for (i, copy) in copies.iter().enumerate() {
            let exposes = credentials.exposes(copy);
            assert_eq!(
                Some(exposes),
                result["exposes"][i].as_bool(),
                "copy {i} {context}"
            );
            *seen
                .entry(if exposes { "exposed copy" } else { "safe copy" })
                .or_default() += 1;
        }
        // Sharing the creation key and the first-attempt key selection.
        let store_key = log
            .entries
            .iter()
            .find_map(|applied| match &applied.entry.change {
                StoreChange::CreateStore { key, .. } => Some(*key),
                _ => None,
            })
            .unwrap();
        let shared = credentials.shares(copies, &Audience::Store, store_key);
        for (m, lean) in [0u8, 1, 2].iter().zip(result["shared"].as_array().unwrap()) {
            let mine: BTreeSet<[usize; 2]> = shared
                .iter()
                .filter(|copy| copy.member == crate::replay::tests::member(*m))
                .map(|copy| {
                    [
                        Numbers::member(&copy.member) as usize,
                        numbers.sealing(copy.sealing),
                    ]
                })
                .collect();
            let expected: BTreeSet<[usize; 2]> = match lean.as_array() {
                Some(appended) => appended
                    .iter()
                    .map(|copy| serde_json::from_value::<[usize; 2]>(copy.clone()).unwrap())
                    .filter(|[member, sealing]| {
                        // Production reserves only a copy it has not observed.
                        !copies.iter().any(|copy| {
                            Numbers::member(&copy.member) as usize == *member
                                && numbers.sealing(copy.sealing) == *sealing
                        })
                    })
                    .collect(),
                None => BTreeSet::new(),
            };
            assert_eq!(mine, expected, "sharing with {m} {context}");
        }
        let ring = devices[plan.me].custody.read().unwrap().unwrap();
        let held_key = |audience: &Audience, id: KeyId| {
            (audience == &Audience::Store && ring.store_key_ids().any(|k| k == id))
                .then(|| ring.store_key(id).unwrap().hash())
        };
        let selected = crate::key_introductions::select(
            &crate::key_introductions::introductions(log),
            &credentials,
            &crate::replay::tests::member(plan.me as u8),
            Some(copies),
            held_key,
            &Audience::Store,
        )
        .ok()
        .map(|key| introduction(log, &Audience::Store, key));
        assert_eq!(
            json!(selected),
            result["selected"][0],
            "selection by {} {context}",
            plan.me
        );
        *seen
            .entry(if selected.is_some() {
                "key selected"
            } else {
                "no usable key"
            })
            .or_default() += 1;
    }
    for outcome in [
        "replacement kept",
        "replacement beaten",
        "pair sends",
        "pair stopped",
        "exposed copy",
        "safe copy",
        "key selected",
        "no usable key",
    ] {
        assert!(
            seen.get(outcome).is_some_and(|&n| n > 0),
            "{outcome}: {seen:?}"
        );
    }
}
