//! Audience keys exposed to replaced or losing credentials (§11): a kept
//! replacement keeps its member, but every copy to the retired credential
//! exposes its key; only a current audience member rotates; sharing and
//! rotation reach the current credential; and a copy opens only with the
//! exact private credential it names. The keyed differential compares the
//! same credentials with `KeySelection.MemberKeys` and
//! `KeyPhases.MemberKeys.rotationCopies`.

use super::*;
use crate::entry_signature::tests::{store_key, Numbers};
use crate::key_introductions::exposed;
use crate::key_introductions::tests::landed_log;
use crate::member_credentials::Credentials;
use crate::object_author::tests::member_key_history::{self as keyed, Case, BEN, CAROL};
use crate::operation_data::{Data, EntryWork, Intent};
use crate::replay::tests::{self as history, History};
use crate::store_log_sync::key_rotation::tests::{finish_rotation, observed};
use crate::store_log_sync::tests::key_distribution::{CopyAttempt, CopyStorage};
use crate::{PendingReason, PendingRecord, PendingSubject};
use coven_crypto::{MemberIdentity, MemberKeys, SealingKey};
use coven_database::{DropReason, StoreLog};
use coven_format::sealed_key::{KeyCopyPrefix, SealedKey};
use coven_foundation::id_source::{CircleId, MemberId};
use coven_merge::Audience;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

fn rekey(n: u8, keys: coven_format::store_log::MemberPublicKeys) -> StoreChange {
    StoreChange::ReplaceMemberKeys {
        member: history::member(n),
        keys,
    }
}

/// The store key's copy to member `n` at `sealing`.
fn copy_to(n: u8, sealing: coven_crypto::SealingKey) -> KeyCopyPrefix {
    KeyCopyPrefix {
        audience: Audience::Store,
        key: history::key(1),
        member: history::member(n),
        sealing,
    }
}

/// Ana and Carol (admins) replace Ben's keys concurrently; Carol's earlier
/// one wins. Ben then replaces the winner himself. Each kept replacement
/// retires the pair its own recorded view bound, while the losing
/// candidate's and a never-published candidate's copies expose the key
/// because they name a credential that is not Ben's current one, though Ben
/// stays a member (§11, `MemberKeys.retired`, `exposed`). Only Ben's
/// current credential routes and receives history.
#[test]
fn replacements_retire_bound_credentials_and_losing_candidates_expose_their_copies() {
    let mut h = History::new();
    h.all(0, 0, history::add(1, MemberRole::Member));
    h.all(0, 0, history::add(2, MemberRole::Admin));
    let prefix: Vec<usize> = (0..h.entries.len()).collect();
    let (carols, anas, bens, never) = (
        history::keys(5),
        history::keys(6),
        history::keys(7),
        history::keys(8),
    );
    h.push(2, 2, &prefix, rekey(1, carols));
    h.push(0, 0, &prefix, rekey(1, anas));
    h.all(1, 1, rekey(1, bens));
    let log = landed_log(&h, &vec![0; h.entries.len()]);
    let outcome = |i: usize| log.replay.entries[&h.entries[i].position].clone();
    assert_eq!(outcome(3), EntryOutcome::Kept);
    assert!(matches!(
        outcome(4),
        EntryOutcome::Dropped(DropReason::BeatenBy(_))
    ));
    assert_eq!(outcome(5), EntryOutcome::Kept);
    let credentials = Credentials::new(&log);
    assert_eq!(
        credentials.retired,
        [history::keys(1).sealing, carols.sealing].into()
    );
    assert_eq!(credentials.route(&history::member(1)), Some(bens.sealing));
    for (sealing, exposes) in [
        (history::keys(1).sealing, true),
        (carols.sealing, true),
        (anas.sealing, true),
        (never.sealing, true),
        (bens.sealing, false),
    ] {
        assert_eq!(credentials.exposes(&copy_to(1, sealing)), exposes);
    }
    // Ana's own credential is current and in the audience; an outsider's is not.
    assert!(!credentials.exposes(&copy_to(0, history::keys(0).sealing)));
    assert!(credentials.exposes(&copy_to(4, history::keys(4).sealing)));
    // Sharing reaches Ben only at his current key, past the losing
    // candidate's copy; the old copies stay and keep the key exposed.
    let observed = [
        copy_to(1, anas.sealing),
        copy_to(0, history::keys(0).sealing),
    ];
    let shared = credentials.shares(&observed, &Audience::Store, history::key(1));
    assert_eq!(
        shared,
        [
            copy_to(1, bens.sealing),
            copy_to(2, history::keys(2).sealing)
        ]
    );
    let mut after = observed.to_vec();
    after.extend(shared);
    assert!(exposed(
        &credentials,
        &after,
        &Audience::Store,
        history::key(1)
    ));
    assert!(credentials
        .shares(&after, &Audience::Store, history::key(1))
        .is_empty());
    // A rotation routes every member to a current credential.
    let rotation = credentials
        .rotation_copies(&Audience::Store, history::key(2))
        .unwrap();
    assert!(rotation.iter().all(|copy| !credentials.exposes(copy)));
    assert_eq!(rotation.len(), 3);
}

/// Ben's id with a fresh private pair no entry has bound.
fn fresh_ben() -> MemberIdentity {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([0x42; 64]);
    MemberIdentity::new(
        member(2).member(),
        MemberKeys::from_secret_bytes(&bytes).unwrap(),
    )
}

fn sealing(identity: &MemberIdentity) -> SealingKey {
    public(identity).sealing
}

/// The sealing keys of every stored copy of `key` addressed to `to`.
async fn copies_to(
    storage: &dyn Storage,
    audience: &Audience,
    key: KeyId,
    to: &MemberId,
) -> Vec<SealingKey> {
    let mut sealings = Vec::new();
    for object in storage.list(&ObjectPrefix::keys(store(1))).await.unwrap() {
        let bytes = storage.read(&object.path).await.unwrap();
        let prefix = SealedKey::decode(&bytes).unwrap().prefix().clone();
        if &prefix.audience == audience && prefix.key == key && prefix.member == *to {
            sealings.push(prefix.sealing);
        }
    }
    sealings.sort();
    sealings
}

/// A first attempt from a fresh complete observation, as a new pass makes.
async fn selection(device: &Device, audience: &Audience) -> Result<KeyId, SyncError> {
    let ring = device.custody.read().unwrap().unwrap();
    crate::write_seal::first_attempt(
        &device.log().await,
        &observed(device).await,
        &ring,
        &device.member.member(),
        audience,
    )
}

fn unavailable(selected: Result<KeyId, SyncError>, id: KeyId) -> bool {
    matches!(selected, Err(SyncError::KeyUnavailable(key)) if key == id)
}

async fn make_circle(device: &mut Device, id: CircleId, key: KeyId, name: &str) {
    let key_hash = device.sync.test_key(Audience::Circle(id), key);
    device
        .sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: id,
            name: name.into(),
            key,
            key_hash,
        })
        .await
        .unwrap();
}

async fn join(device: &mut Device, id: CircleId, who: &MemberIdentity) {
    device
        .sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle: id,
            member: who.member(),
        })
        .await
        .unwrap();
}

async fn replace_ben(device: &mut Device) {
    device
        .sync
        .make_and_upload_entry(StoreChange::ReplaceMemberKeys {
            member: member(2).member(),
            keys: public(&fresh_ben()),
        })
        .await
        .unwrap();
}

/// Ana (admin, device 1) creates the store and adds Ben (device 2) and
/// Carol (device 3) as members; both register.
async fn household() -> (Arc<coven_storage::test_utils::MemoryStorage>, [Device; 3]) {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let mut carol = device(storage.clone(), 3, member(3), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ana.add(&carol.member, MemberRole::Member).await;
    for (who, name) in [(&mut ben, "Ben"), (&mut carol, "Carol")] {
        who.sync().await;
        who.sync.load_new_device(name, None).await.unwrap();
    }
    ana.sync().await;
    (storage, [ana, ben, carol])
}

/// Ana replaces Ben's keys for no reason but routine. Ben stays in the store
/// and in Gifts, yet his old copies expose both audience keys. The binding's
/// prerequisites reached his candidate first; Ana's rotations of both
/// audiences seal only to his new credential.
#[tokio::test]
async fn a_benign_replacement_exposes_old_copies_until_current_members_rotate() {
    let (storage, [mut ana, ben, _carol]) = household().await;
    let gifts = Audience::Circle(circle(1));
    make_circle(&mut ana, circle(1), key(10), "Gifts").await;
    join(&mut ana, circle(1), &ben.member).await;
    assert_eq!(selection(&ana, &Audience::Store).await.unwrap(), key(1));
    assert_eq!(selection(&ana, &gifts).await.unwrap(), key(10));
    replace_ben(&mut ana).await;
    let ben_id = ben.member.member();
    for (audience, id) in [(Audience::Store, key(1)), (gifts.clone(), key(10))] {
        let mut both = vec![sealing(&member(2)), sealing(&fresh_ben())];
        both.sort();
        assert_eq!(
            copies_to(storage.as_ref(), &audience, id, &ben_id).await,
            both
        );
    }
    let log = ana.log().await;
    assert!(log.replay.state.reads_circle(circle(1), &ben_id));
    assert_eq!(Credentials::new(&log).retired, [sealing(&member(2))].into());
    assert!(unavailable(selection(&ana, &Audience::Store).await, key(1)));
    assert!(unavailable(selection(&ana, &gifts).await, key(10)));
    ana.catch_up().await;
    assert!(ana.sync.rotate_exposed_keys().await.unwrap());
    finish_rotation(&mut ana).await;
    ana.catch_up().await;
    for (audience, old) in [(Audience::Store, key(1)), (gifts, key(10))] {
        let fresh = selection(&ana, &audience).await.unwrap();
        assert_ne!(fresh, old);
        assert_eq!(
            copies_to(storage.as_ref(), &audience, fresh, &ben_id).await,
            [sealing(&fresh_ben())]
        );
        // The old copies stay for good; the old key still reads.
        assert!(copies_to(storage.as_ref(), &audience, old, &ben_id)
            .await
            .contains(&sealing(&member(2))));
    }
}

/// Ben's old credential reaches Gifts (Ben, Carol) and Garden (Ben alone).
/// Ana, a store admin in neither, rotates the store but can rotate neither
/// circle. Carol, the one online member of an affected circle, rotates
/// Gifts; Garden stays exposed until one of its own members rotates it.
#[tokio::test]
async fn only_a_current_circle_member_rotates_its_exposed_key() {
    let (storage, [mut ana, mut ben, mut carol]) = household().await;
    let (gifts, garden) = (Audience::Circle(circle(1)), Audience::Circle(circle(2)));
    make_circle(&mut ben, circle(1), key(10), "Gifts").await;
    join(&mut ben, circle(1), &carol.member).await;
    make_circle(&mut ben, circle(2), key(20), "Garden").await;
    carol.sync().await;
    ana.sync().await;
    replace_ben(&mut ana).await;
    ana.catch_up().await;
    assert!(ana.sync.rotate_exposed_keys().await.unwrap());
    let rotations: Vec<_> = ana
        .db
        .operations()
        .await
        .unwrap()
        .iter()
        .filter_map(|record| match Data::read(record).unwrap() {
            Data::Entry(EntryWork {
                intent: Intent::RotateKey { audience },
                ..
            }) => Some(audience),
            _ => None,
        })
        .collect();
    assert_eq!(rotations, [Audience::Store]);
    let outside = ana
        .sync
        .make_and_upload_entry(StoreChange::RotateKey {
            audience: garden.clone(),
            key: key(21),
            key_hash: [0; 32],
        })
        .await;
    assert!(
        matches!(
            outside,
            Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
        ),
        "{outside:?}"
    );
    finish_rotation(&mut ana).await;
    carol.sync().await;
    assert!(unavailable(selection(&carol, &gifts).await, key(10)));
    assert!(carol.sync.rotate_exposed_keys().await.unwrap());
    finish_rotation(&mut carol).await;
    carol.catch_up().await;
    let fresh = selection(&carol, &gifts).await.unwrap();
    assert_ne!(fresh, key(10));
    assert_eq!(
        copies_to(storage.as_ref(), &gifts, fresh, &ben.member.member()).await,
        [sealing(&fresh_ben())]
    );
    let log = carol.log().await;
    let copies: Vec<_> = observed(&carol)
        .await
        .into_iter()
        .map(|copy| copy.prefix)
        .collect();
    assert!(exposed(&Credentials::new(&log), &copies, &garden, key(20)));
    assert_eq!(
        crate::key_introductions::newest(&log, &garden),
        Some(key(20))
    );
}

/// After Ana rotates past Ben's old credential, a copy of the new key to
/// that credential lands after her pass observed the copy logs. That pass
/// may still first-send with the new key (the residual window); her next
/// observation retires it (§11).
#[tokio::test]
async fn an_old_credential_copy_after_the_pass_exposes_the_rotated_key() {
    let (storage, [mut ana, _ben, mut carol]) = household().await;
    replace_ben(&mut ana).await;
    ana.catch_up().await;
    ana.sync.rotate_exposed_keys().await.unwrap();
    finish_rotation(&mut ana).await;
    ana.catch_up().await;
    let rotated = selection(&ana, &Audience::Store).await.unwrap();
    let pass = ana.sync.copy_observation().unwrap();
    // Carol's stale reservation of the new key for Ben's old credential.
    carol.sync().await;
    let material = carol
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(rotated)
        .unwrap()
        .clone();
    let (ben_id, old) = (member(2).member(), sealing(&member(2)));
    carol
        .db
        .prepare_key_upload(move |path| keys::store_copy(path, &material, &ben_id, &old))
        .await
        .unwrap();
    object::upload_keys(store(1), Some(storage.as_ref()), &carol.db, &carol.send)
        .await
        .unwrap();
    let ring = ana.custody.read().unwrap().unwrap();
    let log = ana.log().await;
    let first = |observation: &[keys::ObservedCopy]| {
        crate::write_seal::first_attempt(
            &log,
            observation,
            &ring,
            &ana.member.member(),
            &Audience::Store,
        )
    };
    assert_eq!(first(&pass).unwrap(), rotated);
    ana.catch_up().await;
    assert!(unavailable(
        first(&ana.sync.copy_observation().unwrap()),
        rotated
    ));
}

/// Ben (in Garden with Carol) has his keys replaced by Ana, who holds no
/// Garden key. Returns his new installation, caught up.
async fn new_installation() -> (
    Arc<coven_storage::test_utils::MemoryStorage>,
    [Device; 3],
    Device,
) {
    let (storage, [mut ana, mut ben, mut carol]) = household().await;
    make_circle(&mut ben, circle(2), key(20), "Garden").await;
    join(&mut ben, circle(2), &carol.member).await;
    carol.sync().await;
    ana.sync().await;
    replace_ben(&mut ana).await;
    let mut restored = device(storage.clone(), 5, fresh_ben(), store(1)).await;
    restored.sync().await;
    (storage, [ana, ben, carol], restored)
}

/// This installation's record for its member's copy of Garden's key.
async fn garden_records(device: &Device) -> Vec<PendingRecord> {
    let own = device.db.local_store_log().await.unwrap().device;
    device
        .db
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| {
            record.reported_by == own
                && matches!(&record.subject, PendingSubject::KeyCopy { key: k, .. } if *k == key(20))
        })
        .collect()
}

fn garden_copy(sealing: SealingKey) -> PendingSubject {
    PendingSubject::KeyCopy {
        audience: Audience::Circle(circle(2)),
        key: key(20),
        member: member(2).member(),
        sealing,
    }
}

/// Ben's new installation holds only his new pair. The store key's
/// prerequisite copy names it and opens; Garden's only copy names the old
/// credential, which this custody lacks, so the key waits under the new
/// credential's subject, and an earlier record for the old credential never
/// stands in for it. Carol, a current Garden member, shares the key with
/// the new credential; her interrupted copy retries the same bytes after
/// she reopens, and then it opens.
#[tokio::test]
async fn a_new_installation_opens_only_copies_naming_the_credential_it_holds() {
    let (storage, [_ana, _ben, mut carol], mut restored) = new_installation().await;
    let ring = restored.custody.read().unwrap().unwrap();
    assert!(ring.store_key(key(1)).is_ok());
    assert!(ring.circle_key(circle(2), key(20)).is_err());
    let garden = Audience::Circle(circle(2));
    assert_eq!(
        copies_to(storage.as_ref(), &garden, key(20), &member(2).member()).await,
        [sealing(&member(2))]
    );
    let wait = garden_records(&restored).await;
    assert_eq!(wait.len(), 1);
    assert_eq!(wait[0].subject, garden_copy(sealing(&fresh_ben())));
    assert!(matches!(
        wait[0].reason,
        PendingReason::KeyUnavailable { key: k, .. } if k == key(20)
    ));
    // An old-credential record is not this credential's: the next reading
    // replaces it with the new credential's wait.
    let own = restored.db.local_store_log().await.unwrap().device;
    restored
        .db
        .record_pending(PendingRecord {
            subject: garden_copy(sealing(&member(2))),
            reason: PendingReason::KeyUnavailable {
                audience: garden.clone(),
                key: key(20),
            },
            reported_by: own,
        })
        .await
        .unwrap();
    restored.sync().await;
    let wait: Vec<_> = garden_records(&restored)
        .await
        .into_iter()
        .map(|record| record.subject)
        .collect();
    assert_eq!(wait, [garden_copy(sealing(&fresh_ben()))]);
    // Carol shares Garden's key with Ben's current credential only.
    let carol_device = carol.db.local_store_log().await.unwrap().device;
    let path =
        crate::store_log_sync::tests::key_distribution::next_copy(storage.as_ref(), carol_device)
            .await;
    let fault = Arc::new(CopyStorage::new(
        storage.clone(),
        vec![path.clone()],
        CopyAttempt::InterruptOnce,
    ));
    carol.sync.storage = Some(fault.clone());
    assert!(carol.sync.sync_store_log().await.is_err());
    let fixed = carol.db.key_uploads().await.unwrap().remove(0);
    assert_eq!(fixed.path(store(1)), path);
    carol.restart(storage.clone()).await;
    carol.sync.storage = Some(fault.clone());
    carol.sync().await;
    assert_eq!(storage.read(&path).await.unwrap(), fixed.bytes);
    assert_eq!(fault.attempts(), vec![(path, fixed.bytes); 2]);
    let mut both = vec![sealing(&member(2)), sealing(&fresh_ben())];
    both.sort();
    assert_eq!(
        copies_to(storage.as_ref(), &garden, key(20), &member(2).member()).await,
        both
    );
    restored.sync().await;
    let ring = restored.custody.read().unwrap().unwrap();
    assert!(ring.circle_key(circle(2), key(20)).is_ok());
    assert!(garden_records(&restored).await.is_empty());
    // The key Garden's members read with stays exposed by the old copy.
    let log = carol.log().await;
    let copies: Vec<_> = observed(&carol)
        .await
        .into_iter()
        .map(|copy| copy.prefix)
        .collect();
    assert!(exposed(&Credentials::new(&log), &copies, &garden, key(20)));
}

/// Two copies name Ben's current credential but fail its checks: one holds
/// other bytes under Garden's key id, and one was sealed to his old
/// credential under a prefix naming the new one. Each is refused by itself,
/// and neither exposes the key: exposure follows a copy's named recipient.
/// A later valid copy from Carol supplies the key.
#[tokio::test]
async fn refused_copies_to_the_current_credential_neither_supply_nor_expose() {
    let (storage, [ana, _ben, mut carol], mut restored) = new_installation().await;
    let real = carol
        .custody
        .read()
        .unwrap()
        .unwrap()
        .circle_key(circle(2), key(20))
        .unwrap()
        .clone();
    let (ben_id, current, old) = (
        member(2).member(),
        sealing(&fresh_ben()),
        sealing(&member(2)),
    );
    ana.db
        .prepare_key_upload(move |path| {
            let forged = coven_crypto::CircleKey::generate(circle(2), key(20))?;
            keys::circle_copy(path, &forged, &ben_id, &current)
        })
        .await
        .unwrap();
    let mismatched = real.clone();
    ana.db
        .prepare_key_upload(move |path| {
            let prefix = KeyCopyPrefix {
                audience: Audience::Circle(circle(2)),
                key: key(20),
                member: ben_id,
                sealing: current,
            };
            let sealed =
                coven_crypto::seal_circle_key(&mismatched, &old, path.as_str(), &prefix.encode()?)?;
            Ok::<_, SyncError>(prefix.encode_box(&sealed)?)
        })
        .await
        .unwrap();
    object::upload_keys(store(1), Some(storage.as_ref()), &ana.db, &ana.send)
        .await
        .unwrap();
    restored.sync().await;
    assert!(restored
        .custody
        .read()
        .unwrap()
        .unwrap()
        .circle_key(circle(2), key(20))
        .is_err());
    let refused = garden_records(&restored).await;
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].subject, garden_copy(current));
    assert!(matches!(refused[0].reason, PendingReason::Refused(_)));
    let log = restored.log().await;
    let credentials = Credentials::new(&log);
    let planted: Vec<_> = observed(&restored)
        .await
        .into_iter()
        .filter(|copy| copy.prefix.key == key(20) && copy.prefix.sealing == current)
        .map(|copy| copy.prefix)
        .collect();
    assert_eq!(planted.len(), 2);
    assert!(planted.iter().all(|copy| !credentials.exposes(copy)));
    // Carol observes copies at Ben's current credential, so her pass shares
    // nothing more; she supplies the valid copy herself.
    carol.sync().await;
    assert!(carol.db.key_uploads().await.unwrap().is_empty());
    let supplied = real.clone();
    carol
        .db
        .prepare_key_upload(move |path| keys::circle_copy(path, &supplied, &ben_id, &current))
        .await
        .unwrap();
    object::upload_keys(store(1), Some(storage.as_ref()), &carol.db, &carol.send)
        .await
        .unwrap();
    restored.sync().await;
    assert_eq!(
        restored
            .custody
            .read()
            .unwrap()
            .unwrap()
            .circle_key(circle(2), key(20))
            .unwrap()
            .hash(),
        real.hash()
    );
    assert!(garden_records(&restored).await.is_empty());
}

/// Retired credentials, routes, copy exposure, rotation and sharing routes
/// and first-attempt selection of the production replay against
/// `KeySelection.MemberKeys` and `KeyPhases.MemberKeys.rotationCopies`.
fn compare_credentials(
    case: &Case,
    log: &StoreLog,
    input: &Value,
    result: &Value,
    numbers: &mut Numbers,
    seen: &mut BTreeMap<&'static str, usize>,
) {
    let credentials = Credentials::new(log);
    let retired: BTreeSet<usize> = credentials
        .retired
        .iter()
        .map(|sealing| numbers.sealing(*sealing))
        .collect();
    let lean: BTreeSet<usize> = serde_json::from_value(result["retired"].clone()).unwrap();
    assert_eq!(retired, lean, "retired credentials of {input}");
    let routes: BTreeSet<[usize; 2]> = log
        .replay
        .state
        .members
        .keys()
        .filter_map(|id| {
            let sealing = credentials.route(id)?;
            Some([Numbers::member(id) as usize, numbers.sealing(sealing)])
        })
        .collect();
    let lean: BTreeSet<[usize; 2]> = serde_json::from_value(result["routes"].clone()).unwrap();
    assert_eq!(routes, lean, "routes of {input}");
    let prefixes: Vec<_> = case
        .copies
        .iter()
        .enumerate()
        .map(|(i, to)| keyed::copy_prefix(i, to))
        .collect();
    for (i, prefix) in prefixes.iter().enumerate() {
        let exposes = credentials.exposes(prefix);
        assert_eq!(
            Some(exposes),
            result["exposes"][i].as_bool(),
            "copy {i} of {input}"
        );
        *seen
            .entry(if !exposes {
                "current copy"
            } else if credentials.retired.contains(&prefix.sealing) {
                "retired copy"
            } else if crate::key_introductions::belongs(
                &log.replay.state,
                &coven_merge::Audience::Store,
                &prefix.member,
            ) {
                "noncurrent copy"
            } else {
                "outside copy"
            })
            .or_default() += 1;
    }
    let pairs = |copies: &[coven_format::sealed_key::KeyCopyPrefix], numbers: &mut Numbers| {
        let mut pairs: Vec<[usize; 2]> = copies
            .iter()
            .map(|copy| {
                [
                    Numbers::member(&copy.member) as usize,
                    numbers.sealing(copy.sealing),
                ]
            })
            .collect();
        pairs.sort();
        json!(pairs)
    };
    let key = store_key().id();
    let audience = coven_merge::Audience::Store;
    let rotation = match credentials.rotation_copies(&audience, key) {
        Ok(copies) => pairs(&copies, numbers),
        Err(_) => Value::Null,
    };
    assert_eq!(
        rotation, result["rotations"][0],
        "rotation copies of {input}"
    );
    // `MemberKeys.share` appends a copy whenever custody and membership
    // allow; production reserves it only when that exact copy is unobserved.
    let shared = credentials.shares(&prefixes, &audience, key);
    for (m, lean) in [0, BEN, CAROL]
        .iter()
        .zip(result["shared"].as_array().unwrap())
    {
        let mine: Vec<_> = shared
            .iter()
            .filter(|copy| copy.member == history::member(*m) && case.held)
            .cloned()
            .collect();
        let expected = match lean.as_array() {
            Some(appended) => {
                let [member, sealing] =
                    serde_json::from_value::<[usize; 2]>(appended[0].clone()).unwrap();
                let observed = prefixes.iter().any(|copy| {
                    Numbers::member(&copy.member) as usize == member
                        && numbers.sealing(copy.sealing) == sealing
                });
                *seen
                    .entry(if observed {
                        "share observed"
                    } else {
                        "share routed"
                    })
                    .or_default() += 1;
                if observed {
                    json!([])
                } else {
                    json!([[member, sealing]])
                }
            }
            None => json!([]),
        };
        assert_eq!(
            pairs(&mine, numbers),
            expected,
            "sharing with {m} in {input}"
        );
    }
    // Custody holds exactly the committed creation key.
    let (_, _, hash) =
        crate::key_introductions::introduction(&case.history.entries[0].change).unwrap();
    let held = |_: &coven_merge::Audience, id| (case.held && id == key).then_some(hash);
    let selected = crate::key_introductions::select(
        &crate::key_introductions::introductions(log),
        &credentials,
        &history::member(case.me),
        Some(&prefixes),
        held,
        &audience,
    )
    .ok()
    .map(|_| 0);
    assert_eq!(
        json!(selected),
        result["selected"][0],
        "first attempt by {} in {input}",
        case.me
    );
    *seen
        .entry(if selected.is_some() {
            "key selected"
        } else {
            "no usable key"
        })
        .or_default() += 1;
}

#[tokio::test]
#[ignore = "requires COVEN_MEMBER_KEY_LEAN, supplied by scripts/check.sh"]
async fn lean_differential_member_key_credentials() {
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
        let received = keyed::receive(case, &case.received);
        compare_credentials(case, &received.log, input, result, numbers, &mut seen);
    }
    for outcome in [
        "current copy",
        "retired copy",
        "noncurrent copy",
        "outside copy",
        "share routed",
        "share observed",
        "key selected",
        "no usable key",
    ] {
        assert!(
            seen.get(outcome).is_some_and(|&n| n > 0),
            "{outcome}: {seen:?}"
        );
    }
}
