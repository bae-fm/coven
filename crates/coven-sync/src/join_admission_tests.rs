//! Recovery invites bind new keys to an existing member id (§12.2, §12.3):
//! the admin fixes the target before the invite exists, approval binds the
//! request's exact pair after its prerequisite copies, and the joining
//! installation registers only on its approving writer's own kept binding.
//! A losing approval is reported, never published again over the winner.

use super::invites::{joiner, request};
use super::*;
use crate::entry_signature::tests::Numbers;
use crate::join_admission::{admitted, BindingSource};
use crate::object_author::tests::member_key_history::{self as keyed, BEN, CAROL};
use crate::replay::tests as history;
use coven_database::{DropReason, EntryOutcome, EntryTarget};
use coven_format::sealed_key::SealedKey;
use coven_format::store_log::MemberPublicKeys;
use coven_foundation::id_source::MemberId;
use serde_json::Value;
use std::collections::BTreeSet;

fn account(email: &str) -> InviteAccess {
    InviteAccess::ProviderAccount {
        email: email.into(),
    }
}

/// Ana's recovery invite for `target`, sharing `email`.
async fn recovery(d: &mut Device, target: MemberId, email: &str) -> (OperationId, Invite) {
    let id = begin(
        d,
        Command::Invite(
            InviteTarget::ReplaceMemberKeys { member: target },
            account(email),
        ),
    )
    .await;
    let Output::Invite(invite) = finish(d, id).await else {
        panic!()
    };
    (id, invite)
}

/// Carol's new installation, holding `keys` and a copy of Ana's keyring as
/// its initial-key wait would have opened it.
async fn installation(storage: &Arc<MemoryStorage>, ana: &Device, keys: &MemberIdentity) -> Device {
    let joined = device(
        Arc::new(MemoryStorage::for_recipient(storage, "cat@example.com").unwrap()),
        9,
        keys.clone(),
        store(1),
    )
    .await;
    joined
        .custody
        .persist(&ana.custody.read().unwrap().unwrap())
        .unwrap();
    joined
}

/// The sealing keys of every stored copy of `audience`'s key `id` to `to`.
async fn sealings(
    storage: &dyn Storage,
    audience: &Audience,
    id: KeyId,
    to: &MemberId,
) -> BTreeSet<coven_crypto::SealingKey> {
    let mut found = BTreeSet::new();
    for object in storage.list(&ObjectPrefix::keys(store(1))).await.unwrap() {
        let bytes = storage.read(&object.path).await.unwrap();
        let prefix = SealedKey::decode(&bytes).unwrap().prefix().clone();
        if &prefix.audience == audience && prefix.key == id && prefix.member == *to {
            found.insert(prefix.sealing);
        }
    }
    found
}

/// The request for `invite`, read and approved by Ana's open operation.
async fn approve(a: &mut Device, operation: OperationId) {
    assert!(matches!(
        step(a, operation).await.unwrap(),
        Progress::Advanced
    ));
    let requests = a.sync.current_join_requests().await.unwrap();
    assert_eq!(
        begin(a, Command::Approve(requests[0].clone())).await,
        operation
    );
}

/// Carol (member 3) reads Garden with Ana and loses every device. Ana's
/// recovery invite fixes Carol's id; approval shares the store and Garden
/// keys with the requested credential before binding exactly its pair.
/// Carol keeps her role, access, circle and device ids. Her new
/// installation is admitted by Ana's binding alone, while her old
/// installation and an old restore code report the replacement.
#[tokio::test]
async fn a_recovery_invite_binds_new_keys_to_the_existing_member() {
    let storage = google();
    let [mut a, b, mut c] = accounts(storage.clone()).await;
    let carol = c.member.member();
    let create = begin(&mut a, Command::CreateCircle("Garden".into())).await;
    let Output::CircleId(garden) = finish(&mut a, create).await else {
        panic!()
    };
    let add = begin(&mut a, Command::AddCircleMember(garden, carol)).await;
    finish(&mut a, add).await;
    let before = a.log().await.replay.state;
    let (operation, invite) = recovery(&mut a, carol, "cat@example.com").await;
    assert_eq!(invite.member, carol);
    let keys = joiner(&invite, 9);
    assert_eq!(keys.member(), carol);
    let joining = MemoryStorage::for_recipient(&storage, "cat@example.com").unwrap();
    request(&joining, &invite, &keys, "Carol's new phone").await;
    approve(&mut a, operation).await;
    assert!(matches!(finish(&mut a, operation).await, Output::Unit));
    let log = a.log().await;
    let after = &log.replay.state;
    let (old, new) = (&before.members[&carol], &after.members[&carol]);
    assert_eq!(new.keys, public(&keys));
    assert_eq!((new.role, &new.access), (old.role, &old.access));
    assert!(after.reads_circle(garden, &carol));
    assert_eq!(before.devices, after.devices);
    // Every prerequisite names the requested credential; the old copies stay.
    let ring = a.custody.read().unwrap().unwrap();
    let held: Vec<_> = ring
        .store_key_ids()
        .map(|id| (Audience::Store, id))
        .chain(
            ring.circle_key_ids(garden)
                .map(|id| (Audience::Circle(garden), id)),
        )
        .collect();
    for (audience, id) in held {
        assert_eq!(
            sealings(&joining, &audience, id, &carol).await,
            [public(&c.member).sealing, public(&keys).sealing].into()
        );
    }
    assert!(a.sync.current_join_requests().await.unwrap().is_empty());
    // Only the approving writer's binding admits the new installation.
    let approver = a.device().await;
    let mut joined = installation(&storage, &a, &keys).await;
    assert!(!joined
        .sync
        .bootstrap_member(BindingSource::Join {
            approver: b.device().await
        })
        .await
        .unwrap());
    assert!(joined
        .sync
        .bootstrap_member(BindingSource::Join { approver })
        .await
        .unwrap());
    joined
        .sync
        .load_new_device("Carol's new phone", None)
        .await
        .unwrap();
    let state = joined.log().await.replay.state;
    let devices: Vec<_> = state
        .devices
        .values()
        .filter(|d| d.member == carol && d.active())
        .collect();
    assert_eq!(devices.len(), 2);
    let replaced = c.sync.sync_store_log().await;
    assert!(
        matches!(replaced, Err(SyncError::MemberKeysReplaced { member }) if member == carol),
        "{replaced:?}"
    );
    // A restore code holding Carol's old pair cannot bind it back.
    let mut stale = installation(&storage, &a, &c.member).await;
    let restored = stale.sync.bootstrap_member(BindingSource::Restore).await;
    assert!(
        matches!(restored, Err(SyncError::MemberKeysReplaced { member }) if member == carol),
        "{restored:?}"
    );
}

/// Who publishes a replacement of Carol while Ana's approval is in flight.
#[derive(Clone, Copy, Debug)]
enum Racer {
    /// Ben, another admin, earlier by timestamp, with the requested pair
    /// or another fresh one.
    Admin { equal: bool },
    /// Carol herself, from her old installation.
    Carol,
}

/// Ana's approval is fixed but its publication interrupted; then a
/// concurrent replacement of Carol lands. Another admin's earlier one wins,
/// even with exactly the requested pair: the approval is reported rejected,
/// no new entry overrides the winner, and the invite takes back only its own
/// grant, so Carol's account stays shared. Carol's self-replacement loses to
/// Ana's approval whatever the timestamps (§9).
#[tokio::test]
async fn a_concurrent_replacement_decides_the_approval_without_a_new_attempt() {
    for racer in [
        Racer::Admin { equal: false },
        Racer::Admin { equal: true },
        Racer::Carol,
    ] {
        let storage = google();
        let [mut a, mut b, mut c] = accounts(storage.clone()).await;
        let carol = c.member.member();
        // Ana's clock runs ahead, so Ben's concurrent entry is earlier.
        a.clock.set(UNIX_EPOCH + Duration::from_secs(1_000_000));
        let (operation, invite) = recovery(&mut a, carol, "cat@example.com").await;
        let keys = joiner(&invite, 9);
        let joining = MemoryStorage::for_recipient(&storage, "cat@example.com").unwrap();
        request(&joining, &invite, &keys, "Carol's new phone").await;
        approve(&mut a, operation).await;
        let local = a.db.local_store_log().await.unwrap();
        let next = local
            .log
            .entries
            .iter()
            .filter(|e| e.entry.position.device == local.device)
            .count() as u64
            + 1;
        a.interrupt_entry(next).await;
        loop {
            match step(&mut a, operation).await {
                Ok(Progress::Advanced) => (),
                Err(SyncError::Storage(_)) => break,
                Ok(Progress::Waiting(reason)) => panic!("{racer:?}: waits {reason:?}"),
                Ok(Progress::Reply(reply)) => panic!("{racer:?}: replies {:?}", reply.err()),
                Ok(_) => panic!("{racer:?}: finished"),
                Err(error) => panic!("{racer:?}: {error:?}"),
            }
        }
        a.restore_storage();
        let fresh = joiner(&invite, 8);
        let (racer_device, pair) = match racer {
            Racer::Admin { equal } => (&mut b, if equal { public(&keys) } else { public(&fresh) }),
            Racer::Carol => (&mut c, public(&fresh)),
        };
        let published = racer_device
            .sync
            .make_and_upload_entry(StoreChange::ReplaceMemberKeys {
                member: carol,
                keys: pair,
            })
            .await;
        // Carol's old installation still holds the pair her own kept
        // replacement retired; installing the new one is custody work (E9).
        let raced = match (racer, published) {
            (_, Ok(id)) => id,
            (Racer::Carol, Err(SyncError::MemberKeysReplaced { .. })) => {
                let local = c.db.local_store_log().await.unwrap();
                local
                    .log
                    .entries
                    .iter()
                    .rev()
                    .find(|e| e.entry.position.device == local.device)
                    .unwrap()
                    .entry
                    .position
            }
            (_, Err(error)) => panic!("{racer:?}: {error:?}"),
        };
        // The approval's reply, if any: only a failure is reported early.
        let approval = loop {
            match step(&mut a, operation).await.unwrap() {
                Progress::Advanced => (),
                Progress::Reply(result) => break result.err(),
                Progress::Finished(Output::Unit) => break None,
                _ => panic!("{racer:?}: unexpected progress"),
            }
        };
        let log = a.log().await;
        let ours = log
            .entries
            .iter()
            .find(|e| e.entry.position.device == local.device && e.entry.position.number == next)
            .unwrap()
            .entry
            .position;
        let mut joined = installation(&storage, &a, &keys).await;
        let admission = joined
            .sync
            .bootstrap_member(BindingSource::Join {
                approver: local.device,
            })
            .await;
        match racer {
            Racer::Admin { .. } => {
                assert!(
                    matches!(&approval, Some(SyncError::Rejected(DropReason::BeatenBy(id))) if *id == raced),
                    "{racer:?}: {approval:?}"
                );
                finish(&mut a, operation).await;
                assert!(
                    matches!(&admission, Err(SyncError::Rejected(DropReason::BeatenBy(id))) if *id == raced),
                    "{racer:?}: {admission:?}"
                );
                assert_eq!(log.replay.state.members[&carol].keys, pair);
            }
            Racer::Carol => {
                assert!(approval.is_none(), "{approval:?}");
                assert_eq!(
                    log.replay.entries[&raced],
                    EntryOutcome::Dropped(DropReason::BeatenBy(ours))
                );
                assert!(admission.unwrap(), "{racer:?}");
            }
        }
        // Ana published no second attempt, and her request is gone.
        let log = a.log().await;
        assert_eq!(
            log.entries
                .iter()
                .filter(|e| e.entry.position.device == local.device)
                .count() as u64,
            next
        );
        assert!(matches!(
            storage.read(&ObjectPath::join_request(store(1), invite.id)).await,
            Err(error) if error.failure() == StorageFailure::NotFound
        ));
        drive_access(&mut a).await;
        assert!(joining.list(&ObjectPrefix::all(store(1))).await.is_ok());
        assert!(a
            .db
            .operations()
            .await
            .unwrap()
            .iter()
            .all(|r| r.id != operation));
    }
}

/// The admin fixes an existing active target that is not herself, and an
/// account invite shares only that member's recorded account. Approval
/// refuses a request whose pair reuses bound keys. Declining takes back no
/// access the member already holds.
#[tokio::test]
async fn recovery_targets_and_requests_are_checked_before_anything_is_bound() {
    let storage = google();
    let [mut a, b, mut c] = accounts(storage.clone()).await;
    let carol = c.member.member();
    let invite = |target| {
        Command::Invite(
            InviteTarget::ReplaceMemberKeys { member: target },
            account("cat@example.com"),
        )
    };
    let unknown = MemberId(uuid::Uuid::from_u128(77));
    for (command, expected) in [
        (
            invite(unknown),
            DropReason::TargetGone(EntryTarget::Member(unknown)),
        ),
        (invite(a.member.member()), DropReason::NotAllowed),
    ] {
        let result = a.sync.begin_operation_call(command).await;
        assert!(
            matches!(&result, Err(SyncError::Rejected(reason)) if *reason == expected),
            "{:?}",
            result.err()
        );
    }
    // Carol is no admin; Ana cannot share another account for Carol.
    assert!(matches!(
        c.sync.begin_operation_call(invite(b.member.member())).await,
        Err(SyncError::Rejected(DropReason::NotAllowed))
    ));
    let other = a
        .sync
        .begin_operation_call(Command::Invite(
            InviteTarget::ReplaceMemberKeys { member: carol },
            account("new@example.com"),
        ))
        .await;
    assert!(
        matches!(&other, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::InvalidConfiguration),
        "{:?}",
        other.err()
    );
    assert!(a.db.operations().await.unwrap().is_empty());
    // A request reusing Ben's pair is refused at approval.
    let (operation, invite) = recovery(&mut a, carol, "cat@example.com").await;
    let joining = MemoryStorage::for_recipient(&storage, "cat@example.com").unwrap();
    let reused = MemberIdentity::new(carol, b.member.keys().clone());
    request(&joining, &invite, &reused, "Carol's new phone").await;
    step(&mut a, operation).await.unwrap();
    let requests = a.sync.current_join_requests().await.unwrap();
    assert!(matches!(
        a.sync
            .begin_operation_call(Command::Approve(requests[0].clone()))
            .await,
        Err(SyncError::Rejected(DropReason::KeysAlreadyUsed))
    ));
    // Declining keeps Carol's own recorded account shared.
    begin(&mut a, Command::Decline(requests[0].clone())).await;
    finish(&mut a, operation).await;
    drive_access(&mut a).await;
    assert!(joining.list(&ObjectPrefix::all(store(1))).await.is_ok());
    let state = a.log().await.replay.state;
    assert_eq!(state.members[&carol].keys, public(&c.member));
    c.sync().await;
}

/// What a join or restore decision projects to in the keyed model.
#[derive(Debug, PartialEq, Eq)]
enum Decision {
    Admitted,
    Waiting,
    Rejected,
    Replaced,
    Removed,
}

fn decision(result: Result<bool, SyncError>) -> Decision {
    match result {
        Ok(true) => Decision::Admitted,
        Ok(false) => Decision::Waiting,
        Err(SyncError::Rejected(_)) => Decision::Rejected,
        Err(SyncError::MemberKeysReplaced { .. }) => Decision::Replaced,
        Err(SyncError::Removed) => Decision::Removed,
        Err(error) => panic!("{error:?}"),
    }
}

/// Join and restore decisions on the production replay of each generated
/// history against `Keyed.resolve`: every addition or replacement in the
/// history is treated as an invite's approval from its writer's device, and
/// every fixture pair as a restore code for each member. A join is admitted
/// exactly when that writer's binding of the id to the pair is kept and still
/// current; a dropped one is rejected even when another writer bound the same
/// pair. A restore needs the id's current pair.
#[tokio::test]
#[ignore = "requires COVEN_MEMBER_KEY_LEAN, supplied by scripts/check.sh"]
async fn lean_differential_join_admission() {
    let keyed::Run {
        cases,
        mut numbers,
        results,
        ..
    } = keyed::run();
    let mut seen = BTreeSet::new();
    for ((case, result), numbers) in cases.iter().zip(&results).zip(&mut numbers) {
        let log = keyed::receive(case, &case.received).log;
        let set = |field: &str| -> BTreeSet<usize> {
            serde_json::from_value(result[field].clone()).unwrap()
        };
        let (kept, dropped) = (set("kept"), set("dropped"));
        let active: BTreeSet<u64> =
            serde_json::from_value::<Vec<[u64; 2]>>(result["members"].clone())
                .unwrap()
                .into_iter()
                .map(|[m, _]| m)
                .collect();
        let current: Vec<[usize; 3]> = serde_json::from_value(result["keys"].clone()).unwrap();
        let mut pair = |keys: &MemberPublicKeys| {
            [numbers.signing(keys.signing), numbers.sealing(keys.sealing)]
        };
        let bound = |m: u64, keys: [usize; 2]| {
            if !active.contains(&m) {
                return Decision::Removed;
            }
            if current.contains(&[m as usize, keys[0], keys[1]]) {
                Decision::Admitted
            } else {
                Decision::Replaced
            }
        };
        let h = &case.history;
        let binding = |i: usize| match &h.entries[i].change {
            StoreChange::AddMember { member, keys, .. }
            | StoreChange::ReplaceMemberKeys { member, keys } => Some((*member, *keys)),
            _ => None,
        };
        for i in 0..h.entries.len() {
            let Some((member, keys)) = binding(i) else {
                continue;
            };
            let approver = h.entries[i].position.device;
            let matching: Vec<usize> = (0..h.entries.len())
                .filter(|&j| {
                    h.entries[j].position.device == approver && binding(j) == Some((member, keys))
                })
                .collect();
            let m = Numbers::member(&member);
            let numbered = pair(&keys);
            // A refused signature is the entry's own pending refusal, not
            // a replay fate (§19.1).
            let signed = |j: &usize| result["signatures"][*j]["current"].as_bool().unwrap();
            let expected = if matching.iter().any(|j| kept.contains(j)) {
                bound(m, numbered)
            } else if matching.iter().any(|j| dropped.contains(j) && signed(j)) {
                Decision::Rejected
            } else {
                Decision::Waiting
            };
            let actual = decision(admitted(
                &log,
                &member,
                &keys,
                BindingSource::Join { approver },
            ));
            assert_eq!(actual, expected, "approval {i} of {}", case_text(result));
            seen.insert(format!("join {expected:?}"));
        }
        for m in [0, BEN, CAROL] {
            let member = history::member(m);
            for identity in keyed::identities() {
                let keys = identity.keys().public_keys();
                let expected = if active.contains(&(m as u64)) {
                    bound(m as u64, pair(&keys))
                } else {
                    Decision::Waiting
                };
                let actual = decision(admitted(&log, &member, &keys, BindingSource::Restore));
                assert_eq!(actual, expected, "restore of {m} in {}", case_text(result));
                seen.insert(format!("restore {expected:?}"));
            }
        }
    }
    for outcome in [
        "join Admitted",
        "join Rejected",
        "join Replaced",
        "join Waiting",
        "join Removed",
        "restore Admitted",
        "restore Replaced",
    ] {
        assert!(seen.contains(outcome), "{outcome}: {seen:?}");
    }
}

fn case_text(result: &Value) -> String {
    result.to_string()
}
