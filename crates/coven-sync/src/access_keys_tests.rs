use super::*;
use coven_storage::StorageFailure;

fn read(key: &str, reply: Result<KeyPresence, StorageError>) -> Option<KeyObservation> {
    Some(KeyObservation {
        access_key_id: key.into(),
        reply,
    })
}

#[test]
fn deletion_clears_only_with_a_read_of_that_key_absent() {
    assert!(verify_deletion("old", Ok(()), read("old", Ok(KeyPresence::Absent))).is_ok());
    assert!(matches!(
        verify_deletion(
            "old",
            Err(StorageFailure::PermissionDenied.into()),
            read("old", Ok(KeyPresence::Absent))
        ),
        Err(DeleteAccessKeyError::DeleteFailed(_))
    ));
    assert!(matches!(
        verify_deletion("old", Ok(()), None),
        Err(DeleteAccessKeyError::ObservationMissing)
    ));
    assert!(matches!(
        verify_deletion("old", Ok(()), read("other", Ok(KeyPresence::Absent))),
        Err(DeleteAccessKeyError::WrongKey)
    ));
    assert!(matches!(
        verify_deletion("old", Ok(()), read("old", Ok(KeyPresence::Present))),
        Err(DeleteAccessKeyError::StillPresent)
    ));
    assert!(matches!(
        verify_deletion(
            "old",
            Ok(()),
            read("old", Err(StorageFailure::Network.into()))
        ),
        Err(DeleteAccessKeyError::ObservationFailed(_))
    ));
}

use crate::replay::tests::{device as add_device, keys, member, remove, History};
use crate::replay::ReplayBatch;
use coven_database::StoreLog;
use coven_format::store_log::MemberRole;
use serde_json::{json, Value};

fn s3(id: &str) -> MemberAccess {
    MemberAccess::S3AccessKey {
        access_key_id: id.into(),
        principal: coven_format::S3Principal::Console,
    }
}

fn add(n: u8, role: MemberRole, key: &str) -> StoreChange {
    StoreChange::AddMember {
        keys: keys(n),
        role,
        access: s3(key),
    }
}

fn set(n: u8, key: &str) -> StoreChange {
    StoreChange::SetAccess {
        member: member(n),
        access: s3(key),
    }
}

/// Ana creates the store; Ben and Carol join with their own keys and devices.
fn household(ben: MemberRole) -> History {
    let mut h = History::new();
    h.all(0, 0, add(1, ben, "ben-1"));
    h.all(1, 1, add_device(1));
    h.all(0, 0, add(2, MemberRole::Member, "carol-1"));
    h.all(2, 2, add_device(2));
    h
}

enum Event {
    Entries(Vec<usize>),
    Confirm(&'static str),
    Deletion {
        key: &'static str,
        deleted: bool,
        read: Option<(&'static str, &'static str)>,
    },
}

impl Event {
    fn json(&self) -> Value {
        match self {
            Self::Entries(ids) => json!({ "entries": ids }),
            Self::Confirm(key) => json!({ "confirm": key }),
            Self::Deletion { key, deleted, read } => json!({ "deletion": {
                "key": key, "deleted": deleted,
                "observation": read.map(|(key, reply)| json!({ "key": key, "reply": reply })),
            }}),
        }
    }
}

fn reply(error: &DeleteAccessKeyError) -> &'static str {
    match error {
        DeleteAccessKeyError::DeleteFailed(_) => "deleteFailed",
        DeleteAccessKeyError::ObservationMissing => "observationMissing",
        DeleteAccessKeyError::WrongKey => "wrongKey",
        DeleteAccessKeyError::StillPresent => "stillPresent",
        DeleteAccessKeyError::ObservationFailed(_) => "observationFailed",
    }
}

/// Apply each event through the production obligation computation, the
/// database journal and the deletion check, reporting the pending key ids.
async fn rust_journal(h: &History, stored: &[u64], events: &[Event]) -> Vec<Value> {
    let d = crate::store_log_sync::tests::device(
        std::sync::Arc::new(
            coven_storage::test_utils::MemoryStorage::builder()
                .build()
                .unwrap(),
        ),
        9,
        crate::store_log_sync::tests::member(9),
        crate::store_log_sync::tests::store(1),
    )
    .await;
    let publications = crate::tests::publication_times(h, stored);
    let mut log = StoreLog::default();
    let mut results = Vec::new();
    for event in events {
        let outcome = match event {
            Event::Entries(ids) => {
                let mut batch = ReplayBatch::new(&log);
                let mut ids = ids.clone();
                ids.sort();
                for i in ids {
                    if !batch.contains(h.entries[i].position) {
                        batch.push_checked(h.entries[i].clone(), &publications);
                    }
                }
                if let Some((incoming, replay)) = batch.finish() {
                    let found = deletions(&log.entries, &incoming, &replay).unwrap();
                    d.db.record_access_key_deletions::<crate::PendingRecord>(None, found)
                        .await
                        .unwrap();
                    log.entries.extend(incoming);
                    log.replay = replay;
                }
                Ok(())
            }
            Event::Confirm(key) => {
                d.db.clear_access_key((*key).into()).await.unwrap();
                Ok(())
            }
            Event::Deletion { key, deleted, read } => {
                let deleted = if *deleted {
                    Ok(())
                } else {
                    Err(StorageFailure::PermissionDenied.into())
                };
                let observation = read.map(|(id, reply)| KeyObservation {
                    access_key_id: id.into(),
                    reply: match reply {
                        "absent" => Ok(KeyPresence::Absent),
                        "present" => Ok(KeyPresence::Present),
                        _ => Err(StorageFailure::Network.into()),
                    },
                });
                let verdict = verify_deletion(key, deleted, observation);
                if verdict.is_ok() {
                    d.db.clear_access_key((*key).into()).await.unwrap();
                }
                verdict
            }
        };
        let pending: Vec<String> = crate::store_log_sync::tests::access_keys(&d.db)
            .await
            .into_iter()
            .map(|key| key.access_key_id)
            .collect();
        results.push(json!({
            "pending": pending,
            "reply": outcome.as_ref().map_or_else(reply, |()| "ok"),
        }));
    }
    results
}

/// Lean compares key ids; recorded accesses carry them in place of the
/// serialized access used by the replay runners.
fn lean_input(h: &History, stored: &[u64], events: &[Event]) -> Value {
    let mut input = crate::tests::input(h, stored, &[]);
    let object = input.as_object_mut().unwrap();
    object.remove("received");
    for entry in object["entries"].as_array_mut().unwrap() {
        let action = entry["action"].as_object_mut().unwrap();
        if let Some(access) = action.get("access") {
            let access: MemberAccess = serde_json::from_str(access.as_str().unwrap()).unwrap();
            let MemberAccess::S3AccessKey { access_key_id, .. } = access else {
                panic!("S3 histories record keys")
            };
            action.insert("access".into(), json!(access_key_id));
        }
    }
    object.insert(
        "events".into(),
        events.iter().map(Event::json).collect::<Vec<_>>().into(),
    );
    input
}

fn clearing_events(keys: [&'static str; 2]) -> Vec<Event> {
    let [first, second] = keys;
    vec![
        Event::Confirm(first),
        Event::Deletion {
            key: second,
            deleted: false,
            read: Some((second, "absent")),
        },
        Event::Deletion {
            key: second,
            deleted: true,
            read: None,
        },
        Event::Deletion {
            key: second,
            deleted: true,
            read: Some((first, "absent")),
        },
        Event::Deletion {
            key: second,
            deleted: true,
            read: Some((second, "present")),
        },
        Event::Deletion {
            key: second,
            deleted: true,
            read: Some((second, "failed")),
        },
        Event::Deletion {
            key: second,
            deleted: true,
            read: Some((second, "absent")),
        },
    ]
}

fn cases() -> Vec<(History, Vec<u64>, Vec<Event>)> {
    let mut cases = Vec::new();
    // Two admins, or an admin and Carol, replace Carol's key concurrently;
    // both receipt orders, then her removal and every clearing path.
    for carol_self in [false, true] {
        for second_first in [false, true] {
            let mut h = household(MemberRole::Admin);
            h.push(0, 0, &[0, 1, 2, 3, 4], set(2, "carol-ana"));
            if carol_self {
                h.push(2, 2, &[0, 1, 2, 3, 4], set(2, "carol-self"));
            } else {
                h.push(1, 1, &[0, 1, 2, 3, 4], set(2, "carol-ben"));
            }
            h.all(0, 0, remove(2));
            let (first, second) = if second_first { (6, 5) } else { (5, 6) };
            let other = if carol_self {
                "carol-self"
            } else {
                "carol-ben"
            };
            let mut events = vec![
                Event::Entries(vec![0, 1, 2, 3, 4, first]),
                Event::Entries(vec![second]),
                Event::Entries(vec![7]),
            ];
            events.extend(clearing_events(["carol-1", other]));
            events.push(Event::Confirm("carol-ana"));
            cases.push((h, vec![1; 8], events));
        }
    }
    // A member cannot record another member's key; the key is still listed.
    let mut h = household(MemberRole::Member);
    h.all(1, 1, set(2, "carol-by-ben"));
    cases.push((h, vec![1; 6], vec![Event::Entries((0..6).collect())]));
    // Removal first, then Carol's replacement landing after its thirty days.
    let mut h = household(MemberRole::Admin);
    h.push(0, 0, &[0, 1, 2, 3, 4], remove(2));
    h.push(2, 2, &[0, 1, 2, 3, 4], set(2, "carol-late"));
    cases.push((
        h,
        vec![1, 1, 1, 1, 1, 2, 40],
        vec![
            Event::Entries((0..6).collect()),
            Event::Confirm("carol-1"),
            Event::Entries(vec![6]),
        ],
    ));
    // Ana and Ben remove each other; replay returning Ana keeps her key listed.
    let mut h = household(MemberRole::Admin);
    h.push(0, 0, &[0, 1, 2, 3, 4], remove(1));
    h.push(1, 1, &[0, 1, 2, 3, 4], remove(0));
    cases.push((
        h,
        vec![1; 7],
        vec![
            Event::Entries(vec![0, 1, 2, 3, 4, 6]),
            Event::Entries(vec![5]),
        ],
    ));
    // Recording the same key again is no replacement; a new key is.
    let mut h = household(MemberRole::Admin);
    h.all(2, 2, set(2, "carol-1"));
    h.all(2, 2, set(2, "carol-2"));
    cases.push((
        h,
        vec![1; 7],
        vec![
            Event::Entries((0..6).collect()),
            Event::Entries(vec![6]),
            Event::Confirm("carol-1"),
            Event::Confirm("carol-1"),
        ],
    ));
    cases
}

#[tokio::test]
async fn replacements_and_removals_list_every_key_until_cleared() {
    let cases = cases();
    let (h, stored, events) = &cases[0];
    let results = rust_journal(h, stored, events).await;
    let pending = |i: usize| results[i]["pending"].clone();
    // The first replacement retires Carol's own key; the losing one is
    // listed once the winner arrives; removal adds the winner.
    assert_eq!(pending(0), json!(["carol-1"]));
    assert_eq!(pending(1), json!(["carol-1", "carol-ben"]));
    assert_eq!(pending(2), json!(["carol-1", "carol-ana", "carol-ben"]));
    assert_eq!(pending(3), json!(["carol-ana", "carol-ben"]));
    for failed in &results[4..9] {
        assert_eq!(failed["pending"], json!(["carol-ana", "carol-ben"]));
        assert_ne!(failed["reply"], json!("ok"));
    }
    assert_eq!(pending(9), json!(["carol-ana"]));
    assert_eq!(pending(10), json!([]));
}

#[tokio::test]
#[ignore = "requires COVEN_ACCESS_KEY_LEAN; scripts/check.sh supplies it"]
async fn lean_differential_access_keys() {
    let mut inputs = Vec::new();
    let mut rust = Vec::new();
    for (h, stored, events) in cases() {
        inputs.push(lean_input(&h, &stored, &events));
        rust.push(rust_journal(&h, &stored, &events).await);
    }
    let output = crate::tests::run_lean("COVEN_ACCESS_KEY_LEAN", &json!({ "histories": inputs }));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lean: Vec<Vec<Value>> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(lean, rust);
}
