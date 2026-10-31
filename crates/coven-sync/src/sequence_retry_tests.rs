//! Compare the operation owner's evidence and restart decisions with Waiting.live.
use super::*;
use crate::sequence_sync::tests::{Device, Group};
use coven_database::{NewOperation, ProviderAccessChange};
use coven_foundation::clock::Clock;
use serde_json::{json, Value};
use std::time::{Duration, UNIX_EPOCH};

fn operation(kind: &str) -> NewOperation {
    NewOperation {
        kind: kind.into(),
        data: Vec::new(),
        started_by: "evidence comparison".into(),
        pauses_writes: false,
    }
}

async fn commit(device: &Device, prerequisite: u64) {
    let account = format!("prerequisite{prerequisite}@example.com");
    let intended = match device.database().provider_access(account.clone()).await.unwrap() {
        Some(state) => !state.intended,
        None => true,
    };
    let id = device
        .database()
        .start_operation_with_access::<PendingRecord>(
            operation("evidence-input"),
            vec![ProviderAccessChange {
                account,
                intended,
                initially_granted: true,
                needs_invitation: false,
                owner: true,
                operation: operation("provider-access"),
            }],
        )
        .await
        .unwrap();
    device.database().finish_operation(id).await.unwrap();
}

async fn history(actions: &[Value]) -> Value {
    let group = Group::new();
    group.clock.set(UNIX_EPOCH + Duration::from_secs(1_000_000));
    let mut device = group.device(1).await;
    let id = device.database().start_operation(operation("evidence-wait")).await.unwrap();
    let mut observed = Vec::new();
    for action in actions {
        match action["op"].as_str().unwrap() {
            "advance" => group
                .clock
                .set(group.clock.now() + Duration::from_secs(action["seconds"].as_u64().unwrap())),
            "back" => group
                .clock
                .set(group.clock.now() - Duration::from_secs(action["seconds"].as_u64().unwrap())),
            "commit" => commit(&device, action["prereq"].as_u64().unwrap()).await,
            "reopen" => device = group.reopen(device).await,
            op @ ("attempt" | "cut") => {
                let record = device.database().operation(id).await.unwrap().unwrap();
                let due = device.sync.operation_ready(&record, &group.ring).await.unwrap();
                if due {
                    let scope = device.sync.waits.begin_attempt(id);
                    for prerequisite in action["raced"].as_array().unwrap() {
                        commit(&device, prerequisite.as_u64().unwrap()).await;
                    }
                    if op == "attempt" {
                        let prerequisite = action["prereq"].as_u64().unwrap();
                        // Its own write to the very prerequisite it waits for
                        // must not wake the wait which this attempt records.
                        scope.run(commit(&device, prerequisite)).await;
                        let error = SyncError::Pending(Box::new(PendingRecord::new(
                            PendingSubject::Operation {
                                id,
                                kind: crate::OperationKind::RetainFiles,
                            },
                            PendingReason::PendingOwner {
                                account: format!("prerequisite{prerequisite}@example.com"),
                            },
                        )));
                        device
                            .sync
                            .record_operation_error(
                                id,
                                crate::OperationKind::RetainFiles,
                                crate::sequence_sync::OperationFailure::Returned(&error),
                                &group.ring,
                            )
                            .await
                            .unwrap();
                    }
                }
                if op == "cut" {
                    device = group.reopen(device).await;
                }
                let failures = device
                    .database()
                    .retry_wait(RetrySubject::Operation(id))
                    .await
                    .unwrap()
                    .map_or(0, |wait| wait.failures);
                observed.push(json!({"due":due,"failures":failures}));
            }
            op => panic!("unknown evidence event {op}"),
        }
    }
    device.database().close().await.unwrap();
    json!(observed)
}

#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_waiting_evidence() {
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for prerequisite in [0, 1] {
        let unrelated = prerequisite + 1;
        let attempt = |raced: Vec<u64>| json!({"op":"attempt","prereq":prerequisite,"raced":raced});
        let mut actions = vec![
            json!({"op":"cut","raced":[]}),
            attempt(vec![]),
            attempt(vec![]),
            json!({"op":"commit","prereq":unrelated}),
            attempt(vec![]),
            json!({"op":"commit","prereq":prerequisite}),
            attempt(vec![unrelated]),
            attempt(vec![]),
            json!({"op":"commit","prereq":prerequisite}),
            attempt(vec![prerequisite]),
            attempt(vec![]),
            attempt(vec![]),
            json!({"op":"back","seconds":100}),
            json!({"op":"reopen"}),
            attempt(vec![]),
            json!({"op":"advance","seconds":3}),
            attempt(vec![]),
            json!({"op":"advance","seconds":300}),
            json!({"op":"cut","raced":[prerequisite]}),
            attempt(vec![]),
        ];
        for _ in 0..10 {
            actions.extend([json!({"op":"advance","seconds":300}), attempt(vec![])]);
        }
        // A restart loses observed evidence, keeps the exponent, and a
        // later commit can wake the reopened wait before its full deadline.
        actions.extend([
            json!({"op":"commit","prereq":prerequisite}),
            json!({"op":"reopen"}),
            attempt(vec![]),
            json!({"op":"commit","prereq":prerequisite}),
            attempt(vec![]),
        ]);
        actual.push(history(&actions).await);
        inputs.push(json!({"check":"evidence","actions":actions}));
    }
    assert_eq!(actual, crate::tests::io_runner(&inputs), "{inputs:?}");
}
