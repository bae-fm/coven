use super::*;
use crate::sequence_sync::tests::*;
use coven_storage::{test_utils::Faults, Storage};
use std::time::Duration;

async fn parent(ana: &Device) -> Option<OperationRecord> {
    ana.database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == REPLACE_ACCESS_KEY)
}

#[tokio::test]
async fn a_lost_delivery_reply_retries_in_a_normal_pass_after_backoff() {
    delivery_retry(false).await;
}

async fn delivery_retry(compare: bool) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let admin = Arc::new(group.storage.key_admin());
    ana.storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(
        matches!(ana.sync.replace_member_access_key(ben.identity.member(), admin.clone()).await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network)
    );
    let record = parent(&ana).await.unwrap();
    let work: ReplaceKeyWork = serde_json::from_slice(&record.data).unwrap();
    let Step::Delivering { key, delivery } = work.step else {
        panic!("delivery waits");
    };
    let path = ObjectPath::access_key(group.store, &work.member, work.attempt);
    let wait = ana
        .database()
        .retry_wait(coven_database::RetrySubject::Operation(record.id))
        .await
        .unwrap()
        .unwrap();
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.pass().await.unwrap();
    assert_eq!(parent(&ana).await.unwrap().last_step, record.last_step);
    if compare {
        compare_state(
            &ana,
            record.id,
            "delivering",
            false,
            true,
            None,
            Some("network"),
        )
        .await;
    }
    group.clock.set(wait.not_before);
    ana.sync.pass().await.unwrap();
    if compare {
        compare_state(
            &ana,
            record.id,
            "delivering",
            true,
            true,
            None,
            Some("network"),
        )
        .await;
    }
    assert!(
        parent(&ana).await.is_none(),
        "a due parent must resume in a normal pass"
    );
    assert_eq!(group.storage.read(&path).await.unwrap(), delivery);
    assert_eq!(admin.calls(), ["mint", "probe"]);
    ben.sync.pass().await.unwrap();
    assert_eq!(
        ben.sync
            .delivered_access_key(Some("ben"))
            .await
            .unwrap()
            .unwrap()
            .access_key_id,
        key.credentials.access_key_id
    );
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn dropping_the_waiter_keeps_the_provider_request_owned() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(
            ObjectPrefix::access_keys(group.store, &ben.identity.member()),
            started,
            held,
        )
        .await;
    let owner = ana.sync.clone();
    let member = ben.identity.member();
    let admin = Arc::new(group.storage.key_admin());
    let waiter = tokio::spawn(async move { owner.replace_member_access_key(member, admin).await });
    observed.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(
        resume.send(()).is_ok(),
        "dropping the caller must not drop the provider request"
    );
    let session = tokio::time::timeout(Duration::from_secs(5), ana.sync.session.lock())
        .await
        .unwrap();
    assert!(parent(&ana).await.is_none());
    drop(session);
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_resumed_claim_delivers_its_rejection_to_the_replacement_parent() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    let admin = Arc::new(group.storage.key_admin());
    let next = ana.database().applied_slot().await.unwrap() + 1;
    ana.storage
        .set_faults(Faults {
            create_failures: [(
                ObjectPath::slot(group.store, next.try_into().unwrap()),
                (StorageFailure::Network, 500),
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        ana.sync
            .replace_member_access_key(ben.identity.member(), admin.clone())
            .await,
        Err(SyncError::Storage(_))
    ));
    let record = parent(&ana).await.unwrap();
    let work: ReplaceKeyWork = serde_json::from_slice(&record.data).unwrap();
    assert!(matches!(work.step, Step::Recording { .. }));
    let path = ObjectPath::access_key(group.store, &work.member, work.attempt);
    let wait = ana
        .database()
        .retry_wait(coven_database::RetrySubject::Operation(record.id))
        .await
        .unwrap()
        .unwrap();
    ana.storage.set_faults(Faults::none()).await;
    carol
        .sync
        .administer(StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    group.clock.set(wait.not_before);
    let result = ana.sync.pass().await;
    result.unwrap();
    assert!(
        parent(&ana).await.is_none(),
        "the parent must receive the child's rejection and undo"
    );
    assert!(admin.users().is_empty());
    assert!(group.storage.status(&path).await.unwrap().is_none());
    for device in [&ana, &ben, &carol] {
        device.database().close().await.unwrap();
    }
}

async fn rejected_undo(reopen: bool, compare: bool) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    carol.sync.pass().await.unwrap();
    let admin = Arc::new(group.storage.key_admin());
    let (delivery_started, delivery_observed) = tokio::sync::oneshot::channel();
    let (delivery_resume, delivery_held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(
            ObjectPrefix::access_keys(group.store, &ben.identity.member()),
            delivery_started,
            delivery_held,
        )
        .await;
    let replace = ana
        .sync
        .replace_member_access_key(ben.identity.member(), admin.clone());
    let remove = async {
        delivery_observed.await.unwrap();
        let record = parent(&ana).await.unwrap();
        let work: ReplaceKeyWork = serde_json::from_slice(&record.data).unwrap();
        let path = ObjectPath::access_key(group.store, &work.member, work.attempt);
        let (claim_started, claim_observed) = tokio::sync::oneshot::channel();
        let (claim_resume, claim_held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(ObjectPrefix::all(group.store), claim_started, claim_held)
            .await;
        delivery_resume.send(()).unwrap();
        claim_observed.await.unwrap();
        carol
            .sync
            .administer(StoreChange::RemoveMember {
                member: ben.identity.member(),
            })
            .await
            .unwrap();
        ana.storage
            .set_faults(Faults {
                delete_failures: [(path.clone(), StorageFailure::Network)].into(),
                ..Faults::none()
            })
            .await;
        claim_resume.send(()).unwrap();
        path
    };
    let (result, path) = tokio::join!(replace, remove);
    let Err(SyncError::Cleanup { operation, cleanup }) = result else {
        panic!("undo must report both causes: {result:?}");
    };
    assert!(matches!(
        *operation,
        SyncError::Rejected(OperationRejection::TargetGone(_))
    ));
    assert!(
        matches!(*cleanup, SyncError::Storage(error) if error.failure() == StorageFailure::Network)
    );
    let record = parent(&ana).await.unwrap();
    let pending = ana
        .database()
        .operation_pending::<PendingRecord>(record.id)
        .await
        .unwrap()
        .unwrap();
    let encoded = serde_json::to_value(&pending.reason).unwrap();
    assert!(
        encoded["Cleanup"]["operation"]["Rejected"]["TargetGone"].is_object(),
        "{encoded}"
    );
    assert_eq!(
        encoded["Cleanup"]["cleanup"]["Storage"]["failure"],
        "Network"
    );
    assert_eq!(pending.retry(), crate::Retry::Automatic);
    if compare {
        compare_state(&ana, record.id, "undo", true, true, Some("network"), None).await;
    }
    let wait = ana
        .database()
        .retry_wait(coven_database::RetrySubject::Operation(record.id))
        .await
        .unwrap()
        .unwrap();
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_some());
    assert_eq!(admin.users().len(), 1);
    if compare {
        compare_state(&ana, record.id, "undo", false, true, None, Some("network")).await;
    }
    let ana = if reopen { group.reopen(ana).await } else { ana };
    group.clock.set(wait.not_before);
    let result = ana.sync.pass().await;
    result.unwrap();
    if reopen {
        if compare {
            compare_state(&ana, record.id, "undo", true, false, None, Some("network")).await;
        }
        let pending = ana
            .database()
            .operation_pending::<PendingRecord>(record.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(pending.reason).unwrap()["Cleanup"]["cleanup"],
            "AdminAccessRequired"
        );
        assert_eq!(admin.users().len(), 1);
        assert!(matches!(
            ana.sync
                .retry_with_admin_access(record.id, admin.clone())
                .await,
            Err(SyncError::Rejected(OperationRejection::TargetGone(_)))
        ));
    }
    if compare {
        compare_state(&ana, record.id, "undo", true, true, None, None).await;
    }
    assert!(parent(&ana).await.is_none());
    assert!(group.storage.status(&path).await.unwrap().is_none());
    assert!(admin.users().is_empty());
    for device in [&ana, &ben, &carol] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_failed_replacement_undo_keeps_both_causes_and_retries() {
    rejected_undo(false, false).await;
}

#[tokio::test]
async fn a_reopened_undo_keeps_the_rejection_while_requesting_admin_access() {
    rejected_undo(true, false).await;
}

async fn compare_state(
    ana: &Device,
    id: OperationId,
    phase: &str,
    due: bool,
    admin: bool,
    failure: Option<&str>,
    previous_failure: Option<&str>,
) {
    use serde_json::json;
    let retained = ana.database().operation(id).await.unwrap().is_some();
    let pending = ana
        .database()
        .operation_pending::<PendingRecord>(id)
        .await
        .unwrap();
    let (rejection, actual_failure) = match pending.as_ref().map(|p| &p.reason) {
        Some(PendingReason::Cleanup { operation, cleanup }) => {
            assert!(matches!(
                operation.as_ref(),
                PendingReason::Rejected(OperationRejection::TargetGone(_))
            ));
            (Some(1), Some(failure_name(cleanup)))
        }
        Some(reason) => (None, Some(failure_name(reason))),
        None => (None, None),
    };
    let input = json!({"kind":"accessReplacement", "phase":phase,"reason":1,
        "due":due,"admin":admin,"rejection":null,"failure":failure,
        "previous_failure":previous_failure});
    let actual = json!({"retained":retained,"rejection":rejection,"failure":actual_failure});
    assert_eq!(
        actual,
        crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input)
    );
}

fn failure_name(reason: &PendingReason) -> &'static str {
    match reason {
        PendingReason::Storage {
            failure: StorageFailure::Network,
            ..
        } => "network",
        PendingReason::AdminAccessRequired => "admin",
        reason => panic!("unexpected replacement failure: {reason:?}"),
    }
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN"]
async fn access_replacement_retries_and_undo_match_lean() {
    delivery_retry(true).await;
    for reopen in [false, true] {
        rejected_undo(reopen, true).await;
    }
}

/// Coven's own operation whose request got no reply waits its backoff; a
/// pass before then leaves it. The app's network report ends that wait,
/// keeping its failure count, and the next pass resumes it (§6.7).
#[tokio::test]
async fn a_network_report_ends_an_offline_operation_wait() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let admin = Arc::new(group.storage.key_admin());
    ana.storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(ana
        .sync
        .replace_member_access_key(ben.identity.member(), admin.clone())
        .await
        .is_err());
    let record = parent(&ana).await.unwrap();
    let subject = coven_database::RetrySubject::Operation(record.id);
    let wait = ana.database().retry_wait(subject).await.unwrap().unwrap();
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.pass().await.unwrap();
    assert_eq!(parent(&ana).await.unwrap().last_step, record.last_step);
    ana.sync.waits.network_changed();
    // The file worker's report leaves the operation to the connection.
    ana.sync
        .waits
        .end_offline_waits(crate::NetworkOwner::Files)
        .await
        .unwrap();
    assert_eq!(
        ana.database().retry_wait(subject).await.unwrap(),
        Some(wait)
    );
    assert!(ana
        .sync
        .waits
        .end_offline_waits(crate::NetworkOwner::Connection)
        .await
        .unwrap());
    let ended = ana.database().retry_wait(subject).await.unwrap().unwrap();
    assert_eq!(
        (ended.failures, ended.wait),
        (wait.failures, Duration::ZERO)
    );
    ana.sync.pass().await.unwrap();
    assert!(
        parent(&ana).await.is_none(),
        "the report let the operation resume"
    );
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}
