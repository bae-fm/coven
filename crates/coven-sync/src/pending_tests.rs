use super::*;
use coven_database::DropReason;

#[test]
fn conditional_conflicts_retry_without_becoming_throttling() {
    let record = PendingRecord {
        subject: PendingSubject::Connection,
        reason: PendingReason::Storage {
            provider: CloudProvider::S3,
            failure: StorageFailure::Conflict,
        },
        reported_by: DeviceId(1),
    };
    assert_eq!(record.retry(), Retry::Automatic);
}

#[test]
fn a_dropped_operation_waits_for_the_app_but_a_dropped_entry_or_write_is_final() {
    let record = |subject| PendingRecord {
        subject,
        reason: PendingReason::Dropped(DropReason::NotAllowed),
        reported_by: DeviceId(1),
    };
    assert_eq!(
        record(PendingSubject::Operation {
            id: OperationId(1),
            kind: OperationKind::AddCircleMember,
        })
        .retry(),
        Retry::AppAction
    );
    for subject in [
        PendingSubject::Entry(EntryId {
            device: DeviceId(1),
            number: 1,
        }),
        PendingSubject::Write(coven_merge::WriteId {
            device: DeviceId(1),
            number: 1,
        }),
    ] {
        assert_eq!(record(subject).retry(), Retry::Never);
    }
}

/// SD `Pending`: each observation of a subject keeps exactly its first unmet
/// condition, replacing the previous one; an observation with every condition
/// met removes the record; the retry class follows the reason. The Rust side
/// persists through the store's real pending table and reads `pending()`.
#[tokio::test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
async fn data_differential_pending_records() {
    use crate::device_log_sync::tests::{group, storage, STORE_ID};
    use coven_storage::ObjectPath;
    use serde_json::{json, Value};

    let object = |n: u64| ObjectPath::device_log(STORE_ID, DeviceId(1), n.try_into().unwrap());
    let reasons: Vec<(PendingReason, Value)> = vec![
        (
            PendingReason::Missing { path: object(4) },
            json!(["missing", object(4).as_str()]),
        ),
        (
            PendingReason::Waits(Prerequisite::Object(object(3))),
            json!(["waits", ["object", object(3).as_str()]]),
        ),
        (
            PendingReason::Waits(Prerequisite::DeviceRegistration(DeviceId(3))),
            json!(["waits", ["deviceRegistration", 3]]),
        ),
        (
            PendingReason::Waits(Prerequisite::Reload(Audience::Store)),
            json!(["waits", ["reload", 0]]),
        ),
        (
            PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key: KeyId(uuid::Uuid::from_u128(5)),
            },
            json!(["keyUnavailable", 0, 5]),
        ),
        (
            PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 4 }),
            json!(["updateRequired", 4]),
        ),
        (PendingReason::NoStorage, json!(["noStorage"])),
        (
            PendingReason::Storage {
                provider: CloudProvider::S3,
                failure: StorageFailure::Network,
            },
            json!(["storage", true]),
        ),
        (
            PendingReason::Storage {
                provider: CloudProvider::S3,
                failure: StorageFailure::PermissionDenied,
            },
            json!(["storage", false]),
        ),
        (
            PendingReason::Refused(Refusal::Signature { cause: None }),
            json!(["refused"]),
        ),
        (
            PendingReason::InvalidPositions(Refusal::Signature { cause: None }),
            json!(["invalidPositions"]),
        ),
        (
            PendingReason::Dropped(DropReason::LandedTooLate),
            json!(["dropped", "landedTooLate"]),
        ),
        (PendingReason::Disagrees, json!(["disagrees"])),
        (
            PendingReason::Failed(LocalFailure::Database),
            json!(["failed", "database"]),
        ),
        (PendingReason::Paused, json!(["paused"])),
        (PendingReason::Removed, json!(["removed"])),
    ];
    let reason_json = |reason: &PendingReason| -> Value {
        reasons
            .iter()
            .find(|(known, _)| known == reason && format!("{known:?}") == format!("{reason:?}"))
            .unwrap_or_else(|| panic!("unencoded reason {reason:?}"))
            .1
            .clone()
    };
    let write = |number| {
        PendingSubject::Write(coven_merge::WriteId {
            device: DeviceId(1),
            number,
        })
    };
    let subjects: Vec<(PendingSubject, Value)> = vec![
        (write(1), json!(["write", 1, 1])),
        (write(2), json!(["write", 1, 2])),
        (
            PendingSubject::Positions(DeviceId(3)),
            json!(["positions", 3]),
        ),
        (
            PendingSubject::Agreement {
                audience: Audience::Store,
                peer: DeviceId(3),
            },
            json!(["agreement", 0, 3]),
        ),
    ];
    let retry = |retry: Retry| match retry {
        Retry::Automatic => "automatic",
        Retry::AfterUpdate => "afterUpdate",
        Retry::AppAction => "appAction",
        Retry::Never => "never",
    };
    let sorted = |records: Vec<Value>| {
        let mut records = records;
        records.sort_by_key(|record| record.to_string());
        Value::Array(records)
    };
    let mut seed = 0x5eed_u64;
    let mut next = |bound: usize| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) as usize % bound
    };
    let mut cases = Vec::new();
    let mut expected = Vec::new();
    for _ in 0..12 {
        let devices = group(storage(), 2).await;
        let db = &devices[1].db;
        let own = DeviceId(2);
        let mut steps = Vec::new();
        let mut observed = Vec::new();
        for _ in 0..10 {
            let (subject, subject_json) = subjects[next(subjects.len())].clone();
            let conditions: Vec<(bool, usize)> = (0..next(4))
                .map(|_| (next(3) == 0, next(reasons.len())))
                .collect();
            steps.push(json!({
                "subject": subject_json,
                "reporter": own.0,
                "conditions": conditions
                    .iter()
                    .map(|(met, reason)| json!([met, reasons[*reason].1]))
                    .collect::<Vec<_>>(),
            }));
            match conditions.iter().find(|(met, _)| !met) {
                Some((_, reason)) => db
                    .record_pending(PendingRecord {
                        subject: subject.clone(),
                        reason: reasons[*reason].0.clone(),
                        reported_by: own,
                    })
                    .await
                    .unwrap(),
                None => db.clear_pending(subject.clone()).await.unwrap(),
            }
            let visible = db.pending::<PendingRecord>().await.unwrap();
            observed.push(sorted(
                visible
                    .iter()
                    .map(|record| {
                        json!([
                            subjects
                                .iter()
                                .find(|(s, _)| *s == record.subject)
                                .unwrap()
                                .1,
                            record.reported_by.0,
                            reason_json(&record.reason),
                            retry(record.retry()),
                        ])
                    })
                    .collect(),
            ));
        }
        cases.push(Value::Array(steps));
        expected.push(observed);
    }
    let output = crate::tests::run_lean("COVEN_STORELOG_DATA_LEAN", &json!({ "pending": cases }));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lean: Vec<Vec<Value>> = serde_json::from_slice(&output.stdout).unwrap();
    let lean: Vec<Vec<Value>> = lean
        .into_iter()
        .map(|case| {
            case.into_iter()
                .map(|step| sorted(step.as_array().unwrap().clone()))
                .collect()
        })
        .collect();
    assert_eq!(expected, lean);
}
