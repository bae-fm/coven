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
/// met removes the record; the retry class follows the reason. Positions a
/// snapshot load installs end this device's waits for the writes they cover
/// and keep verdicts on their bytes (`Pending.cover`). A completed positions
/// pass replaces every peer's positions record, and an audience snapshot
/// listing that audience's snapshot records (`Pending.replace`), keeping a
/// refusal this version may not retry for each snapshot it still lists
/// (`Listing.keepingRefusals`); neither touches this device's own positions
/// record. A completed store-log scan replaces this device's entry waits and
/// keeps its refusals and drops (`Pending.scanEntries`). The Rust side
/// persists through the store's real pending table, its covered-write
/// clearing and the replacement calls the positions scan, snapshot catalog and
/// store-log scan make, reads refusals and their retry policy back as the
/// catalog does, and reads `pending()`.
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
    let write = |device, number| {
        PendingSubject::Write(coven_merge::WriteId {
            device: DeviceId(device),
            number,
        })
    };
    let entry = |device, number| {
        PendingSubject::Entry(coven_format::value::EntryId {
            device: DeviceId(device),
            number,
        })
    };
    let circle = Audience::Circle(coven_foundation::id_source::CircleId(
        uuid::Uuid::from_u128(1),
    ));
    let snapshot = |audience: &Audience, number| {
        PendingSubject::Snapshot(coven_format::store_log::SnapshotId {
            device: DeviceId(1),
            number,
            audience: audience.clone(),
        })
    };
    // Writes of devices 7 and 8 are the ones a load's positions can cover:
    // neither device exists in the group, so no other progress moves them.
    let mut subjects: Vec<(PendingSubject, Value)> = vec![
        (write(1, 1), json!(["write", 1, 1])),
        (write(1, 2), json!(["write", 1, 2])),
        (write(8, 1), json!(["write", 8, 1])),
        (entry(1, 4), json!(["entry", 1, 4])),
        (entry(3, 1), json!(["entry", 3, 1])),
        (entry(3, 2), json!(["entry", 3, 2])),
        (
            // The model's subject has one credential per member; this
            // projection names that member's only sealing key.
            PendingSubject::KeyCopy {
                audience: Audience::Store,
                key: KeyId(uuid::Uuid::from_u128(5)),
                member: crate::store_log_sync::tests::member(2).member(),
                sealing: crate::store_log_sync::tests::member(2)
                    .keys()
                    .public_keys()
                    .sealing,
            },
            json!(["keyCopy", 0, 5, 2]),
        ),
        (
            PendingSubject::Positions(DeviceId(3)),
            json!(["positions", 3]),
        ),
        (
            PendingSubject::Positions(DeviceId(4)),
            json!(["positions", 4]),
        ),
        // This device's own positions: only its publication clears it.
        (
            PendingSubject::Positions(DeviceId(2)),
            json!(["positions", 2]),
        ),
        (
            PendingSubject::Agreement {
                audience: Audience::Store,
                peer: DeviceId(3),
            },
            json!(["agreement", 0, 3]),
        ),
        (snapshot(&Audience::Store, 1), json!(["snapshot", 0, 1])),
        (snapshot(&Audience::Store, 2), json!(["snapshot", 0, 2])),
        (snapshot(&circle, 1), json!(["snapshot", 1, 1])),
    ];
    for number in 1..=3 {
        subjects.push((write(7, number), json!(["write", 7, number])));
    }
    /// The class one completed listing decides: every peer's positions, or
    /// one audience's snapshots (`Pending.Scan.decides`).
    fn decides(class: &Option<Audience>, subject: &PendingSubject) -> bool {
        match (class, subject) {
            (None, PendingSubject::Positions(device)) => *device != DeviceId(2),
            (Some(audience), PendingSubject::Snapshot(id)) => id.audience == *audience,
            _ => false,
        }
    }
    /// The records one store-log scan decides: this device's entry waits
    /// (`Pending.decidedByScan`, Rust `replace_entry_waits`).
    fn scan_decides(record: &PendingRecord) -> bool {
        matches!(record.subject, PendingSubject::Entry(_))
            && matches!(
                record.reason,
                PendingReason::Waits(_)
                    | PendingReason::KeyUnavailable { .. }
                    | PendingReason::UpdateRequired(_)
            )
    }
    let entry_waits: Vec<usize> = (0..reasons.len())
        .filter(|i| {
            scan_decides(&PendingRecord {
                subject: entry(1, 1),
                reason: reasons[*i].0.clone(),
                reported_by: DeviceId(2),
            })
        })
        .collect();
    let refused = reasons
        .iter()
        .position(|(reason, _)| matches!(reason, PendingReason::Refused(_)))
        .expect("an encoded refusal");
    let classes = [
        (None, json!(["positions"])),
        (Some(Audience::Store), json!(["snapshots", 0])),
        (Some(circle.clone()), json!(["snapshots", 1])),
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
    let encode = |conditions: &[(bool, usize)]| -> Value {
        conditions
            .iter()
            .map(|(met, reason)| json!([met, reasons[*reason].1]))
            .collect()
    };
    for _ in 0..12 {
        let devices = group(storage(), 2).await;
        let db = &devices[1].db;
        let own = DeviceId(2);
        let mut steps = Vec::new();
        let mut observed = Vec::new();
        // The positions table holds one row per device, so each is covered once.
        let mut uncovered = vec![7_u64, 8];
        for _ in 0..16 {
            let kind = next(8);
            if kind == 0 && !uncovered.is_empty() {
                let device = uncovered.remove(next(uncovered.len()));
                let number = 1 + next(3) as u64;
                db.test_cover_positions(vec![(device, number)])
                    .await
                    .unwrap();
                steps.push(json!({ "cover": [[device, number]] }));
            } else if kind <= 2 {
                let (class, class_json) = classes[next(classes.len())].clone();
                let mut listed = Vec::new();
                let mut listed_subjects = Vec::new();
                let mut records = Vec::new();
                for (subject, subject_json) in subjects.iter().filter(|(s, _)| decides(&class, s)) {
                    if next(2) == 0 {
                        continue;
                    }
                    let conditions: Vec<(bool, usize)> = (0..next(4))
                        .map(|_| (next(3) == 0, next(reasons.len())))
                        .collect();
                    listed.push(json!({
                        "subject": subject_json,
                        "conditions": encode(&conditions),
                    }));
                    listed_subjects.push(subject.clone());
                    // As `snapshot_candidates`: a listed snapshot this device
                    // refused, and may not check again, keeps that refusal.
                    let refusal = match class {
                        Some(_) => db
                            .pending::<PendingRecord>()
                            .await
                            .unwrap()
                            .into_iter()
                            .find(|r| {
                                r.reported_by == own
                                    && r.subject == *subject
                                    && matches!(r.reason, PendingReason::Refused(_))
                            }),
                        None => None,
                    };
                    let refusal = match refusal {
                        Some(record)
                            if !db
                                .pending_can_retry::<PendingRecord>(subject.clone())
                                .await
                                .unwrap() =>
                        {
                            Some(record)
                        }
                        _ => None,
                    };
                    if let Some(record) = refusal {
                        records.push(record);
                    } else if let Some((_, reason)) = conditions.iter().find(|(met, _)| !met) {
                        records.push(PendingRecord {
                            subject: subject.clone(),
                            reason: reasons[*reason].0.clone(),
                            reported_by: own,
                        });
                    }
                }
                if class.is_none() {
                    // As `posted_positions::scan`: the subjects it decides are
                    // every peer positions record this device holds plus every
                    // listed peer, committed through the same local replacement.
                    let mut decided: Vec<PendingSubject> = db
                        .pending::<PendingRecord>()
                        .await
                        .unwrap()
                        .into_iter()
                        .filter(|r| r.reported_by == own && decides(&class, &r.subject))
                        .map(|r| r.subject)
                        .collect();
                    decided.extend(listed_subjects);
                    db.replace_pending_subjects(decided, records).await.unwrap();
                } else {
                    db.replace_local_pending(
                        move |record: &PendingRecord| decides(&class, &record.subject),
                        records,
                    )
                    .await
                    .unwrap();
                }
                steps.push(json!({ "listing": class_json, "observed": listed }));
            } else if kind == 3 {
                // As `replace_entry_waits`: one completed scan's waits replace
                // every entry wait this device recorded.
                let mut waits = Vec::new();
                let mut records = Vec::new();
                for (subject, _) in subjects
                    .iter()
                    .filter(|(s, _)| matches!(s, PendingSubject::Entry(_)))
                {
                    if next(2) == 0 {
                        continue;
                    }
                    let reason = entry_waits[next(entry_waits.len())];
                    let PendingSubject::Entry(id) = subject else {
                        unreachable!("filtered to entries")
                    };
                    waits.push(json!([[id.device.0, id.number], reasons[reason].1]));
                    records.push(PendingRecord {
                        subject: subject.clone(),
                        reason: reasons[reason].0.clone(),
                        reported_by: own,
                    });
                }
                db.replace_local_pending(scan_decides, records)
                    .await
                    .unwrap();
                steps.push(json!({ "entries": waits }));
            } else {
                let (subject, subject_json) = subjects[next(subjects.len())].clone();
                // As `refuse_snapshot`: loading records a body that failed its
                // check, which later listings must keep.
                let conditions: Vec<(bool, usize)> =
                    if matches!(subject, PendingSubject::Snapshot(_)) && next(2) == 0 {
                        vec![(false, refused)]
                    } else {
                        (0..next(4))
                            .map(|_| (next(3) == 0, next(reasons.len())))
                            .collect()
                    };
                steps.push(json!({
                    "subject": subject_json,
                    "reporter": own.0,
                    "conditions": encode(&conditions),
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
        cases.push(json!({ "device": own.0, "steps": steps }));
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
