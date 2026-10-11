use super::*;
use crate::objects::PostedPositions;
use crate::value::{EntryPositions, WritePositions};
use crate::wire::{MAX_BYTES, MAX_ITEMS, MAX_OBJECT};
use crate::{encode_frame, Object};
use coven_foundation::id_source::CircleId;
use uuid::Uuid;

fn post(pending: Vec<PendingReport>) -> PostedPositions {
    PostedPositions {
        device: DeviceId(1),
        writes: WritePositions(Vec::new()),
        store_log: EntryPositions(Vec::new()),
        schema_version: 1,
        fingerprints: Vec::new(),
        pending,
    }
}

fn report(subject: PendingSubject, reason: PendingReason) -> PendingReport {
    PendingReport { subject, reason }
}

fn bytes(value: &impl Wire) -> Vec<u8> {
    let mut out = Encoder::new();
    value.put(&mut out).unwrap();
    out.bytes
}

fn round_trip<T: Wire + std::fmt::Debug + PartialEq>(value: T, expected: &str) {
    let expected = crate::tests::hex(expected);
    assert_eq!(bytes(&value), expected);
    let mut decoder = Decoder::new(&expected).unwrap();
    assert_eq!(T::get(&mut decoder).unwrap(), value);
    decoder.finish().unwrap();
    for end in 0..expected.len() {
        assert!(T::get(&mut Decoder::new(&expected[..end]).unwrap()).is_err());
    }
}

fn subjects() -> Vec<PendingSubject> {
    vec![
        PendingSubject::Write(WriteId {
            device: DeviceId(2),
            number: 3,
        }),
        PendingSubject::Entry(EntryId {
            device: DeviceId(2),
            number: 3,
        }),
        PendingSubject::KeyCopy {
            audience: Audience::Store,
            key: KeyId(Uuid::from_u128(4)),
            member: crate::test_utils::member().signing,
        },
        PendingSubject::File {
            device: DeviceId(1),
            file: FileId(Uuid::from_u128(5)),
        },
        PendingSubject::Snapshot(SnapshotId {
            audience: Audience::Store,
            device: DeviceId(2),
            number: 3,
        }),
        PendingSubject::Positions(DeviceId(2)),
    ]
}

#[test]
fn every_subject_has_its_d8_tag_and_exact_field_order() {
    let member = hex::encode(crate::test_utils::member().signing.to_bytes());
    let expected = [
        "0000000000000000020000000000000003".to_owned(),
        "0100000000000000020000000000000003".to_owned(),
        format!("020000000000000000000000000000000004{member}"),
        "03000000000000000100000000000000000000000000000005".to_owned(),
        "040000000000000000020000000000000003".to_owned(),
        "050000000000000002".to_owned(),
    ];
    for (subject, expected) in subjects().into_iter().zip(expected) {
        round_trip(subject, &expected);
    }
    round_trip(
        PendingSubject::Snapshot(SnapshotId {
            audience: Audience::Circle(CircleId(Uuid::from_u128(6))),
            device: DeviceId(2),
            number: 3,
        }),
        "04010000000000000000000000000000000600000000000000020000000000000003",
    );
    round_trip(
        PendingSubject::KeyCopy {
            audience: Audience::Circle(CircleId(Uuid::from_u128(6))),
            key: KeyId(Uuid::from_u128(4)),
            member: crate::test_utils::member().signing,
        },
        &format!("02010000000000000000000000000000000600000000000000000000000000000004{member}"),
    );
}

#[test]
fn every_reason_and_nested_tag_has_exact_d8_bytes() {
    let failures = [
        RefusalCode::Decryption,
        RefusalCode::Signature,
        RefusalCode::Parse,
        RefusalCode::InvalidWrite,
        RefusalCode::NotAuthorized,
        RefusalCode::InvalidCausality,
        RefusalCode::WrongIdentity,
        RefusalCode::ContentHash,
    ];
    for (tag, failure) in failures.into_iter().enumerate() {
        round_trip(failure, &format!("{tag:02x}"));
        round_trip(PendingReason::Refused(failure), &format!("00{tag:02x}"));
        round_trip(
            PendingReason::InvalidPositions(failure),
            &format!("06{tag:02x}"),
        );
    }
    let path = ObjectPath::parse("devices/2/3").unwrap();
    for (reason, expected) in [
        (
            PendingReason::Missing { path: path.clone() },
            "010000000b646576696365732f322f33",
        ),
        (
            PendingReason::Waits(Prerequisite::Object(path)),
            "02000000000b646576696365732f322f33",
        ),
        (
            PendingReason::Waits(Prerequisite::DeviceRegistration(DeviceId(2))),
            "02010000000000000002",
        ),
        (
            PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key: KeyId(Uuid::from_u128(4)),
            },
            "030000000000000000000000000000000004",
        ),
        (
            PendingReason::KeyUnavailable {
                audience: Audience::Circle(CircleId(Uuid::from_u128(6))),
                key: KeyId(Uuid::from_u128(4)),
            },
            "03010000000000000000000000000000000600000000000000000000000000000004",
        ),
        (
            PendingReason::UpdateRequired(RequiredUpdate::AppSchema {
                version: 0x01020304,
            }),
            "040001020304",
        ),
        (
            PendingReason::UpdateRequired(RequiredUpdate::CovenFormat { version: 0x0102 }),
            "040100000102",
        ),
        (
            PendingReason::FileUnavailable(FileSourceFailure::Missing),
            "0500",
        ),
        (
            PendingReason::FileUnavailable(FileSourceFailure::Changed),
            "0501",
        ),
        (
            PendingReason::FileUnavailable(FileSourceFailure::Integrity),
            "0502",
        ),
    ] {
        round_trip(reason, expected);
    }
}

fn rejects_both(pending: Vec<PendingReport>) {
    let posted = post(pending);
    assert!(Object::PostedPositions(posted.clone()).encode().is_err());
    assert!(Object::decode(&encode_frame(8, &posted).unwrap()).is_err());
}

#[test]
fn reason_subject_and_poster_restrictions_apply_on_both_boundaries() {
    for subject in subjects() {
        for failure in 0..=7 {
            for reason in [
                PendingReason::Refused(RefusalCode::try_from(failure).unwrap()),
                PendingReason::InvalidPositions(RefusalCode::try_from(failure).unwrap()),
            ] {
                let allowed = match reason {
                    PendingReason::Refused(_) => {
                        !matches!(subject, PendingSubject::Positions(_))
                            && (failure != 7 || matches!(subject, PendingSubject::File { .. }))
                    }
                    PendingReason::InvalidPositions(_) => {
                        matches!(subject, PendingSubject::Positions(_)) && failure != 7
                    }
                    _ => unreachable!(),
                };
                let pending = vec![report(subject.clone(), reason)];
                if allowed {
                    let object = Object::PostedPositions(post(pending));
                    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
                } else {
                    rejects_both(pending);
                }
            }
        }
        for failure in [
            FileSourceFailure::Missing,
            FileSourceFailure::Changed,
            FileSourceFailure::Integrity,
        ] {
            let pending = vec![report(
                subject.clone(),
                PendingReason::FileUnavailable(failure),
            )];
            if matches!(subject, PendingSubject::File { .. }) {
                let object = Object::PostedPositions(post(pending));
                assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
            } else {
                rejects_both(pending);
            }
        }
    }
    rejects_both(vec![report(
        PendingSubject::File {
            device: DeviceId(2),
            file: FileId(Uuid::from_u128(5)),
        },
        PendingReason::FileUnavailable(FileSourceFailure::Missing),
    )]);
    for subject in [
        PendingSubject::Write(WriteId {
            device: DeviceId(2),
            number: 0,
        }),
        PendingSubject::Entry(EntryId {
            device: DeviceId(2),
            number: 0,
        }),
        PendingSubject::Snapshot(SnapshotId {
            audience: Audience::Store,
            device: DeviceId(2),
            number: 0,
        }),
    ] {
        rejects_both(vec![report(
            subject,
            PendingReason::Refused(RefusalCode::Parse),
        )]);
    }
}

#[test]
fn unknown_and_local_only_tags_are_rejected() {
    // Local operations, retention, audiences, agreement, joins and connections
    // have no subject tags. Provider state, drops and local waits have no tags.
    for tag in 6..=255 {
        assert_eq!(
            PendingSubject::get(&mut Decoder::new(&[tag]).unwrap()),
            Err(Error::UnknownTag {
                field: "pending subject",
                tag
            })
        );
    }
    for tag in 7..=255 {
        assert_eq!(
            PendingReason::get(&mut Decoder::new(&[tag]).unwrap()),
            Err(Error::UnknownTag {
                field: "pending reason",
                tag
            })
        );
    }
    for tag in 2..=255 {
        assert_eq!(
            Prerequisite::get(&mut Decoder::new(&[tag]).unwrap()),
            Err(Error::UnknownTag {
                field: "pending prerequisite",
                tag
            })
        );
        assert_eq!(
            RequiredUpdate::get(&mut Decoder::new(&[tag]).unwrap()),
            Err(Error::UnknownTag {
                field: "required update",
                tag
            })
        );
    }
    for tag in 8..=255 {
        assert_eq!(
            RefusalCode::try_from(tag),
            Err(Error::UnknownTag {
                field: "refusal",
                tag
            })
        );
    }
    // Uploader removal/replacement are derived locally, never source failures.
    for tag in 3..=255 {
        assert_eq!(
            FileSourceFailure::try_from(tag),
            Err(Error::UnknownTag {
                field: "file source failure",
                tag
            })
        );
    }
}

#[test]
fn format_updates_are_u32_on_wire_but_must_fit_u16() {
    round_trip(
        RequiredUpdate::CovenFormat { version: u16::MAX },
        "010000ffff",
    );
    round_trip(
        RequiredUpdate::AppSchema { version: u32::MAX },
        "00ffffffff",
    );
    for version in [65_536u32, u32::MAX] {
        let mut raw = vec![1];
        raw.extend(version.to_be_bytes());
        assert_eq!(
            RequiredUpdate::get(&mut Decoder::new(&raw).unwrap()),
            Err(Error::Invalid {
                field: "required format version",
                rule: Rule::Kind
            })
        );
    }
}

#[test]
fn pending_paths_reject_local_noncanonical_and_malformed_text() {
    for path in [
        "/Users/member/photo.jpg",
        "../devices/2/3",
        "devices/02/3",
        "devices/2/0",
        "devices/2/3/",
        "",
        "devices/2/3\0",
    ] {
        for prefix in [vec![1], vec![2, 0]] {
            let mut raw = prefix;
            raw.extend(bytes(&path.to_owned()));
            assert_eq!(
                PendingReason::get(&mut Decoder::new(&raw).unwrap()),
                Err(Error::Invalid {
                    field: "pending object path",
                    rule: Rule::Kind
                })
            );
        }
    }
    assert_eq!(
        PendingReason::get(&mut Decoder::new(&[1, 0, 0, 0, 1, 255]).unwrap()),
        Err(Error::Utf8)
    );
    let mut raw = vec![1];
    raw.extend(((MAX_BYTES + 1) as u32).to_be_bytes());
    assert!(matches!(
        PendingReason::get(&mut Decoder::new(&raw).unwrap()),
        Err(Error::Limit { field: "bytes", .. })
    ));
    let mut prefix = vec![8, 0, 1];
    prefix.extend(((MAX_OBJECT - crate::FRAME_PREFIX_LEN + 1) as u32).to_be_bytes());
    assert!(matches!(
        Object::decode(&prefix),
        Err(Error::Limit {
            field: "frame payload",
            ..
        })
    ));
}

#[test]
fn reports_follow_subject_fields_and_reject_duplicates_regardless_of_reason() {
    let mut all = subjects();
    let member = crate::test_utils::member().signing;
    let other_member: MemberId = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        .parse()
        .unwrap();
    for device in [1, 2, 256] {
        for number in [1, 2, 256] {
            all.push(PendingSubject::Write(WriteId {
                device: DeviceId(device),
                number,
            }));
            all.push(PendingSubject::Entry(EntryId {
                device: DeviceId(device),
                number,
            }));
            for audience in [
                Audience::Store,
                Audience::Circle(CircleId(Uuid::from_u128(1))),
                Audience::Circle(CircleId(Uuid::from_u128(256))),
            ] {
                all.push(PendingSubject::Snapshot(SnapshotId {
                    audience,
                    device: DeviceId(device),
                    number,
                }));
            }
            all.push(PendingSubject::File {
                device: DeviceId(device),
                file: FileId(Uuid::from_u128(number as u128)),
            });
        }
        all.push(PendingSubject::Positions(DeviceId(device)));
    }
    for audience in [
        Audience::Store,
        Audience::Circle(CircleId(Uuid::from_u128(1))),
    ] {
        for key in [1, 256] {
            for member in [member.clone(), other_member.clone()] {
                all.push(PendingSubject::KeyCopy {
                    audience: audience.clone(),
                    key: KeyId(Uuid::from_u128(key)),
                    member,
                });
            }
        }
    }
    let mut by_wire = all.clone();
    by_wire.sort_by_key(bytes);
    all.sort();
    assert_eq!(all, by_wire);
    all.dedup();
    let pending: Vec<_> = all
        .into_iter()
        .map(|subject| {
            report(
                subject,
                PendingReason::Waits(Prerequisite::DeviceRegistration(DeviceId(7))),
            )
        })
        .collect();
    let object = Object::PostedPositions(post(pending.clone()));
    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    for index in 0..pending.len() - 1 {
        let mut reversed = pending.clone();
        reversed.swap(index, index + 1);
        rejects_both(reversed);
    }
    for mut duplicate in pending {
        let first = duplicate.clone();
        duplicate.reason = PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 2 });
        rejects_both(vec![first.clone(), first]);
        rejects_both(vec![
            duplicate.clone(),
            report(
                duplicate.subject.clone(),
                PendingReason::Waits(Prerequisite::DeviceRegistration(DeviceId(7))),
            ),
        ]);
    }
}

#[test]
fn key_copy_reports_must_name_the_authenticated_posters_member() {
    let member = crate::test_utils::member().signing;
    let pending = report(
        subjects().remove(2),
        PendingReason::Refused(RefusalCode::Decryption),
    );
    let posted = post(vec![pending]);
    posted.validate_reporter(&member).unwrap();
    let other = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        .parse()
        .unwrap();
    assert!(posted.validate_reporter(&other).is_err());
}

#[test]
fn report_count_and_total_frame_collections_are_bounded_without_truncation() {
    let pending: Vec<_> = (0..MAX_ITEMS)
        .map(|device| {
            report(
                PendingSubject::Positions(DeviceId(device as u64)),
                PendingReason::InvalidPositions(RefusalCode::Signature),
            )
        })
        .collect();
    let mut posted = post(pending);
    let object = Object::PostedPositions(posted.clone());
    let encoded = object.encode().unwrap();
    assert_eq!(Object::decode(&encoded).unwrap(), object);
    posted.pending.push(report(
        PendingSubject::Positions(DeviceId(MAX_ITEMS as u64)),
        PendingReason::InvalidPositions(RefusalCode::Signature),
    ));
    assert!(matches!(
        Object::PostedPositions(posted.clone()).encode(),
        Err(Error::Limit {
            field: "collection",
            ..
        })
    ));
    let mut oversized = encoded.clone();
    oversized[31..35].copy_from_slice(&((MAX_ITEMS + 1) as u32).to_be_bytes());
    assert!(matches!(
        Object::decode(&oversized),
        Err(Error::Limit {
            field: "collection",
            ..
        })
    ));
    posted.pending.pop();
    posted.writes.0.push(WriteId {
        device: DeviceId(1),
        number: 1,
    });
    assert!(matches!(
        Object::PostedPositions(posted).encode(),
        Err(Error::Limit {
            field: "total collection items",
            ..
        })
    ));
    // Add a write position to a maximal valid frame without using its bounded encoder.
    let mut oversized = encoded;
    oversized[15..19].copy_from_slice(&1u32.to_be_bytes());
    oversized.splice(19..19, [1u64.to_be_bytes(), 1u64.to_be_bytes()].concat());
    let length = (oversized.len() - crate::FRAME_PREFIX_LEN) as u32;
    oversized[3..7].copy_from_slice(&length.to_be_bytes());
    assert!(matches!(
        Object::decode(&oversized),
        Err(Error::Limit {
            field: "total collection items",
            ..
        })
    ));
}

#[test]
fn pinned_pending_frame_covers_the_complete_report_grammar() {
    let mut reports: Vec<_> = (0..7)
        .map(|failure| {
            report(
                PendingSubject::Write(WriteId {
                    device: DeviceId(2),
                    number: u64::from(failure) + 1,
                }),
                PendingReason::Refused(RefusalCode::try_from(failure).unwrap()),
            )
        })
        .collect();
    for (index, reason) in [
        PendingReason::Missing {
            path: ObjectPath::parse("devices/2/3").unwrap(),
        },
        PendingReason::Waits(Prerequisite::Object(
            ObjectPath::parse("store-log/3/1").unwrap(),
        )),
        PendingReason::Waits(Prerequisite::DeviceRegistration(DeviceId(3))),
        PendingReason::KeyUnavailable {
            audience: Audience::Store,
            key: KeyId(Uuid::from_u128(4)),
        },
        PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 3 }),
        PendingReason::UpdateRequired(RequiredUpdate::CovenFormat { version: u16::MAX }),
    ]
    .into_iter()
    .enumerate()
    {
        reports.push(report(
            PendingSubject::Write(WriteId {
                device: DeviceId(2),
                number: index as u64 + 8,
            }),
            reason,
        ));
    }
    reports.push(report(
        subjects().remove(1),
        PendingReason::Refused(RefusalCode::NotAuthorized),
    ));
    reports.push(report(
        subjects().remove(2),
        PendingReason::Refused(RefusalCode::Decryption),
    ));
    for (index, reason) in [
        PendingReason::Refused(RefusalCode::ContentHash),
        PendingReason::FileUnavailable(FileSourceFailure::Missing),
        PendingReason::FileUnavailable(FileSourceFailure::Changed),
        PendingReason::FileUnavailable(FileSourceFailure::Integrity),
    ]
    .into_iter()
    .enumerate()
    {
        reports.push(report(
            PendingSubject::File {
                device: DeviceId(1),
                file: FileId(Uuid::from_u128(index as u128 + 1)),
            },
            reason,
        ));
    }
    reports.push(report(
        PendingSubject::Snapshot(SnapshotId {
            audience: Audience::Circle(CircleId(Uuid::from_u128(6))),
            device: DeviceId(2),
            number: 3,
        }),
        PendingReason::KeyUnavailable {
            audience: Audience::Circle(CircleId(Uuid::from_u128(6))),
            key: KeyId(Uuid::from_u128(4)),
        },
    ));
    reports.push(report(
        PendingSubject::Positions(DeviceId(2)),
        PendingReason::InvalidPositions(RefusalCode::Signature),
    ));
    let expected = Object::PostedPositions(post(reports));
    let encoded = crate::tests::hex(include_str!("../fixtures/pending-reports.hex"));
    assert_eq!(expected.encode().unwrap(), encoded);
    assert_eq!(Object::decode(&encoded).unwrap(), expected);
    for end in 0..encoded.len() {
        assert!(Object::decode(&encoded[..end]).is_err());
    }
    crate::tests::mutations(&encoded, |changed| {
        if let Ok(decoded) = Object::decode(changed) {
            assert_eq!(decoded.encode().unwrap(), changed);
        }
    });
}
