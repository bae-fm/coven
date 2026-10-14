use super::*;
use crate::snapshot::{SnapshotDecoder, SnapshotEncoder, SnapshotRecord};
use crate::test_utils;
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Audience, Parent};

fn encoder(count: u64) -> (SnapshotEncoder, SnapshotDecoder) {
    let mut header = test_utils::snapshot_header();
    header.counts = [0, 0, 0, 0, count, 0];
    let (encoder, frame) = SnapshotEncoder::start(header.clone()).unwrap();
    (
        encoder,
        SnapshotDecoder::start(&frame, &test_utils::snapshot_prefix(&header)).unwrap(),
    )
}
fn raw(loss: &Loss) -> Vec<u8> {
    crate::encode_frame_with(6, |out| {
        4u8.put(out)?;
        loss.put(out)
    })
    .unwrap()
}

#[test]
fn every_loss_uses_one_section_without_write_headers_or_merge_metadata() {
    let empty = test_utils::TestOracle {
        writes: BTreeMap::new(),
    };
    let mut losses = test_utils::retained_losses();
    losses.extend([test_utils::concurrent_loss(), test_utils::excluded_loss()]);
    for cause in [
        LostWriteCause::SchemaChange(2),
        LostWriteCause::Reset(test_utils::loss_entry()),
    ] {
        let mut losses = losses.clone();
        losses.last_mut().unwrap().cause = LossCause::Excluded {
            write: test_utils::write().header.position,
            cause,
        };
        let (mut encoder, mut decoder) = encoder(losses.len() as u64);
        for loss in losses {
            let record = SnapshotRecord::Loss(loss);
            let frame = encoder.record(record.clone()).unwrap();
            assert_eq!(frame[crate::FRAME_PREFIX_LEN], 4);
            assert_eq!(decoder.frame(&frame, &empty).unwrap(), Some(record));
        }
        decoder.frame(&encoder.finish().unwrap(), &empty).unwrap();
        decoder.finish().unwrap();
    }
}

#[test]
fn invalid_losses_leave_order_and_counts_unchanged() {
    let original = test_utils::retained_losses();
    let mut invalid = Vec::new();
    for generation in [0, 2] {
        for original in &original {
            let mut loss = original.clone();
            loss.generation = generation;
            invalid.push(loss);
        }
    }
    for empty in [true, false] {
        let mut loss = original[1].clone();
        if empty {
            let LossValues::Row(cells) = &mut loss.values else {
                unreachable!()
            };
            cells.clear();
        } else {
            loss.cause = LossCause::Rules(BTreeSet::new());
        }
        invalid.push(loss);
    }
    for write in [
        WriteId {
            device: DeviceId(1),
            number: 0,
        },
        WriteId {
            device: DeviceId(3),
            number: 1,
        },
    ] {
        for setter in [true, false] {
            let mut loss = original[0].clone();
            if setter {
                let LossValues::Cell { cell, .. } = &mut loss.values else {
                    unreachable!()
                };
                cell.write = write;
            } else {
                loss.cause = LossCause::Write(write);
            }
            invalid.push(loss);
        }
    }
    for rule in [
        Rule::DeletedCircle(test_utils::loss_entry()),
        Rule::OtherAudience,
    ] {
        let mut loss = original[1].clone();
        loss.cause = LossCause::Rules([rule].into());
        invalid.push(loss);
    }
    let mut wrong_shape = original[0].clone();
    wrong_shape.cause = original[1].cause.clone();
    invalid.push(wrong_shape);
    let mut circle = original[1].clone();
    circle.row.audience = Audience::Circle(CircleId(uuid::Uuid::from_u128(1)));
    invalid.push(circle);
    for mut loss in original.clone() {
        let cell = match &mut loss.values {
            LossValues::Cell { cell, .. } => cell,
            LossValues::Row(cells) => cells.values_mut().next().unwrap(),
        };
        cell.value.parents.insert(
            coven_merge::ForeignKey::new(["parent"], "t", ["id"]),
            Parent {
                row: test_utils::row(),
                generation: 1,
            },
        );
        invalid.push(loss);
    }
    for cause in [
        LostWriteCause::SchemaChange(0),
        LostWriteCause::SchemaChange(3),
        LostWriteCause::Reset(crate::value::EntryId {
            number: 3,
            ..test_utils::loss_entry()
        }),
    ] {
        let mut loss = test_utils::excluded_loss();
        loss.cause = LossCause::Excluded {
            write: test_utils::write().header.position,
            cause,
        };
        invalid.push(loss);
    }
    let mut active = test_utils::excluded_loss();
    active.retired = false;
    invalid.push(active);
    let (mut encoder, mut decoder) = encoder(2);
    for loss in invalid {
        assert!(
            encoder.record(SnapshotRecord::Loss(loss.clone())).is_err(),
            "{loss:?}"
        );
        assert!(
            decoder.frame(&raw(&loss), &test_utils::oracle()).is_err(),
            "{loss:?}"
        );
    }
    for loss in original {
        let frame = encoder.record(SnapshotRecord::Loss(loss)).unwrap();
        decoder.frame(&frame, &test_utils::oracle()).unwrap();
    }
    decoder
        .frame(&encoder.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    decoder.finish().unwrap();
}

#[test]
fn identities_order_cells_then_rows_and_reject_duplicates() {
    let mut losses = test_utils::retained_losses();
    let mut repeated = losses[1].clone();
    let LossValues::Row(cells) = &mut repeated.values else {
        unreachable!()
    };
    cells.get_mut("x").unwrap().write = test_utils::position();
    losses.push(repeated);
    let mut next = losses[0].clone();
    next.row.key = crate::key::encode_key(&[Value::Text("later".into())]).unwrap();
    losses.push(next);
    let (mut encoder, mut decoder) = encoder(losses.len() as u64);
    for loss in losses {
        let record = SnapshotRecord::Loss(loss.clone());
        decoder
            .frame(
                &encoder.record(record.clone()).unwrap(),
                &test_utils::oracle(),
            )
            .unwrap();
        assert!(encoder.record(record).is_err());
        assert!(decoder.frame(&raw(&loss), &test_utils::oracle()).is_err());
    }
    decoder
        .frame(&encoder.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    decoder.finish().unwrap();
}

#[test]
fn circle_losses_keep_every_original_removal_reason() {
    let mut loss = test_utils::retained_losses().remove(1);
    loss.row.audience = Audience::Circle(CircleId(uuid::Uuid::from_u128(1)));
    let LossCause::Rules(rules) = &mut loss.cause else {
        unreachable!()
    };
    rules.extend([
        Rule::DeletedCircle(test_utils::loss_entry()),
        Rule::OtherAudience,
        Rule::Unique(["old_name"].into()),
        Rule::ForeignKey(coven_merge::ForeignKey::new(["p"], "t", ["id"])),
    ]);
    let mut header = test_utils::snapshot_header();
    header.id.audience = loss.row.audience.clone();
    header.counts = [0, 0, 0, 0, 1, 0];
    let (mut encoder, first) = SnapshotEncoder::start(header).unwrap();
    let mut decoder =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(encoder.header())).unwrap();
    let expected = SnapshotRecord::Loss(loss);
    assert_eq!(
        decoder
            .frame(
                &encoder.record(expected.clone()).unwrap(),
                &test_utils::oracle()
            )
            .unwrap(),
        Some(expected)
    );
    decoder
        .frame(&encoder.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    decoder.finish().unwrap();
}

#[test]
fn circle_loss_requires_its_entry_in_snapshot_coverage() {
    let mut loss = test_utils::retained_losses().remove(1);
    loss.row.audience = Audience::Circle(CircleId(uuid::Uuid::from_u128(1)));
    let entry = test_utils::loss_entry();
    loss.cause = LossCause::Rules([Rule::DeletedCircle(entry)].into());
    let mut header = test_utils::snapshot_header();
    header.id.audience = loss.row.audience.clone();
    header.counts = [0, 0, 0, 0, 1, 0];
    let (mut encoder, first) = SnapshotEncoder::start(header.clone()).unwrap();
    let mut decoder =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(&header)).unwrap();
    for invalid in [
        crate::value::EntryId { number: 0, ..entry },
        crate::value::EntryId {
            number: entry.number + 1,
            ..entry
        },
        crate::value::EntryId {
            device: DeviceId(2),
            ..entry
        },
    ] {
        let mut refused = loss.clone();
        refused.cause = LossCause::Rules([Rule::DeletedCircle(invalid)].into());
        assert!(encoder
            .record(SnapshotRecord::Loss(refused.clone()))
            .is_err());
        assert!(decoder
            .frame(&raw(&refused), &test_utils::oracle())
            .is_err());
    }
    let expected = SnapshotRecord::Loss(loss);
    assert_eq!(
        decoder
            .frame(
                &encoder.record(expected.clone()).unwrap(),
                &test_utils::oracle()
            )
            .unwrap(),
        Some(expected)
    );
    decoder
        .frame(&encoder.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    decoder.finish().unwrap();
}

/// A dependent change's loss keeps its write and failed sources in role
/// order with repeats, even as an empty delete; the sources must name the
/// same row, be present and be covered by the snapshot's positions.
#[test]
fn dependency_losses_round_trip_with_their_sources() {
    let empty = test_utils::TestOracle {
        writes: BTreeMap::new(),
    };
    let mut loss = test_utils::excluded_loss();
    let write = test_utils::write().header.position;
    let source = crate::write_sources::ChangeId {
        write,
        row: loss.row.clone(),
    };
    loss.cause = LossCause::Dependency {
        write,
        sources: vec![source.clone(), source.clone()],
    };
    let (mut encoder, mut decoder) = encoder(2);
    let mut empty_delete = loss.clone();
    empty_delete.values = LossValues::Row(BTreeMap::new());
    empty_delete.generation = loss.generation + 1;
    for loss in [loss.clone(), empty_delete] {
        let record = SnapshotRecord::Loss(loss);
        let frame = encoder.record(record.clone()).unwrap();
        assert_eq!(decoder.frame(&frame, &empty).unwrap(), Some(record));
    }
    decoder.frame(&encoder.finish().unwrap(), &empty).unwrap();
    decoder.finish().unwrap();
    for invalid in [
        LossCause::Dependency {
            write,
            sources: vec![],
        },
        LossCause::Dependency {
            write,
            sources: vec![crate::write_sources::ChangeId {
                write,
                row: coven_merge::RowId {
                    key: b"other".to_vec(),
                    ..loss.row.clone()
                },
            }],
        },
    ] {
        let mut bad = loss.clone();
        bad.cause = invalid;
        assert!(bad.validate().is_err());
    }
    let mut unfrozen = loss;
    unfrozen.retired = false;
    assert!(unfrozen.validate().is_err());
}
