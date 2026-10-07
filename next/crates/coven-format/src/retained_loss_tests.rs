use super::*;
use crate::snapshot::{SnapshotDecoder, SnapshotEncoder, SnapshotRecord};
use crate::test_utils;
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Audience, Parent};

fn encoder(count: u64) -> (SnapshotEncoder, SnapshotDecoder) {
    let mut header = test_utils::snapshot_header();
    header.counts = [0, 0, 0, 0, 0, count];
    let (encoder, frame) = SnapshotEncoder::start(header.clone()).unwrap();
    (
        encoder,
        SnapshotDecoder::start(&frame, &test_utils::snapshot_prefix(&header)).unwrap(),
    )
}

fn raw(loss: &RetainedLoss) -> Vec<u8> {
    crate::encode_frame_with(6, |out| {
        5u8.put(out)?;
        loss.put(out)
    })
    .unwrap()
}

#[test]
fn losses_round_trip_without_any_merge_history_or_write_oracle() {
    let (mut encoder, mut decoder) = encoder(2);
    let empty = test_utils::TestOracle {
        writes: BTreeMap::new(),
    };
    for loss in test_utils::retained_losses() {
        let record = SnapshotRecord::RetainedLoss(loss);
        let frame = encoder.record(record.clone()).unwrap();
        assert_eq!(frame[crate::FRAME_PREFIX_LEN], 5);
        assert_eq!(decoder.frame(&frame, &empty).unwrap(), Some(record));
    }
    decoder.frame(&encoder.finish().unwrap(), &empty).unwrap();
    decoder.finish().unwrap();
}

#[test]
fn invalid_losses_leave_order_and_counts_unchanged() {
    let original = test_utils::retained_losses();
    let mut invalid = Vec::new();
    for generation in [0, 2] {
        let mut loss = original[1].clone();
        let RetainedValues::Row { generation: g, .. } = &mut loss.values else {
            unreachable!()
        };
        *g = generation;
        invalid.push(loss);
    }
    for empty in [true, false] {
        let mut loss = original[1].clone();
        let RetainedValues::Row {
            cells, replaced_by, ..
        } = &mut loss.values
        else {
            unreachable!()
        };
        if empty {
            cells.clear();
        } else {
            replaced_by.clear();
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
            let RetainedValues::Cell { key, value } = &mut loss.values else {
                unreachable!()
            };
            if setter {
                key.write = write;
            } else {
                value.replaced_by = write;
            }
            invalid.push(loss);
        }
        let mut loss = original[1].clone();
        let RetainedValues::Row { cells, .. } = &mut loss.values else {
            unreachable!()
        };
        cells.get_mut("x").unwrap().write = write;
        invalid.push(loss);
    }
    let mut circle = original[1].clone();
    circle.row.audience = Audience::Circle(CircleId(uuid::Uuid::from_u128(1)));
    invalid.push(circle);
    for rule in [Rule::DeletedCircle, Rule::OtherAudience] {
        let mut loss = original[1].clone();
        let RetainedValues::Row { replaced_by, .. } = &mut loss.values else {
            unreachable!()
        };
        *replaced_by = [rule].into();
        invalid.push(loss);
    }
    for generation in [0, 2] {
        let mut loss = original[0].clone();
        let RetainedValues::Cell { value, .. } = &mut loss.values else {
            unreachable!()
        };
        value.incarnation = generation;
        invalid.push(loss);
    }
    for parent in [
        Parent {
            row: test_utils::row(),
            generation: 2,
        },
        Parent {
            row: RowId {
                audience: Audience::Circle(CircleId(uuid::Uuid::from_u128(1))),
                ..test_utils::row()
            },
            generation: 1,
        },
    ] {
        let mut loss = original[0].clone();
        let RetainedValues::Cell { value, .. } = &mut loss.values else {
            unreachable!()
        };
        value.value.parents.insert(
            coven_merge::ForeignKey::new(["parent"], "t", ["id"]),
            parent,
        );
        invalid.push(loss);
    }
    let (mut encoder, mut decoder) = encoder(2);
    for loss in invalid {
        assert!(
            encoder
                .record(SnapshotRecord::RetainedLoss(loss.clone()))
                .is_err(),
            "{loss:?}"
        );
        assert!(
            decoder.frame(&raw(&loss), &test_utils::oracle()).is_err(),
            "{loss:?}"
        );
    }
    for loss in original {
        let frame = encoder.record(SnapshotRecord::RetainedLoss(loss)).unwrap();
        decoder.frame(&frame, &test_utils::oracle()).unwrap();
    }
    decoder
        .frame(&encoder.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    decoder.finish().unwrap();
}

#[test]
fn row_order_and_multiple_losses_use_stable_identities() {
    let mut losses = test_utils::retained_losses();
    let mut repeated = losses[1].clone();
    let RetainedValues::Row { cells, .. } = &mut repeated.values else {
        unreachable!()
    };
    cells.get_mut("x").unwrap().write = test_utils::position();
    losses.push(repeated);
    let mut next = losses[0].clone();
    next.row.key = crate::key::encode_key(&[Value::Text("later".into())]).unwrap();
    losses.push(next);
    let (mut encoder, mut decoder) = encoder(losses.len() as u64);
    for loss in losses {
        let record = SnapshotRecord::RetainedLoss(loss.clone());
        decoder
            .frame(
                &encoder.record(record.clone()).unwrap(),
                &test_utils::oracle(),
            )
            .unwrap();
        assert!(encoder.record(record).is_err());
        assert!(decoder.frame(&raw(&loss), &test_utils::oracle()).is_err());
    }
    assert!(encoder
        .record(SnapshotRecord::RetainedLoss(
            test_utils::retained_losses().remove(0)
        ))
        .is_err());
    decoder
        .frame(&encoder.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    decoder.finish().unwrap();
}

#[test]
fn circle_losses_keep_all_original_removal_reasons() {
    let mut loss = test_utils::retained_losses().remove(1);
    loss.row.audience = Audience::Circle(CircleId(uuid::Uuid::from_u128(1)));
    let RetainedValues::Row { replaced_by, .. } = &mut loss.values else {
        unreachable!()
    };
    replaced_by.extend([
        Rule::DeletedCircle,
        Rule::OtherAudience,
        Rule::Unique(["old_name"].into()),
    ]);
    let mut header = test_utils::snapshot_header();
    header.id.audience = loss.row.audience.clone();
    header.counts = [0, 0, 0, 0, 0, 1];
    let (mut encoder, first) = SnapshotEncoder::start(header.clone()).unwrap();
    let mut decoder =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(encoder.header())).unwrap();
    let expected = SnapshotRecord::RetainedLoss(loss);
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
fn frozen_losses_cannot_carry_live_references() {
    for mut loss in test_utils::retained_losses() {
        let value = match &mut loss.values {
            RetainedValues::Cell { value, .. } => &mut value.value,
            RetainedValues::Row { cells, .. } => &mut cells.values_mut().next().unwrap().value,
        };
        value.parents.insert(
            coven_merge::ForeignKey::new(["parent"], "t", ["id"]),
            Parent {
                row: test_utils::row(),
                generation: 1,
            },
        );
        let (mut encoder, mut decoder) = encoder(1);
        assert!(encoder
            .record(SnapshotRecord::RetainedLoss(loss.clone()))
            .is_err());
        assert!(decoder.frame(&raw(&loss), &test_utils::oracle()).is_err());
    }
}
