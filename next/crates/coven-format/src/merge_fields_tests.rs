use super::*;
use crate::snapshot::{SnapshotEncoder, SnapshotRecord};
use crate::test_utils as fixture;
use crate::value::EntryId;
use coven_foundation::id_source::DeviceId;
use coven_merge::{RowId, RowState};

fn take_field<T: std::fmt::Debug + PartialEq>(
    input: &mut Decoder<'_>,
    value: &T,
    encode: impl Fn(&T) -> Result<Vec<u8>, Error>,
    decode: impl Fn(&[u8]) -> Result<T, Error>,
) {
    let bytes = encode(value).unwrap();
    assert_eq!(input.take(bytes.len()).unwrap(), bytes);
    assert_eq!(decode(&bytes).unwrap(), *value);
    for end in 0..bytes.len() {
        assert!(
            decode(&bytes[..end]).is_err(),
            "accepted truncated field at {end}"
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(matches!(decode(&trailing), Err(Error::TrailingBytes)));
}

#[test]
fn metadata_fields_are_the_bytes_in_snapshot_merge_records() {
    let mut merged = fixture::merge_row();
    let mut cells = merged.state.cells().clone();
    cells.get_mut("x").unwrap().value.parents.insert(
        "parent_fk".into(),
        Parent {
            row: fixture::row(),
            generation: 1,
        },
    );
    merged.state = RowState::from_parts(
        merged.state.row().clone(),
        merged.state.generations().clone(),
        cells,
        merged.state.lost().clone(),
        &fixture::oracle(),
    )
    .unwrap();
    merged.removed = BTreeSet::from([
        Rule::ForeignKey("parent_fk".into()),
        Rule::Check("valid".into()),
        Rule::Unique("distinct".into()),
    ]);
    let mut header = fixture::snapshot_header();
    header.counts = [0, 0, 0, 1, 0];
    let (mut encoder, _) = SnapshotEncoder::start(header).unwrap();
    let frame = encoder
        .record(SnapshotRecord::Merge(merged.clone()))
        .unwrap();
    let mut input = Decoder::new(&frame[crate::FRAME_PREFIX_LEN..]).unwrap();
    assert_eq!(u8::get(&mut input).unwrap(), 3);
    assert_eq!(RowId::get(&mut input).unwrap(), *merged.state.row());
    assert_eq!(
        <BTreeMap<u64, WriteId> as Wire>::get(&mut input).unwrap(),
        *merged.state.generations()
    );
    assert_eq!(
        u32::get(&mut input).unwrap() as usize,
        merged.state.cells().len()
    );
    for (name, cell) in merged.state.cells() {
        assert_eq!(String::get(&mut input).unwrap(), *name);
        take_field(&mut input, &cell.write, encode_write_id, decode_write_id);
        // Check the whole column field and its nested parent map against the
        // same snapshot bytes, rather than constructing a parallel encoding.
        let bytes = encode_column_value(&cell.value).unwrap();
        let mut column = Decoder::new(&bytes).unwrap();
        assert_eq!(Value::get(&mut column).unwrap(), cell.value.value);
        take_field(
            &mut column,
            &cell.value.parents,
            encode_parents,
            decode_parents,
        );
        column.finish().unwrap();
        take_field(
            &mut input,
            &cell.value,
            encode_column_value,
            decode_column_value,
        );
    }
    assert_eq!(
        u32::get(&mut input).unwrap() as usize,
        merged.state.lost().len()
    );
    for (key, lost) in merged.state.lost() {
        assert_eq!(String::get(&mut input).unwrap(), key.column);
        take_field(&mut input, &key.write, encode_write_id, decode_write_id);
        assert_eq!(u64::get(&mut input).unwrap(), lost.incarnation);
        take_field(
            &mut input,
            &lost.value,
            encode_column_value,
            decode_column_value,
        );
        take_field(
            &mut input,
            &lost.replaced_by,
            encode_write_id,
            decode_write_id,
        );
    }
    take_field(&mut input, &merged.removed, encode_rules, decode_rules);
    input.finish().unwrap();
    encoder.finish().unwrap();
}

#[test]
fn applied_write_and_lost_write_fields_match_snapshot_records() {
    let (mut encoder, _) = SnapshotEncoder::start(fixture::snapshot_header()).unwrap();
    for record in fixture::snapshot_records() {
        let bytes = encoder.record(record.clone()).unwrap();
        let mut input = Decoder::new(&bytes[crate::FRAME_PREFIX_LEN + 1..]).unwrap();
        match record {
            SnapshotRecord::Write(write) => {
                take_field(&mut input, &write.id, encode_write_id, decode_write_id);
                take_field(
                    &mut input,
                    &write.timestamp,
                    encode_timestamp,
                    decode_timestamp,
                );
                take_field(
                    &mut input,
                    &write.had_read,
                    encode_write_positions,
                    decode_write_positions,
                );
                input.finish().unwrap();
            }
            SnapshotRecord::LostWrite(write) => {
                assert_eq!(
                    crate::write::WriteRecord::get(&mut input).unwrap(),
                    write.write
                );
                take_field(
                    &mut input,
                    &write.cause,
                    encode_lost_write_cause,
                    decode_lost_write_cause,
                );
                input.finish().unwrap();
            }
            SnapshotRecord::Synced(row) => {
                assert_eq!(RowId::get(&mut input).unwrap(), row.row);
                take_field(&mut input, &row.columns, encode_columns, decode_columns);
                input.finish().unwrap();
            }
            SnapshotRecord::Column(_) | SnapshotRecord::Merge(_) => {}
        }
    }
    encoder.finish().unwrap();
}

#[test]
fn removed_setters_and_rules_use_canonical_maps_and_sets() {
    let setters = BTreeMap::from([
        ("x".into(), fixture::position()),
        (
            "y".into(),
            WriteId {
                device: DeviceId(u64::MAX),
                number: u64::MAX,
            },
        ),
    ]);
    let bytes = encode_setters(&setters).unwrap();
    let mut input = Decoder::new(&bytes).unwrap();
    assert_eq!(u32::get(&mut input).unwrap(), 2);
    for (name, write) in &setters {
        assert_eq!(String::get(&mut input).unwrap(), *name);
        take_field(&mut input, write, encode_write_id, decode_write_id);
    }
    input.finish().unwrap();
    take_field(
        &mut Decoder::new(&bytes).unwrap(),
        &setters,
        encode_setters,
        decode_setters,
    );
    let rules = BTreeSet::from([Rule::DeletedCircle, Rule::OtherAudience]);
    let bytes = encode_rules(&rules).unwrap();
    take_field(
        &mut Decoder::new(&bytes).unwrap(),
        &rules,
        encode_rules,
        decode_rules,
    );
    let cause = LostWriteCause::Reset(EntryId {
        device: DeviceId(u64::MAX),
        number: u64::MAX,
    });
    let bytes = encode_lost_write_cause(&cause).unwrap();
    take_field(
        &mut Decoder::new(&bytes).unwrap(),
        &cause,
        encode_lost_write_cause,
        decode_lost_write_cause,
    );
}

#[test]
fn field_codecs_refuse_invalid_values_and_noncanonical_bytes() {
    let invalid = WriteId {
        device: DeviceId(1),
        number: 0,
    };
    assert!(encode_write_id(&invalid).is_err());
    assert!(decode_write_id(&[0; 16]).is_err());
    assert!(encode_write_positions(&WritePositions(vec![fixture::position(); 2])).is_err());
    assert!(encode_setters(&BTreeMap::from([("x".into(), invalid)])).is_err());
    assert!(encode_parents(&BTreeMap::from([(
        "fk".into(),
        Parent {
            row: RowId {
                key: vec![255],
                ..fixture::row()
            },
            generation: 1
        }
    )]))
    .is_err());
    assert!(encode_columns(&BTreeMap::from([("".into(), fixture::column(Value::Null))])).is_err());
    for real in [f64::NAN.to_bits(), (-0.0_f64).to_bits()] {
        assert!(encode_column_value(&fixture::column(Value::Real(real))).is_err());
    }
    assert!(encode_rules(&BTreeSet::from([Rule::Check("".into())])).is_err());
    assert!(decode_rules(&[0, 0, 0, 2, 3, 2]).is_err());
    assert!(decode_rules(&[0, 0, 0, 2, 2, 2]).is_err());
    assert!(decode_rules(&[0, 1, 0, 1]).is_err());
    assert!(decode_lost_write_cause(&[255]).is_err());
    assert!(encode_lost_write_cause(&LostWriteCause::Reset(EntryId {
        device: DeviceId(1),
        number: 0
    }))
    .is_err());
}
