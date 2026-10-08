use super::*;
use crate::error::Rule as FormatRule;
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
        assert_eq!(
            decode(&bytes[..end]),
            Err(Error::Truncated),
            "truncated field at {end}"
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
        coven_merge::ForeignKey::new(["parent_fk"], "t", ["id"]),
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
    input.finish().unwrap();
    encoder.finish().unwrap();
}

#[test]
fn applied_write_and_loss_fields_match_snapshot_records() {
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
            SnapshotRecord::Loss(loss) => {
                assert_eq!(
                    input
                        .take(bytes.len() - crate::FRAME_PREFIX_LEN - 1)
                        .unwrap(),
                    encode_loss(&loss).unwrap()
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
    for error in [
        encode_write_id(&invalid).unwrap_err(),
        decode_write_id(&[0; 16]).unwrap_err(),
        encode_setters(&BTreeMap::from([("x".into(), invalid)])).unwrap_err(),
        encode_lost_write_cause(&LostWriteCause::Reset(EntryId {
            device: DeviceId(1),
            number: 0,
        }))
        .unwrap_err(),
    ] {
        assert_eq!(
            error,
            Error::Invalid {
                field: "log number",
                rule: FormatRule::Required
            }
        );
    }
    assert_eq!(
        encode_write_positions(&WritePositions(vec![fixture::position(); 2])),
        Err(Error::Invalid {
            field: "positions",
            rule: FormatRule::Order
        })
    );
    assert_eq!(
        encode_parents(&BTreeMap::from([(
            coven_merge::ForeignKey::new(["fk"], "t", ["id"]),
            Parent {
                row: RowId {
                    key: vec![255],
                    ..fixture::row()
                },
                generation: 1
            }
        )])),
        Err(Error::Invalid {
            field: "key",
            rule: FormatRule::KeyEncoding
        })
    );
    assert_eq!(
        encode_columns(&BTreeMap::from([("".into(), fixture::column(Value::Null))])),
        Err(Error::Invalid {
            field: "name",
            rule: FormatRule::Required
        })
    );
    for real in [f64::NAN.to_bits(), (-0.0_f64).to_bits()] {
        assert_eq!(
            encode_column_value(&fixture::column(Value::Real(real))),
            Err(Error::Invalid {
                field: "real",
                rule: FormatRule::Real
            })
        );
    }
    for bytes in [[0, 0, 0, 2, 3, 2], [0, 0, 0, 2, 2, 2]] {
        assert_eq!(
            decode_rules(&bytes),
            Err(Error::Invalid {
                field: "set",
                rule: FormatRule::Order
            })
        );
    }
    assert_eq!(
        decode_rules(&[0, 1, 0, 1]),
        Err(Error::Limit {
            field: "collection",
            actual: 65_537,
            maximum: crate::wire::MAX_ITEMS,
        })
    );
    assert_eq!(
        decode_lost_write_cause(&[255]),
        Err(Error::UnknownTag {
            field: "lost write cause",
            tag: 255
        })
    );
}

#[test]
fn foreign_key_identity_keeps_both_sides_and_column_order() {
    use coven_merge::ForeignKey;
    let keys = [
        ForeignKey::new(["parent"], "lefts", ["id"]),
        ForeignKey::new(["parent"], "rights", ["id"]),
        ForeignKey::new(["parent"], "rights", ["alternate"]),
        ForeignKey::new(["a", "bc"], "pairs", ["id", "locale"]),
        ForeignKey::new(["ab", "c"], "pairs", ["id", "locale"]),
        ForeignKey::new(["a", "bc"], "pairs", ["locale", "id"]),
    ];
    let parents = keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                Parent {
                    row: RowId {
                        table: key.parent.clone(),
                        ..fixture::row()
                    },
                    generation: 1,
                },
            )
        })
        .collect();
    let bytes = encode_parents(&parents).unwrap();
    assert_eq!(decode_parents(&bytes).unwrap(), parents);
    assert_eq!(parents.len(), keys.len());
    let rules = keys.into_iter().map(Rule::ForeignKey).collect();
    assert_eq!(decode_rules(&encode_rules(&rules).unwrap()).unwrap(), rules);
    for (key, field, rule) in [
        (
            ForeignKey::new([], "parents", []),
            "constraint columns",
            FormatRule::Required,
        ),
        (
            ForeignKey::new(["parent"], "", ["id"]),
            "name",
            FormatRule::Required,
        ),
        (
            ForeignKey::new(["parent"], "parents", [""]),
            "name",
            FormatRule::Required,
        ),
        (
            ForeignKey::new(["parent"], "parents", ["id", "extra"]),
            "foreign key columns",
            FormatRule::ForeignKeyColumns,
        ),
    ] {
        let rules = [Rule::ForeignKey(key)].into();
        // Bypass validation to exercise rejection by the field decoder too.
        for error in [
            encode_rules(&rules).unwrap_err(),
            decode_rules(&encode(&rules).unwrap()).unwrap_err(),
        ] {
            assert_eq!(error, Error::Invalid { field, rule });
        }
    }
}

#[test]
fn unique_identity_retains_terms_order_and_partial_predicate() {
    use coven_merge::UniqueConstraint;
    let identities = [
        UniqueConstraint::from(["title"]),
        UniqueConstraint::from(["lower(title)"]),
        UniqueConstraint {
            terms: vec!["title".into()],
            partial: Some("active=1".into()),
        },
        UniqueConstraint::from(["folder", "title"]),
        UniqueConstraint::from(["title", "folder"]),
        UniqueConstraint::from(["(1)"]),
    ];
    let mut encodings = BTreeSet::new();
    for identity in &identities {
        let bytes = encode_unique_constraint(identity).unwrap();
        take_field(
            &mut Decoder::new(&bytes).unwrap(),
            identity,
            encode_unique_constraint,
            decode_unique_constraint,
        );
        encodings.insert(bytes);
    }
    assert_eq!(encodings.len(), identities.len());
    let rules = identities.into_iter().map(Rule::Unique).collect();
    assert_eq!(decode_rules(&encode_rules(&rules).unwrap()).unwrap(), rules);
    let empty = UniqueConstraint::from([]);
    for error in [
        encode_unique_constraint(&empty).unwrap_err(),
        decode_unique_constraint(&encode(&empty).unwrap()).unwrap_err(),
    ] {
        assert_eq!(
            error,
            Error::Invalid {
                field: "unique terms",
                rule: FormatRule::Required
            }
        );
    }
}
#[test]
fn schema_loss_uses_a_positive_u32_while_reset_retains_its_entry() {
    use crate::snapshot_rows::LostWriteCause;
    for version in [1, u32::MAX] {
        let cause = LostWriteCause::SchemaChange(version);
        let bytes = encode_lost_write_cause(&cause).unwrap();
        assert_eq!(bytes, [vec![0], version.to_be_bytes().to_vec()].concat());
        assert_eq!(decode_lost_write_cause(&bytes).unwrap(), cause);
    }
    for error in [
        encode_lost_write_cause(&LostWriteCause::SchemaChange(0)).unwrap_err(),
        decode_lost_write_cause(&[0, 0, 0, 0, 0]).unwrap_err(),
    ] {
        assert_eq!(
            error,
            Error::Invalid {
                field: "breaking schema version",
                rule: FormatRule::Required
            }
        );
    }
}

#[test]
fn check_and_unique_expressions_have_text_bounds() {
    use coven_merge::UniqueConstraint;
    for text in [
        String::new(),
        "a\0b".into(),
        "x".repeat(crate::wire::MAX_BYTES),
        "x".repeat(crate::wire::MAX_BYTES + 1),
    ] {
        for rule in [
            Rule::Check(text.clone()),
            Rule::Unique(UniqueConstraint {
                terms: vec![text.clone()],
                partial: None,
            }),
            Rule::Unique(UniqueConstraint {
                terms: vec!["id".into()],
                partial: Some(text.clone()),
            }),
        ] {
            let rules = BTreeSet::from([rule]);
            let encoded = encode_rules(&rules);
            if text.len() <= crate::wire::MAX_BYTES {
                assert_eq!(decode_rules(&encoded.unwrap()).unwrap(), rules);
            } else {
                assert_eq!(
                    encoded,
                    Err(Error::Limit {
                        field: "text",
                        actual: text.len(),
                        maximum: crate::wire::MAX_BYTES
                    })
                );
            }
        }
    }
}
