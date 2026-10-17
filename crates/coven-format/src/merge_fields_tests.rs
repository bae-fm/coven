use super::*;
use crate::error::Rule as FormatRule;
use crate::snapshot::{SnapshotEncoder, SnapshotRecord};
use crate::test_utils as fixture;
use crate::value::Slot;
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
    header.counts = [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0];
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
                assert_eq!(Slot::get(&mut input).unwrap(), write.slot);
                take_field(
                    &mut input,
                    &write.timestamp,
                    encode_timestamp,
                    decode_timestamp,
                );
                assert_eq!(u64::get(&mut input).unwrap(), write.read_through);
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
            SnapshotRecord::Authored(authored) => {
                // The verdict is stored locally with the same bytes it ends with.
                let verdict = encode_change_verdict(&authored.verdict).unwrap();
                assert!(bytes.ends_with(&verdict));
                assert_eq!(decode_change_verdict(&verdict).unwrap(), authored.verdict);
            }
            SnapshotRecord::Column(_)
            | SnapshotRecord::Merge(_)
            | SnapshotRecord::Creation(_)
            | SnapshotRecord::Evidence(_)
            | SnapshotRecord::Admin(_)
            | SnapshotRecord::KeyCopy(_)
            | SnapshotRecord::Announcement(_)
            | SnapshotRecord::Claim(_) => {}
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
    let rules = BTreeSet::from([
        Rule::DeletedCircle(fixture::loss_entry()),
        Rule::OtherAudience,
    ]);
    let bytes = encode_rules(&rules).unwrap();
    take_field(
        &mut Decoder::new(&bytes).unwrap(),
        &rules,
        encode_rules,
        decode_rules,
    );
    let exclusion = (
        WriteId {
            device: DeviceId(u64::MAX),
            number: u64::MAX,
        },
        u32::MAX,
    );
    let bytes = encode_exclusion(exclusion.0, exclusion.1).unwrap();
    take_field(
        &mut Decoder::new(&bytes).unwrap(),
        &exclusion,
        |(write, version)| encode_exclusion(*write, *version),
        decode_exclusion,
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
        encode_exclusion(invalid, 1).unwrap_err(),
    ] {
        assert_eq!(
            error,
            Error::Invalid {
                field: "positive number",
                rule: FormatRule::Required
            }
        );
    }
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
    for bytes in [
        vec![0, 0, 0, 2, 3, 3],
        [
            vec![0, 0, 0, 2, 3],
            encode(&Rule::DeletedCircle(fixture::loss_entry())).unwrap(),
        ]
        .concat(),
    ] {
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
        decode_change_verdict(&[255]),
        Err(Error::UnknownTag {
            field: "change verdict",
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
fn schema_loss_uses_a_positive_u32_while_reset_ignores_retain_their_entry() {
    use crate::snapshot_rows::ChangeVerdict;
    let write = WriteId {
        device: DeviceId(1),
        number: 1,
    };
    for version in [1, u32::MAX] {
        let bytes = encode_exclusion(write, version).unwrap();
        assert_eq!(&bytes[16..], version.to_be_bytes());
        assert_eq!(decode_exclusion(&bytes).unwrap(), (write, version));
        let verdict = ChangeVerdict::Excluded(version);
        let bytes = encode_change_verdict(&verdict).unwrap();
        assert_eq!(bytes, [vec![2], version.to_be_bytes().to_vec()].concat());
        assert_eq!(decode_change_verdict(&bytes).unwrap(), verdict);
    }
    // Resets create no losses; an ignored change keeps the reset's entry.
    let entry = Slot(u64::MAX);
    let bytes = encode_change_verdict(&ChangeVerdict::Ignored(entry)).unwrap();
    assert_eq!(bytes[0], 5);
    assert_eq!(
        decode_change_verdict(&bytes).unwrap(),
        ChangeVerdict::Ignored(entry)
    );
    for error in [
        encode_exclusion(write, 0).unwrap_err(),
        decode_exclusion(&[[0; 7].as_slice(), &[1], &[0; 7], &[1], &[0; 4]].concat()).unwrap_err(),
        encode_change_verdict(&ChangeVerdict::Excluded(0)).unwrap_err(),
        decode_change_verdict(&[2, 0, 0, 0, 0]).unwrap_err(),
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

#[test]
fn constraint_order_is_logical_with_absent_predicates_first() {
    use coven_merge::{ForeignKey, UniqueConstraint};
    let foreign = [
        ForeignKey::new(["aa"], "z", ["z"]),
        ForeignKey::new(["b"], "aa", ["z"]),
        ForeignKey::new(["b"], "b", ["aa"]),
        ForeignKey::new(["b"], "b", ["b"]),
    ];
    assert!(foreign.windows(2).all(|p| p[0] < p[1]));
    let unique = [
        UniqueConstraint::from(["aa"]),
        UniqueConstraint::from(["b"]),
        UniqueConstraint {
            terms: vec!["b".into()],
            partial: Some(String::new()),
        },
        UniqueConstraint {
            terms: vec!["b".into()],
            partial: Some("active=1".into()),
        },
    ];
    assert!(unique.windows(2).all(|p| p[0] < p[1]));
    let rules = foreign
        .into_iter()
        .map(Rule::ForeignKey)
        .chain(unique.into_iter().map(Rule::Unique))
        .collect();
    let bytes = encode_rules(&rules).unwrap();
    assert_eq!(decode_rules(&bytes).unwrap(), rules);
}

#[test]
fn deleted_circle_rules_keep_canonical_entry_identities_and_refuse_zero() {
    let entries = [Slot(1), Slot(u64::MAX), Slot(u64::MAX - 1)];
    let rules = entries.map(Rule::DeletedCircle);
    let expected = BTreeSet::from(rules.clone());
    assert_eq!(
        decode_rules(&encode_rules(&expected).unwrap()).unwrap(),
        expected
    );
    for noncanonical in [
        vec![rules[1].clone(), rules[0].clone()],
        vec![rules[0].clone(), rules[0].clone()],
    ] {
        assert_eq!(
            decode_rules(&encode(&noncanonical).unwrap()),
            Err(Error::Invalid {
                field: "set",
                rule: FormatRule::Order,
            })
        );
    }
    let invalid = BTreeSet::from([Rule::DeletedCircle(Slot(0))]);
    for error in [
        encode_rules(&invalid).unwrap_err(),
        decode_rules(&[0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap_err(),
    ] {
        assert_eq!(
            error,
            Error::Invalid {
                field: "positive number",
                rule: FormatRule::Required
            }
        );
    }
}

#[test]
fn unnamed_check_rules_retain_full_expressions_in_loss_fields() {
    let rules: BTreeSet<_> = [
        "\"n\">0",
        "\"n\"<10",
        "\"n\">1",
        "[n] /* expression */ > 0",
        "named check",
    ]
    .into_iter()
    .map(|expression| Rule::Check(expression.into()))
    .collect();
    let encoded = encode_rules(&rules).unwrap();
    assert_eq!(decode_rules(&encoded).unwrap(), rules);
    let loss = crate::loss::Loss::removed(&fixture::merge_row().state, rules);
    let bytes = encode_loss(&loss).unwrap();
    assert_eq!(decode_loss(&bytes).unwrap(), loss);
    let mut header = fixture::snapshot_header();
    header.counts = [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
    let (mut encoder, start) = SnapshotEncoder::start(header.clone()).unwrap();
    let mut decoder =
        crate::snapshot::SnapshotDecoder::start(&start, &fixture::snapshot_prefix(&header))
            .unwrap();
    let record = SnapshotRecord::Loss(loss);
    let frame = encoder.record(record.clone()).unwrap();
    assert_eq!(
        decoder.frame(&frame, &fixture::oracle()).unwrap(),
        Some(record)
    );
    decoder
        .frame(&encoder.finish().unwrap(), &fixture::oracle())
        .unwrap();
    decoder.finish().unwrap();
}
