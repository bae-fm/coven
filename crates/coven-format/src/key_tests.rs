use super::*;
use crate::tests::Generator;

fn real(n: f64) -> Value {
    Value::Real(n.to_bits())
}
fn ordered(values: &[Value]) {
    let keys: Vec<_> = values
        .iter()
        .map(|v| encode_key(std::slice::from_ref(v)).unwrap())
        .collect();
    assert!(keys.windows(2).all(|p| p[0] < p[1]), "{values:?}");
    for key in keys {
        assert_eq!(encode_key(&decode_key(&key).unwrap()).unwrap(), key);
    }
}
#[test]
fn integers_reals_and_mixed_types_follow_sqlite_order() {
    ordered(&[
        Value::Integer(i64::MIN),
        Value::Integer(-1000),
        Value::Integer(-2),
        Value::Integer(-1),
        Value::Integer(0),
        Value::Integer(1),
        Value::Integer(1000),
        Value::Integer(i64::MAX),
    ]);
    ordered(&[
        real(f64::NEG_INFINITY),
        real(-f64::MAX),
        real(-2.0),
        real(-1.5),
        real(-f64::MIN_POSITIVE),
        real(-f64::from_bits(1)),
        real(0.0),
        real(f64::from_bits(1)),
        real(f64::MIN_POSITIVE),
        real(1.5),
        real(2.0),
        real(f64::MAX),
        real(f64::INFINITY),
    ]);
    ordered(&[
        real(f64::NEG_INFINITY),
        real(-1e30),
        Value::Integer(i64::MIN),
        Value::Integer(-2),
        real(-1.5),
        Value::Integer(-1),
        real(-0.5),
        Value::Integer(0),
        real(0.5),
        Value::Integer(1),
        real(1.5),
        Value::Integer(2),
        Value::Integer(9_007_199_254_740_991),
        real(9_007_199_254_740_992.0),
        Value::Integer(9_007_199_254_740_993),
        Value::Integer(i64::MAX),
        real(9_223_372_036_854_775_808.0),
        real(f64::INFINITY),
        Value::Text("".into()),
        Value::Text("0".into()),
        Value::Blob(vec![]),
        Value::Blob(vec![0]),
    ]);
}
#[test]
fn equal_numbers_have_identical_keys_without_losing_integer_precision() {
    for n in [
        i64::MIN,
        -9_007_199_254_740_992,
        -1,
        0,
        1,
        9_007_199_254_740_992,
    ] {
        let integer = encode_key(&[Value::Integer(n)]).unwrap();
        assert_eq!(integer, encode_key(&[real(n as f64)]).unwrap());
        assert_eq!(decode_key(&integer).unwrap(), vec![Value::Integer(n)]);
    }
    for n in [i64::MIN + 1, 9_007_199_254_740_993, i64::MAX] {
        assert_eq!(
            decode_key(&encode_key(&[Value::Integer(n)]).unwrap()).unwrap(),
            [Value::Integer(n)]
        );
    }
}
#[test]
fn text_blobs_and_composite_prefixes_sort_without_length_prefix_interference() {
    ordered(&[
        Value::Text("".into()),
        Value::Text("\0".into()),
        Value::Text("\0a".into()),
        Value::Text("a".into()),
        Value::Text("aa".into()),
        Value::Text("b".into()),
        Value::Text("é".into()),
        Value::Text("日本語".into()),
    ]);
    ordered(&[
        Value::Blob(vec![]),
        Value::Blob(vec![0]),
        Value::Blob(vec![0, 0]),
        Value::Blob(vec![0, 255]),
        Value::Blob(vec![1]),
        Value::Blob(vec![255]),
    ]);
    let keys = [
        vec![Value::Integer(-1)],
        vec![Value::Integer(-1), Value::Blob(vec![255])],
        vec![Value::Integer(0)],
        vec![Value::Text("a".into())],
        vec![Value::Text("a".into()), Value::Integer(-1)],
        vec![Value::Text("a".into()), Value::Text("".into())],
        vec![Value::Text("a\0".into())],
    ];
    let encoded: Vec<_> = keys.iter().map(|k| encode_key(k).unwrap()).collect();
    assert!(encoded.windows(2).all(|p| p[0] < p[1]));
    for (key, bytes) in keys.iter().zip(encoded) {
        assert_eq!(&decode_key(&bytes).unwrap(), key);
    }
}
#[test]
fn null_nan_negative_zero_and_noncanonical_keys_are_refused() {
    for values in [
        vec![],
        vec![Value::Null],
        vec![real(f64::NAN)],
        vec![real(-0.0)],
    ] {
        assert!(encode_key(&values).is_err());
    }
    for bytes in [
        &[][..],
        &[0],
        &[0x20],
        &[0x20, 0, 1],
        &[0x20, 255, 0, 0],
        &[0x30, 0],
        &[0x13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255],
    ] {
        assert!(decode_key(bytes).is_err(), "{bytes:?}");
    }
    let mut number = encode_key(&[real(0.5)]).unwrap();
    *number.last_mut().unwrap() = 1;
    assert!(decode_key(&number).is_err());
}
#[test]
fn generated_real_values_round_trip_and_compare_numerically() {
    let mut source = Generator(0x1234_5678_9abc_def0u64);
    let mut values = Vec::new();
    for _ in 0..10000 {
        let value = f64::from_bits(source.next());
        if !value.is_nan() && value != 0.0 {
            values.push(value);
        }
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values.dedup();
    ordered(&values.into_iter().map(real).collect::<Vec<_>>());
}

#[test]
fn arbitrary_key_bytes_are_rejected_or_reencode_identically() {
    let mut source = Generator(0x89ab_cdef_0123_4567u64);
    for n in 0..25_000 {
        let mut bytes = Vec::new();
        for _ in 0..n % 64 {
            bytes.push(source.next() as u8);
        }
        if let Ok(values) = decode_key(&bytes) {
            assert_eq!(encode_key(&values).unwrap(), bytes);
        }
    }
}

#[test]
fn ordered_key_example_and_escaped_zero_bytes_match_d3() {
    let values = [Value::Integer(7), Value::Text("a".into())];
    let expected = crate::tests::hex("130434e00000000000000020610000");
    assert_eq!(encode_key(&values).unwrap(), expected);
    assert_eq!(decode_key(&expected).unwrap(), values);
    let values = [Value::Text("\0".into()), Value::Blob(vec![0, 255])];
    let expected = crate::tests::hex("2000ff00003000ffff0000");
    assert_eq!(encode_key(&values).unwrap(), expected);
    assert_eq!(decode_key(&expected).unwrap(), values);
}

#[test]
fn key_component_budget_is_independent_of_frame_collections() {
    let values = vec![Value::Integer(0); MAX_ITEMS];
    let bytes = encode_key(&values).unwrap();
    assert_eq!(bytes, vec![0x12; MAX_ITEMS]);
    assert_eq!(decode_key(&bytes).unwrap(), values);
    let mut row = crate::test_utils::write().parts.remove(0).rows.remove(0);
    row.row.key = bytes;
    assert_eq!(
        crate::write::RowChange::decode(&row.encode().unwrap()).unwrap(),
        row
    );
    let mut values = values;
    values.push(Value::Integer(0));
    let expected = || Error::Limit {
        field: "key components",
        actual: MAX_ITEMS + 1,
        maximum: MAX_ITEMS,
    };
    assert_eq!(encode_key(&values), Err(expected()));
    assert_eq!(decode_key(&vec![0x12; MAX_ITEMS + 1]), Err(expected()));
}

#[test]
fn encoded_key_byte_bound_includes_tags_escaping_and_terminators() {
    for value in [
        Value::Text("x".repeat(MAX_BYTES - 3)),
        Value::Blob(vec![1; MAX_BYTES - 3]),
    ] {
        let values = [value];
        let bytes = encode_key(&values).unwrap();
        assert_eq!(bytes.len(), MAX_BYTES);
        assert_eq!(decode_key(&bytes).unwrap(), values);
        let mut oversized = bytes;
        oversized.push(0x12);
        assert_eq!(
            decode_key(&oversized),
            Err(Error::Limit {
                field: "key bytes",
                actual: MAX_BYTES + 1,
                maximum: MAX_BYTES,
            })
        );
    }
    for value in [
        Value::Blob(vec![1; MAX_BYTES - 2]),
        Value::Blob(vec![0; MAX_BYTES / 2 - 1]),
    ] {
        assert_eq!(
            encode_key(&[value]),
            Err(Error::Limit {
                field: "key bytes",
                actual: MAX_BYTES + 1,
                maximum: MAX_BYTES,
            })
        );
    }
}
