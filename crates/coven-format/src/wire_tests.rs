use super::*;

#[test]
fn nested_collection_budget_is_identical_when_encoding_and_decoding() {
    let values = vec![vec![0u8; 33_000], vec![0u8; 33_000]];
    let mut out = Encoder::new();
    assert!(matches!(
        values.put(&mut out),
        Err(Error::Limit {
            field: "total collection items",
            ..
        })
    ));
    let mut bytes = vec![];
    bytes.extend_from_slice(&2u32.to_be_bytes());
    for _ in 0..2 {
        bytes.extend_from_slice(&33_000u32.to_be_bytes());
        bytes.extend_from_slice(&vec![0; 33_000]);
    }
    let mut input = Decoder::new(&bytes).unwrap();
    assert!(matches!(
        Vec::<Vec<u8>>::get(&mut input),
        Err(Error::Limit {
            field: "total collection items",
            ..
        })
    ));
}

#[test]
fn maps_and_sets_reject_duplicate_or_descending_keys_before_collecting() {
    for keys in [[2u8, 1], [1, 1]] {
        let bytes = [0, 0, 0, 2, keys[0], 3, keys[1], 4];
        let mut input = Decoder::new(&bytes).unwrap();
        assert!(matches!(
            <BTreeMap<u8, u8> as Wire>::get(&mut input),
            Err(Error::Invalid {
                rule: Rule::Order,
                ..
            })
        ));
        let bytes = [0, 0, 0, 2, keys[0], keys[1]];
        let mut input = Decoder::new(&bytes).unwrap();
        assert!(matches!(
            <BTreeSet<u8> as Wire>::get(&mut input),
            Err(Error::Invalid {
                rule: Rule::Order,
                ..
            })
        ));
    }
    let mut input = Decoder::new(&[0, 0, 0, 2, 1, 2, 3]).unwrap();
    assert_eq!(
        <BTreeMap<u8, u8> as Wire>::get(&mut input),
        Err(Error::Truncated)
    );
    let mut input = Decoder::new(&[255; 4]).unwrap();
    assert!(matches!(
        <BTreeMap<u8, u8> as Wire>::get(&mut input),
        Err(Error::Limit { .. })
    ));
}

#[test]
fn name_length_is_checked_before_reading_or_allocating_the_name() {
    let frame = crate::encode_frame(2, &1025u32).unwrap();
    assert!(matches!(
        crate::write::RowChange::decode(&frame),
        Err(crate::Error::Limit { field: "name", .. })
    ));
}

#[test]
fn integers_and_options_have_fixed_big_endian_bytes() {
    let mut out = Encoder::new();
    0x12u8.put(&mut out).unwrap();
    0x3456u16.put(&mut out).unwrap();
    0x789a_bcdeu32.put(&mut out).unwrap();
    0x0123_4567_89ab_cdefu64.put(&mut out).unwrap();
    (-2i64).put(&mut out).unwrap();
    None::<u16>.put(&mut out).unwrap();
    Some(0x1234u16).put(&mut out).unwrap();
    let expected = crate::tests::hex("123456789abcde0123456789abcdeffffffffffffffffe00011234");
    assert_eq!(out.bytes, expected);
    let mut input = Decoder::new(&expected).unwrap();
    assert_eq!(u8::get(&mut input).unwrap(), 0x12);
    assert_eq!(u16::get(&mut input).unwrap(), 0x3456);
    assert_eq!(u32::get(&mut input).unwrap(), 0x789a_bcde);
    assert_eq!(u64::get(&mut input).unwrap(), 0x0123_4567_89ab_cdef);
    assert_eq!(i64::get(&mut input).unwrap(), -2);
    assert_eq!(Option::<u16>::get(&mut input).unwrap(), None);
    assert_eq!(Option::<u16>::get(&mut input).unwrap(), Some(0x1234));
    input.finish().unwrap();
}

#[test]
fn byte_bounds_and_remaining_lengths_apply_before_copying() {
    for length in [0, MAX_BYTES] {
        let value = vec![0xa5; length];
        let mut out = Encoder::new();
        put_blob(&value, &mut out).unwrap();
        assert_eq!(&out.bytes[..4], &(length as u32).to_be_bytes());
        let mut input = Decoder::new(&out.bytes).unwrap();
        assert_eq!(get_blob(&mut input).unwrap(), value);
        input.finish().unwrap();
    }
    let length = MAX_BYTES + 1;
    let expected = Error::Limit {
        field: "bytes",
        actual: length,
        maximum: MAX_BYTES,
    };
    assert_eq!(
        put_blob(&vec![0; length], &mut Encoder::new()),
        Err(expected)
    );
    let prefix = (length as u32).to_be_bytes();
    assert_eq!(
        get_blob(&mut Decoder::new(&prefix).unwrap()),
        Err(Error::Limit {
            field: "bytes",
            actual: length,
            maximum: MAX_BYTES,
        })
    );
    for length in [1, MAX_BYTES] {
        let prefix = (length as u32).to_be_bytes();
        assert_eq!(
            get_blob(&mut Decoder::new(&prefix).unwrap()),
            Err(Error::Truncated)
        );
    }
}

#[test]
fn text_preserves_empty_nul_and_distinct_unicode_spellings() {
    let mut encodings = BTreeSet::new();
    for value in ["", "\0", "é", "e\u{301}"] {
        let mut out = Encoder::new();
        value.to_owned().put(&mut out).unwrap();
        assert_eq!(&out.bytes[4..], value.as_bytes());
        let mut input = Decoder::new(&out.bytes).unwrap();
        assert_eq!(String::get(&mut input).unwrap(), value);
        input.finish().unwrap();
        assert!(encodings.insert(out.bytes));
    }
}

#[test]
fn frame_bound_counts_the_seven_byte_prefix() {
    let payload = MAX_OBJECT - crate::FRAME_PREFIX_LEN;
    let mut prefix = vec![7, 0, 1];
    prefix.extend_from_slice(&(payload as u32).to_be_bytes());
    assert_eq!(crate::frame_length(&prefix), Ok(MAX_OBJECT));
    assert!(matches!(decode_frame(&prefix), Err(Error::Truncated)));
    prefix[3..].copy_from_slice(&((payload + 1) as u32).to_be_bytes());
    assert_eq!(
        crate::frame_length(&prefix),
        Err(Error::Limit {
            field: "frame payload",
            actual: payload + 1,
            maximum: payload,
        })
    );
}
