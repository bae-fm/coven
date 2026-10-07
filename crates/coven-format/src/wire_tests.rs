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
