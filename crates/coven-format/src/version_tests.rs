use super::*;
use crate::{
    sealed_slot::{SlotKind, SlotObjectPrefix},
    sealed_write::WriteObjectPrefix,
    Object,
};

#[test]
fn recorded_write_format_round_trips_the_fixture_prefix() {
    let bytes = crate::tests::hex(include_str!("../fixtures/sealed-write.hex"));
    let prefix_bytes = &bytes[..WriteObjectPrefix::length(&bytes).unwrap()];
    let prefix = WriteObjectPrefix::decode(prefix_bytes).unwrap();
    assert_eq!(prefix.format, FormatVersion::V1);
    assert_eq!(prefix.encode().unwrap(), prefix_bytes);
}

#[test]
fn recorded_entry_format_encodes_both_its_frame_and_envelope() {
    let entry = crate::test_utils::creation_change();
    let object = Object::Admin(entry.clone());
    let frame = object.encode_in(FormatVersion::V1).unwrap();
    let format = FormatVersion::decode(&frame).unwrap();
    assert_eq!(format.number(), 1);
    assert_eq!(
        Object::decode(&frame).unwrap().encode_in(format).unwrap(),
        frame
    );
    let prefix = SlotObjectPrefix {
        format,
        kind: SlotKind::Admin,
        key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
    };
    let bytes = crate::tests::hex(include_str!("../fixtures/sealed-admin.hex"));
    let encoded = prefix.encode().unwrap();
    assert_eq!(encoded, bytes[..encoded.len()]);
}

#[test]
fn unsupported_formats_are_reported_before_version_specific_fields() {
    type Decode = fn(&[u8]) -> Result<(), Error>;
    let decoders: [(u8, Decode); 8] = [
        (4, |bytes| Object::decode(bytes).map(|_| ())),
        (12, |bytes| Object::decode(bytes).map(|_| ())),
        (32, |bytes| WriteObjectPrefix::decode(bytes).map(|_| ())),
        (33, |bytes| {
            crate::sealed_slot::SlotObjectPrefix::decode(bytes).map(|_| ())
        }),
        (34, |bytes| {
            crate::sealed_snapshot::SnapshotObjectPrefix::decode(bytes).map(|_| ())
        }),
        (36, |bytes| {
            crate::sealed_single::SingleChunkObject::decode(bytes).map(|_| ())
        }),
        (38, |bytes| {
            crate::file::FileHeader::decode(bytes).map(|_| ())
        }),
        (39, |bytes| {
            crate::sealed_floor::FloorPrefixDecoder::start(bytes).map(|_| ())
        }),
    ];
    for version in [2, u16::MAX, 0] {
        for (kind, decode) in decoders {
            let mut prefix = vec![kind];
            prefix.extend_from_slice(&version.to_be_bytes());
            assert_eq!(
                FormatVersion::decode(&prefix),
                Err(Error::UnsupportedVersion(version))
            );
            assert_eq!(
                decode(&prefix),
                Err(Error::UnsupportedVersion(version)),
                "kind {kind}"
            );
        }
    }
    for prefix in [&[][..], &[4], &[4, 0]] {
        assert_eq!(FormatVersion::decode(prefix), Err(Error::Truncated));
    }
}
