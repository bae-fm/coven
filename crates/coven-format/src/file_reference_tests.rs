use super::*;

const ID: &str = "00112233-4455-6677-8899-aabbccddeeff";
const KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

#[test]
fn references_use_canonical_decimal_uuid_and_lowercase_key_encoding() {
    for device in [0, 1, 42, u64::MAX] {
        let reference = FileReference {
            device: DeviceId(device),
            id: FileId(uuid::Uuid::from_u128(0x00112233445566778899aabbccddeeff)),
            key: FileKey::from_bytes(std::array::from_fn(|i| (i % 16) as u8 * 17)),
        };
        let expected = format!("file {device} {ID} {KEY}");
        assert_eq!(reference.encode().as_str(), expected);
        let decoded = FileReference::decode(&expected).unwrap();
        assert_eq!(decoded.device, reference.device);
        assert_eq!(decoded.id, reference.id);
        assert_eq!(
            decoded.key.to_secret_bytes().as_bytes(),
            reference.key.to_secret_bytes().as_bytes()
        );
        assert_eq!(decoded.encode().as_str(), expected);
    }
}

#[test]
fn references_refuse_malformed_components_and_noncanonical_spellings() {
    for device in [
        "",
        "01",
        "00",
        "+1",
        "-1",
        "1.0",
        "0x1",
        "１",
        "18446744073709551616",
    ] {
        assert!(FileReference::decode(&format!("file {device} {ID} {KEY}")).is_err());
    }
    for id in [
        String::new(),
        ID.to_uppercase(),
        ID.replace('-', ""),
        format!("{{{ID}}}"),
        format!("urn:uuid:{ID}"),
        ID.replace('f', "g"),
        "not-a-uuid".into(),
    ] {
        assert!(FileReference::decode(&format!("file 1 {id} {KEY}")).is_err());
    }
    for key in [
        String::new(),
        KEY.to_uppercase(),
        KEY[..63].into(),
        format!("{KEY}0"),
        format!("{KEY}00"),
        KEY.replace('f', "g"),
        format!("{}é", &KEY[..62]),
        "音".repeat(32),
    ] {
        assert!(FileReference::decode(&format!("file 1 {ID} {key}")).is_err());
    }
}

#[test]
fn references_require_exact_components_and_ascii_spaces_without_trailing_input() {
    let valid = format!("file 1 {ID} {KEY}");
    for bad in [
        format!("uploaded 1 {ID} {KEY}"),
        format!("on 1 {ID} {KEY}"),
        "on 1".into(),
        "1".into(),
        valid.replacen("file", "File", 1),
        format!(" {valid}"),
        format!("{valid} "),
        format!("{valid}\n"),
        format!("{valid}\r\n"),
        format!("{valid}\0"),
        format!("{valid} extra"),
        format!("file {ID} {KEY}"),
        format!("file 1 {KEY}"),
        format!("file 1 {ID}"),
    ] {
        assert!(FileReference::decode(&bad).is_err(), "{bad:?}");
    }
    for (index, _) in valid.match_indices(' ') {
        for separator in ["", "  ", "\t", "\n", "\r", "\0", "\u{a0}"] {
            let mut bad = valid.clone();
            bad.replace_range(index..index + 1, separator);
            assert!(FileReference::decode(&bad).is_err(), "{bad:?}");
        }
    }
}

#[test]
fn references_and_diagnostics_never_disclose_local_paths_or_keys() {
    let text = format!("file 1 {ID} {KEY}");
    let reference = FileReference::decode(&text).unwrap();
    let encoded = reference.encode();
    assert_eq!(encoded.as_str(), text);
    assert!(!format!("{reference:?}").contains(KEY));
    assert!(!format!("{encoded:?}").contains(KEY));
    for path in [
        "/Users/ana/Music/private.flac",
        r"C:\Users\ana\Music\private.flac",
    ] {
        assert!(!encoded.as_str().contains(path));
        for bad in [
            path.into(),
            format!("on 1 {path}"),
            format!("file {path} {ID} {KEY}"),
            format!("file 1 {path} {KEY}"),
            format!("file 1 {ID} {path}"),
            format!("{text} {path}"),
        ] {
            let error = FileReference::decode(&bad).unwrap_err();
            assert_eq!(
                error,
                Error::Invalid {
                    field: "file reference",
                    rule: Rule::KeyEncoding
                }
            );
            for diagnostic in [error.to_string(), format!("{error:?}")] {
                assert!(!diagnostic.contains(path));
                assert!(!diagnostic.contains(KEY));
            }
        }
    }
}

#[test]
fn file_reference_matches_the_pinned_text_and_path() {
    let mut fixture = include_str!("../fixtures/uploaded-file.txt").lines();
    let path = fixture.next().unwrap();
    let text = fixture.next().unwrap();
    assert!(fixture.next().is_none());
    let reference = FileReference {
        device: DeviceId(1),
        id: FileId(uuid::Uuid::from_bytes([0x11; 16])),
        key: FileKey::from_bytes([0x77; 32]),
    };
    assert_eq!(reference.encode().as_str(), text);
    let decoded = FileReference::decode(text).unwrap();
    assert_eq!(decoded.encode().as_str(), text);
    assert_eq!(
        crate::path::ObjectPath::file(decoded.device, decoded.id).as_str(),
        path
    );
}

#[test]
fn every_truncation_is_rejected_and_accepted_mutations_reencode_exactly() {
    let text = format!("file 1 {ID} {KEY}");
    for end in 0..text.len() {
        assert!(FileReference::decode(&text[..end]).is_err());
    }
    for index in 0..text.len() {
        for bit in 0..8 {
            let mut bytes = text.as_bytes().to_vec();
            bytes[index] ^= 1 << bit;
            if let Ok(changed) = std::str::from_utf8(&bytes) {
                if let Ok(reference) = FileReference::decode(changed) {
                    assert_eq!(reference.encode().as_str(), changed);
                }
            }
        }
    }
}
