use super::*;

#[test]
fn references_carry_the_uploader_and_refuse_noncanonical_or_secret_leaking_encodings() {
    let reference = UploadedFileReference {
        device: DeviceId(u64::MAX),
        id: FileId(uuid::Uuid::from_u128(0xabcd)),
        key: FileKey::from_bytes([0xef; 32]),
    };
    let text = reference.encode();
    let decoded = UploadedFileReference::decode(text.as_str()).unwrap();
    assert_eq!(decoded.device, reference.device);
    assert_eq!(decoded.id, reference.id);
    assert_eq!(decoded.encode().as_str(), text.as_str());
    assert!(!format!("{decoded:?}").contains(&"ef".repeat(32)));
    assert!(!format!("{text:?}").contains(&"ef".repeat(32)));
    for bad in [
        text.as_str()
            .replace("18446744073709551615", "18446744073709551616"),
        text.as_str().replace("18446744073709551615", "01"),
        text.as_str().replace("abcd", "ABCD"),
        text.as_str().replace("ef", "EF"),
        text.as_str().replace("uploaded ", "uploaded  "),
        text.as_str()
            .replace("uploaded 18446744073709551615 ", "uploaded "),
        format!("{} extra", text.as_str()),
        text.as_str()[..text.as_str().len() - 1].into(),
    ] {
        assert!(UploadedFileReference::decode(&bad).is_err());
    }
}

#[test]
fn uploaded_file_reference_matches_the_pinned_text_and_path() {
    let mut fixture = include_str!("../fixtures/uploaded-file.txt").lines();
    let path = fixture.next().unwrap();
    let text = fixture.next().unwrap();
    assert!(fixture.next().is_none());
    let reference = UploadedFileReference {
        device: DeviceId(1),
        id: FileId(uuid::Uuid::from_bytes([0x11; 16])),
        key: FileKey::from_bytes([0x77; 32]),
    };
    assert_eq!(reference.encode().as_str(), text);
    let decoded = UploadedFileReference::decode(text).unwrap();
    assert_eq!(decoded.encode().as_str(), text);
    assert_eq!(
        crate::path::ObjectPath::file(decoded.device, decoded.id).as_str(),
        path
    );
}
