use super::*;
use crate::test_utils::STORE_ID;

const PATH: &str =
    "10101010-1010-1010-1010-101010101010/files/1/11111111-1111-1111-1111-111111111111";

fn example(size: usize) -> (FileKey, Vec<u8>, Vec<u8>) {
    let key = FileKey::from_bytes([0x77; 32]);
    let plain: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
    let header = FileHeader::new(size as u64);
    let mut stored = header.encode().to_vec();
    for (index, chunk) in plain.chunks(CHUNK_SIZE).enumerate() {
        stored.extend(header.seal_chunk(&key, PATH, index as u64, chunk).unwrap());
    }
    (key, plain, stored)
}

#[test]
fn header_bytes_limits_offsets_and_empty_files_are_exact() {
    let header = FileHeader::new(65_537);
    assert_eq!(
        header.encode().as_slice(),
        [38, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1]
    );
    assert_eq!(header.size(), 65_537);
    assert_eq!(header.chunk_count(), 2);
    assert_eq!(
        header.chunk(0).unwrap(),
        FileChunk {
            index: 0,
            offset: 11,
            plaintext_length: 65_536
        }
    );
    assert_eq!(
        header.chunk(1).unwrap(),
        FileChunk {
            index: 1,
            offset: 11 + 65_536 + 16,
            plaintext_length: 1
        }
    );
    assert_eq!(header.encrypted_size().unwrap(), 11 + 65_537 + 32);
    assert!(header.chunk(2).is_err());
    assert!(header.chunk(u64::MAX).is_err());
    let empty = FileHeader::new(0);
    assert_eq!(FileHeader::decode(&empty.encode()).unwrap(), empty);
    assert_eq!(empty.chunk_count(), 0);
    assert!(empty.chunk(0).is_err());
    assert_eq!(empty.encrypted_size().unwrap(), 11);
    let huge = FileHeader::new(u64::MAX);
    assert!(huge.encrypted_size().is_err());
    assert!(huge.chunk(huge.chunk_count() - 1).is_err());
    for end in 0..FILE_HEADER_LEN {
        assert!(FileHeader::decode(&header.encode()[..end]).is_err());
    }
    let mut bytes = header.encode().to_vec();
    bytes.push(0);
    assert_eq!(FileHeader::decode(&bytes), Err(Error::TrailingBytes));
    let mut bytes = header.encode();
    bytes[2] = 2;
    assert_eq!(
        FileHeader::decode(&bytes),
        Err(Error::UnsupportedVersion(2))
    );
    assert!(matches!(
        FileHeader::decode(&[38, 0, 2]),
        Err(Error::UnsupportedVersion(2))
    ));
    bytes[0] = 7;
    assert!(matches!(
        FileHeader::decode(&bytes),
        Err(Error::UnknownTag { .. })
    ));
}

#[test]
fn each_chunk_opens_independently_and_requires_its_exact_length() {
    for size in [0, 1, 65_535, 65_536, 65_537, 131_072, 196_637] {
        let (key, plain, stored) = example(size);
        let header = FileHeader::decode(&stored[..FILE_HEADER_LEN]).unwrap();
        assert_eq!(header.encrypted_size().unwrap(), stored.len() as u64);
        for index in 0..header.chunk_count() {
            let chunk = header.chunk(index).unwrap();
            let start = chunk.offset as usize;
            let bytes = &stored[start..start + chunk.plaintext_length + FILE_CHUNK_TAG_LEN];
            let offset = index as usize * CHUNK_SIZE;
            assert_eq!(
                header.open_chunk(&key, PATH, index, bytes).unwrap(),
                plain[offset..offset + chunk.plaintext_length]
            );
            for end in 0..bytes.len() {
                assert!(matches!(
                    header.open_chunk(&key, PATH, index, &bytes[..end]),
                    Err(FileError::Format(Error::Truncated))
                ));
            }
            let mut extra = bytes.to_vec();
            extra.push(0);
            assert!(matches!(
                header.open_chunk(&key, PATH, index, &extra),
                Err(FileError::Format(Error::TrailingBytes))
            ));
        }
    }
    let header = FileHeader::new(65_537);
    let key = FileKey::from_bytes([0x77; 32]);
    assert!(header.seal_chunk(&key, PATH, 0, &[0; 65_535]).is_err());
    assert!(header.seal_chunk(&key, PATH, 1, &[0; 2]).is_err());
}

#[test]
fn an_unrequested_damaged_chunk_does_not_prevent_opening_another() {
    let (key, plain, mut stored) = example(3 * CHUNK_SIZE + 29);
    let header = FileHeader::decode(&stored[..FILE_HEADER_LEN]).unwrap();
    for index in [0, 2, 3] {
        let chunk = header.chunk(index).unwrap();
        stored[chunk.offset as usize] ^= 1;
        let start = chunk.offset as usize;
        assert!(header
            .open_chunk(
                &key,
                PATH,
                index,
                &stored[start..start + chunk.plaintext_length + FILE_CHUNK_TAG_LEN]
            )
            .is_err());
    }
    let chunk = header.chunk(1).unwrap();
    let start = chunk.offset as usize;
    let bytes = &stored[start..start + chunk.plaintext_length + FILE_CHUNK_TAG_LEN];
    assert_eq!(
        header.open_chunk(&key, PATH, 1, bytes).unwrap(),
        plain[CHUNK_SIZE..2 * CHUNK_SIZE]
    );
    assert!(header
        .open_chunk(&FileKey::from_bytes([0x78; 32]), PATH, 1, bytes)
        .is_err());
}

// Public test material sealed independently with Python ctypes and libsodium.
#[test]
fn file_fixture_opens_and_reseals_each_chunk() {
    let bytes = crate::tests::hex(include_str!("../fixtures/file.hex"));
    let mut reference = include_str!("../fixtures/file-reference.txt").lines();
    assert_eq!(reference.next().unwrap(), PATH);
    let reference =
        crate::file_reference::FileReference::decode(reference.next().unwrap()).unwrap();
    let path = crate::path::ObjectPath::file(STORE_ID, reference.device, reference.id);
    assert_eq!(path.as_str(), PATH);
    let key = reference.key;
    let header = FileHeader::decode(&bytes[..FILE_HEADER_LEN]).unwrap();
    assert_eq!(header.size(), 65_565);
    assert_eq!(header.chunk_count(), 2);
    assert_eq!(header.encrypted_size().unwrap(), bytes.len() as u64);
    let plain: Vec<_> = (0..65_565).map(|i| (i % 251) as u8).collect();
    let mut encoded = header.encode().to_vec();
    for (index, plain) in plain.chunks(CHUNK_SIZE).enumerate() {
        let chunk = header.chunk(index as u64).unwrap();
        let start = chunk.offset as usize;
        let sealed = &bytes[start..start + chunk.plaintext_length + FILE_CHUNK_TAG_LEN];
        assert_eq!(
            header.open_chunk(&key, PATH, index as u64, sealed).unwrap(),
            plain
        );
        for wrong_path in [
            "10101010-1010-1010-1010-101010101010/files/2/11111111-1111-1111-1111-111111111111",
            "10101010-1010-1010-1010-101010101010/files/11111111-1111-1111-1111-111111111111",
        ] {
            assert!(header
                .open_chunk(&key, wrong_path, index as u64, sealed)
                .is_err());
        }
        encoded.extend(header.seal_chunk(&key, PATH, index as u64, plain).unwrap());
    }
    assert_eq!(encoded, bytes);
    crate::tests::mutations(&header.encode(), |changed| {
        if let Ok(header) = FileHeader::decode(changed) {
            assert_eq!(header.encode(), changed);
        }
    });
}
