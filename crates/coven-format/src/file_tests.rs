use super::*;

const PATH: &str = "files/1/11111111-1111-1111-1111-111111111111";

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
    assert_eq!(
        FileObject::decode(&empty.encode())
            .unwrap()
            .encode()
            .unwrap(),
        empty.encode()
    );
    let huge = FileHeader::new(u64::MAX);
    assert!(huge.encrypted_size().is_err());
    assert!(huge.chunk(huge.chunk_count() - 1).is_err());
    assert!(huge.range(0..u64::MAX).is_err());
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
        FileObject::decode(&[38, 0, 2]),
        Err(Error::UnsupportedVersion(2))
    ));
    bytes[0] = 7;
    assert!(matches!(
        FileHeader::decode(&bytes),
        Err(Error::UnknownTag { .. })
    ));
}

#[test]
fn range_reads_fetch_and_open_only_the_covering_chunks() {
    let (key, plain, mut stored) = example(3 * 65_536 + 29);
    let object = FileObject::decode(&stored).unwrap();
    assert_eq!(object.encode().unwrap(), stored);
    for range in [
        0..0,
        0..1,
        0..65_536,
        65_535..65_537,
        65_536..131_072,
        130_000..196_608,
        196_608..196_637,
        196_637..196_637,
        0..196_637,
    ] {
        let selected = object.header().range(range.clone()).unwrap();
        let fetch = selected.encrypted_range().unwrap();
        let bytes = &stored[fetch.start as usize..fetch.end as usize];
        assert_eq!(
            selected.open(&key, PATH, bytes).unwrap(),
            plain[range.start as usize..range.end as usize]
        );
        assert_eq!(
            object.read_range(&key, PATH, range.clone()).unwrap(),
            plain[range.start as usize..range.end as usize]
        );
        let mut extra = bytes.to_vec();
        extra.push(0);
        assert!(selected.open(&key, PATH, &extra).is_err());
        if !bytes.is_empty() {
            assert!(selected
                .open(&key, PATH, &bytes[..bytes.len() - 1])
                .is_err());
        }
    }
    let selected = object.header().range(65_537..131_071).unwrap();
    let fetch = selected.encrypted_range().unwrap();
    assert_eq!(fetch, 11 + 65_552..11 + 2 * 65_552);
    for (start, end) in [(2, 1), (0, 196_638), (u64::MAX, u64::MAX)] {
        assert!(object.header().range(Range { start, end }).is_err());
    }
    // Damage every unrequested chunk. The selected middle chunk still opens.
    let header = object.header();
    for index in [0, 2, 3] {
        let offset = header.chunk(index).unwrap().offset as usize;
        stored[offset] ^= 1;
    }
    let object = FileObject::decode(&stored).unwrap();
    assert_eq!(
        object.read_range(&key, PATH, 65_537..131_071).unwrap(),
        plain[65_537..131_071]
    );
    assert!(object.read_range(&key, PATH, 0..1).is_err());
    assert!(object
        .read_range(
            &key,
            "files/1/22222222-2222-2222-2222-222222222222",
            65_537..131_071
        )
        .is_err());
    let wrong = FileKey::from_bytes([0x78; 32]);
    assert!(object.read_range(&wrong, PATH, 65_537..131_071).is_err());
}

#[test]
fn file_sizes_and_chunk_lengths_are_checked_before_opening() {
    for size in [0, 1, 65_535, 65_536, 65_537, 131_072] {
        let (key, plain, mut bytes) = example(size);
        let object = FileObject::decode(&bytes).unwrap();
        assert_eq!(
            object.read_range(&key, PATH, 0..size as u64).unwrap(),
            plain
        );
        for end in 0..bytes.len() {
            assert!(FileObject::decode(&bytes[..end]).is_err());
        }
        bytes.push(0);
        assert!(matches!(
            FileObject::decode(&bytes),
            Err(Error::TrailingBytes)
        ));
    }
    let header = FileHeader::new(65_537);
    let key = FileKey::from_bytes([0x77; 32]);
    assert!(header.seal_chunk(&key, PATH, 0, &[0; 65_535]).is_err());
    assert!(header.seal_chunk(&key, PATH, 1, &[0; 2]).is_err());
    assert!(header.open_chunk(&key, PATH, 1, &[0; 16]).is_err());
    assert!(header.open_chunk(&key, PATH, 1, &[0; 18]).is_err());
}

// Public test material sealed independently with Python ctypes and libsodium.
#[test]
fn file_fixture_opens_reencodes_and_decodes_every_mutation() {
    let bytes = crate::tests::hex(include_str!("../fixtures/file.hex"));
    let mut reference = include_str!("../fixtures/uploaded-file.txt").lines();
    assert_eq!(reference.next().unwrap(), PATH);
    let reference =
        crate::file_reference::UploadedFileReference::decode(reference.next().unwrap()).unwrap();
    let path = crate::path::ObjectPath::file(reference.device, reference.id);
    assert_eq!(path.as_str(), PATH);
    let key = reference.key;
    let file = FileObject::decode(&bytes).unwrap();
    assert_eq!(file.header().size(), 65_565);
    assert_eq!(file.header().chunk_count(), 2);
    assert_eq!(file.encode().unwrap(), bytes);
    let plain: Vec<_> = (0..65_565).map(|i| (i % 251) as u8).collect();
    assert_eq!(file.read_range(&key, PATH, 0..65_565).unwrap(), plain);
    for wrong_path in [
        "files/2/11111111-1111-1111-1111-111111111111",
        "files/11111111-1111-1111-1111-111111111111",
    ] {
        assert!(file.read_range(&key, wrong_path, 0..65_565).is_err());
    }
    assert_eq!(
        file.read_range(&key, PATH, 65_535..65_538).unwrap(),
        plain[65_535..65_538]
    );
    let mut encoded = file.header().encode().to_vec();
    for (index, chunk) in plain.chunks(65_536).enumerate() {
        encoded.extend(
            file.header()
                .seal_chunk(&key, PATH, index as u64, chunk)
                .unwrap(),
        );
    }
    assert_eq!(encoded, bytes);
    crate::tests::mutations(&bytes, |changed| {
        if let Ok(file) = FileObject::decode(changed) {
            assert_eq!(file.encode().unwrap(), changed);
        }
    });
}
