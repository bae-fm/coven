use super::*;

const PATH: &str = "files/11111111-1111-1111-1111-111111111111";

fn example(size: usize, chunk_size: u32) -> (FileKey, Vec<u8>, Vec<u8>) {
    let key = FileKey::from_bytes([0x77; 32]);
    let plain: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
    let header = FileHeader::with_chunk_size(size as u64, chunk_size).unwrap();
    let mut stored = header.encode().to_vec();
    for (index, chunk) in plain.chunks(chunk_size as usize).enumerate() {
        stored.extend(header.seal_chunk(&key, PATH, index as u64, chunk).unwrap());
    }
    (key, plain, stored)
}

#[test]
fn header_bytes_limits_offsets_and_empty_files_are_exact() {
    let header = FileHeader::new(65_537);
    assert_eq!(
        header.encode(),
        [38, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1]
    );
    assert_eq!(header.chunk_size(), DEFAULT_CHUNK_SIZE);
    assert_eq!(header.size(), 65_537);
    assert_eq!(header.chunk_count(), 2);
    assert_eq!(
        header.chunk(0).unwrap(),
        FileChunk {
            index: 0,
            offset: 15,
            plaintext_length: 65_536
        }
    );
    assert_eq!(
        header.chunk(1).unwrap(),
        FileChunk {
            index: 1,
            offset: 15 + 65_536 + 16,
            plaintext_length: 1
        }
    );
    assert_eq!(header.encrypted_size().unwrap(), 15 + 65_537 + 32);
    assert!(header.chunk(2).is_err());
    assert!(header.chunk(u64::MAX).is_err());
    for size in [MIN_CHUNK_SIZE, 4097, DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE] {
        let header = FileHeader::with_chunk_size(0, size).unwrap();
        assert_eq!(FileHeader::decode(&header.encode()).unwrap(), header);
        assert_eq!(header.chunk_count(), 0);
        assert!(header.chunk(0).is_err());
        assert_eq!(
            FileObject::decode(&header.encode())
                .unwrap()
                .encode()
                .unwrap(),
            header.encode()
        );
    }
    for chunk_size in [0, 1, MIN_CHUNK_SIZE - 1, MAX_CHUNK_SIZE + 1, u32::MAX] {
        assert!(FileHeader::with_chunk_size(1, chunk_size).is_err());
        let mut bytes = header.encode();
        bytes[3..7].copy_from_slice(&chunk_size.to_be_bytes());
        assert!(FileHeader::decode(&bytes).is_err());
    }
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
    let (key, plain, mut stored) = example(3 * 4096 + 29, 4096);
    let object = FileObject::decode(&stored).unwrap();
    assert_eq!(object.encode().unwrap(), stored);
    for range in [
        0..0,
        0..1,
        0..4096,
        4095..4097,
        4096..8192,
        8000..12_288,
        12_288..12_317,
        12_317..12_317,
        0..12_317,
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
    let selected = object.header().range(4097..8191).unwrap();
    let fetch = selected.encrypted_range().unwrap();
    assert_eq!(fetch, 15 + 4112..15 + 2 * 4112);
    for (start, end) in [(2, 1), (0, 12_318), (u64::MAX, u64::MAX)] {
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
        object.read_range(&key, PATH, 4097..8191).unwrap(),
        plain[4097..8191]
    );
    assert!(object.read_range(&key, PATH, 0..1).is_err());
    assert!(object
        .read_range(
            &key,
            "files/22222222-2222-2222-2222-222222222222",
            4097..8191
        )
        .is_err());
    let wrong = FileKey::from_bytes([0x78; 32]);
    assert!(object.read_range(&wrong, PATH, 4097..8191).is_err());
}

#[test]
fn file_sizes_and_chunk_lengths_are_checked_before_opening() {
    for size in [0, 1, 4095, 4096, 4097, 8192] {
        let (key, plain, mut bytes) = example(size, 4096);
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
    let header = FileHeader::with_chunk_size(4097, 4096).unwrap();
    let key = FileKey::from_bytes([0x77; 32]);
    assert!(header.seal_chunk(&key, PATH, 0, &[0; 4095]).is_err());
    assert!(header.seal_chunk(&key, PATH, 1, &[0; 2]).is_err());
    assert!(header.open_chunk(&key, PATH, 1, &[0; 16]).is_err());
    assert!(header.open_chunk(&key, PATH, 1, &[0; 18]).is_err());
}

#[test]
fn files_reject_chunks_smaller_than_four_kibibytes() {
    assert!(FileHeader::with_chunk_size(1, 4095).is_err());
}

#[test]
fn non_power_of_two_chunks_and_eight_mib_chunks_share_the_range_layout() {
    for chunk_size in [4097, DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE] {
        let (key, plain, stored) = example(8195, chunk_size);
        let object = FileObject::decode(&stored).unwrap();
        for range in [0..8195, 4096..8195, 4097..4098] {
            assert_eq!(
                object.read_range(&key, PATH, range.clone()).unwrap(),
                plain[range.start as usize..range.end as usize]
            );
        }
    }
}

// Public test material sealed independently with Python ctypes and libsodium.
#[test]
fn file_fixture_opens_reencodes_and_decodes_every_mutation() {
    let bytes = crate::tests::hex(include_str!("../fixtures/file.hex"));
    let key = FileKey::from_bytes([0x77; 32]);
    let file = FileObject::decode(&bytes).unwrap();
    assert_eq!(file.header().chunk_size(), 4096);
    assert_eq!(file.header().size(), 4125);
    assert_eq!(file.header().chunk_count(), 2);
    assert_eq!(file.encode().unwrap(), bytes);
    let plain: Vec<_> = (0..4125).map(|i| (i % 251) as u8).collect();
    assert_eq!(file.read_range(&key, PATH, 0..4125).unwrap(), plain);
    assert_eq!(
        file.read_range(&key, PATH, 4095..4098).unwrap(),
        plain[4095..4098]
    );
    let mut encoded = file.header().encode().to_vec();
    for (index, chunk) in plain.chunks(4096).enumerate() {
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
