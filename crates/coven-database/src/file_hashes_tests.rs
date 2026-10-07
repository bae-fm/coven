use super::*;

#[test]
fn hashes_follow_file_chunks_regardless_of_input_fragmentation() {
    let size = DEFAULT_CHUNK_SIZE as usize;
    for length in [0, 1, size - 1, size, size + 1, size * 3 + 19] {
        let bytes = (0..length).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let mut whole = ContentHasher::new();
        whole.update(&bytes);
        let expected = whole.finish();
        for fragment in [1, 37, size, size + 13] {
            let mut hasher = FileHasher::new();
            for piece in bytes.chunks(fragment) {
                hasher.update(piece);
            }
            let hashes = hasher.finish();
            assert_eq!(hashes.content, expected);
            assert_eq!(hashes.chunks.len(), length.div_ceil(size));
            for (hash, chunk) in hashes.chunks.iter().zip(bytes.chunks(size)) {
                let mut expected = ContentHasher::new();
                expected.update(chunk);
                assert_eq!(*hash, expected.finish());
            }
        }
    }
}
