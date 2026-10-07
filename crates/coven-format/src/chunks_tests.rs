use super::*;

#[test]
fn frames_cross_chunks_and_network_fragments_without_being_retained_together() {
    let frames = crate::test_utils::chunked_snapshot_frames();
    let chunks: Vec<_> = PlaintextChunks::new(frames.clone().into_iter().map(Ok))
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(chunks.len() > 1);
    assert!(chunks[..chunks.len() - 1]
        .iter()
        .all(|c| c.len() == CHUNK_SIZE));
    for size in [1, 6, 7, 31, CHUNK_SIZE] {
        let mut decoder = FrameDecoder::new(None);
        let mut decoded = Vec::new();
        for fragment in chunks.iter().flat_map(|c| c.chunks(size)) {
            let mut bytes = fragment;
            while let Some(frame) = decoder.next(&mut bytes).unwrap() {
                decoded.push(frame);
            }
        }
        decoder.finish().unwrap();
        assert_eq!(decoded, frames);
    }
}

#[test]
fn announced_lengths_are_checked_before_growing_the_frame_buffer() {
    for (length, remaining) in [(100u32, Some(7)), (u32::MAX, None)] {
        let mut prefix = vec![2, 0, 1];
        prefix.extend_from_slice(&length.to_be_bytes());
        let mut decoder = FrameDecoder::new(remaining);
        assert!(decoder.next(&mut prefix.as_slice()).is_err());
        assert_eq!(decoder.bytes.len(), FRAME_PREFIX_LEN);
        assert!(decoder.bytes.capacity() < 100);
    }
    let mut decoder = FrameDecoder::new(None);
    decoder.next(&mut [13, 0].as_slice()).unwrap();
    assert_eq!(decoder.finish(), Err(Error::Truncated));
}
