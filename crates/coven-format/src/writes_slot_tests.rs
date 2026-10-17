use super::*;
use crate::test_utils as f;

fn body() -> (Vec<u8>, WriteHeader) {
    let header = f::write().header;
    let (mut encoder, mut bytes) =
        WritesSlotEncoder::start(f::slot_header(8, 4), &f::slot_path(8), 1).unwrap();
    bytes.extend(encoder.write(&header, 100_001).unwrap());
    bytes.extend(vec![42; 100_001]);
    encoder.finish().unwrap();
    (bytes, header)
}

#[test]
fn batch_streams_at_arbitrary_boundaries_without_a_collection_limit() {
    let (bytes, header) = body();
    for size in [1, 7, 47, 65_536, bytes.len()] {
        let mut decoder = WritesSlotDecoder::new(f::slot_path(8));
        let mut received = 0;
        let mut ended = 0;
        for chunk in bytes.chunks(size) {
            let mut input = chunk;
            while let Some(piece) = decoder.next(&mut input).unwrap() {
                match piece {
                    BatchPiece::Header(h) => assert_eq!(h, f::slot_header(8, 4)),
                    BatchPiece::Write { length } => {
                        assert_eq!(length, 100_001);
                        decoder.check_header(&header).unwrap();
                    }
                    BatchPiece::Bytes(b) => {
                        assert!(b.iter().all(|b| *b == 42));
                        received += b.len();
                    }
                    BatchPiece::EndWrite => ended += 1,
                }
            }
        }
        decoder.finish().unwrap();
        assert_eq!(received, 100_001);
        assert_eq!(ended, 1);
    }
}

#[test]
fn batch_refuses_zero_lengths_truncation_unchecked_headers_and_trailing_bytes() {
    let (bytes, header) = body();
    let decode = |bytes: &[u8], check: bool| -> Result<(), crate::Error> {
        let mut decoder = WritesSlotDecoder::new(f::slot_path(8));
        let mut input = bytes;
        while let Some(piece) = decoder.next(&mut input)? {
            if check && matches!(piece, BatchPiece::Write { .. }) {
                decoder.check_header(&header)?;
            }
        }
        decoder.finish()
    };
    for end in [0, 46, 47, 54, 55, 62, 63, bytes.len() - 1] {
        assert!(decode(&bytes[..end], true).is_err());
    }
    assert!(decode(&bytes, false).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode(&trailing, true).is_err());
    for at in [47, 55] {
        let mut zero = bytes.clone();
        zero[at..at + 8].fill(0);
        assert!(decode(&zero, true).is_err());
    }
}

#[test]
fn batch_checks_device_read_through_and_original_write_order() {
    let header = f::write().header;
    let (mut encoder, _) =
        WritesSlotEncoder::start(f::slot_header(8, 4), &f::slot_path(8), 2).unwrap();
    let mut other = header.clone();
    other.position.device.0 += 1;
    assert!(encoder.write(&other, 1).is_err());
    let mut future = header.clone();
    future.read_through = 8;
    assert!(encoder.write(&future, 1).is_err());
    encoder.write(&header, 1).unwrap();
    assert!(encoder.write(&header, 1).is_err());
    assert!(encoder.finish().is_err());
    assert!(WritesSlotEncoder::start(f::slot_header(8, 4), &f::slot_path(8), 0).is_err());
    assert_eq!(
        identity(f::STORE_ID, header.position).unwrap(),
        format!("coven/write/{}/1/3", f::STORE_ID)
    );
}
