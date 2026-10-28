use super::*;
use std::io::{self, Cursor, SeekFrom};

struct ObservedReader {
    bytes: Cursor<Vec<u8>>,
    requested: Vec<usize>,
}

impl Read for ObservedReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.requested.push(output.len());
        self.bytes.read(output)
    }
}

impl Seek for ObservedReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.bytes.seek(from)
    }
}

fn object_path() -> ObjectPath {
    ObjectPath::parse("10101010-1010-1010-1010-101010101010/snapshots/store/1/1").unwrap()
}

#[test]
fn a_header_without_its_advertised_payload_never_requests_a_payload_buffer() {
    let mut input = ObservedReader {
        bytes: Cursor::new(vec![0x05, 0x00, 0x01, 0x00, 0xff, 0xff, 0xf9]),
        requested: Vec::new(),
    };
    assert!(matches!(
        frame(&mut input, &object_path()),
        Err(SyncError::Damaged(_))
    ));
    assert_eq!(input.requested, [coven_format::FRAME_PREFIX_LEN]);
    assert_eq!(input.bytes.position(), 7);
}

#[test]
fn bounded_reads_reject_lengths_past_the_remaining_input_before_reading() {
    for position in [0, 2, 4, 5] {
        let mut input = ObservedReader {
            bytes: Cursor::new(vec![1, 2, 3, 4]),
            requested: Vec::new(),
        };
        input.bytes.set_position(position);
        assert!(read_exact(&mut input, 5, &object_path()).is_err());
        assert!(input.requested.is_empty());
        assert_eq!(input.bytes.position(), position);
    }
}

#[test]
fn frame_reads_stop_at_the_declared_boundary() {
    let first = [5, 0, 1, 0, 0, 0, 3, 10, 11, 12];
    let second = [7, 0, 1, 0, 0, 0, 0];
    let mut input = Cursor::new([first.as_slice(), second.as_slice()].concat());
    assert_eq!(frame(&mut input, &object_path()).unwrap(), first);
    assert_eq!(input.position(), first.len() as u64);
    assert_eq!(frame(&mut input, &object_path()).unwrap(), second);
}
