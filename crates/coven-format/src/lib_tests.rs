use super::*;
use crate::error::Rule;
use zeroize::Zeroizing;

pub(super) use crate::test_utils::fixture_bytes as hex;

#[test]
fn every_object_and_snapshot_section_has_pinned_bytes() {
    let expected: Vec<_> = include_str!("../fixtures/v1.hex").lines().map(hex).collect();
    let objects = test_utils::encoded_examples();
    assert_eq!(objects.len(), expected.len());
    for (bytes, expected) in objects.into_iter().zip(expected) {
        assert_eq!(bytes.as_slice(), expected);
    }
    #[cfg(not(windows))]
    generator_reproduces_every_fixture_it_writes();
}

/// Every fixture `fixtures/generate-deterministic.py` writes.
#[cfg(not(windows))]
const GENERATED_FIXTURES: [&str; 20] = [
    "snapshot-settlement-claims.hex",
    "sealed-refused-writes-slot.hex",
    "sealed-reset-ignored-writes-slot.hex",
    "sealed-migration-write.hex",
    "sealed-circle-snapshot.hex",
    "sealed-announcement.hex",
    "sealed-circle-announcement.hex",
    "snapshot-claims.hex",
    "codes.txt",
    "invite-code.hex",
    "restore-code.hex",
    "sealed-circle-key.hex",
    "sealed-join-request.hex",
    "sealed-snapshot.hex",
    "sealed-store-key.hex",
    "sealed-admin.hex",
    "sealed-write.hex",
    "sealed-writes-slot.hex",
    "sealed-floor.hex",
    "v1.hex",
];

/// The generator encodes the member-bearing frames and codes from D2–D13 and
/// seals every sealed fixture with Python and libsodium, independently of this
/// crate (D14). Its output must equal the checked-in fixtures byte for byte,
/// which the encoder must equal in turn, so neither the generator, the pinned
/// hex nor the encoder can change alone. This needs `python3` with libsodium
/// installed. Windows is left out: the generator loads libsodium the Unix way,
/// and the bytes it checks are the same on every platform.
#[cfg(not(windows))]
fn generator_reproduces_every_fixture_it_writes() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let out = tempfile::tempdir().unwrap();
    let run = std::process::Command::new("python3")
        .arg(fixtures.join("generate-deterministic.py"))
        .arg(out.path())
        .output()
        .expect("python3 runs fixtures/generate-deterministic.py");
    assert!(
        run.status.success(),
        "fixtures/generate-deterministic.py failed:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let written: std::collections::BTreeSet<_> = std::fs::read_dir(out.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(written, GENERATED_FIXTURES.iter().map(|name| name.to_string()).collect());
    for name in GENERATED_FIXTURES {
        let generated = std::fs::read_to_string(out.path().join(name)).unwrap();
        let pinned = std::fs::read_to_string(fixtures.join(name)).unwrap();
        let mut lines = generated.lines().zip(pinned.lines()).enumerate();
        if let Some((line, (generated, pinned))) = lines.find(|(_, (g, p))| g != p) {
            let at = generated
                .bytes()
                .zip(pinned.bytes())
                .position(|(g, p)| g != p)
                .unwrap_or(generated.len().min(pinned.len()));
            let excerpt = |text: &str| text[at..text.len().min(at + 64)].to_string();
            panic!(
                "fixtures/{name} line {} differs from the generator's from hex digit {at}:\n\
                 generator {}\n\
                 pinned    {}",
                line + 1,
                excerpt(generated),
                excerpt(pinned)
            );
        }
        assert_eq!(
            generated.lines().count(),
            pinned.lines().count(),
            "fixtures/{name} and the generator's have different line counts"
        );
        assert!(generated == pinned, "fixtures/{name} and the generator's differ in line endings");
    }
}

#[test]
fn pinned_chunks_decode_with_their_declared_boundaries() {
    let pieces: Vec<_> = include_str!("../fixtures/v1.hex").lines().map(hex).collect();
    let mut index = test_utils::frame_examples().len();
    for frame in &pieces[..index] {
        assert_eq!(reencode(frame).unwrap().as_slice(), frame);
    }
    let prefix = crate::sealed_write::WriteObjectPrefix::decode(&pieces[index]).unwrap();
    assert_eq!(prefix.encode().unwrap(), pieces[index]);
    index += 1;
    let header = crate::write_stream::WriteHeaderFrame::decode(&pieces[index]).unwrap();
    index += 1;
    assert_eq!(header.parts.len(), prefix.part_keys.len());
    let mut parts = Vec::new();
    for part in header.parts {
        let mut frames = Vec::new();
        let mut decoder = crate::write_stream::PartDecoder::new(part.clone()).unwrap();
        for _ in 0..part.chunk_count() {
            frames.extend(decoder.chunk(&pieces[index]).unwrap());
            index += 1;
        }
        decoder.finish().unwrap();
        let mut rows = Vec::new();
        let mut executions = Vec::new();
        for frame in frames {
            match frame {
                crate::dismissal::WriteFrame::Change(row) => rows.push(row),
                crate::dismissal::WriteFrame::Execution(execution) => executions.push(execution),
                crate::dismissal::WriteFrame::Dismissal(_) => {
                    panic!("fixture contains row changes and executions")
                }
            }
        }
        parts.push(crate::write::WritePart {
            audience: part.audience,
            rows,
            executions,
            dismissals: Vec::new(),
        });
    }
    assert_eq!(
        crate::write::WriteRecord { header: header.header, parts },
        test_utils::chunked_write()
    );
    let prefix = crate::sealed_snapshot::SnapshotObjectPrefix::decode(&pieces[index]).unwrap();
    index += 1;
    let mut decoder = crate::snapshot_stream::SnapshotChunkDecoder::new(prefix);
    let mut oracle = test_utils::TestOracle { writes: Default::default() };
    let mut count = 0;
    for piece in &pieces[index..] {
        let mut bytes = piece.as_slice();
        while let Some(record) = decoder.next_record(&mut bytes, &oracle).unwrap() {
            if let crate::snapshot::SnapshotRecord::Write(write) = record {
                oracle.writes.insert(write.id, write);
            }
            count += 1;
        }
    }
    decoder.finish().unwrap();
    assert_eq!(count, test_utils::snapshot_records().len());
}

// A reproducible generator exercises arbitrary bytes without a randomness
// capability or an external dependency. Canonical fixtures supply deep valid
// structures, then every bit and every truncation boundary is exercised.
pub(crate) struct Generator(pub(crate) u64);
impl Generator {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn reencode(bytes: &[u8]) -> Result<Zeroizing<Vec<u8>>, Error> {
    frame_length(bytes)?;
    match bytes[0] {
        1 => Ok(Zeroizing::new(crate::write_stream::WriteHeaderFrame::decode(bytes)?.encode()?)),
        2 => Ok(Zeroizing::new(crate::write::RowChange::decode(bytes)?.encode()?)),
        3 => Ok(Zeroizing::new(crate::dismissal::Dismissal::decode(bytes)?.encode()?)),
        5 => {
            let reader = crate::snapshot::SnapshotDecoder::start(
                bytes,
                &test_utils::snapshot_prefix(&test_utils::snapshot_header()),
            )?;
            Ok(Zeroizing::new(crate::snapshot::SnapshotEncoder::start(reader.header().clone())?.1))
        }
        6 => {
            let (_, mut input) = crate::wire::decode_frame(bytes)?;
            let record = crate::snapshot::SnapshotRecord::get(&mut input, &test_utils::oracle())?;
            input.finish()?;
            record.validate()?;
            Ok(Zeroizing::new(encode_frame_with(6, |out| record.put(out))?))
        }
        7 => {
            crate::wire::decode_frame(bytes)?.1.finish()?;
            Ok(Zeroizing::new(encode_frame_with(7, |_| Ok(()))?))
        }
        _ => Ok(Zeroizing::new(Object::decode(bytes)?.encode()?)),
    }
}
fn decode_or_typed_error(bytes: &[u8]) {
    match reencode(bytes) {
        Ok(encoded) => assert_eq!(encoded.as_slice(), bytes),
        Err(error) => {
            let _: Error = error;
        }
    }
}

#[test]
fn arbitrary_truncated_and_bit_flipped_inputs_never_panic() {
    let mut source = Generator(0xb5f2_106d_7538_901b);
    for n in 0..25_000 {
        let mut bytes: Vec<_> = (0..n % 1024).map(|_| source.next() as u8).collect();
        decode_or_typed_error(&bytes);
        if bytes.len() >= 7 {
            // Reach the payload decoder as well as the prefix checks.
            bytes[0] = 1 + source.next() as u8 % 12;
            bytes[1..3].copy_from_slice(&1u16.to_be_bytes());
            let len = (bytes.len() - 7) as u32;
            bytes[3..7].copy_from_slice(&len.to_be_bytes());
            decode_or_typed_error(&bytes);
        }
    }
    for bytes in
        include_str!("../fixtures/v1.hex").lines().take(test_utils::frame_examples().len()).map(hex)
    {
        for end in 0..bytes.len() {
            assert!(reencode(&bytes[..end]).is_err());
        }
        for index in 0..bytes.len() {
            for bit in 0..8 {
                let mut changed = bytes.clone();
                changed[index] ^= 1 << bit;
                decode_or_typed_error(&changed);
            }
        }
    }
}

#[test]
fn prefix_limits_versions_and_trailing_bytes_are_typed_errors() {
    assert_eq!(Object::decode(&[0; 7]), Err(Error::UnknownTag { field: "object kind", tag: 0 }));
    assert_eq!(Object::decode(&[5, 0, 2, 0, 0, 0, 0]), Err(Error::UnsupportedVersion(2)));
    assert!(matches!(
        frame_length(&[1, 0, 1, 255, 255, 255, 255]),
        Err(Error::Limit { field: "frame payload", .. })
    ));
    let mut end = encode_frame_with(7, |_| Ok(())).unwrap();
    end.push(0);
    assert_eq!(reencode(&end), Err(Error::TrailingBytes));
    end[6] = 1;
    assert_eq!(reencode(&end), Err(Error::TrailingBytes));
}

#[test]
fn hostile_nested_lengths_do_not_drive_unbounded_allocations() {
    // A valid write header up to the part count, followed by a hostile count.
    let mut bytes = crate::write_stream::WriteEncoder::new(&test_utils::write())
        .unwrap()
        .header_frame()
        .to_vec();
    bytes[52..56].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        crate::write_stream::WriteHeaderFrame::decode(&bytes),
        Err(Error::Limit { field: "collection", .. })
    ));
    let mut source = wire::Decoder::new(&[0, 1, 0, 0]).unwrap();
    assert_eq!(Vec::<String>::get(&mut source), Err(Error::Truncated));
    let mut source = wire::Decoder::new(&[255, 255, 255, 255]).unwrap();
    assert!(matches!(wire::get_blob(&mut source), Err(Error::Limit { field: "bytes", .. })));
}

#[test]
fn malformed_strings_and_keys_remain_distinguishable() {
    let mut input = wire::Decoder::new(&[0, 0, 0, 1, 255]).unwrap();
    assert_eq!(String::get(&mut input), Err(Error::Utf8));
    let mut write = test_utils::write();
    write.parts[0].rows[0].row.key = vec![0];
    assert!(matches!(
        test_utils::write_plaintext(&write),
        Err(Error::Invalid { rule: Rule::KeyEncoding, .. })
    ));
}

pub(super) fn mutations(bytes: &[u8], mut decode: impl FnMut(&[u8])) {
    for end in 0..bytes.len() {
        decode(&bytes[..end]);
    }
    let mut changed = bytes.to_vec();
    for index in 0..bytes.len() {
        for bit in 0..8 {
            changed[index] ^= 1 << bit;
            decode(&changed);
            changed[index] ^= 1 << bit;
        }
    }
    changed.push(0);
    decode(&changed);
}
