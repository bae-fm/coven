use super::*;
use coven_crypto::ObjectHasher;
use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
use coven_format::write::WriteRecord;
use coven_format::write_stream::{decode_plaintext, WriteEncoder};

pub(super) async fn queued(db: &Database) -> WriteRecord {
    db.read_oldest_upload(|upload| {
        let coven_database::WaitingUpload::Plaintext {
            header_frame,
            parts,
            ..
        } = upload
        else {
            panic!("plaintext")
        };
        let mut bytes = header_frame;
        for (_, part) in parts {
            for chunk in part {
                bytes.extend(chunk?);
            }
        }
        Ok::<_, DbError>(decode_plaintext(&bytes).unwrap())
    })
    .await
    .unwrap()
    .unwrap()
}

pub(super) fn seal(
    record: &WriteRecord,
    path: &ObjectPath,
    alter: impl Fn(u64, &mut Vec<u8>),
) -> Vec<u8> {
    let encoder = WriteEncoder::new(record).unwrap();
    let prefix = WriteObjectPrefix {
        store_key: KeyId(Uuid::from_u128(1)),
        part_keys: vec![KeyId(Uuid::from_u128(1)); record.parts.len()],
    };
    let mut layout = WriteObjectLayout::new(
        prefix,
        encoder.header_frame(),
        encoder
            .header()
            .parts
            .iter()
            .map(|p| p.plaintext_length)
            .collect(),
    )
    .unwrap();
    let aad = layout.prefix().unwrap();
    let mut bytes = aad.clone();
    let key = StoreKey::from_bytes(KeyId(Uuid::from_u128(1)), [7; 32]).derive();
    let mut put = |plain: &[u8]| {
        let coordinate = layout.next_chunk().unwrap();
        let mut plain = plain.to_vec();
        alter(coordinate.section, &mut plain);
        let sealed = key
            .seal_object_chunk(
                path.as_str(),
                &aad,
                coordinate.section,
                coordinate.index,
                &plain,
            )
            .unwrap();
        bytes.extend(layout.encode_chunk(&sealed).unwrap());
    };
    put(encoder.header_frame());
    for part in 0..record.parts.len() {
        for chunk in encoder.part_chunks(part).unwrap() {
            put(&chunk.unwrap());
        }
    }
    let mut hash = ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend(
        layout
            .signature(&member().sign_object(path.as_str(), &hash.finish()))
            .unwrap(),
    );
    layout.finish(&[]).unwrap();
    bytes
}

pub(super) async fn publish(storage: &MemoryStorage, record: &WriteRecord) {
    let path = crate::write_seal::path(record.header.position);
    storage
        .create(&path, &seal(record, &path, |_, _| {}))
        .await
        .unwrap();
}
