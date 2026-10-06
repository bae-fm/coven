//! Deterministic examples for consumer tests, built from the production owners' types.

use crate::codes::{InviteCode, RestoreCode};
use crate::key::encode_key;
use crate::objects::*;
use crate::snapshot::{SnapshotEncoder, SnapshotHeader, SnapshotRecord};
use crate::snapshot_rows::*;
use crate::store_log::*;
use crate::value::*;
use crate::write::*;
use crate::Object;
use coven_crypto::{InviteSecret, MemberKeys, SecretBytes};
use coven_foundation::id_source::{CircleId, DeviceId, InviteId, KeyId, StoreId};
use coven_merge::{
    Audience, Cell, Change, ColumnValue, LostKey, LostValue, MergeError, Operation, Parent, RowId,
    RowState, Timestamp, WriteId, WriteOracle,
};
use std::collections::BTreeMap;
use uuid::Uuid;
use zeroize::Zeroizing;

/// A write in device one's log.
pub fn position() -> WriteId {
    WriteId {
        device: DeviceId(1),
        number: 3,
    }
}
/// The fixture write's timestamp.
pub fn timestamp() -> Timestamp {
    Timestamp::new(6, 3, DeviceId(1)).unwrap()
}
/// A row with a text key in the store.
pub fn row() -> RowId {
    RowId {
        table: "t".into(),
        key: encode_key(&[Value::Text("k".into())]).unwrap(),
        audience: Audience::Store,
    }
}
/// A value with no foreign-key metadata.
pub fn column(value: Value) -> ColumnValue<Value> {
    ColumnValue {
        value,
        parents: BTreeMap::new(),
    }
}
/// Member keys with fixed, public test seeds.
pub fn member_keys() -> MemberKeys {
    let mut bytes = Zeroizing::new(Vec::with_capacity(69));
    bytes.extend_from_slice(b"CVMK\x01");
    bytes.extend_from_slice(&[0x33; 64]);
    MemberKeys::from_secret_bytes(&bytes).unwrap()
}
/// The public halves of the test member keys.
pub fn member() -> MemberPublicKeys {
    let keys = member_keys();
    MemberPublicKeys {
        signing: keys.member_id(),
        sealing: keys.sealing_public_key(),
    }
}
/// A write with old/new values and named foreign-key metadata on its new value.
pub fn write() -> WriteRecord {
    let mut new = column(Value::Integer(2));
    new.parents.insert(
        coven_merge::ForeignKey::new(["parent_fk"], "p", ["id"]),
        Parent {
            row: RowId {
                table: "p".into(),
                key: encode_key(&[Value::Text("q".into())]).unwrap(),
                audience: Audience::Store,
            },
            generation: 1,
        },
    );
    WriteRecord {
        header: WriteHeader {
            position: position(),
            timestamp: timestamp(),
            had_read: WritePositions(vec![WriteId {
                device: DeviceId(2),
                number: 1,
            }]),
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: Audience::Store,
            rows: vec![RowChange {
                row: row(),
                change: Change {
                    generation: 1,
                    operation: Operation::Update(BTreeMap::from([("x".into(), new)])),
                },
                old: BTreeMap::from([("x".into(), Value::Integer(1))]),
            }],
        }],
    }
}
/// The first store-log entry.
pub fn store_log() -> StoreLogEntry {
    StoreLogEntry {
        position: EntryId {
            device: DeviceId(1),
            number: 1,
        },
        timestamp: timestamp(),
        author: member().signing,
        had_read: EntryPositions(vec![]),
        change: StoreChange::CreateStore {
            store: StoreId(Uuid::from_bytes([0x10; 16])),
            name: "S".into(),
            admin: member(),
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
        },
    }
}
/// A member removal replacing store/circle keys and deleting circles left empty.
pub fn member_removal() -> StoreLogEntry {
    StoreLogEntry {
        position: EntryId {
            device: DeviceId(1),
            number: 2,
        },
        timestamp: Timestamp::new(7, 0, DeviceId(1)).unwrap(),
        author: member().signing,
        had_read: EntryPositions(vec![EntryId {
            device: DeviceId(1),
            number: 1,
        }]),
        change: StoreChange::RemoveMember {
            member: "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
                .parse()
                .unwrap(),
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([2; 16])),
            circle_keys: vec![
                CircleKeyId {
                    circle: CircleId(Uuid::from_u128(1)),
                    key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([5; 16])),
                },
                CircleKeyId {
                    circle: CircleId(Uuid::from_u128(3)),
                    key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([2; 16])),
                },
            ],
            deleted_circles: vec![CircleId(Uuid::from_u128(2)), CircleId(Uuid::from_u128(4))],
        },
    }
}
/// A reset after the fixture's create-store entry.
pub fn loss_entry() -> EntryId {
    EntryId {
        device: DeviceId(1),
        number: 2,
    }
}
/// An oracle of applied metadata that tests may populate from streamed records.
#[derive(Clone)]
pub struct TestOracle {
    /// Metadata indexed by merge's write identity; no write bodies are reconstructed.
    pub writes: BTreeMap<WriteId, AppliedWrite>,
}
impl WriteOracle for TestOracle {
    fn timestamp(&self, id: WriteId) -> Option<Timestamp> {
        self.writes.get(&id).map(|w| w.timestamp)
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MergeError> {
        let write = self
            .writes
            .get(&reader)
            .ok_or(MergeError::MissingWrite(reader))?;
        if !self.writes.contains_key(&earlier) {
            return Err(MergeError::MissingWrite(earlier));
        }
        Ok(
            (reader.device == earlier.device && earlier.number < reader.number)
                || write.had_read.covers(earlier),
        )
    }
}
/// Three applied writes, including two concurrent setters of the fixture row.
pub fn oracle() -> TestOracle {
    let first = WriteId {
        device: DeviceId(1),
        number: 1,
    };
    let winner = WriteId {
        device: DeviceId(1),
        number: 2,
    };
    let loser = WriteId {
        device: DeviceId(2),
        number: 1,
    };
    TestOracle {
        writes: [
            AppliedWrite {
                id: first,
                timestamp: Timestamp::new(1, 0, first.device).unwrap(),
                had_read: WritePositions(vec![]),
            },
            AppliedWrite {
                id: winner,
                timestamp: Timestamp::new(4, 0, winner.device).unwrap(),
                had_read: WritePositions(vec![]),
            },
            AppliedWrite {
                id: loser,
                timestamp: Timestamp::new(3, 0, loser.device).unwrap(),
                had_read: WritePositions(vec![first]),
            },
        ]
        .into_iter()
        .map(|w| (w.id, w))
        .collect(),
    }
}
/// The fixture's merged row, with an actual concurrent loss validated by merge.
pub fn merge_row() -> MergeRow {
    let state = RowState::from_parts(
        row(),
        BTreeMap::from([(
            1,
            WriteId {
                device: DeviceId(1),
                number: 1,
            },
        )]),
        BTreeMap::from([(
            "x".into(),
            Cell {
                write: WriteId {
                    device: DeviceId(1),
                    number: 2,
                },
                value: column(Value::Integer(2)),
            },
        )]),
        BTreeMap::from([(
            LostKey {
                column: "x".into(),
                write: WriteId {
                    device: DeviceId(2),
                    number: 1,
                },
            },
            LostValue {
                incarnation: 1,
                value: column(Value::Integer(1)),
                replaced_by: WriteId {
                    device: DeviceId(1),
                    number: 2,
                },
            },
        )]),
        &oracle(),
    )
    .unwrap();
    MergeRow {
        state,
        removed: Default::default(),
    }
}
/// The header for the fixture's five snapshot sections.
pub fn snapshot_header() -> SnapshotHeader {
    SnapshotHeader {
        id: SnapshotId {
            device: DeviceId(1),
            number: 1,
            audience: Audience::Store,
        },
        schema_version: 2,
        writes: WritePositions(vec![
            position(),
            WriteId {
                device: DeviceId(2),
                number: 1,
            },
        ]),
        store_log: EntryPositions(vec![loss_entry()]),
        counts: [1, 3, 1, 1, 1],
    }
}
/// Records in their canonical section and identity order.
pub fn snapshot_records() -> Vec<SnapshotRecord> {
    let mut records = vec![SnapshotRecord::Synced(SyncedRow {
        row: row(),
        columns: BTreeMap::from([("x".into(), column(Value::Integer(2)))]),
    })];
    records.extend(oracle().writes.into_values().map(SnapshotRecord::Write));
    records.push(SnapshotRecord::Column(SyncedColumn {
        table: "t".into(),
        column: "x".into(),
    }));
    records.push(SnapshotRecord::Merge(merge_row()));
    records.push(SnapshotRecord::LostWrite(lost_write()));
    records.push(SnapshotRecord::LostWriteRow(lost_write_row()));
    records
}
/// Header of the fixture's excluded write.
pub fn lost_write() -> LostWrite {
    LostWrite {
        header: write().header,
        audience: coven_merge::Audience::Store,
        row_count: 1,
        cause: LostWriteCause::SchemaChange(2),
    }
}
/// The fixture's excluded row, following its lost-write header.
pub fn lost_write_row() -> LostWriteRow {
    LostWriteRow {
        change: write().parts.remove(0).rows.remove(0),
    }
}
/// A plaintext queue value produced by the format's queue encoder.
pub fn write_plaintext(record: &WriteRecord) -> Result<Vec<u8>, crate::Error> {
    let encoder = crate::write_stream::WriteEncoder::new(record)?;
    let length =
        usize::try_from(encoder.plaintext_length()).map_err(|_| crate::Error::Allocation)?;
    let mut bytes = vec![0; length];
    encoder.encode_plaintext(&mut bytes)?;
    Ok(bytes)
}
/// A complete example snapshot emitted by the production streaming encoder.
pub fn snapshot_frames() -> Vec<Vec<u8>> {
    let (mut encoder, header) = SnapshotEncoder::start(snapshot_header()).unwrap();
    let mut frames = vec![header];
    for record in snapshot_records() {
        frames.push(encoder.record(record).unwrap());
    }
    frames.push(encoder.finish().unwrap());
    frames
}
/// Restore contents with test seeds and a test credential byte.
pub fn restore() -> RestoreCode {
    RestoreCode {
        store: StoreId(Uuid::from_bytes([0x10; 16])),
        name: "S".into(),
        member_keys: member_keys(),
        storage: SecretBytes::new(vec![0x44]),
    }
}
/// Invite contents with a fixed test secret.
pub fn invite() -> InviteCode {
    InviteCode {
        store: StoreId(Uuid::from_bytes([0x10; 16])),
        name: "S".into(),
        invite: InviteId(Uuid::from_bytes([0x55; 16])),
        secret: InviteSecret::from_bytes([0x66; 32]),
        storage: SecretBytes::new(vec![0x44]),
    }
}
/// Every independently decoded ordinary object kind.
pub fn objects() -> Vec<Object> {
    vec![
        Object::StoreLog(store_log()),
        Object::StoreLog(member_removal()),
        Object::FileHeader(FileHeader {
            chunk_size: 65_536,
            total_size: 3,
        }),
        Object::JoinRequest(JoinRequest {
            invite: InviteId(Uuid::from_bytes([0x55; 16])),
            keys: member(),
            device_name: "D".into(),
        }),
        Object::PostedPositions(PostedPositions {
            device: DeviceId(1),
            writes: WritePositions(vec![position()]),
            store_log: EntryPositions(vec![]),
            fingerprints: vec![Fingerprint {
                audience: Audience::Store,
                key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
                bytes: coven_crypto::Fingerprint::from_bytes([0x88; 32]),
            }],
        }),
        Object::FileChunk(FileChunk {
            index: 0,
            bytes: vec![1, 2, 3],
        }),
    ]
}
/// A two-audience write with a row frame spanning three chunks.
pub fn chunked_write() -> WriteRecord {
    let mut record = write();
    let mut circle = record.parts[0].clone();
    circle.audience = Audience::Circle(CircleId(Uuid::from_u128(1)));
    circle.rows[0].row.audience = circle.audience.clone();
    record.parts.push(circle);
    record.parts[0].rows[0].change.operation = Operation::Update(BTreeMap::from([(
        "x".into(),
        column(Value::Blob(vec![0x41; crate::chunks::CHUNK_SIZE * 2 + 29])),
    )]));
    record
}
/// A snapshot with a synced row spanning chunk boundaries.
pub fn chunked_snapshot_frames() -> Vec<Vec<u8>> {
    let (mut encoder, header) = SnapshotEncoder::start(snapshot_header()).unwrap();
    let mut records = snapshot_records();
    let SnapshotRecord::Synced(row) = &mut records[0] else {
        unreachable!()
    };
    row.columns.insert(
        "x".into(),
        column(Value::Blob(vec![0x42; crate::chunks::CHUNK_SIZE + 31])),
    );
    let mut frames = vec![header];
    for record in records {
        frames.push(encoder.record(record).unwrap());
    }
    frames.push(encoder.finish().unwrap());
    frames
}
/// Bounded canonical frames used to exercise every byte and truncation.
pub fn frame_examples() -> Vec<Zeroizing<Vec<u8>>> {
    let mut frames: Vec<_> = objects()
        .iter()
        .map(|v| Zeroizing::new(v.encode().unwrap()))
        .collect();
    let record = write();
    let encoder = crate::write_stream::WriteEncoder::new(&record).unwrap();
    frames.push(Zeroizing::new(encoder.header_frame().to_vec()));
    frames.push(Zeroizing::new(record.parts[0].rows[0].encode().unwrap()));
    frames.extend(snapshot_frames().into_iter().map(Zeroizing::new));
    frames
}
/// Pinned plaintext frames, then a write prefix/header/part chunks and a
/// snapshot prefix/plaintext chunks. Crypto supplies sealed bytes separately.
pub fn encoded_examples() -> Vec<Zeroizing<Vec<u8>>> {
    let mut pieces = frame_examples();
    let record = chunked_write();
    let encoder = crate::write_stream::WriteEncoder::new(&record).unwrap();
    pieces.push(Zeroizing::new(
        crate::sealed_write::WriteObjectPrefix {
            store_key: KeyId(Uuid::from_bytes([1; 16])),
            part_keys: vec![
                KeyId(Uuid::from_bytes([1; 16])),
                KeyId(Uuid::from_bytes([2; 16])),
            ],
        }
        .encode()
        .unwrap(),
    ));
    pieces.push(Zeroizing::new(encoder.header_frame().to_vec()));
    for index in 0..record.parts.len() {
        pieces.extend(
            encoder
                .part_chunks(index)
                .unwrap()
                .map(|v| Zeroizing::new(v.unwrap())),
        );
    }
    pieces.push(Zeroizing::new(
        crate::sealed_snapshot::SnapshotObjectPrefix {
            audience: Audience::Store,
            key: KeyId(Uuid::from_bytes([1; 16])),
        }
        .encode()
        .unwrap(),
    ));
    pieces.extend(
        crate::chunks::PlaintextChunks::new(chunked_snapshot_frames().into_iter().map(Ok))
            .map(|v| Zeroizing::new(v.unwrap())),
    );
    pieces
}
