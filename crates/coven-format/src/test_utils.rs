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
            store_log_read: crate::value::EntryPositions(Vec::new()),
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
            dismissals: Vec::new(),
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
            access: crate::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
            },
            store: StoreId(Uuid::from_bytes([0x10; 16])),
            name: "S".into(),
            admin: member(),
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
            device_name: "D".into(),
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
        had_read: EntryPositions(vec![]),
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
        BTreeMap::new(),
        &oracle(),
    )
    .unwrap();
    MergeRow { state }
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
        counts: [1, 4, 1, 1, 4],
    }
}
/// The sealed prefix carrying a snapshot's coverage, with a fixed test key.
pub fn snapshot_prefix(header: &SnapshotHeader) -> crate::sealed_snapshot::SnapshotObjectPrefix {
    crate::sealed_snapshot::SnapshotObjectPrefix {
        audience: header.id.audience.clone(),
        key: KeyId(Uuid::from_bytes([1; 16])),
        writes: header.writes.clone(),
        store_log: header.store_log.clone(),
    }
}
/// Records in their canonical section and identity order.
pub fn snapshot_records() -> Vec<SnapshotRecord> {
    let mut records = vec![SnapshotRecord::Synced(SyncedRow {
        row: row(),
        columns: BTreeMap::from([("x".into(), column(Value::Integer(2)))]),
    })];
    let mut writes = oracle().writes;
    let header = write().header;
    writes.insert(
        header.position,
        AppliedWrite {
            id: header.position,
            timestamp: header.timestamp,
            had_read: header.had_read,
        },
    );
    records.extend(writes.into_values().map(SnapshotRecord::Write));
    records.push(SnapshotRecord::Column(SyncedColumn {
        table: "t".into(),
        column: "x".into(),
    }));
    records.push(SnapshotRecord::Merge(merge_row()));
    records.extend(
        retained_losses()
            .into_iter()
            .chain([concurrent_loss(), excluded_loss()])
            .map(SnapshotRecord::Loss),
    );
    records
}
/// A concurrent cell loss of the fixture row.
pub fn concurrent_loss() -> crate::loss::Loss {
    crate::loss::Loss::cell(
        row(),
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
    )
}
/// Both kinds of loss after their row's merge history has been discarded.
pub fn retained_losses() -> Vec<crate::loss::Loss> {
    use crate::loss::Loss;
    let row = RowId {
        table: "removed".into(),
        ..row()
    };
    let mut cell = concurrent_loss();
    cell.row = row.clone();
    cell.retired = true;
    let mut removed = Loss::removed(
        &merge_row().state,
        [coven_merge::Rule::Check("valid".into())].into(),
    );
    removed.row = row;
    removed.retired = true;
    vec![cell, removed]
}
/// The row values of a write excluded by a schema change.
pub fn excluded_loss() -> crate::loss::Loss {
    use crate::loss::{Loss, LossCause, LossValues};
    let change = write().parts.remove(0).rows.remove(0);
    let Operation::Update(values) = change.change.operation else {
        unreachable!()
    };
    Loss {
        row: change.row,
        generation: change.change.generation,
        retired: true,
        values: LossValues::Row(
            values
                .into_iter()
                .map(|(name, value)| {
                    (
                        name,
                        Cell {
                            write: position(),
                            value,
                        },
                    )
                })
                .collect(),
        ),
        cause: LossCause::Excluded {
            write: write().header.position,
            cause: LostWriteCause::SchemaChange(2),
        },
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
/// Each store-log change in its canonical tag order.
pub fn store_changes() -> Vec<StoreChange> {
    let c = CircleId(uuid::Uuid::from_bytes([2; 16]));
    let m = member().signing;
    let snapshot = snapshot_header().id;
    vec![
        store_log().change,
        StoreChange::AddMember {
            access: crate::MemberAccess::ProviderAccount("member@example.test".into()),
            keys: member(),
            role: MemberRole::Member,
        },
        member_removal().change,
        StoreChange::ChangeRole {
            member: m.clone(),
            role: MemberRole::Admin,
        },
        StoreChange::AddDevice {
            device: DeviceId(2),
            name: "D".into(),
        },
        StoreChange::RemoveDevice {
            device: DeviceId(2),
        },
        StoreChange::CreateCircle {
            circle: c,
            name: "C".into(),
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
        },
        StoreChange::RenameCircle {
            circle: c,
            name: "N".into(),
        },
        StoreChange::DeleteCircle { circle: c },
        StoreChange::AddCircleMember {
            circle: c,
            member: m.clone(),
        },
        StoreChange::RemoveCircleMember {
            circle: c,
            member: m.clone(),
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([2; 16])),
        },
        StoreChange::RaiseSchema {
            version: 2,
            snapshot: snapshot.clone(),
        },
        StoreChange::Reset {
            snapshot: SnapshotId {
                audience: Audience::Circle(c),
                ..snapshot.clone()
            },
        },
        StoreChange::SetAccess {
            access: crate::MemberAccess::S3AccessKey {
                access_key_id: "replacement-key".into(),
            },
        },
    ]
}
/// Every independently decoded ordinary object kind.
pub fn objects() -> Vec<Object> {
    let mut objects = vec![
        Object::StoreLog(store_log()),
        Object::StoreLog(member_removal()),
        Object::StoreLog(StoreLogEntry {
            change: StoreChange::RaiseSchema {
                version: 3,
                snapshot: SnapshotId {
                    audience: Audience::Circle(CircleId(Uuid::from_u128(7))),
                    device: DeviceId(9),
                    number: 11,
                },
            },
            ..store_log()
        }),
        Object::JoinRequest(JoinRequest {
            invite: InviteId(Uuid::from_bytes([0x55; 16])),
            keys: member(),
            device_name: "D".into(),
        }),
        Object::PostedPositions(PostedPositions {
            stuck: vec![
                crate::stuck::StuckRecord {
                    object: crate::stuck::LogObject::Write(WriteId {
                        device: DeviceId(2),
                        number: 4,
                    }),
                    failure: crate::stuck::StuckFailure::InvalidWrite,
                },
                crate::stuck::StuckRecord {
                    object: crate::stuck::LogObject::Entry(EntryId {
                        device: DeviceId(3),
                        number: 2,
                    }),
                    failure: crate::stuck::StuckFailure::Signature,
                },
            ],
            schema_version: 1,
            device: DeviceId(1),
            writes: WritePositions(vec![position()]),
            store_log: EntryPositions(vec![]),
            fingerprints: vec![Fingerprint {
                audience: Audience::Store,
                key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
                bytes: coven_crypto::Fingerprint::from_bytes([0x88; 32]),
            }],
        }),
    ];
    objects.extend(
        store_changes()
            .into_iter()
            .enumerate()
            .filter_map(|(index, change)| {
                if matches!(index, 0 | 2 | 11) {
                    return None;
                }
                Some(Object::StoreLog(StoreLogEntry {
                    change,
                    ..store_log()
                }))
            }),
    );
    objects
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
    frames.push(Zeroizing::new(
        crate::dismissal::Dismissal {
            row: row(),
            column: "x".into(),
            write: position(),
        }
        .encode()
        .unwrap(),
    ));
    let mut migration = record.clone();
    migration.header.disposition = WriteDisposition::Migration;
    migration.parts.clear();
    frames.push(Zeroizing::new(write_plaintext(&migration).unwrap()));
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
            writes: crate::test_utils::snapshot_header().writes,
            store_log: crate::test_utils::snapshot_header().store_log,
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
