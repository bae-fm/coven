//! Deterministic examples for consumer tests, built from the production owners' types.

/// The store shared by the deterministic storage and format fixtures.
pub const STORE_ID: coven_foundation::id_source::StoreId =
    coven_foundation::id_source::StoreId(uuid::Uuid::from_bytes([0x10; 16]));

use crate::codes::{InviteCode, RestoreCode};
use crate::key::encode_key;
use crate::objects::*;
use crate::snapshot::{SnapshotEncoder, SnapshotHeader, SnapshotRecord};
use crate::snapshot_rows::*;
use crate::store_log::*;
use crate::value::*;
use crate::write::*;
use crate::Object;
use coven_crypto::{InviteSecret, MemberIdentity, MemberKeys, SecretBytes};
use coven_foundation::id_source::{CircleId, DeviceId, InviteId, KeyId, MemberId, StoreId};
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
/// A private pair whose signing and sealing seeds are both `seed`: public test data.
pub fn keys_from_seed(seed: u8) -> MemberKeys {
    let mut bytes = Zeroizing::new(Vec::with_capacity(69));
    bytes.extend_from_slice(b"CVMK\x01");
    bytes.extend_from_slice(&[seed; 64]);
    MemberKeys::from_secret_bytes(&bytes).unwrap()
}
/// The fixture member Ana's stable id, independent of every key she holds.
pub fn member_id() -> MemberId {
    MemberId(Uuid::from_bytes([0x4d; 16]))
}
/// Ana's original private pair, with fixed public test seeds 0x33.
pub fn member_keys() -> MemberKeys {
    keys_from_seed(0x33)
}
/// Ana's id with her original pair, as identity custody holds it.
pub fn identity() -> MemberIdentity {
    MemberIdentity::new(member_id(), member_keys())
}
/// The public halves of Ana's original pair.
pub fn member() -> MemberPublicKeys {
    member_keys().public_keys()
}
/// Ana's replacement private pair, with fixed public test seeds 0x34 (§9).
pub fn replacement_keys() -> MemberKeys {
    keys_from_seed(0x34)
}
/// The public halves of Ana's replacement pair, bound to the same id.
pub fn replacement() -> MemberPublicKeys {
    replacement_keys().public_keys()
}
/// Another member, Ben, whom the fixtures add and remove.
pub fn other_member_id() -> MemberId {
    MemberId(Uuid::from_bytes([0x42; 16]))
}
/// Ben's initial public pair, with fixed public test seeds 0x35.
pub fn other_member() -> MemberPublicKeys {
    keys_from_seed(0x35).public_keys()
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
            executions: vec![transient_execution(Audience::Store)],
            rows: vec![RowChange {
                row: row(),
                creator: None,
                execution: Execution {
                    steps: vec![Step {
                        sequence: 0,
                        change: Change {
                            generation: 1,
                            operation: Operation::Update(BTreeMap::from([(
                                "x".into(),
                                new.clone(),
                            )])),
                        },
                        key_update: false,
                    }],
                    sources: Default::default(),
                },
                change: Change {
                    generation: 1,
                    operation: Operation::Update(BTreeMap::from([("x".into(), new)])),
                },
                old: BTreeMap::from([("x".into(), Value::Integer(1))]),
                sources: crate::write_sources::WriteSources {
                    generation: Some(crate::write_sources::ChangeId {
                        write: WriteId {
                            device: DeviceId(1),
                            number: 1,
                        },
                        row: row(),
                    }),
                    setters: [(
                        "x".into(),
                        crate::write_sources::CellSource {
                            change: crate::write_sources::ChangeId {
                                write: WriteId {
                                    device: DeviceId(2),
                                    number: 1,
                                },
                                row: row(),
                            },
                            column: "x".into(),
                        },
                    )]
                    .into(),
                },
            }],
        }],
    }
}
/// A row the fixture write inserts and deletes again: no net change, two
/// operations, which still reach the write's authorship leaves (D5).
pub fn transient_execution(audience: Audience) -> RowExecution {
    let row = RowId {
        key: encode_key(&[Value::Text("l".into())]).unwrap(),
        audience,
        ..row()
    };
    RowExecution {
        row,
        execution: Execution {
            steps: vec![
                Step {
                    sequence: 1,
                    change: Change {
                        generation: 0,
                        operation: Operation::Insert(BTreeMap::from([(
                            "x".into(),
                            column(Value::Integer(5)),
                        )])),
                    },
                    key_update: false,
                },
                Step {
                    sequence: 2,
                    change: Change {
                        generation: 1,
                        operation: Operation::Delete,
                    },
                    key_update: false,
                },
            ],
            sources: Default::default(),
        },
    }
}
/// The public fixture's store key, whose bytes are committed by creation.
pub fn store_key() -> coven_crypto::StoreKey {
    coven_crypto::StoreKey::from_bytes(KeyId(Uuid::from_bytes([1; 16])), [17; 32])
}
/// A signed clock object for device one, with public fixture key and nonce material.
pub fn sealed_clock() -> Vec<u8> {
    hex::decode(include_str!("../fixtures/sealed-clock.hex").trim()).unwrap()
}
/// The first store-log entry.
pub fn store_log() -> StoreLogEntry {
    StoreLogEntry {
        position: EntryId {
            device: DeviceId(1),
            number: 1,
        },
        timestamp: timestamp(),
        author: member_id(),
        had_read: EntryPositions(vec![]),
        change: StoreChange::CreateStore {
            access: crate::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
                principal: crate::S3Principal::Console,
            },
            store: StoreId(Uuid::from_bytes([0x10; 16])),
            name: "S".into(),
            admin: member(),
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
            key_hash: store_key().hash(),
            device_name: "D".into(),
        },
    }
}
/// A member removal deleting circles left empty in its author's view.
pub fn member_removal() -> StoreLogEntry {
    StoreLogEntry {
        position: EntryId {
            device: DeviceId(1),
            number: 2,
        },
        timestamp: Timestamp::new(7, 0, DeviceId(1)).unwrap(),
        author: member_id(),
        had_read: EntryPositions(vec![]),
        change: StoreChange::RemoveMember {
            member: other_member_id(),
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
        counts: [1, 4, 1, 1, 4, 1, 1, 1],
    }
}
/// The fixture's catch-up storage time, with a subsecond fraction.
pub fn caught_at() -> crate::value::StorageTime {
    crate::value::StorageTime::new(1_760_400_000, 123_456_789).unwrap()
}
/// The sealed prefix carrying a snapshot's coverage, with a fixed test key.
pub fn snapshot_prefix(header: &SnapshotHeader) -> crate::sealed_snapshot::SnapshotObjectPrefix {
    crate::sealed_snapshot::SnapshotObjectPrefix {
        audience: header.id.audience.clone(),
        key: KeyId(Uuid::from_bytes([1; 16])),
        caught_at: caught_at(),
        raise: std::num::NonZeroU32::new(header.schema_version),
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
    records.push(SnapshotRecord::Authored(authored_change()));
    records.push(SnapshotRecord::Creation(creation()));
    records.push(SnapshotRecord::Evidence(write_evidence()));
    records
}
/// The fixture row's selected creation, by device one's first write.
pub fn creation() -> crate::authorship::Creation {
    let write = WriteId {
        device: DeviceId(1),
        number: 1,
    };
    crate::authorship::Creation {
        row: row(),
        write,
        creator: crate::write_sources::ChangeId { write, row: row() },
    }
}
/// Evidence for the fixture write's store part, opening its updated cell.
pub fn write_evidence() -> crate::authorship::WriteEvidence {
    let leaves = crate::authorship::part_leaves(&write().parts[0]);
    let hasher = store_key().derive().authorship_hasher();
    let proof = crate::authorship::Multiproof::prove(&hasher, &leaves, |leaf| {
        matches!(leaf, crate::authorship::Leaf::Cell { .. })
    })
    .unwrap();
    let mut header = 7u32.to_be_bytes().to_vec();
    header.extend([0x23; 24 + 7 + 16]);
    crate::authorship::WriteEvidence {
        write: position(),
        prefix: crate::sealed_write::WriteObjectPrefix {
            format: crate::FormatVersion::CURRENT,
            store_key: KeyId(Uuid::from_bytes([1; 16])),
            part_keys: vec![KeyId(Uuid::from_bytes([1; 16]))],
        }
        .encode()
        .unwrap(),
        header,
        roots: vec![crate::authorship::root(&hasher, &leaves).unwrap()],
        signature: [0x22; 64],
        proof,
    }
}
/// The fixture write's row change as source evidence, applied by its author.
pub fn authored_change() -> crate::snapshot_rows::AuthoredChange {
    let write = write();
    crate::snapshot_rows::AuthoredChange {
        header: write.header,
        complete: true,
        change: OriginalRow::Change(write.parts[0].rows[0].clone()),
        verdict: crate::snapshot_rows::ChangeVerdict::Applied,
    }
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
    cell.frozen = true;
    let mut removed = Loss::removed(
        &merge_row().state,
        [coven_merge::Rule::Check("valid".into())].into(),
    );
    removed.row = row;
    removed.frozen = true;
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
        frozen: true,
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
            version: 2,
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
/// Restore contents with test seeds and an S3 location without credentials.
pub fn restore() -> RestoreCode {
    RestoreCode {
        store: StoreId(Uuid::from_bytes([0x10; 16])),
        name: "S".into(),
        member: member_id(),
        member_keys: member_keys(),
        storage: SecretBytes::new(
            br#"{"S3":{"bucket":"bucket","region":"region","endpoint":null,"profile":null,"prefix":"store"}}"#
                .to_vec(),
        ),
    }
}
/// Invite contents with a fixed test secret.
pub fn invite() -> InviteCode {
    InviteCode {
        store: StoreId(Uuid::from_bytes([0x10; 16])),
        name: "S".into(),
        invite: InviteId(Uuid::from_bytes([0x55; 16])),
        // A re-binding invite names Ana's existing id (D13).
        member: member_id(),
        secret: InviteSecret::from_bytes([0x66; 32]),
        initial_key: store_key().id(),
        inviting_writer: DeviceId(1),
        key_copy_number: 12,
        storage: SecretBytes::new(vec![0x44]),
    }
}
/// Each store-log change in its canonical tag order.
pub fn store_changes() -> Vec<StoreChange> {
    let c = CircleId(uuid::Uuid::from_bytes([2; 16]));
    let m = member_id();
    let snapshot = snapshot_header().id;
    vec![
        store_log().change,
        StoreChange::AddMember {
            member: other_member_id(),
            access: crate::MemberAccess::ProviderAccount("member@example.test".into()),
            keys: other_member(),
            role: MemberRole::Member,
        },
        member_removal().change,
        StoreChange::ChangeRole {
            member: m,
            role: MemberRole::Admin,
        },
        StoreChange::AddDevice {
            name: "D".into(),
            replaces: None,
        },
        StoreChange::RemoveDevice {
            device: DeviceId(2),
        },
        StoreChange::CreateCircle {
            circle: c,
            name: "C".into(),
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
            key_hash: store_key().hash(),
        },
        StoreChange::RenameCircle {
            circle: c,
            name: "N".into(),
        },
        StoreChange::DeleteCircle { circle: c },
        StoreChange::AddCircleMember {
            circle: c,
            member: m,
        },
        StoreChange::RemoveCircleMember {
            circle: c,
            member: m,
        },
        StoreChange::RaiseSchema {
            version: 2,
            snapshot: snapshot.clone(),
            caught_at: crate::value::StorageTime::new(1_700_000_000, 5).expect("a valid time"),
        },
        StoreChange::Reset {
            snapshot: SnapshotId {
                audience: Audience::Circle(c),
                ..snapshot.clone()
            },
        },
        StoreChange::SetAccess {
            member: m,
            access: crate::MemberAccess::S3AccessKey {
                access_key_id: "replacement-key".into(),
                principal: crate::S3Principal::IamUser {
                    user: "coven-member".into(),
                    policy: crate::IamPolicy::Inline {
                        name: "coven-store".into(),
                    },
                },
            },
        },
        StoreChange::RotateKey {
            audience: Audience::Store,
            key: KeyId(Uuid::from_bytes([3; 16])),
            key_hash: [0x91; 32],
        },
        StoreChange::RotateKey {
            audience: Audience::Circle(c),
            key: KeyId(Uuid::from_bytes([4; 16])),
            key_hash: [0xa2; 32],
        },
        StoreChange::ReplaceMemberKeys {
            member: m,
            keys: replacement(),
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
                caught_at: crate::value::StorageTime::new(1_700_000_100, 0).expect("a valid time"),
            },
            ..store_log()
        }),
        Object::JoinRequest(join_request()),
        Object::PostedPositions(PostedPositions {
            pending: vec![
                crate::pending::PendingReport {
                    subject: crate::pending::PendingSubject::Write(WriteId {
                        device: DeviceId(2),
                        number: 4,
                    }),
                    reason: crate::pending::PendingReason::Refused(
                        crate::pending::RefusalCode::InvalidWrite,
                    ),
                },
                crate::pending::PendingReport {
                    subject: crate::pending::PendingSubject::Entry(EntryId {
                        device: DeviceId(3),
                        number: 2,
                    }),
                    reason: crate::pending::PendingReason::Refused(
                        crate::pending::RefusalCode::Signature,
                    ),
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
    objects.push(Object::ClockObservation(clock()));
    objects
}
/// Ana's request answering the re-binding invite with her replacement pair,
/// which signs it (D8, D13).
pub fn join_request() -> JoinRequest {
    JoinRequest {
        invite: InviteId(Uuid::from_bytes([0x55; 16])),
        member: member_id(),
        keys: replacement(),
        device_name: "D".into(),
    }
}
/// Device one's clock frame, with the store log it had read (D8).
pub fn clock() -> ClockObservation {
    ClockObservation {
        device: DeviceId(1),
        store_log_read: EntryPositions(vec![EntryId {
            device: DeviceId(1),
            number: 2,
        }]),
    }
}
/// A two-audience write with a row frame spanning three chunks.
pub fn chunked_write() -> WriteRecord {
    let mut record = write();
    let mut circle = record.parts[0].clone();
    circle.audience = Audience::Circle(CircleId(Uuid::from_u128(1)));
    circle.rows[0].row.audience = circle.audience.clone();
    circle.executions = vec![transient_execution(circle.audience.clone())];
    record.parts.push(circle);
    let blob = Operation::Update(BTreeMap::from([(
        "x".into(),
        column(Value::Blob(vec![0x41; crate::chunks::CHUNK_SIZE * 2 + 29])),
    )]));
    record.parts[0].rows[0].change.operation = blob.clone();
    record.parts[0].rows[0].execution.steps[0].change.operation = blob;
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
            format: crate::FormatVersion::CURRENT,
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
            caught_at: caught_at(),
            raise: std::num::NonZeroU32::new(crate::test_utils::snapshot_header().schema_version),
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
