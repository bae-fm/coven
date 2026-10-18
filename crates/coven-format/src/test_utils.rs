//! Deterministic examples for consumer tests, built from the production owners' types.

/// The store shared by the deterministic storage and format fixtures.
pub const STORE_ID: coven_foundation::id_source::StoreId =
    coven_foundation::id_source::StoreId(uuid::Uuid::from_bytes([0x10; 16]));

use crate::admin::*;
use crate::codes::{InviteCode, RestoreCode};
use crate::key::encode_key;
use crate::objects::*;
use crate::snapshot::{SnapshotEncoder, SnapshotHeader, SnapshotRecord};
use crate::snapshot_rows::*;
use crate::value::*;
use crate::write::*;
use crate::Object;
use coven_crypto::{InviteSecret, MemberIdentity, MemberKeys, SecretBytes};
use coven_foundation::id_source::{CircleId, DeviceId, InviteId, KeyId, MemberId, StoreId};
use coven_merge::{
    Audience, Cell, Change, ColumnValue, LostKey, LostValue, MergeError, Operation, Parent, RowId,
    RowState, Timestamp, WriteId, WriteOracle,
};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;
use zeroize::Zeroizing;

/// Decode a pinned hexadecimal fixture for consumer tests.
pub fn fixture_bytes(text: &str) -> Vec<u8> {
    hex::decode(text.trim()).expect("pinned hexadecimal fixture")
}

/// An original write by device one.
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
/// Ana's replacement private pair, with fixed public test seeds 0x34 (§10).
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
            read_through: 5,
            position: position(),
            timestamp: timestamp(),
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
/// The first admin change, carried by slot one.
pub fn creation_change() -> StoreChange {
    StoreChange::CreateStore {
        access: crate::MemberAccess::S3AccessKey {
            access_key_id: "fixture-access-key".into(),
            principal: crate::S3Principal::Console,
        },
        store: STORE_ID,
        name: "S".into(),
        admin: member(),
        key: store_key().id(),
        key_hash: store_key().hash(),
        device_name: "D".into(),
    }
}
/// A member-removal admin change.
pub fn member_removal() -> StoreChange {
    StoreChange::RemoveMember {
        member: other_member_id(),
    }
}
/// A reset's slot used by loss examples.
pub fn loss_entry() -> Slot {
    Slot(2)
}
/// A canonical path for one positive slot number.
pub fn slot_path(number: u64) -> crate::path::ObjectPath {
    crate::path::ObjectPath::slot(STORE_ID, number.try_into().unwrap())
}
/// The deterministic header of a device-one claim.
pub fn slot_header(slot: u64, claim: u64) -> crate::sequence::SlotHeader {
    crate::sequence::SlotHeader {
        device: DeviceId(1),
        claim,
        author: member_id(),
        after: slot - 1,
    }
}
/// Seal and sign a bounded slot with public test keys and deterministic nonces.
pub fn bounded_slot(
    slot: u64,
    claim: u64,
    kind: crate::sealed_slot::SlotKind,
    frame: crate::sequence::SlotFrame,
) -> Vec<u8> {
    let path = slot_path(slot);
    let prefix = crate::sealed_slot::SlotObjectPrefix {
        format: crate::FormatVersion::CURRENT,
        key: store_key().id(),
        kind,
    };
    let plain = frame.encode(&slot_header(slot, claim), &path).unwrap();
    let chunk = store_key().derive().reseal_object_chunk(
        path.as_str(),
        &prefix.encode().unwrap(),
        0,
        0,
        &plain,
    );
    let mut bytes = prefix.encode_chunk(&chunk).unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend_from_slice(
        member_keys()
            .sign_object(path.as_str(), &hash.finish())
            .as_bytes(),
    );
    bytes
}
/// An earlier snapshot announcement preserved by the fixture snapshot.
pub fn announcement() -> crate::sequence::SnapshotAnnouncement {
    crate::sequence::SnapshotAnnouncement {
        snapshot: snapshot_header().id,
        covers: 6,
        purpose: crate::sequence::Purpose::Ordinary,
        digest: [0x99; 32],
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
                || self.writes[&earlier].slot.0 <= write.read_through,
        )
    }
}
impl crate::snapshot::WriteSlots for TestOracle {
    fn slot(&self, write: WriteId) -> Option<Slot> {
        self.writes.get(&write).map(|write| write.slot)
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
                slot: Slot(3),
                timestamp: Timestamp::new(1, 0, first.device).unwrap(),
                read_through: 0,
            },
            AppliedWrite {
                id: winner,
                slot: Slot(5),
                timestamp: Timestamp::new(4, 0, winner.device).unwrap(),
                read_through: 0,
            },
            AppliedWrite {
                id: loser,
                slot: Slot(4),
                timestamp: Timestamp::new(3, 0, loser.device).unwrap(),
                read_through: 3,
            },
            AppliedWrite {
                id: position(),
                slot: Slot(6),
                timestamp: timestamp(),
                read_through: 5,
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
/// The header for the fixture's twelve snapshot sections.
pub fn snapshot_header() -> SnapshotHeader {
    SnapshotHeader {
        id: SnapshotId {
            device: DeviceId(1),
            number: 1,
            audience: Audience::Store,
        },
        schema_version: 2,
        covers: 9,
        counts: [1, 4, 1, 1, 4, 1, 1, 1, 1, 1, 1, 2],
    }
}
/// The sealed prefix carrying a snapshot's coverage, with a fixed test key.
pub fn snapshot_prefix(header: &SnapshotHeader) -> crate::sealed_snapshot::SnapshotObjectPrefix {
    crate::sealed_snapshot::SnapshotObjectPrefix {
        audience: header.id.audience.clone(),
        key: KeyId(Uuid::from_bytes([1; 16])),
        covers: header.covers,
        purpose: crate::sequence::Purpose::Raise(header.schema_version),
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
            slot: Slot(6),
            read_through: header.read_through,
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
    records.push(SnapshotRecord::Admin(crate::floor::OriginalSlot {
        slot: Slot(1),
        object: bounded_slot(
            1,
            1,
            crate::sealed_slot::SlotKind::Admin,
            crate::sequence::SlotFrame::Admin(creation_change()),
        ),
    }));
    records.push(SnapshotRecord::KeyCopy(crate::floor::OriginalSlot {
        slot: Slot(2),
        object: hex::decode(include_str!("../fixtures/sealed-store-key.hex").trim()).unwrap(),
    }));
    records.push(SnapshotRecord::Announcement(crate::floor::OriginalSlot {
        slot: Slot(7),
        object: bounded_slot(
            7,
            6,
            crate::sealed_slot::SlotKind::Announcement,
            crate::sequence::SlotFrame::Announcement(announcement()),
        ),
    }));
    records.push(SnapshotRecord::Claim(crate::snapshot::LastClaim {
        device: DeviceId(1),
        number: 8,
        digest: Some([0x81; 32]),
    }));
    records.push(SnapshotRecord::Claim(crate::snapshot::LastClaim {
        device: DeviceId(2),
        number: 1,
        digest: Some([0x21; 32]),
    }));
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
                .map(|(name, mut value)| {
                    value.parents.clear();
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
        read_through: 12,
        storage: SecretBytes::new(vec![0x44]),
    }
}
/// Each admin operation in its canonical tag order.
pub fn store_changes() -> Vec<StoreChange> {
    let c = CircleId(uuid::Uuid::from_bytes([2; 16]));
    let m = member_id();
    vec![
        creation_change(),
        StoreChange::AddMember {
            member: other_member_id(),
            access: crate::MemberAccess::ProviderAccount("member@example.test".into()),
            keys: other_member(),
            role: MemberRole::Member,
        },
        member_removal(),
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
            announcement: Slot(7),
        },
        StoreChange::Reset {
            announcement: Slot(8),
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
            expected: member(),
            keys: replacement(),
        },
        StoreChange::RenameStore { name: "N".into() },
    ]
}
/// Every independently decoded ordinary object kind.
pub fn objects() -> Vec<Object> {
    store_changes()
        .into_iter()
        .map(Object::Admin)
        .chain([Object::JoinRequest(join_request())])
        .collect()
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
            target: crate::dismissal::DismissalTarget::Cell("x".into()),
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
        snapshot_prefix(&snapshot_header()).encode().unwrap(),
    ));
    pieces.extend(
        crate::chunks::PlaintextChunks::new(chunked_snapshot_frames().into_iter().map(Ok))
            .map(|v| Zeroizing::new(v.unwrap())),
    );
    pieces
}

/// The fixture author has read these original writes.
pub fn past() -> BTreeSet<WriteId> {
    [(1, 1), (1, 2), (2, 1)]
        .map(|(device, number)| WriteId {
            device: DeviceId(device),
            number,
        })
        .into()
}
/// Validate the fixture write against its explicitly supplied past.
pub fn check_past(record: &WriteRecord) -> Result<(), crate::Error> {
    let past = past();
    for part in &record.parts {
        for row in &part.rows {
            row.validate_past(&past)?;
        }
        for execution in &part.executions {
            execution.execution.sources.validate_past(&past)?;
        }
        for dismissal in &part.dismissals {
            dismissal.validate_past(&past)?;
        }
    }
    Ok(())
}
