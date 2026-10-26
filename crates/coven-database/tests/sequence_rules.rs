//! Shared-sequence rules exercised through the database API.
use coven_database::*;
use coven_format::{
    sequence::SlotHeader,
    value::Value,
    write::{RowChange as WireChange, WriteDisposition, WriteHeader, WritePart, WriteRecord},
};
use coven_foundation::{
    files::StoreDirectories,
    id_source::{DeviceId, IdSource, MemberId, SequentialIds, StoreId},
};
use coven_merge::{Audience, Change, ColumnValue, Operation, Timestamp};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::Cursor};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pending {
    subject: u64,
    cutoff: Option<(u64, String)>,
}
impl PendingValue for Pending {
    type Subject = u64;
    fn subject(&self) -> &u64 {
        &self.subject
    }
    fn file(
        &self,
    ) -> Option<(
        coven_foundation::id_source::DeviceId,
        coven_foundation::id_source::FileId,
    )> {
        None
    }
    fn operation(&self) -> Option<OperationId> {
        None
    }
    fn slot(&self) -> Option<Slot> {
        Some(Slot(self.subject))
    }
    fn ends_when_covered(&self) -> bool {
        false
    }
    fn can_retry(&self, _: &str, _: u32) -> bool {
        false
    }
    fn reload(_: Audience) -> Self {
        Self {
            subject: 0,
            cutoff: None,
        }
    }
    fn provider_access(id: OperationId, _: String, _: bool, _: bool) -> Self {
        Self {
            subject: id.0 as u64,
            cutoff: None,
        }
    }
    fn writes_refused(slot: Slot, cutoff: Slot, cause: WriteCutoff) -> Self {
        Self {
            subject: slot.0,
            cutoff: Some((cutoff.0, format!("{cause:?}"))),
        }
    }
}
async fn open(owned: bool) -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let ids = SequentialIds::new();
    let store = StoreDirectories::new(dir.path().into())
        .create_store_dir(StoreId(ids.new_id()), &ids)
        .unwrap();
    let table = SyncedTable::new("items", RowIdentity::SharedKey);
    let table = if owned { table.owner_only() } else { table };
    let db = DatabaseBuilder::new(store)
        .synced_tables(vec![table])
        .migrations(vec![Migration::addition(
            1,
            "items",
            "CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,value TEXT NOT NULL UNIQUE)",
        )])
        .open()
        .await
        .unwrap();
    (dir, db)
}
fn member(n: u128) -> MemberId {
    MemberId(uuid::Uuid::from_u128(n))
}
fn signer() -> coven_crypto::SigningKey {
    coven_format::test_utils::keys_from_seed(1)
        .public_keys()
        .signing
}
fn insert(device: u64, number: u64, time: u64, key: &str, value: &str) -> WriteRecord {
    let id = WriteId {
        device: DeviceId(device),
        number,
    };
    WriteRecord {
        header: WriteHeader {
            position: id,
            timestamp: Timestamp::new(time, 0, id.device).unwrap(),
            read_through: 0,
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: Audience::Store,
            rows: vec![WireChange {
                row: coven_merge::RowId {
                    table: "items".into(),
                    key: coven_format::key::encode_key(&[Value::Text(key.into())]).unwrap(),
                    audience: Audience::Store,
                },
                change: Change {
                    generation: 0,
                    operation: Operation::Insert(
                        [("id", key), ("value", value)]
                            .into_iter()
                            .map(|(k, v)| {
                                (
                                    k.into(),
                                    ColumnValue {
                                        value: Value::Text(v.into()),
                                        parents: BTreeMap::new(),
                                    },
                                )
                            })
                            .collect(),
                    ),
                },
                old: BTreeMap::new(),
                sources: Default::default(),
                execution: Default::default(),
                creator: None,
            }],
            executions: vec![],
            dismissals: vec![],
        }],
    }
}
fn stream(record: WriteRecord) -> DownloadedWriteStream<Cursor<Vec<u8>>> {
    let encoder = coven_format::write_stream::WriteEncoder::new(&record).unwrap();
    let key = coven_crypto::StoreKey::from_bytes(
        coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(1)),
        [1; 32],
    );
    let hasher = key.derive().authorship_hasher();
    let roots = record
        .parts
        .iter()
        .map(|p| {
            coven_format::authorship::root(&hasher, &coven_format::authorship::part_leaves(p))
                .unwrap()
        })
        .collect();
    DownloadedWriteStream {
        header: encoder.header().clone(),
        signer: signer(),
        parts: (0..record.parts.len())
            .map(|i| {
                DownloadedPartStream::Opened(Cursor::new(
                    encoder
                        .part_chunks(i)
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap()
                        .concat(),
                ))
            })
            .collect(),
        authorship: WriteAuthorship {
            evidence: HeaderEvidence {
                prefix: vec![],
                header: vec![],
                roots,
                signature: [0; 64],
            },
            hashers: record.parts.iter().map(|_| Some(hasher.clone())).collect(),
        },
    }
}
async fn apply(
    db: &Database,
    slot: u64,
    writes: Vec<WriteRecord>,
) -> Result<ApplyOutcome, DbError> {
    apply_claim(db, slot, slot, [0x31; 32], writes).await
}
async fn apply_claim(
    db: &Database,
    slot: u64,
    claim: u64,
    digest: [u8; 32],
    writes: Vec<WriteRecord>,
) -> Result<ApplyOutcome, DbError> {
    let device = writes[0].header.position.device;
    db.apply_slot::<Pending, _, _>(
        IncomingSlot {
            digest,
            slot: Slot(slot),
            header: SlotHeader {
                device,
                claim,
                author: member(device.0 as u128),
                after: slot - 1,
            },
            content: SlotContent::Writes(writes.into_iter().map(stream).collect()),
        },
        || Ok(()),
        None,
    )
    .await
}

#[tokio::test]
async fn greatest_claim_keeps_its_digest_and_equal_claim_uses_the_later_slot() {
    let (_temp, db) = open(false).await;
    for (slot, claim, digest, expected) in [
        (1, 3, [1; 32], [1; 32]),
        (2, 2, [2; 32], [1; 32]),
        (3, 3, [3; 32], [3; 32]),
    ] {
        apply_claim(
            &db,
            slot,
            claim,
            digest,
            vec![insert(10, slot, slot, &slot.to_string(), &slot.to_string())],
        )
        .await
        .unwrap();
        assert_eq!(
            db.last_claims().await.unwrap(),
            [coven_format::snapshot::LastClaim {
                device: DeviceId(10),
                number: 3,
                digest: Some(expected),
            }]
        );
    }
    db.close().await.unwrap();
}
async fn rows(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        Ok(
            sql.query("SELECT id,value FROM items ORDER BY id", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?,
        )
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn applied_slot_and_all_effects_commit_together() {
    let (_temp, db) = open(false).await;
    let one = insert(10, 1, 1, "a", "first");
    let two = insert(10, 2, 2, "b", "second");
    let incoming = IncomingSlot {
        digest: [0x31; 32],
        slot: Slot(1),
        header: SlotHeader {
            device: DeviceId(10),
            claim: 1,
            author: member(10),
            after: 0,
        },
        content: SlotContent::Writes(vec![stream(one.clone()), stream(two.clone())]),
    };
    assert!(db
        .apply_slot::<Pending, _, _>(incoming, || Err(DbError::TransactionEnded), None)
        .await
        .is_err());
    assert_eq!(db.applied_slot().await.unwrap(), 0);
    assert!(rows(&db).await.is_empty());
    assert!(db.last_claims().await.unwrap().is_empty());
    assert!(apply(&db, 2, vec![one.clone()]).await.is_err());
    assert_eq!(
        apply(&db, 1, vec![one, two]).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(db.applied_slot().await.unwrap(), 1);
    assert_eq!(rows(&db).await.len(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn first_slot_and_then_original_number_win_unique_values() {
    // Unique/LiveClaims: timestamps never move a later claim before an earlier slot.
    for timestamps in [[100, 1], [1, 100]] {
        let (_temp, db) = open(false).await;
        apply(&db, 1, vec![insert(10, 1, timestamps[0], "a", "same")])
            .await
            .unwrap();
        apply(&db, 2, vec![insert(20, 1, timestamps[1], "b", "same")])
            .await
            .unwrap();
        assert_eq!(rows(&db).await, [("a".into(), "same".into())]);
        assert_eq!(db.lost_values().await.unwrap().len(), 1);
        db.close().await.unwrap();
    }
    let (_temp, db) = open(false).await;
    apply(
        &db,
        1,
        vec![insert(10, 1, 1, "a", "same"), insert(10, 2, 2, "b", "same")],
    )
    .await
    .unwrap();
    assert_eq!(rows(&db).await, [("a".into(), "same".into())]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn counted_claim_precedes_unclaimed_local_edit() {
    let (_temp, db) = open(false).await;
    db.write(|sql| {
        sql.execute("INSERT INTO items VALUES('local','same')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    apply(&db, 1, vec![insert(10, 1, 1, "remote", "same")])
        .await
        .unwrap();
    assert_eq!(rows(&db).await, [("remote".into(), "same".into())]);
    assert_eq!(db.lost_values().await.unwrap().len(), 1);
    assert!(db
        .read_oldest_upload(|_| Ok::<_, DbError>(()))
        .await
        .unwrap()
        .is_some());
    db.close().await.unwrap();
}

#[tokio::test]
async fn read_through_is_the_committed_prefix() {
    // Causality: every earlier slot is read, even if the last slot has an older clock.
    let (_temp, db) = open(false).await;
    apply(&db, 1, vec![insert(10, 1, 100, "a", "first")])
        .await
        .unwrap();
    apply(&db, 2, vec![insert(20, 1, 1, "b", "second")])
        .await
        .unwrap();
    db.write(|sql| {
        sql.execute("UPDATE items SET value='changed' WHERE id='a'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let header = db
        .read_oldest_upload(|u| Ok::<_, DbError>(u.header.header))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(header.read_through, 2);
    assert!(header.timestamp.milliseconds() >= 100);
    assert!(db.lost_values().await.unwrap().is_empty());
    db.close().await.unwrap();
}

async fn admin(
    db: &Database,
    slot: u64,
    change: coven_format::admin::StoreChange,
    state: StoreState,
) {
    db.apply_slot::<Pending, std::io::Empty, _>(
        IncomingSlot {
            digest: [0x31; 32],
            slot: Slot(slot),
            header: SlotHeader {
                device: DeviceId(99),
                claim: slot,
                author: member(99),
                after: slot - 1,
            },
            content: SlotContent::Admin {
                original: coven_format::floor::OriginalSlot {
                    slot: Slot(slot),
                    object: vec![slot as u8],
                },
                effect: Some(AdminEffect {
                    change,
                    state,
                    boundary: None,
                    reload: vec![],
                    provider: Default::default(),
                }),
            },
        },
        || Ok(()),
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn cutoff_consumes_every_kind_of_removed_author_without_effects() {
    use coven_format::admin::StoreChange;
    for change in [
        StoreChange::RemoveDevice {
            device: DeviceId(10),
        },
        StoreChange::RemoveMember { member: member(10) },
        StoreChange::ReplaceMemberKeys {
            member: member(10),
            expected: coven_format::test_utils::keys_from_seed(1).public_keys(),
            keys: coven_format::test_utils::keys_from_seed(2).public_keys(),
        },
    ] {
        let (_temp, db) = open(false).await;
        apply(&db, 1, vec![insert(10, 1, 1, "a", "first")])
            .await
            .unwrap();
        let cause = match &change {
            StoreChange::RemoveDevice { .. } => "DeviceRemoved",
            StoreChange::RemoveMember { .. } => "MemberRemoved",
            StoreChange::ReplaceMemberKeys { .. } => "MemberKeysReplaced",
            _ => unreachable!(),
        };
        let replaced_key = matches!(change, StoreChange::ReplaceMemberKeys { .. });
        admin(&db, 2, change, db.store_state().await.unwrap()).await;
        // Cutoff: an old authored view cannot move the cut-off past the containing slot.
        apply(&db, 3, vec![insert(10, 2, 2, "refused", "available")])
            .await
            .unwrap();
        assert_eq!(rows(&db).await, [("a".into(), "first".into())]);
        assert_eq!(db.applied_slot().await.unwrap(), 3);
        assert_eq!(
            db.last_claims()
                .await
                .unwrap()
                .iter()
                .find(|c| c.device == DeviceId(10))
                .unwrap()
                .number,
            3
        );
        assert_eq!(
            db.pending::<Pending>()
                .await
                .unwrap()
                .iter()
                .map(|p| p.subject)
                .collect::<Vec<_>>(),
            [3]
        );
        assert_eq!(
            db.pending::<Pending>().await.unwrap()[0].cutoff,
            Some((2, cause.into()))
        );
        assert!(db.lost_values().await.unwrap().is_empty());
        apply(&db, 4, vec![insert(20, 1, 3, "b", "available")])
            .await
            .unwrap();
        assert_eq!(rows(&db).await.len(), 2);
        if replaced_key {
            let mut write = stream(insert(10, 3, 4, "new-key", "accepted"));
            write.signer = coven_format::test_utils::keys_from_seed(2)
                .public_keys()
                .signing;
            db.apply_slot::<Pending, _, _>(
                IncomingSlot {
                    digest: [0x31; 32],
                    slot: Slot(5),
                    header: SlotHeader {
                        device: DeviceId(10),
                        claim: 5,
                        author: member(10),
                        after: 4,
                    },
                    content: SlotContent::Writes(vec![write]),
                },
                || Ok(()),
                None,
            )
            .await
            .unwrap();
            assert!(rows(&db)
                .await
                .contains(&("new-key".into(), "accepted".into())));
        }
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_released_unique_value_returns_to_its_earliest_live_loser() {
    // LiveClaims.older_loser_returns: 1 wins; deleting 1 returns 2 ahead of 3.
    let (_temp, db) = open(false).await;
    for (slot, device, key) in [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")] {
        apply(&db, slot, vec![insert(device, 1, slot, key, "same")])
            .await
            .unwrap();
    }
    assert_eq!(rows(&db).await, [("a".into(), "same".into())]);
    db.write(|sql| {
        sql.execute("DELETE FROM items WHERE id='a'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let deletion = db
        .read_oldest_upload(|u| {
            let mut bytes = u.header_frame;
            for (_, part) in u.parts {
                for chunk in part {
                    bytes.extend(chunk?);
                }
            }
            Ok::<_, DbError>(coven_format::write_stream::decode_plaintext(&bytes).unwrap())
        })
        .await
        .unwrap()
        .unwrap();
    apply(&db, 4, vec![deletion]).await.unwrap();
    assert_eq!(rows(&db).await, [("b".into(), "same".into())]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_first_counted_creation_keeps_the_owner() {
    for times in [[100, 1], [1, 100]] {
        let (_temp, db) = open(true).await;
        let mut state = StoreState::default();
        for (device, seed) in [(10, 1), (20, 2)] {
            state.members.insert(
                member(device),
                StoreMember {
                    keys: coven_format::test_utils::keys_from_seed(seed).public_keys(),
                    access: coven_format::MemberAccess::S3AccessKey {
                        access_key_id: format!("fixture-{device}"),
                    },
                    role: coven_format::admin::MemberRole::Member,
                    removed: false,
                },
            );
            state.devices.insert(
                DeviceId(device as u64),
                StoreDevice {
                    member: member(device),
                    name: format!("device-{device}"),
                    state: DeviceState::Active,
                },
            );
        }
        admin(
            &db,
            1,
            coven_format::admin::StoreChange::AddDevice {
                name: "fixture".into(),
                replaces: None,
            },
            state,
        )
        .await;
        apply(&db, 2, vec![insert(10, 1, times[0], "a", "first")])
            .await
            .unwrap();
        apply(&db, 3, vec![insert(20, 1, times[1], "a", "later")])
            .await
            .unwrap();
        assert_eq!(rows(&db).await, [("a".into(), "first".into())]);
        assert!(db
            .lost_values()
            .await
            .unwrap()
            .iter()
            .any(|loss| matches!(loss.replaced_by, Replacement::NotAuthorized { .. })));
        db.close().await.unwrap();
    }
}

struct NoSnapshotEvidence;
impl Authorship for NoSnapshotEvidence {
    fn header(
        &self,
        _: &coven_format::authorship::WriteEvidence,
    ) -> Result<coven_format::write_stream::WriteHeaderFrame, DbError> {
        panic!("empty prefixes carry no evidence")
    }
    fn hasher(
        &self,
        _: &Audience,
        _: coven_foundation::id_source::KeyId,
    ) -> Result<coven_crypto::AuthorshipHasher, DbError> {
        panic!("empty prefixes carry no evidence")
    }
}

#[tokio::test]
async fn snapshot_claim_digests_survive_loading_and_writing() {
    use coven_format::{
        admin::SnapshotId,
        sealed_snapshot::SnapshotObjectPrefix,
        sequence::Purpose,
        snapshot::{LastClaim, SnapshotEncoder, SnapshotHeader, SnapshotRecord, SECTIONS},
    };
    let (_temp, db) = open(false).await;
    let id = SnapshotId {
        device: DeviceId(99),
        number: 1,
        audience: Audience::Store,
    };
    let prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(1)),
        covers: 2,
        purpose: Purpose::Ordinary,
    };
    let claims = vec![
        LastClaim {
            device: DeviceId(10),
            number: 1,
            digest: Some([0x42; 32]),
        },
        LastClaim {
            device: DeviceId(20),
            number: 0,
            digest: None,
        },
    ];
    let mut counts = [0; SECTIONS];
    counts[11] = claims.len() as u64;
    let (mut encoder, mut bytes) = SnapshotEncoder::start(SnapshotHeader {
        id: id.clone(),
        schema_version: 1,
        covers: prefix.covers,
        counts,
    })
    .unwrap();
    for claim in &claims {
        bytes.extend(
            encoder
                .record(SnapshotRecord::Claim(claim.clone()))
                .unwrap(),
        );
    }
    bytes.extend(encoder.finish().unwrap());
    for _ in 0..2 {
        db.load_snapshots::<Pending, _, std::io::Empty>(SnapshotReload {
            snapshots: vec![SnapshotSource::Stored {
                id: id.clone(),
                prefix: prefix.clone(),
                input: Cursor::new(bytes),
                author: member(99),
            }],
            slots: vec![],
            target: 2,
            state: StoreState::default(),
            cutoffs: vec![],
            provider: Default::default(),
            boundaries: vec![],
            operation: None,
            authorship: std::sync::Arc::new(NoSnapshotEvidence),
        })
        .await
        .unwrap();
        assert_eq!(db.last_claims().await.unwrap(), claims);
        let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = output.clone();
        db.write_snapshot(
            id.clone(),
            |_| Ok::<_, std::convert::Infallible>(()),
            move |frame| {
                captured.lock().unwrap().extend(frame);
                Ok(())
            },
        )
        .await
        .unwrap();
        bytes = std::mem::take(&mut *output.lock().unwrap());
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn reload_commits_the_fixed_prefix_and_preserves_queued_originals() {
    let (_temp, db) = open(false).await;
    db.write(|sql| {
        sql.execute("INSERT INTO items VALUES('local','same')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let queued = db
        .read_oldest_upload(|u| Ok::<_, DbError>(u.header))
        .await
        .unwrap()
        .unwrap();
    let input = |target| SnapshotReload {
        snapshots: vec![SnapshotSource::<Cursor<Vec<u8>>>::Empty(Audience::Store)],
        slots: vec![IncomingSlot {
            digest: [0x31; 32],
            slot: Slot(1),
            header: SlotHeader {
                device: DeviceId(10),
                claim: 1,
                author: member(10),
                after: 0,
            },
            content: SlotContent::Writes(vec![stream(insert(10, 1, 1, "remote", "same"))]),
        }],
        target,
        state: StoreState::default(),
        cutoffs: vec![],
        provider: Default::default(),
        boundaries: vec![],
        operation: None,
        authorship: std::sync::Arc::new(NoSnapshotEvidence),
    };
    assert!(db.load_snapshots::<Pending, _, _>(input(2)).await.is_err());
    assert_eq!(db.applied_slot().await.unwrap(), 0);
    assert_eq!(rows(&db).await, [("local".into(), "same".into())]);
    assert!(db.last_claims().await.unwrap().is_empty());
    assert!(db.lost_values().await.unwrap().is_empty());
    for _ in 0..2 {
        db.load_snapshots::<Pending, _, _>(input(1)).await.unwrap();
        assert_eq!(db.applied_slot().await.unwrap(), 1);
        assert_eq!(rows(&db).await, [("remote".into(), "same".into())]);
        assert_eq!(db.lost_values().await.unwrap().len(), 1);
        assert_eq!(
            db.read_oldest_upload(|u| Ok::<_, DbError>(u.header))
                .await
                .unwrap()
                .unwrap(),
            queued
        );
        assert_eq!(
            db.last_claims().await.unwrap(),
            [coven_format::snapshot::LastClaim {
                device: DeviceId(10),
                number: 1,
                digest: Some([0x31; 32]),
            }]
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_admin_slot_waits_until_its_required_reload_can_commit() {
    let (_temp, db) = open(false).await;
    let incoming = || IncomingSlot::<DownloadedWriteStream<std::io::Empty>> {
        digest: [0x31; 32],
        slot: Slot(1),
        header: SlotHeader {
            device: DeviceId(10),
            claim: 1,
            author: member(10),
            after: 0,
        },
        content: SlotContent::Admin {
            original: coven_format::floor::OriginalSlot {
                slot: Slot(1),
                object: vec![1],
            },
            effect: Some(AdminEffect {
                change: coven_format::admin::StoreChange::AddDevice {
                    name: "fixture".into(),
                    replaces: None,
                },
                state: StoreState::default(),
                boundary: None,
                reload: vec![Audience::Store],
                provider: Default::default(),
            }),
        },
    };
    assert_eq!(
        db.apply_slot::<Pending, _, _>(incoming(), || Ok(()), None)
            .await
            .unwrap(),
        ApplyOutcome::Waiting(WriteWait::Reload(Audience::Store))
    );
    assert_eq!(db.applied_slot().await.unwrap(), 0);
    assert!(db.last_claims().await.unwrap().is_empty());
    let reload = |target| SnapshotReload {
        snapshots: vec![SnapshotSource::<std::io::Empty>::Empty(Audience::Store)],
        slots: vec![incoming()],
        target,
        state: StoreState::default(),
        cutoffs: vec![],
        provider: Default::default(),
        boundaries: vec![],
        operation: None,
        authorship: std::sync::Arc::new(NoSnapshotEvidence),
    };
    assert!(db.load_snapshots::<Pending, _, _>(reload(2)).await.is_err());
    assert_eq!(db.applied_slot().await.unwrap(), 0);
    db.load_snapshots::<Pending, _, _>(reload(1)).await.unwrap();
    assert_eq!(db.applied_slot().await.unwrap(), 1);
    assert!(db.reloading_audiences().await.unwrap().is_empty());
    assert_eq!(db.last_claims().await.unwrap()[0].number, 1);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparing_an_incoming_part_leaves_the_application_writer_available() {
    let (_temp, db) = open(false).await;
    let write = stream(insert(10, 1, 1, "received", "received"));
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let DownloadedPartStream::Opened(bytes) = write.parts.into_iter().next().unwrap() else {
        panic!("opened fixture")
    };
    let write = DownloadedWriteStream {
        header: write.header,
        signer: write.signer,
        authorship: write.authorship,
        parts: vec![DownloadedPartStream::Opened(HeldPart {
            bytes,
            entered: Some(entered),
            resume,
        })],
    };
    let receiving = db.clone();
    let incoming = tokio::spawn(async move {
        receiving
            .apply_slot::<Pending, _, _>(
                IncomingSlot {
                    slot: Slot(1),
                    digest: [0x31; 32],
                    header: SlotHeader {
                        device: DeviceId(10),
                        claim: 1,
                        author: member(10),
                        after: 0,
                    },
                    content: SlotContent::Writes(vec![write]),
                },
                || Ok(()),
                None,
            )
            .await
    });
    started.await.unwrap();
    let written = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        db.write(|sql| {
            sql.execute("INSERT INTO items VALUES('local','while decoding')", [])?;
            Ok(())
        }),
    )
    .await;
    release.send(()).unwrap();
    incoming.await.unwrap().unwrap();
    db.close().await.unwrap();
    written
        .expect("incoming part decoding held the application writer")
        .unwrap();
}

struct HeldPart {
    bytes: Cursor<Vec<u8>>,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    resume: std::sync::mpsc::Receiver<()>,
}
impl std::io::Read for HeldPart {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
            self.resume
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }
        self.bytes.read(output)
    }
}
