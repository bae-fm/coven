//! Sequence fixtures exercise the public database application and claim APIs.
use crate::{
    ApplyOutcome, Database, DbError, DownloadedPart, DownloadedPartStream, DownloadedWrite,
    DownloadedWriteStream, Slot,
};
use coven_format::write::WriteRecord;
use std::io::Cursor;

pub(crate) fn stream(write: DownloadedWrite) -> DownloadedWriteStream<Cursor<Vec<u8>>> {
    use coven_format::{
        dismissal::WriteFrame,
        merge_fields::encode_write_frame,
        write_stream::{PartHeader, WriteHeaderFrame},
    };
    let mut header = WriteHeaderFrame {
        header: write.header,
        parts: vec![],
    };
    let mut parts = Vec::new();
    let mut leaves = Vec::new();
    for part in write.parts {
        match part {
            DownloadedPart::Opened(part) => {
                let mut frames: Vec<_> = part
                    .rows
                    .iter()
                    .cloned()
                    .map(WriteFrame::Change)
                    .chain(part.executions.iter().cloned().map(WriteFrame::Execution))
                    .chain(part.dismissals.iter().cloned().map(WriteFrame::Dismissal))
                    .collect();
                frames.sort_by_key(|frame| match frame {
                    WriteFrame::Change(row) => (row.row.clone(), None),
                    WriteFrame::Execution(row) => (row.row.clone(), None),
                    WriteFrame::Dismissal(d) => (d.row.clone(), Some((d.target.clone(), d.write))),
                });
                let bytes: Vec<_> = frames
                    .iter()
                    .flat_map(|f| encode_write_frame(f).unwrap())
                    .collect();
                header.parts.push(PartHeader {
                    audience: part.audience.clone(),
                    record_count: frames.len() as u64,
                    plaintext_length: bytes.len() as u64,
                });
                leaves.push(coven_format::authorship::part_leaves(&part));
                parts.push(DownloadedPartStream::Opened(Cursor::new(bytes)));
            }
            DownloadedPart::Skipped(audience) => {
                header.parts.push(PartHeader {
                    audience,
                    record_count: 1,
                    plaintext_length: coven_format::FRAME_PREFIX_LEN as u64,
                });
                leaves.push(vec![]);
                parts.push(DownloadedPartStream::Skipped);
            }
        }
    }
    let mut authorship = crate::test_utils::TestAuthorship::for_header(&header, leaves);
    for (part, hasher) in parts.iter().zip(&mut authorship.hashers) {
        if matches!(part, DownloadedPartStream::Skipped) {
            *hasher = None;
        }
    }
    DownloadedWriteStream {
        header,
        parts,
        signer: crate::test_utils::signer(),
        authorship,
    }
}

pub(crate) async fn publish(db: &Database) -> Result<(), DbError> {
    publish_through(db, u64::MAX).await
}

pub(crate) async fn publish_through(db: &Database, through: u64) -> Result<(), DbError> {
    let records: Vec<_> = crate::write::tests::records(db)
        .into_iter()
        .filter(|r| r.header.position.number <= through)
        .collect();
    if records.is_empty() {
        return Ok(());
    }
    let through = records.last().unwrap().header.position.number;
    let applied = db.applied_slot().await?;
    let path = coven_format::path::ObjectPath::slot(
        crate::tests::store_id(),
        (applied + 1).try_into().unwrap(),
    );
    let bytes = records.iter().flat_map(plaintext).collect::<Vec<_>>();
    let compare = bytes.clone();
    let claim = db
        .begin_claim(crate::ClaimWork::Writes(through), move |_| {
            Ok(crate::ClaimAttempt {
                path,
                bytes: Box::new(std::io::Cursor::new(bytes)),
            })
        })
        .await?;
    db.settle_claim(crate::ClaimRead::Kept(Some(Box::new(
        std::io::Cursor::new(compare),
    ))))
    .await?;
    let records: Vec<_> = records
        .into_iter()
        .filter(|record| {
            db.inspect_writer(|db| {
                crate::sequence::write_slot(db, record.header.position)
                    .unwrap()
                    .is_none()
            })
        })
        .collect();
    if records.is_empty() {
        return db.finish_claim(None).await;
    }
    let device = records[0].header.position.device;
    let state = db.store_state().await.unwrap();
    let author = state
        .devices
        .get(&device)
        .map_or_else(|| crate::tests::member(1), |d| d.member);
    let outcome = db
        .apply_slot::<crate::pending::tests::Record, _, _>(
            crate::IncomingSlot {
                digest: [0x31; 32],
                slot: Slot(applied + 1),
                header: coven_format::sequence::SlotHeader {
                    device,
                    claim: claim.number,
                    author,
                    after: applied,
                },
                content: crate::SlotContent::Writes {
                    size: 1,
                    writes: records.into_iter().map(|r| stream(r.into())).collect(),
                },
            },
            || Ok(()),
            None,
        )
        .await?;
    assert_eq!(outcome, ApplyOutcome::Applied);
    db.finish_claim(None).await
}

impl Database {
    pub(crate) async fn receive(&self, write: DownloadedWrite) -> Result<ApplyOutcome, DbError> {
        self.receive_stream(stream(write), || Ok(())).await
    }
    pub(crate) async fn receive_stream<R, F>(
        &self,
        write: DownloadedWriteStream<R>,
        authenticate: F,
    ) -> Result<ApplyOutcome, DbError>
    where
        R: std::io::Read + Send + 'static,
        F: FnOnce() -> Result<(), DbError> + Send + 'static,
    {
        let device = write.header.header.position.device;
        let number = write.header.header.position.number;
        let applied = self.applied_slot().await?;
        let existing = self
            .inspect_writer(|db| crate::sequence::write_slot(db, write.header.header.position))
            .transpose();
        let slot = match existing {
            Some(Ok(slot)) => slot,
            Some(Err(error)) => return Err(error),
            None => Slot(applied + 1),
        };
        let state = self.store_state().await.unwrap();
        let author = state
            .devices
            .get(&device)
            .map_or_else(|| crate::tests::member(1), |d| d.member);
        self.apply_slot::<crate::pending::tests::Record, _, _>(
            crate::IncomingSlot {
                digest: [0x31; 32],
                slot,
                header: coven_format::sequence::SlotHeader {
                    device,
                    claim: number,
                    author,
                    after: slot.0 - 1,
                },
                content: crate::SlotContent::Writes {
                    size: 1,
                    writes: vec![write],
                },
            },
            authenticate,
            None,
        )
        .await
    }
}

pub(crate) fn plaintext(record: &WriteRecord) -> Vec<u8> {
    let encoder = coven_format::write_stream::WriteEncoder::new(record).unwrap();
    let mut bytes = vec![0; encoder.plaintext_length() as usize];
    encoder.encode_plaintext(&mut bytes).unwrap();
    bytes
}

impl Database {
    pub(crate) async fn admin_fixture(
        &self,
        change: coven_format::admin::StoreChange,
        state: crate::StoreState,
        boundary: Option<crate::Boundary>,
    ) -> Result<(), DbError> {
        let applied = self.applied_slot().await?;
        let slot = boundary
            .as_ref()
            .map_or(Slot(applied + 1), crate::Boundary::entry);
        self.apply_slot::<crate::pending::tests::Record, std::io::Empty, _>(
            crate::IncomingSlot {
                digest: [0x31; 32],
                slot,
                header: coven_format::sequence::SlotHeader {
                    device: coven_foundation::id_source::DeviceId(99),
                    claim: slot.0,
                    author: crate::tests::member(1),
                    after: slot.0 - 1,
                },
                content: crate::SlotContent::Admin {
                    original: admin_original(slot, &change),
                    effect: Some(crate::AdminEffect {
                        change,
                        state,
                        boundary,
                        reload: vec![],
                        provider: Default::default(),
                    }),
                },
            },
            || Ok(()),
            None,
        )
        .await
        .map(drop)
    }
}

fn admin_original(
    slot: Slot,
    change: &coven_format::admin::StoreChange,
) -> coven_format::floor::OriginalSlot {
    let path =
        coven_format::path::ObjectPath::slot(crate::tests::store_id(), slot.0.try_into().unwrap());
    let header = coven_format::sequence::SlotHeader {
        device: coven_foundation::id_source::DeviceId(99),
        claim: slot.0,
        author: crate::tests::member(1),
        after: slot.0 - 1,
    };
    // Boundary tests inject the checked policy result directly. Keep the real
    // framed envelope without requiring their synthetic history to contain an
    // announcement; announcement authorization belongs to the sync reader.
    let mut plain = header.encode(&path).unwrap();
    plain.extend(change.encode().unwrap());
    let key = crate::test_utils::TestAuthorship::key();
    let prefix = coven_format::sealed_slot::SlotObjectPrefix {
        format: coven_format::FormatVersion::CURRENT,
        key: key.id(),
        kind: coven_format::sealed_slot::SlotKind::Admin,
    };
    let chunk = key
        .derive()
        .seal_object_chunk(path.as_str(), &prefix.encode().unwrap(), 0, 0, &plain)
        .unwrap();
    let mut object = prefix.encode_chunk(&chunk).unwrap();
    object.extend_from_slice(&[0; 64]);
    coven_format::floor::OriginalSlot { slot, object }
}

pub(crate) async fn delete_circle(
    db: &Database,
    circle: coven_foundation::id_source::CircleId,
) -> Result<Slot, DbError> {
    let mut state = db.store_state().await.unwrap();
    if let Some(slot) = state.circle_deletions.get(&circle) {
        return Ok(*slot);
    }
    let slot = Slot(db.applied_slot().await? + 1);
    state.circle_deletions.insert(circle, slot);
    db.admin_fixture(
        coven_format::admin::StoreChange::DeleteCircle { circle },
        state,
        None,
    )
    .await?;
    Ok(slot)
}

pub(crate) async fn remove_device(
    db: &Database,
    device: coven_foundation::id_source::DeviceId,
) -> Slot {
    let mut state = db.store_state().await.unwrap();
    if let Some(value) = state.devices.get_mut(&device) {
        value.state = crate::DeviceState::Removed;
    }
    let slot = Slot(db.applied_slot().await.unwrap() + 1);
    db.admin_fixture(
        coven_format::admin::StoreChange::RemoveDevice { device },
        state,
        None,
    )
    .await
    .unwrap();
    slot
}

pub(crate) fn data(
    db: &Database,
    audience: coven_merge::Audience,
) -> (
    Vec<coven_merge::RowState<coven_format::value::Value>>,
    Vec<coven_format::loss::Loss>,
) {
    db.inspect_writer(|db| {
        let schema=crate::schema::Schema::read(db).unwrap();
        let store=crate::merge_store::MergeStore::from_schema(db,&schema);
        let rows=db.query("SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1 ORDER BY table_name,key",[crate::write_encoding::audience_text(&audience)],crate::row_queries::read_identity).unwrap().into_iter().map(|row|store.row(&row).unwrap().state).collect();
        let mut losses=Vec::new();
        crate::loss_record::visit(db,&audience,|loss|{losses.push(loss);Ok::<_,DbError>(())}).unwrap();
        (rows,losses)
    })
}

/// A reload of `snapshots`, each staged from its plaintext first (§16.2).
pub(crate) async fn reload<R: std::io::Read, W>(
    db: &Database,
    snapshots: Vec<(
        coven_format::admin::SnapshotId,
        coven_format::sealed_snapshot::SnapshotObjectPrefix,
        R,
        coven_foundation::id_source::MemberId,
    )>,
    writes: Vec<DownloadedWriteStream<W>>,
    authorship: std::sync::Arc<dyn crate::Authorship>,
) -> crate::SnapshotReload<crate::StagedRecords, DownloadedWriteStream<W>> {
    let mut staged = Vec::new();
    for (id, prefix, input, author) in snapshots {
        let covers = prefix.covers;
        let snapshot = crate::test_utils::stage_plaintext(db, id.clone(), prefix, input)
            .await
            .unwrap();
        staged.push((id, covers, snapshot.records(), author));
    }
    let (applied, state, boundaries) = db.inspect_writer(|db| {
        (
            crate::sequence::applied(db).unwrap(),
            crate::store_state_tables::read(db).unwrap(),
            crate::Boundary::load_selected(db)
                .unwrap()
                .into_iter()
                .filter_map(|boundary| {
                    let included = db
                        .query_row(
                            "SELECT included FROM _coven_boundaries WHERE slot=?1",
                            [boundary.entry().0.to_be_bytes()],
                            |r| r.get::<_, Option<Vec<u8>>>(0),
                        )
                        .unwrap();
                    included.map(|included| crate::BoundaryCoverage {
                        boundary,
                        included: crate::write_encoding::counter(included),
                    })
                })
                .collect(),
        )
    });
    let first = staged
        .iter()
        .map(|(_, covers, _, _)| *covers)
        .min()
        .unwrap_or(0);
    let target = applied
        .max(
            staged
                .iter()
                .map(|(_, covers, _, _)| *covers)
                .max()
                .unwrap_or(0),
        )
        .max(first + writes.len() as u64);
    let slots = writes
        .into_iter()
        .enumerate()
        .map(|(i, write)| {
            let slot = Slot(first + i as u64 + 1);
            let device = write.header.header.position.device;
            crate::IncomingSlot {
                digest: [0x31; 32],
                slot,
                header: coven_format::sequence::SlotHeader {
                    device,
                    claim: write.header.header.position.number,
                    author: state
                        .devices
                        .get(&device)
                        .map_or(crate::tests::member(1), |d| d.member),
                    after: slot.0 - 1,
                },
                content: crate::SlotContent::Writes {
                    size: 1,
                    writes: vec![write],
                },
            }
        })
        .collect();
    crate::SnapshotReload {
        snapshots: staged
            .into_iter()
            .map(|(id, _, input, author)| crate::SnapshotSource::Stored { id, input, author })
            .collect(),
        slots,
        target,
        state,
        cutoffs: vec![],
        provider: Default::default(),
        boundaries,
        operation: None,
        authorship,
    }
}
