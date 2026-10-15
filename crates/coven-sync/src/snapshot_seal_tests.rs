//! A breaking migration's raise: the captured prefix uploads in its original
//! records, the sealed raise snapshot follows only its confirmation, and
//! ordinary new-schema writes wait for a kept raise (§17.1, §18.1).
use super::*;
use coven_format::sealed_snapshot::SnapshotObjectPrefix;
use coven_format::write::WriteRecord;
use coven_merge::WriteId;

fn numbers(writes: &[WriteId]) -> Vec<u64> {
    writes.iter().map(|w| w.number).collect()
}

async fn raise_prefix(storage: &MemoryStorage, device: &Device) -> SnapshotObjectPrefix {
    let raised = device.db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    let path = ObjectPath::snapshot(
        STORE_ID,
        raised.snapshot.audience.clone(),
        raised.snapshot.device,
        raised.snapshot.number.try_into().unwrap(),
    );
    let bytes = storage.read(&path).await.unwrap();
    SnapshotObjectPrefix::decode(&bytes[..SnapshotObjectPrefix::length(&bytes).unwrap()]).unwrap()
}

#[tokio::test]
async fn the_old_write_and_marker_publish_the_raise_while_a_new_schema_write_waits() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    // Ana's old-schema write 2 waits unsent when her app updates; the
    // migration appends marker 3, and she then makes new-schema write 4.
    sql(&devices[0].db, "UPDATE notes SET title='old schema'").await;
    update(&mut devices[0], storage.clone(), true, true).await;
    sql(&devices[0].db, "UPDATE notes SET name='new schema'").await;
    let ana = &mut devices[0];
    let device = ana.db.sync_state(Vec::new()).await.unwrap().device;
    ana.catch_up().await;
    // The raise waits for its captured prefix before requesting the snapshot.
    raise_pass(ana).await;
    assert!(ana
        .db
        .store_log()
        .await
        .unwrap()
        .replay
        .state
        .schema
        .is_empty());
    assert!(storage
        .list(&ObjectPrefix::snapshots(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    let waiting = ana.db.pending::<crate::PendingRecord>().await.unwrap();
    assert!(waiting.iter().any(|record| record.reason
        == crate::PendingReason::Waits(crate::Prerequisite::Object(crate::write_seal::path(
            STORE_ID,
            WriteId { device, number: 2 }
        )))));
    // Old write 2 and marker 3 upload; write 4 waits behind the marker.
    assert_eq!(numbers(&ana.writes.upload_writes().await.unwrap()), [2, 3]);
    ana.sync.sync_store_log().await.unwrap();
    let log = ana.db.store_log().await.unwrap();
    assert_eq!(log.replay.state.schema[&Audience::Store].number, 2);
    // The snapshot covers Ana through the marker, not the later write.
    let prefix = raise_prefix(&storage, ana).await;
    assert!(prefix.writes.covers(WriteId { device, number: 3 }));
    assert!(!prefix.writes.covers(WriteId { device, number: 4 }));
    assert!(ana.db.raise_captures().await.unwrap().is_empty());
    // With the raise kept, write 4 sends.
    assert_eq!(numbers(&ana.writes.upload_writes().await.unwrap()), [4]);
    update(&mut devices[1], storage.clone(), true, true).await;
    sync_all(&mut devices).await;
    for device in &devices {
        assert_eq!(name(&device.db).await, "new schema");
        assert!(device.db.lost_values().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_lost_reply_keeps_the_prefix_unconfirmed_until_the_stored_bytes_match() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    seed(&mut devices).await;
    sql(&devices[0].db, "UPDATE notes SET title='old schema'").await;
    update(&mut devices[0], storage.clone(), true, true).await;
    let ana = &mut devices[0];
    let device = ana.db.sync_state(Vec::new()).await.unwrap().device;
    ana.catch_up().await;
    // Write 2 is stored, but its reply is lost.
    storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(ana.writes.upload_writes().await.is_err());
    assert_eq!(
        ana.db
            .unconfirmed_own_prefix(WriteId { device, number: 3 })
            .await
            .unwrap(),
        Some(WriteId { device, number: 2 })
    );
    // The raise does not send its snapshot on the unconfirmed prefix, even
    // after reopening.
    raise_pass(ana).await;
    assert!(storage
        .list(&ObjectPrefix::snapshots(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    update(ana, storage.clone(), true, true).await;
    ana.catch_up().await;
    // The occupied path holds exactly write 2's bytes: that settles it.
    assert_eq!(numbers(&ana.writes.upload_writes().await.unwrap()), [2, 3]);
    ana.sync.sync_store_log().await.unwrap();
    assert_eq!(
        ana.db.store_log().await.unwrap().replay.state.schema[&Audience::Store].number,
        2
    );
    assert!(raise_prefix(&storage, ana)
        .await
        .writes
        .covers(WriteId { device, number: 3 }));
}

#[tokio::test]
async fn a_competing_raise_and_the_authors_reload_keep_every_original_queued_byte() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    devices[0].clock.set(UNIX_EPOCH + Duration::from_secs(2));
    devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(3));
    // Ben's attempted write 1 and untried write 2.
    sql(&devices[1].db, "UPDATE notes SET title='attempted'").await;
    devices[1].catch_up().await;
    storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(devices[1].writes.upload_writes().await.is_err());
    sql(&devices[1].db, "UPDATE notes SET body='untried'").await;
    let original: Vec<WriteRecord> = devices[1].db.test_queued_writes().await.unwrap();
    let attempted = crate::device_log_sync::tests::writes::resealed(&devices[1]).await;
    for device in &mut devices {
        update(device, storage.clone(), true, true).await;
    }
    let queued = devices[1].db.test_queued_writes().await.unwrap();
    assert_eq!(queued[..2], original[..]);
    assert_eq!(
        crate::device_log_sync::tests::writes::resealed(&devices[1]).await,
        attempted
    );
    // Ana's earlier raise is published while Ben's competing one is too.
    publish_raise(&mut devices[0]).await;
    let first =
        devices[0].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    let hidden = [
        crate::store_log_object::path(STORE_ID, first.entry),
        ObjectPath::snapshot(
            STORE_ID,
            first.snapshot.audience.clone(),
            first.snapshot.device,
            first.snapshot.number.try_into().unwrap(),
        ),
    ];
    let mut saved = Vec::new();
    for path in &hidden {
        saved.push(storage.read(path).await.unwrap());
        storage.delete(path).await.unwrap();
    }
    publish_raise(&mut devices[1]).await;
    let losing =
        devices[1].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    assert_ne!(losing.snapshot, first.snapshot);
    for (path, bytes) in hidden.iter().zip(saved) {
        storage.create(path, &bytes).await.unwrap();
    }
    // Ben published his original records unchanged, then reloads from Ana's
    // winning raise and receives them by the same rule as every peer.
    let stored = storage
        .read(&crate::write_seal::path(STORE_ID, attempted.0))
        .await
        .unwrap();
    assert_eq!(stored, attempted.1);
    sync_all(&mut devices).await;
    sync_all(&mut devices).await;
    for device in &devices {
        let log = device.db.store_log().await.unwrap();
        assert_eq!(log.replay.state.schema[&Audience::Store], first);
        assert!(matches!(
            log.replay.entries[&losing.entry],
            coven_database::EntryOutcome::Dropped(_)
        ));
        assert_eq!(name(&device.db).await, "attempted");
        // The losing marker and the converted old writes leave no loss.
        assert!(device.db.lost_values().await.unwrap().is_empty());
        assert_eq!(
            rows_named(&device.db).await,
            ("attempted".into(), "untried".into())
        );
    }
}

/// Ana's write 2 follows her first migration, whose raise never published,
/// and precedes her second. It belongs to the second migration's captured
/// prefix, so it uploads before that raise rather than waiting for a raise
/// to its own version that will never come (§17.1).
#[tokio::test]
async fn a_write_between_unpublished_migrations_joins_the_later_prefix() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    let ana = &mut devices[0];
    let tables = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
    let index = |version: u32, sql: &'static str| Migration::sql(version, "index", sql);
    ana.reopen(
        storage.clone(),
        tables(),
        vec![initial(), index(2, "CREATE INDEX titles ON notes(title)")],
    )
    .await;
    sql(&ana.db, "INSERT INTO notes VALUES('1','between','body')").await;
    ana.reopen(
        storage.clone(),
        tables(),
        vec![
            initial(),
            index(2, "CREATE INDEX titles ON notes(title)"),
            index(3, "CREATE INDEX bodies ON notes(body)"),
        ],
    )
    .await;
    sql(&ana.db, "INSERT INTO notes VALUES('4','after','body')").await;
    ana.catch_up().await;
    assert_eq!(
        numbers(&ana.writes.upload_writes().await.unwrap()),
        [1, 2, 3]
    );
    raise_pass(ana).await;
    let log = ana.db.store_log().await.unwrap();
    assert_eq!(log.replay.state.schema[&Audience::Store].number, 3);
    assert_eq!(
        log.entries
            .iter()
            .filter(|e| matches!(e.entry.change, StoreChange::RaiseSchema { .. }))
            .count(),
        1
    );
    assert_eq!(numbers(&ana.writes.upload_writes().await.unwrap()), [4]);
}

async fn rows_named(db: &Database) -> (String, String) {
    db.read(|sql| {
        Ok(
            sql.query_row("SELECT name,body FROM notes WHERE id='42'", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?,
        )
    })
    .await
    .unwrap()
}

mod differential {
    //! The queue model's raise run (`RaiseUpload`, `RaiseProgress`) against a
    //! real device: app writes, breaking migrations, upload passes with lost
    //! requests and replies, raise publication and crashes.
    use super::*;
    use serde_json::{json, Value};

    struct Rng(u64);
    impl Rng {
        fn pick(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    const INDEXES: [&str; 3] = [
        "CREATE INDEX titles ON notes(title)",
        "CREATE INDEX bodies ON notes(body)",
        "CREATE INDEX both_columns ON notes(title,body)",
    ];

    fn migrations(version: u32) -> Vec<Migration> {
        std::iter::once(initial())
            .chain((2..=version).map(|v| Migration::sql(v, "index", INDEXES[v as usize - 2])))
            .collect()
    }

    /// Migrations may follow app writes made after an earlier unpublished
    /// migration: those writes join the later captured prefix
    /// (`RaiseCaptures.later_capture_never_deadlocks`,
    /// `a_write_between_unpublished_migrations_joins_the_later_prefix`).
    fn generate(rng: &mut Rng) -> Vec<Value> {
        let mut version = 1;
        (0..16)
            .map(|_| match rng.pick(10) {
                0 if version < 4 => {
                    version += 1;
                    json!({"event": "migrate", "version": version})
                }
                0..=2 => json!({"event": "write"}),
                3..=5 => {
                    let fault = [0, 0, 1, 2][rng.pick(4) as usize];
                    json!({"event": "upload", "fault": fault})
                }
                6..=8 => json!({"event": "raise"}),
                _ => json!({"event": "crash"}),
            })
            .collect()
    }

    async fn observed(device: &Device) -> Value {
        let state = device.db.sync_state(Vec::new()).await.unwrap();
        let own = state.device;
        let reserved = state
            .positions
            .0
            .iter()
            .find(|p| p.device == own)
            .map_or(0, |p| p.number);
        let records = device.db.test_queued_writes().await.unwrap();
        let attempts = device.db.test_queued_attempts().await.unwrap();
        let waiting: Vec<_> = records
            .iter()
            .zip(&attempts)
            .map(|(record, (id, tried))| {
                assert_eq!(record.header.position, *id);
                json!({"number": id.number, "tried": tried, "version": record.header.schema_version,
                "kind": match record.header.disposition {
                    coven_format::write::WriteDisposition::Apply => "ordinary",
                    coven_format::write::WriteDisposition::Migration => "migration",
                }})
            })
            .collect();
        let settled = records
            .first()
            .map_or(reserved, |r| r.header.position.number - 1);
        let kept = device
            .db
            .store_log()
            .await
            .unwrap()
            .replay
            .state
            .schema
            .get(&Audience::Store)
            .map_or(0, |v| v.number);
        let stage = match state.breaking_version {
            0 => "none",
            breaking if kept >= breaking => "entryStored",
            _ => "captured",
        };
        json!({"settled": settled, "waiting": waiting, "stage": stage, "kept": kept.max(1)})
    }

    async fn rust_run(events: &[Value]) -> Vec<Value> {
        let storage = storage();
        let mut device = group(storage.clone(), 1).await.pop().unwrap();
        let tables = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
        let mut version = 1;
        let mut rows = 0;
        let mut out = Vec::new();
        for event in events {
            let before = device.db.test_queued_writes().await.unwrap();
            match event["event"].as_str().unwrap() {
                "write" => {
                    rows += 1;
                    device
                        .db
                        .write(move |sql| {
                            sql.execute(
                                "INSERT INTO notes VALUES(?1,'title','body')",
                                [rows.to_string()],
                            )?;
                            Ok(())
                        })
                        .await
                        .unwrap();
                }
                "migrate" => {
                    version = event["version"].as_u64().unwrap() as u32;
                    device
                        .reopen(storage.clone(), tables(), migrations(version))
                        .await;
                    // Migration keeps every original record byte for byte.
                    let after = device.db.test_queued_writes().await.unwrap();
                    assert_eq!(after[..before.len()], before[..]);
                }
                "upload" => {
                    if !device.send.can_send() {
                        device.catch_up().await;
                    }
                    let mut faults = Faults::none();
                    match event["fault"].as_u64().unwrap() {
                        1 => faults.fail_next = 1,
                        2 => faults.lose_completion_reply = true,
                        _ => (),
                    }
                    storage.set_faults(faults).await;
                    let _ = device.writes.upload_writes().await;
                    storage.set_faults(Faults::none()).await;
                }
                "raise" => {
                    // The model's raise event is a pass at which the raise's
                    // retry wait is over (SP waiting): a wait armed before a
                    // reopen otherwise outlasts the fixed clock.
                    let now = coven_foundation::clock::Clock::now(device.clock.as_ref());
                    device.clock.set(now + coven_storage::LONGEST_RETRY_DELAY);
                    raise_pass(&mut device).await
                }
                "crash" => {
                    device
                        .reopen(storage.clone(), tables(), migrations(version))
                        .await
                }
                other => panic!("unknown event {other}"),
            }
            out.push(observed(&device).await);
        }
        out
    }

    #[tokio::test]
    #[ignore = "requires COVEN_QUEUE_LEAN; scripts/check.sh supplies the queue runner"]
    async fn queue_raise_differential_histories() {
        let (mut raised, mut between) = (0, 0);
        for seed in 0..48 {
            let mut rng = Rng(seed * 7919 + 1);
            let events = generate(&mut rng);
            // A write after one migration and before the next.
            let kinds: Vec<_> = events
                .iter()
                .map(|e| e["event"].as_str().unwrap())
                .collect();
            between += usize::from(kinds.iter().enumerate().any(|(i, kind)| {
                *kind == "migrate"
                    && kinds[..i].contains(&"migrate")
                    && kinds[kinds[..i].iter().rposition(|k| *k == "migrate").unwrap()..i]
                        .contains(&"write")
            }));
            let output = crate::tests::run_lean(
                "COVEN_QUEUE_LEAN",
                &json!({"mode": "raise", "app": 1, "events": events}),
            );
            assert!(
                output.status.success(),
                "Lean runner failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let lean: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
            let rust = rust_run(&events).await;
            for (index, (rust, lean)) in rust.iter().zip(&lean).enumerate() {
                assert_eq!(
                    rust, lean,
                    "queue raise differential, seed {seed}, after event {index} of {events:?}"
                );
            }
            raised += usize::from(rust.iter().any(|s| s["stage"] == "entryStored"));
        }
        assert!(raised > 0, "some histories publish a raise");
        assert!(between > 0, "some histories write between two migrations");
    }

    /// IO `SnapshotWrites` on Ana's raise: her capture may contain unsent own
    /// writes, but the snapshot request is admitted only once her captured
    /// position is confirmed, and every covered number is then published
    /// (`pending_run`, `two_audience_raise_keeps_circle_part`).
    #[tokio::test]
    #[ignore = "requires COVEN_IO_RUNNER; scripts/check.sh supplies the IO runner"]
    async fn io_differential_snapshot_writes() {
        let mut requests = (0, 0);
        for seed in 0..32 {
            let mut rng = Rng(seed * 104_729 + 3);
            let storage = storage();
            let mut devices = group(storage.clone(), 2).await;
            let tables = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
            let mut trace = Vec::new();
            let mut admitted = Vec::new();
            let (mut migrated, mut raised, mut rows) = (false, false, 0);
            // The capture each request sends: the migration's, or the one a
            // completed catch-up took again (§15).
            let (mut captured, mut captures) = (None, 0);
            let capture_event = |positions: &coven_format::value::WritePositions| {
                let positions: Vec<_> =
                    positions.0.iter().map(|p| (p.device.0, p.number)).collect();
                json!({"event": "capture", "writer": 1, "positions": positions})
            };
            for _ in 0..14 {
                let choice = rng.pick(8);
                let index = (rng.pick(2)) as usize;
                let writer = index as u64 + 1;
                match choice {
                    0 | 1 => {
                        rows += 1;
                        devices[index]
                            .db
                            .write(move |sql| {
                                sql.execute(
                                    "INSERT INTO notes VALUES(?1,'title','body')",
                                    [rows.to_string()],
                                )?;
                                Ok(())
                            })
                            .await
                            .unwrap();
                        trace.push(json!({"event": "reserve", "writer": writer}));
                    }
                    2 | 3 => {
                        let device = &mut devices[index];
                        if !device.send.can_send() {
                            device.catch_up().await;
                        }
                        for _ in device.writes.upload_writes().await.unwrap() {
                            trace.push(json!({"event": "land", "writer": writer}));
                            trace.push(json!({"event": "confirm", "writer": writer}));
                        }
                    }
                    4 => {
                        devices[0].writes.download_writes().await.unwrap();
                    }
                    5 if !migrated => {
                        migrated = true;
                        devices[0]
                            .reopen(
                                storage.clone(),
                                tables(),
                                vec![
                                    initial(),
                                    Migration::sql(
                                        2,
                                        "index",
                                        "CREATE INDEX titles ON notes(title)",
                                    ),
                                ],
                            )
                            .await;
                        trace.push(json!({"event": "reserve", "writer": 1}));
                        let capture = devices[0].db.raise_captures().await.unwrap().remove(0);
                        trace.push(capture_event(&capture.writes));
                        captured = Some(capture.writes);
                    }
                    _ if migrated && !raised => {
                        let listed = || async {
                            storage
                                .list(&ObjectPrefix::snapshots(STORE_ID))
                                .await
                                .unwrap()
                                .len()
                        };
                        let before = listed().await;
                        raise_pass(&mut devices[0]).await;
                        raised = listed().await > before;
                        if let Some(capture) = devices[0].db.raise_captures().await.unwrap().pop() {
                            if Some(&capture.writes) != captured.as_ref() {
                                trace.push(capture_event(&capture.writes));
                                captured = Some(capture.writes);
                                captures += 1;
                            }
                        }
                        trace.push(json!({"event": "request", "snapshot": captures}));
                        admitted.push(raised);
                        if raised {
                            trace.push(json!({"event": "landSnapshot", "snapshot": captures}));
                        }
                    }
                    _ => {}
                }
            }
            if trace.is_empty() {
                continue;
            }
            let lean = crate::discovery::tests::io_runner(&[
                json!({"check": "snapshot_writes", "events": trace}),
            ])
            .remove(0);
            let lean = lean.as_array().unwrap();
            let mut decisions = admitted.iter();
            for (event, result) in trace.iter().zip(lean) {
                match event["event"].as_str().unwrap() {
                    "capture" => assert_eq!(result["captured"], json!(true), "seed {seed}"),
                    "request" => {
                        let rust = *decisions.next().unwrap();
                        assert_eq!(result["admitted"], json!(rust), "seed {seed}: {trace:?}");
                        if rust {
                            requests.0 += 1;
                        } else {
                            requests.1 += 1;
                        }
                    }
                    "landSnapshot" => {
                        assert_eq!(result["published"], json!(true), "seed {seed}")
                    }
                    _ => {}
                }
            }
        }
        assert!(requests.0 > 0 && requests.1 > 0, "{requests:?}");
    }
}

/// A raise snapshot is prepared after a completed catch-up and captured again
/// when that catch-up applied anything past the migration's capture, and
/// retention holds writes a listed raise snapshot omits (§15, §17.1; io
/// `RaiseHistory`).
mod recapture {
    use super::*;

    const ANA: DeviceId = DeviceId(1);
    const BEN: DeviceId = DeviceId(2);

    /// The signed prefixes of an author's listed store snapshots.
    async fn store_snapshots(
        storage: &MemoryStorage,
        author: DeviceId,
    ) -> Vec<(ObjectPath, SnapshotObjectPrefix)> {
        let mut prefixes = Vec::new();
        for object in storage
            .list(&ObjectPrefix::audience_snapshots(
                STORE_ID,
                &Audience::Store,
            ))
            .await
            .unwrap()
        {
            if object.path.device() != Some(author) {
                continue;
            }
            let bytes = storage.read(&object.path).await.unwrap();
            let length = SnapshotObjectPrefix::signed_length(&bytes).unwrap();
            let (prefix, _) = SnapshotObjectPrefix::decode_signed(&bytes[..length]).unwrap();
            prefixes.push((object.path, prefix));
        }
        prefixes
    }

    async fn stored(storage: &MemoryStorage, write: WriteId) -> bool {
        storage
            .status(&crate::write_seal::path(STORE_ID, write))
            .await
            .unwrap()
            .is_some()
    }

    /// Ben's old-schema edits, uploaded, with his snapshot covering them; two,
    /// so his snapshot counts more writes than Ana's raise snapshot.
    async fn bens_covered_edit(devices: &mut [Device]) -> WriteId {
        sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
        sql(&devices[1].db, "UPDATE notes SET body='list'").await;
        devices[1].writes.upload_writes().await.unwrap();
        devices[1].write_snapshot(Audience::Store).await.unwrap();
        let edit = WriteId {
            device: BEN,
            number: 1,
        };
        assert!(stored(&devices[1].storage, edit).await);
        let (_, bens) = store_snapshots(&devices[1].storage, BEN).await.remove(0);
        assert!(bens.writes.covers(edit));
        edit
    }

    #[tokio::test]
    async fn a_snapshot_landing_before_the_raise_makes_the_device_capture_again() {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        seed(&mut devices).await;
        update(&mut devices[0], storage.clone(), true, true).await;
        let migrated = devices[0].db.raise_captures().await.unwrap().remove(0);
        // Ben, still on the old schema, edits the note and his snapshot of the
        // store lands before Ana's raise.
        let edit = bens_covered_edit(&mut devices).await;
        assert!(!migrated.writes.covers(edit));
        // Ana's catch-up applies the edit under her capture, converting it, so
        // her raise snapshot is captured again and covers it (§15, §17.1).
        publish_raise(&mut devices[0]).await;
        assert_eq!(name(&devices[0].db).await, "Shopping");
        let log = devices[0].db.store_log().await.unwrap();
        let raised = &log.replay.state.schema[&Audience::Store];
        assert_eq!(raised.number, 2);
        let (path, prefix) = store_snapshots(&storage, ANA).await.remove(0);
        assert_eq!(path.snapshot_id().unwrap(), raised.snapshot);
        assert_eq!(prefix.raise.map(|v| v.get()), Some(2));
        assert!(prefix.writes.covers(edit));
        // Ben updates and reloads from that snapshot; both converge with no loss.
        assert!(matches!(
            devices[1].sync.sync_store_log().await,
            Err(SyncError::UpdateRequired(_))
        ));
        update(&mut devices[1], storage.clone(), true, true).await;
        sync_all(&mut devices).await;
        for device in &devices {
            assert_eq!(name(&device.db).await, "Shopping");
            assert!(device.db.lost_values().await.unwrap().is_empty());
        }
        // The published raise leaves no capture behind.
        assert!(devices[0].db.raise_captures().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn retention_keeps_a_write_a_listed_raise_snapshot_omits() {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        seed(&mut devices).await;
        update(&mut devices[0], storage.clone(), true, true).await;
        // Ben, still on the old schema, edits the note twice and prepares a
        // snapshot covering his edits before Ana's marker is stored, so his
        // catch-up completes; his edits are not stored yet either.
        sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
        sql(&devices[1].db, "UPDATE notes SET body='list'").await;
        let data = crate::operation_data::Data::Snapshots(crate::snapshot_data::SnapshotTask {
            job: crate::snapshot_data::SnapshotJob::Write {
                audience: Audience::Store,
                device: BEN,
                trigger: crate::snapshot_data::SnapshotTrigger::Requested,
                attempt: None,
            },
            temporary: Vec::new(),
        });
        let bens = devices[1]
            .db
            .start_operation(data.new_operation("coven").unwrap())
            .await
            .unwrap();
        {
            let _pass = devices[1].caught_up_pass().await;
            assert!(matches!(
                devices[1].step(bens).await.unwrap(),
                crate::operations::Progress::Advanced
            ));
        }
        // Ana's raise snapshot is prepared and lands; her raise entry does not.
        devices[0].catch_up().await;
        devices[0].writes.upload_writes().await.unwrap();
        devices[0].sync.schedule_version_changes().await.unwrap();
        let raise = devices[0].db.operations().await.unwrap()[0].id;
        {
            let _pass = devices[0].caught_up_pass().await;
            for _ in 0..2 {
                assert!(matches!(
                    devices[0].step(raise).await.unwrap(),
                    crate::operations::Progress::Advanced
                ));
            }
        }
        let (_, listed) = store_snapshots(&storage, ANA).await.remove(0);
        assert_eq!(listed.raise.map(|v| v.get()), Some(2));
        // Ben's edits are stored and his snapshot lands after Ana's, covering
        // them and counting more writes; every device posts past them.
        devices[1].writes.upload_writes().await.unwrap();
        while devices[1]
            .db
            .operations()
            .await
            .unwrap()
            .iter()
            .any(|record| record.id == bens)
        {
            assert!(!matches!(
                devices[1].step(bens).await.unwrap(),
                crate::operations::Progress::Waiting(_)
            ));
        }
        let edit = WriteId {
            device: BEN,
            number: 1,
        };
        assert!(stored(&storage, edit).await);
        let (_, ben_snapshot) = store_snapshots(&storage, BEN).await.remove(0);
        assert!(ben_snapshot.writes.covers(edit));
        assert!(!listed.writes.covers(edit));
        devices[0].writes.download_writes().await.unwrap();
        for device in &mut devices {
            device.writes.post_positions().await.unwrap();
        }
        // Under Ana's listed raise snapshot the store's selection omits the
        // edit, so retention keeps it (`kept_late_held`).
        devices[1].sync.run_retention().await.unwrap();
        assert!(stored(&storage, edit).await);
        // Once Ana's raise is kept, its snapshot is the store's selection and
        // the edit still waits for a snapshot that read the raise.
        {
            let _pass = devices[0].caught_up_pass().await;
            devices[0].sync.sync_store_log().await.unwrap();
        }
        assert_eq!(
            devices[0].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].number,
            2
        );
        assert!(matches!(
            devices[1].sync.sync_store_log().await,
            Err(SyncError::UpdateRequired(_))
        ));
        // Ben updates; his reload converts the edit the kept snapshot omits, as
        // Ana did on receipt, and both converge.
        update(&mut devices[1], storage.clone(), true, true).await;
        sync_all(&mut devices).await;
        for device in &devices {
            assert_eq!(name(&device.db).await, "Shopping");
        }
        for device in &mut devices {
            device.writes.post_positions().await.unwrap();
        }
        devices[1].sync.run_retention().await.unwrap();
        assert!(stored(&storage, edit).await);
        // A snapshot that read the raise and covers the edit releases it.
        devices[1].write_snapshot(Audience::Store).await.unwrap();
        for device in &mut devices {
            device.writes.post_positions().await.unwrap();
        }
        devices[1].sync.run_retention().await.unwrap();
        assert!(!stored(&storage, edit).await);
    }
}
