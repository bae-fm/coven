use super::*;
use crate::{DownloadedPartStream, DownloadedWriteStream};
use coven_format::value::Value;
use coven_format::write_stream::WriteEncoder;
use coven_merge::{ColumnValue, Operation};
use std::{collections::BTreeMap, io::Cursor};

fn stream(record: &WriteRecord) -> DownloadedWriteStream<Cursor<Vec<u8>>> {
    let encoder = WriteEncoder::new(record).unwrap();
    DownloadedWriteStream {
        signer: crate::test_utils::signer(),
        authorship: crate::test_utils::TestAuthorship::write(record),
        header: encoder.header().clone(),
        parts: (0..record.parts.len())
            .map(|part| {
                DownloadedPartStream::Opened(Cursor::new(
                    encoder
                        .part_chunks(part)
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap()
                        .concat(),
                ))
            })
            .collect(),
    }
}

async fn rejects_schema_violation(
    record: WriteRecord,
    tables: Vec<SyncedTable>,
    schema: &'static str,
) {
    let mut accepted = Vec::new();
    for streamed in [false, true] {
        let store = TestStore::new();
        let db = store.schema(tables.clone(), schema).await.unwrap();
        let before = crate::tests::data(&db, Audience::Store);
        let result = if streamed {
            db.receive_stream(stream(&record), || Ok(())).await
        } else {
            db.receive(record.clone().into()).await
        };
        match result {
            Ok(outcome) => accepted.push((streamed, outcome)),
            Err(DbError::Snapshot(_)) => {
                assert_eq!(count(&db, "_coven_writes"), 0);
                assert_eq!(db.applied_slot().await.unwrap(), 0);
                assert_eq!(count(&db, "_coven_rows"), 0);
                assert_eq!(crate::tests::data(&db, Audience::Store), before);
            }
            Err(error) => panic!("expected schema validation failure: {error:?}"),
        }
        db.close().await.unwrap();
    }
    assert!(
        accepted.is_empty(),
        "invalid writes accepted (streamed, outcome): {accepted:?}"
    );
}
#[tokio::test]
async fn downloads_refuse_non_uuid_independent_keys() {
    let store = TestStore::new();
    let source = open(&store).await;
    sql(
        &source,
        "INSERT INTO notes VALUES('not-a-uuid','title','body')",
    )
    .await
    .unwrap();
    let record = records(&source).pop().unwrap();
    source.close().await.unwrap();
    rejects_schema_violation(
        record,
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)],
        NOTES,
    )
    .await;
}
#[tokio::test]
async fn downloads_refuse_a_descendant_in_another_audience_than_its_parent() {
    let store = TestStore::new();
    let source = circle_db(&store).await;
    sql(&source, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','store','title'); INSERT INTO children VALUES('00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000001')").await.unwrap();
    let mut record = records(&source).pop().unwrap();
    let index = record.parts[0]
        .rows
        .iter()
        .position(|row| row.row.table == "children")
        .unwrap();
    let mut child = record.parts[0].rows.remove(index);
    let audience = Audience::Circle(CircleId(uuid::Uuid::from_u128(10)));
    child.row.audience = audience.clone();
    record.parts.push(WritePart {
        audience,
        rows: vec![child],
        executions: Vec::new(),
        dismissals: Vec::new(),
    });
    source.close().await.unwrap();
    rejects_schema_violation(record, circle_tables(), CIRCLE_SCHEMA).await;
}
#[tokio::test]
async fn streamed_retarget_is_one_write_including_local_trigger_effects() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let schema="CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE CASCADE); CREATE INDEX links_parent ON links(parent); CREATE TABLE audit(value TEXT); CREATE TRIGGER edited AFTER UPDATE ON links BEGIN INSERT INTO audit VALUES('update'); END";
    let a = sa.schema(references(), schema).await.unwrap();
    let b = sb.schema(references(), schema).await.unwrap();
    sql(
        &a,
        "INSERT INTO parents VALUES('old'); INSERT INTO links VALUES('link','old')",
    )
    .await
    .unwrap();
    b.receive_stream(stream(&records(&a)[0]), || Ok(()))
        .await
        .unwrap();
    sql(&a,"INSERT INTO parents VALUES('new'); UPDATE links SET parent='new'; DELETE FROM parents WHERE id='old'").await.unwrap();
    b.receive_stream(stream(&records(&a)[1]), || Ok(()))
        .await
        .unwrap();
    assert_eq!(count(&b, "links"), 1);
    assert_eq!(count(&b, "audit"), 1);
    assert_eq!(
        crate::tests::data(&a, Audience::Store),
        crate::tests::data(&b, Audience::Store)
    );
}

#[tokio::test]
async fn authentication_failure_rolls_back_rows_slot_losses_and_observation() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = open(&sb).await;
    let mut observed =
        b.subscribe(|sql| Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?));
    assert!(observed
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    sql(
        &a,
        "INSERT INTO notes VALUES('one','first',''),('two','second','')",
    )
    .await
    .unwrap();
    let record = records(&a).remove(0);
    let before = crate::tests::data(&b, Audience::Store);
    assert!(b
        .receive_stream(stream(&record), || Err(DbError::SyncStream(Box::new(
            std::io::Error::other("bad signature")
        ))))
        .await
        .is_err());
    assert_eq!(count(&b, "notes"), 0);
    assert_eq!(count(&b, "_coven_writes"), 0);
    assert_eq!(b.applied_slot().await.unwrap(), 0);
    assert_eq!(crate::tests::data(&b, Audience::Store), before);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), observed.next())
            .await
            .is_err()
    );
    b.receive_stream(stream(&record), || Ok(())).await.unwrap();
    assert_eq!(
        observed
            .next()
            .await
            .expect("the live query answers")
            .unwrap(),
        ["first", "second"]
    );
}

#[tokio::test]
async fn streamed_excluded_rows_use_the_common_snapshot_loss_section() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = crate::write_boundary::tests::version_two(&sb, true).await;
    b.exclude_later_writes(2).await;
    sql(
        &a,
        "INSERT INTO notes VALUES('one','first',''),('two','second','')",
    )
    .await
    .unwrap();
    let record = records(&a).remove(0);
    b.receive_stream(stream(&record), || Ok(())).await.unwrap();
    assert_eq!(count(&b, "notes"), 0);
    assert_eq!(count(&b, "_coven_lost"), 2);
    let snapshot = crate::snapshot_write::tests::frames(&b, Audience::Store).await;
    let (header, records) = crate::snapshot_write::tests::decode(&snapshot);
    assert_eq!(header.counts[4], 2);
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, coven_format::snapshot::SnapshotRecord::Loss(_)))
            .count(),
        2
    );
}

#[tokio::test]
async fn a_write_may_observe_an_earlier_prefix_than_the_latest_slot() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let sc = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = open(&sb).await;
    let c = open(&sc).await;
    sql(&a, "INSERT INTO notes VALUES('a','first','')")
        .await
        .unwrap();
    let wa = records(&a).remove(0);
    b.receive(wa.clone().into()).await.unwrap();
    a.receive(wa.clone().into()).await.unwrap();
    c.receive(wa.into()).await.unwrap();
    sql(&b, "INSERT INTO notes VALUES('b','second','')")
        .await
        .unwrap();
    let wb = records(&b).remove(0);
    c.receive(wb.clone().into()).await.unwrap();
    sql(&c, "INSERT INTO notes VALUES('c','third','')")
        .await
        .unwrap();
    let mut wc = records(&c).remove(0);
    wc.header.read_through = 1;
    a.receive(wb.into()).await.unwrap();
    assert_eq!(
        a.receive_stream(stream(&wc), || Ok(())).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(count(&a, "notes"), 3);
}

#[tokio::test]
async fn unknown_tables_and_columns_are_rejected_without_panicking() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = open(&sb).await;
    sql(&a, "INSERT INTO notes VALUES('one','title','body')")
        .await
        .unwrap();
    let record = records(&a).remove(0);
    for table in [false, true] {
        let mut invalid = record.clone();
        let row = &mut invalid.parts[0].rows[0];
        if table {
            row.row.table = "absent".into();
        } else {
            let Operation::Insert(columns) = &mut row.change.operation else {
                panic!("insert")
            };
            columns.insert(
                "absent".into(),
                ColumnValue {
                    value: coven_format::value::Value::Text("bad".into()),
                    parents: BTreeMap::new(),
                },
            );
        }
        assert!(b.receive_stream(stream(&invalid), || Ok(())).await.is_err());
        assert_eq!(count(&b, "notes"), 0);
    }
}

#[tokio::test]
async fn an_unread_slot_prevents_a_later_slot_from_committing() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = open(&sb).await;
    sql(&a, "INSERT INTO notes VALUES('one','first','')")
        .await
        .unwrap();
    crate::tests::delete_circle(&b, CircleId(uuid::Uuid::from_u128(8)))
        .await
        .unwrap();
    let record = records(&a).remove(0);
    assert!(matches!(
        b.apply_slot::<crate::pending::tests::Record, _, _>(
            crate::IncomingSlot {
                digest: [0x31; 32],
                slot: crate::Slot(3),
                header: coven_format::sequence::SlotHeader {
                    device: record.header.position.device,
                    claim: 1,
                    author: crate::tests::member(1),
                    after: 2
                },
                content: crate::SlotContent::Writes {
                    size: 1,
                    writes: vec![stream(&record)],
                },
            },
            || Ok(()),
            None
        )
        .await,
        Err(DbError::SequenceGap { .. })
    ));
    assert_eq!(count(&b, "notes"), 0);
}

#[tokio::test]
async fn a_panicking_authenticator_rolls_back_without_poisoning_the_writer() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = open(&sb).await;
    sql(&a, "INSERT INTO notes VALUES('one','first','')")
        .await
        .unwrap();
    let record = records(&a).remove(0);
    let input = stream(&record);
    let clone = b.clone();
    assert!(tokio::spawn(async move {
        clone
            .receive_stream(input, || panic!("authenticator panicked"))
            .await
    })
    .await
    .unwrap_err()
    .is_panic());
    assert_eq!(count(&b, "notes"), 0);
    b.receive_stream(stream(&record), || Ok(())).await.unwrap();
    assert_eq!(count(&b, "notes"), 1);
}

mod memory {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MeasuredAllocator;
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    #[global_allocator]
    static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

    fn allocated(size: usize) {
        let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
    // SAFETY: every allocation operation delegates to System with the original
    // pointer and layout. The counters never affect allocation or ownership.
    unsafe impl GlobalAlloc for MeasuredAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                allocated(layout.size());
            }
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe {
                System.dealloc(pointer, layout);
            }
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let next = unsafe { System.realloc(pointer, layout, size) };
            if !next.is_null() {
                LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
                allocated(size);
            }
            next
        }
    }

    fn state_digest(db: &Database) -> coven_crypto::ObjectDigest {
        db.inspect_writer_schema(|db,schema| {
            let store=crate::merge_store::MergeStore::from_schema(db,&schema.schema).without_row_cache();
            let mut digest=coven_crypto::ObjectHasher::new();
            db.for_each("SELECT DISTINCT table_name,key,audience FROM _coven_rows ORDER BY table_name,key,audience",[],|r| {
                let row=crate::row_queries::read_identity(r)?;
                let state=store.row_without_losses(&row)?.state;
                digest.update(&coven_format::merge_fields::encode_merge_row(&coven_format::snapshot_rows::MergeRow{state})?);
                Ok::<_,DbError>(())
            }).unwrap();
            crate::loss_record::visit(db,&Audience::Store,|loss| {
                digest.update(&coven_format::merge_fields::encode_loss(&loss)?);
                Ok::<_,DbError>(())
            }).unwrap();
            digest.finish()
        })
    }

    #[test]
    fn connected_rows_do_not_retain_the_whole_streamed_write_in_memory() {
        const CHILD: &str = "COVEN_STREAM_HEAP_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "download::tests::streams::memory::connected_rows_do_not_retain_the_whole_streamed_write_in_memory", "--nocapture"])
            .env(CHILD, "1").output().unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        // SAFETY: this isolated child has not opened SQLite on any thread.
        assert_eq!(
            unsafe { rusqlite::ffi::sqlite3_config(rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, 1) },
            rusqlite::ffi::SQLITE_OK
        );
        tokio::runtime::Runtime::new().unwrap().block_on(async {
        let ids = SequentialIds::new();
        let sa = TestStore::with_ids(&ids);
        let sb = TestStore::with_ids(&ids);
        let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE CASCADE,body TEXT NOT NULL,file TEXT); CREATE INDEX links_parent ON links(parent)";
        let tables = || {
            let mut declarations = references();
            declarations[1] = declarations[1].clone().file_column(crate::FileColumn::new("file"));
            declarations
        };
        let open = |store: &TestStore| {
            store
                .builder(tables(), vec![crate::Migration::addition(1, "schema", schema)])
                .namespaces(vec![crate::Namespace::new("linked-files", crate::Provenance::AppProvided, 1 << 30)])
                .open()
        };
        let a = open(&sa).await.unwrap();
        let b = open(&sb).await.unwrap();
        a.write_with_files::<_, _, _, _, crate::DbError>(|batch| {
            Ok(batch.put_file("linked-files", b"original".to_vec(), crate::Place::Cloud))
        }, |sql, file| {
            sql.execute("INSERT INTO parents VALUES('parent')", [])?;
            let body = "x".repeat(64 * 1024);
            for i in 0..512 {
                sql.execute("INSERT INTO links(id,parent,body,file) VALUES(?1,'parent',?2,?3)", (i.to_string(), &body, file))?;
            }
            Ok(())
        }).await.unwrap();
        for (source, receiver, statement, expected) in [
            (&a, &b, None, 512),
            (&b, &a, Some("UPDATE links SET body=replace(body,'x','y')"), 512),
            (&b, &a, Some("DELETE FROM parents"), 0),
        ] {
            if let Some(statement) = statement { sql(source, statement).await.unwrap(); }
            let record=records(source).pop().unwrap();
            source.receive_stream(stream(&record),||Ok(())).await.unwrap();
            let prepared = stream(&record);
            // The source bytes represent the remote object: keep them alive
            // throughout measurement so releasing input cannot hide a retained
            // copy in the database's merge views.
            let held: Vec<std::sync::Arc<[u8]>> = prepared.parts.into_iter().map(|part| {
                let DownloadedPartStream::Opened(input) = part else { panic!("readable part") };
                input.into_inner().into()
            }).collect();
            assert!(held.iter().map(|bytes| bytes.len()).sum::<usize>() > 32 * 1024 * 1024);
            let input = || DownloadedWriteStream {
                signer: crate::test_utils::signer(),
                authorship: prepared.authorship.clone(),
                header: prepared.header.clone(),
                parts: held.iter().map(|bytes| DownloadedPartStream::Opened(Cursor::new(bytes.clone()))).collect(),
            };
            let prior = state_digest(receiver);
            let baseline = LIVE.load(Ordering::SeqCst);
            PEAK.store(baseline, Ordering::SeqCst);
            let sqlite_baseline = unsafe {
                rusqlite::ffi::sqlite3_memory_highwater(1);
                rusqlite::ffi::sqlite3_memory_used()
            };
            assert!(sqlite_baseline > 0, "SQLite memory accounting must be enabled");
            let rows = count(receiver, "links");
            assert!(receiver.receive_stream(input(), || Err(DbError::TransactionEnded)).await.is_err());
            assert_eq!(count(receiver, "links"), rows);
            assert_eq!(state_digest(receiver),prior);
            receiver.receive_stream(input(), || Ok(())).await.unwrap();
            let rust_extra = PEAK.load(Ordering::SeqCst).checked_sub(baseline).unwrap();
            let sqlite_extra = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) } - sqlite_baseline;
            let extra = rust_extra + usize::try_from(sqlite_extra).unwrap();
            // Adding separate peaks is a conservative bound even when they occur
            // at different instants. It includes SQLite's page and temp caches.
            assert!(extra < 16 * 1024 * 1024, "{statement:?}: streamed merge retained {extra} additional heap bytes (Rust {rust_extra}, SQLite {sqlite_extra})");
            drop(held);
            assert_eq!(count(receiver, "links"), expected);
            assert_eq!(crate::tests::data(&a,Audience::Store), crate::tests::data(&b,Audience::Store));
            assert_eq!(crate::file_write::tests::owned_paths(&sa).len(), 1, "queued copy");
        }
    });
        std::process::exit(86);
    }
}

#[tokio::test]
async fn retargeting_a_reference_replaces_its_retained_pre_null_value() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let sc = TestStore::with_ids(&ids);
    let a = reference_db(&sa).await;
    let b = reference_db(&sb).await;
    let c = reference_db(&sc).await;
    sql(&a, "INSERT INTO parents VALUES('old'),('new')")
        .await
        .unwrap();
    for db in [&b, &c] {
        db.receive(records(&a)[0].clone().into()).await.unwrap();
    }
    sql(&b, "INSERT INTO links VALUES('link','old')")
        .await
        .unwrap();
    b.receive(records(&b)[0].clone().into()).await.unwrap();
    c.receive(records(&b)[0].clone().into()).await.unwrap();
    // This author has never received the child, so its real delete has no child change.
    sql(&a, "DELETE FROM parents WHERE id='old'").await.unwrap();
    for db in [&b, &c] {
        db.receive_stream(stream(&records(&a)[1]), || Ok(()))
            .await
            .unwrap();
        assert_eq!(count(db, "_coven_written_values"), 1);
    }
    sql(&c, "UPDATE links SET parent='new'").await.unwrap();
    b.receive_stream(stream(&records(&c)[0]), || Ok(()))
        .await
        .unwrap();
    assert_eq!(
        crate::tests::data(&b, Audience::Store),
        crate::tests::data(&c, Audience::Store)
    );
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_constraint_refused_at_commit_reports_itself_and_keeps_the_handle_usable() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,body TEXT); CREATE TABLE parents(id TEXT PRIMARY KEY); CREATE TABLE guard(parent TEXT REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER guarded AFTER INSERT ON notes WHEN coven_applying() BEGIN INSERT INTO guard VALUES('absent'); END";
    let tables = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
    let a = a_store.schema(tables(), schema).await.unwrap();
    let b = b_store.schema(tables(), schema).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('note','title','body')")
        .await
        .unwrap();
    let write = records(&a).pop().unwrap();
    let mut observed =
        b.subscribe(|sql| Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?));
    assert!(observed
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    for streamed in [false, true] {
        let before = crate::tests::contents(&b);
        let error = if streamed {
            b.receive_stream(stream(&write), || Ok(())).await
        } else {
            b.receive(write.clone().into()).await
        }
        .unwrap_err();
        assert!(
            matches!(&error, DbError::Sqlite(rusqlite::Error::SqliteFailure(e, _)) if e.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY),
            "{error:?}"
        );
        assert_eq!(crate::tests::contents(&b), before);
    }
    sql(&b, "INSERT INTO parents VALUES('absent')")
        .await
        .unwrap();
    b.receive(write.into()).await.unwrap();
    assert_eq!(count(&b, "notes"), 1);
    assert_eq!(
        observed
            .next()
            .await
            .expect("the live query answers")
            .unwrap(),
        ["title"]
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}

const SCHEMA: &str = "CREATE TABLE parents(a TEXT NOT NULL,b INT NOT NULL,PRIMARY KEY(a,b)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,a TEXT,b INT,FOREIGN KEY(a,b) REFERENCES parents(a,b))";
fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("parents", RowIdentity::SharedKey).key_columns(["a", "b"]),
        SyncedTable::new("children", RowIdentity::SharedKey),
    ]
}

async fn history(null: bool, hybrid_parent: bool) -> Vec<WriteRecord> {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = sa.schema(tables(), SCHEMA).await.unwrap();
    let b = sb.schema(tables(), SCHEMA).await.unwrap();
    sql(&a, if null { "INSERT INTO children VALUES('child',NULL,NULL)" } else if hybrid_parent {
        "INSERT INTO parents VALUES('A',3),('B',3),('A',5),('B',5); INSERT INTO children VALUES('child','A',3)"
    } else {
        "INSERT INTO parents VALUES('A',3),('B',3),('A',5); INSERT INTO children VALUES('child','A',3)"
    }).await.unwrap();
    let initial = records(&a).remove(0);
    b.receive(initial.clone().into()).await.unwrap();
    sql(&a, "UPDATE children SET a='B'").await.unwrap();
    sql(&b, "UPDATE children SET b=5").await.unwrap();
    let result = vec![
        initial,
        records(&a).pop().unwrap(),
        records(&b).pop().unwrap(),
    ];
    a.close().await.unwrap();
    b.close().await.unwrap();
    result
}

#[tokio::test]
async fn concurrent_composite_reference_columns_keep_one_authored_tuple() {
    compare(false).await;
}

#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN"]
async fn composite_reference_differential() {
    compare(true).await;
}

async fn compare(lean: bool) {
    for (null, hybrid_parent) in [(false, false), (false, true), (true, false)] {
        let original = history(null, hybrid_parent).await;
        for winner in [1, 2] {
            let mut writes = original.clone();
            let base = writes[0].header.timestamp.milliseconds();
            for (i, write) in writes.iter_mut().enumerate().skip(1) {
                write.header.timestamp = coven_merge::Timestamp::new(
                    base + if i == winner { 2 } else { 1 },
                    0,
                    write.header.position.device,
                )
                .unwrap();
            }
            let expected = if winner == 1 {
                (Some("B".to_owned()), if null { None } else { Some(3) })
            } else {
                (if null { None } else { Some("A".to_owned()) }, Some(5))
            };
            let mut states = Vec::new();
            for streamed in [false, true] {
                for order in [[0, 1, 2], [0, 2, 1]] {
                    let fixture = TestStore::new();
                    let db = fixture.schema(tables(), SCHEMA).await.unwrap();
                    for i in order {
                        if streamed {
                            db.receive_stream(stream(&writes[i]), || Ok(()))
                                .await
                                .unwrap();
                        } else {
                            db.receive(writes[i].clone().into()).await.unwrap();
                        }
                    }
                    let tuple = db.inspect_writer(|db| {
                        db.query_row("SELECT a,b FROM children", [], |r| {
                            Ok((r.get::<_, Option<String>>(0)?, r.get::<_, Option<i64>>(1)?))
                        })
                        .unwrap()
                    });
                    assert_eq!(tuple, expected);
                    let child = &writes[1].parts[0].rows[0].row;
                    db.inspect_writer_schema(|writer, schema| {
                        let app = crate::write_rows::AppView::after(writer, schema);
                        let store = crate::merge_store::MergeStore::new(writer, &app);
                        let state = store.row(child).unwrap().state;
                        for column in ["a", "b"] {
                            assert_eq!(state.cells()[column].write, writes[winner].header.position);
                        }
                        assert_eq!(
                            state.cells()["a"].value.parents,
                            state.cells()["b"].value.parents
                        );
                        assert_eq!(state.cells()["a"].value.parents.is_empty(), null);
                        if !null {
                            let parent = state.cells()["a"].value.parents.values().next().unwrap();
                            assert_eq!(
                                coven_format::key::decode_key(&parent.row.key).unwrap(),
                                [
                                    Value::Text(expected.0.clone().unwrap()),
                                    Value::Integer(expected.1.unwrap())
                                ]
                            );
                        }
                        assert_eq!(state.lost().len(), 2);
                    });
                    if lean {
                        compare_lean(&writes, &order, &db);
                    }
                    states.push(crate::tests::data(&db, Audience::Store));
                    db.close().await.unwrap();
                }
            }
            assert!(states.windows(2).all(|pair| pair[0] == pair[1]));
        }
    }
}

fn compare_lean(writes: &[WriteRecord], order: &[usize], db: &Database) {
    use serde_json::json;
    let columns = ["id", "a", "b"];
    let rows: Vec<_> = writes
        .iter()
        .flat_map(|w| &w.parts)
        .flat_map(|p| &p.rows)
        .map(|r| r.row.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let id = |write| {
        writes
            .iter()
            .position(|w| w.header.position == write)
            .unwrap()
    };
    let row_id = |row: &coven_merge::RowId| rows.iter().position(|r| r == row).unwrap();
    let column_id = |column: &str| columns.iter().position(|c| *c == column).unwrap();
    let input_writes: Vec<_> = writes
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let changes: Vec<_> = w
                .parts
                .iter()
                .flat_map(|p| &p.rows)
                .map(|row| {
                    let (kind, values) = match &row.change.operation {
                        Operation::Insert(c) => (0, c),
                        Operation::Update(c) => (1, c),
                        _ => panic!("setter"),
                    };
                    json!({"row": row_id(&row.row),"gen": row.change.generation,"kind": kind,
                "cols": values.keys().map(|c| column_id(c)).collect::<Vec<_>>()})
                })
                .collect();
            json!({"id": i, "ts": crate::test_utils::tests::stamp(w.header.timestamp).to_string(),
            "past": if i == 0 { vec![] } else { vec![0] }, "changes": changes})
        })
        .collect();
    let states = db.inspect_writer_schema(|writer, schema| {
        let app = crate::write_rows::AppView::after(writer, schema);
        let store = crate::merge_store::MergeStore::new(writer, &app);
        rows.iter()
            .map(|r| store.row(r).unwrap().state)
            .collect::<Vec<_>>()
    });
    let input_rows: Vec<_> = states.iter().enumerate().map(|(i,state)| {
        // Each composite key supplies one reference, independent of its column count.
        let references: BTreeMap<_,_> = state.cells().values().flat_map(|c| &c.value.parents).collect();
        let references: Vec<_> = references.values().map(|p| json!({"parent": row_id(&p.row),
            "stale": states[row_id(&p.row)].generation() != p.generation})).collect();
        json!({"id":i,"present":state.present(),"refs":references,"check":false,"deleted":false,"claims":[],"rank":i})
    }).collect();
    let input =
        json!({"mode":"boolean","writes":input_writes,"order":order,"columns":3,"rows":input_rows});
    let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
    let actual = crate::test_utils::tests::run_lean(&runner, &input);
    let expected: Vec<_> = states
        .iter()
        .enumerate()
        .map(|(i, state)| {
            let mut cells: Vec<_> = state
                .cells()
                .iter()
                .map(|(c, v)| [column_id(c), id(v.write)])
                .collect();
            cells.sort();
            let mut lost: Vec<_> = state
                .lost()
                .iter()
                .map(|(k, v)| {
                    [
                        column_id(&k.column),
                        id(k.write),
                        v.incarnation as usize,
                        id(v.replaced_by),
                    ]
                })
                .collect();
            lost.sort();
            json!({"id":i,"gen":state.generation(),
            "gw":state.generations().iter().map(|(g,w)| [*g,id(*w) as u64]).collect::<Vec<_>>(),
            "cells":cells,"lost":lost,"removed":false,"rules":[]})
        })
        .collect();
    assert_eq!(actual, json!(expected));
}

#[tokio::test]
async fn incomplete_or_inconsistent_composite_references_are_refused() {
    for null in [false, true] {
        let writes = history(null, false).await;
        for broken in 0..4 {
            let mut write = writes[1].clone();
            let change = &mut write.parts[0].rows[0];
            let Operation::Update(columns) = &mut change.change.operation else {
                panic!("update")
            };
            let parent = coven_merge::Parent {
                row: coven_merge::RowId {
                    table: "parents".into(),
                    key: coven_format::key::encode_key(&[
                        Value::Text("B".into()),
                        Value::Integer(3),
                    ])
                    .unwrap(),
                    audience: Audience::Store,
                },
                generation: 1,
            };
            match broken {
                0 => {
                    columns.remove("b");
                    change.old.remove("b");
                    change.sources.setters.remove("b");
                }
                1 => {
                    columns.get_mut("b").unwrap().parents.clear();
                    if null {
                        columns.get_mut("a").unwrap().parents = BTreeMap::from([(
                            coven_merge::ForeignKey::new(["a", "b"], "parents", ["a", "b"]),
                            parent.clone(),
                        )]);
                    }
                }
                2 => {
                    columns.values_mut().for_each(|c| c.parents.clear());
                    if null {
                        columns.get_mut("b").unwrap().value = Value::Integer(3);
                    }
                }
                3 => {
                    columns.get_mut("b").unwrap().value = Value::Null;
                    for value in columns.values_mut() {
                        value.parents.insert(
                            coven_merge::ForeignKey::new(["a", "b"], "parents", ["a", "b"]),
                            parent.clone(),
                        );
                    }
                }
                _ => unreachable!(),
            }
            for streamed in [false, true] {
                let fixture = TestStore::new();
                let db = fixture.schema(tables(), SCHEMA).await.unwrap();
                db.receive(writes[0].clone().into()).await.unwrap();
                let before = crate::tests::contents(&db);
                let result = if streamed {
                    db.receive_stream(stream(&write), || Ok(())).await
                } else {
                    db.receive(write.clone().into()).await
                };
                assert!(
                    matches!(result, Err(DbError::Snapshot(_))),
                    "{null} {broken} {streamed}: {result:?}"
                );
                assert_eq!(crate::tests::contents(&db), before);
                db.close().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn overlapping_composite_references_capture_the_connected_columns() {
    let fixture = TestStore::new();
    let schema = "CREATE TABLE parents(a TEXT NOT NULL,b INT NOT NULL,PRIMARY KEY(a,b)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,a TEXT,b INT,c TEXT,FOREIGN KEY(a,b) REFERENCES parents(a,b),FOREIGN KEY(c,b) REFERENCES parents(a,b))";
    let db = fixture.schema(tables(), schema).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('A',3),('B',3); INSERT INTO children VALUES('child','A',3,'A')",
    )
    .await
    .unwrap();
    sql(&db, "UPDATE children SET a='B'").await.unwrap();
    let write = records(&db).pop().unwrap();
    let change = &write.parts[0].rows[0];
    let Operation::Update(columns) = &change.change.operation else {
        panic!("update")
    };
    assert_eq!(
        columns.keys().map(String::as_str).collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    assert_eq!(
        change.old.keys().collect::<Vec<_>>(),
        columns.keys().collect::<Vec<_>>()
    );
    assert_eq!(columns["b"].parents.len(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn generated_composite_reference_keeps_the_inputs_of_its_tuple() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE parents(a TEXT NOT NULL,b INT NOT NULL,PRIMARY KEY(a,b)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,x INT,y INT,a TEXT AS(x+y),b INT,FOREIGN KEY(a,b) REFERENCES parents(a,b))";
    let a = sa.schema(tables(), schema).await.unwrap();
    let b = sb.schema(tables(), schema).await.unwrap();
    sql(&a,"INSERT INTO parents VALUES('3',3),('4',3),('3',5); INSERT INTO children(id,x,y,b) VALUES('child',1,2,3)").await.unwrap();
    b.receive(records(&a)[0].clone().into()).await.unwrap();
    a.receive(records(&a)[0].clone().into()).await.unwrap();
    sql(&a, "UPDATE children SET x=2").await.unwrap();
    sql(&b, "UPDATE children SET b=5").await.unwrap();
    let wa = records(&a).pop().unwrap();
    let wb = records(&b).pop().unwrap();
    for (db, write) in [(&a, wb), (&b, wa)] {
        db.receive(write.into()).await.unwrap();
    }
    assert_eq!(
        crate::tests::data(&a, Audience::Store),
        crate::tests::data(&b, Audience::Store)
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn snapshots_require_one_setter_for_a_composite_reference() {
    use crate::snapshot_write::tests::{decode, frames, id, load_one};
    use coven_format::snapshot::{SnapshotEncoder, SnapshotRecord};
    let sa = TestStore::new();
    let sb = TestStore::new();
    let a = sa.schema(tables(), SCHEMA).await.unwrap();
    let b = sb.schema(tables(), SCHEMA).await.unwrap();
    sql(&a, "INSERT INTO children VALUES('child',NULL,NULL)")
        .await
        .unwrap();
    sql(&a, "UPDATE children SET a='A'").await.unwrap();
    let writes = records(&a);
    let oracle = coven_merge::History::new(
        writes
            .iter()
            .map(|record| crate::test_utils::tests::merge_write(record, &writes)),
    )
    .unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let (header, mut rows) = decode(&snapshot);
    for row in &mut rows {
        if let SnapshotRecord::Merge(merged) = row {
            let state = &merged.state;
            let mut cells = state.cells().clone();
            cells.get_mut("b").unwrap().write = writes[0].header.position;
            merged.state = coven_merge::RowState::from_parts(
                state.row().clone(),
                state.generations().clone(),
                cells,
                state.lost().clone(),
                &oracle,
            )
            .unwrap();
        }
    }
    let (mut encoder, mut bytes) = SnapshotEncoder::start(header).unwrap();
    for row in rows {
        bytes.extend(encoder.record(row).unwrap());
    }
    bytes.extend(encoder.finish().unwrap());
    let before = crate::tests::contents(&b);
    let result = load_one(&b, id(Audience::Store), snapshot.prefix, Cursor::new(bytes)).await;
    assert!(matches!(result, Err(DbError::Snapshot(_))), "{result:?}");
    assert_eq!(crate::tests::contents(&b), before);
    crate::snapshot_write::tests::assert_loaded_losses(&a, &b).await;
    a.close().await.unwrap();
    b.close().await.unwrap();
}
