use super::*;
use crate::{DownloadedPartStream, DownloadedWriteStream};
use coven_format::write_stream::WriteEncoder;
use std::io::Cursor;

fn stream(record: &WriteRecord) -> DownloadedWriteStream<Cursor<Vec<u8>>> {
    let encoder = WriteEncoder::new(record).unwrap();
    DownloadedWriteStream {
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
    b.apply_downloaded_stream(
        stream(&records(&a)[0]),
        coven_format::value::EntryPositions(Vec::new()),
        || Ok(()),
    )
    .await
    .unwrap();
    sql(&a,"INSERT INTO parents VALUES('new'); UPDATE links SET parent='new'; DELETE FROM parents WHERE id='old'").await.unwrap();
    b.apply_downloaded_stream(
        stream(&records(&a)[1]),
        coven_format::value::EntryPositions(Vec::new()),
        || Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(count(&b, "links"), 1);
    assert_eq!(count(&b, "audit"), 1);
    assert_eq!(
        fingerprint(&a, Audience::Store).await,
        fingerprint(&b, Audience::Store).await
    );
}

#[tokio::test]
async fn authentication_failure_rolls_back_rows_positions_losses_and_observation() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = open(&sb).await;
    let mut observed =
        b.subscribe(|sql| Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?));
    assert!(observed.next().await.unwrap().is_empty());
    sql(
        &a,
        "INSERT INTO notes VALUES('one','first',''),('two','second','')",
    )
    .await
    .unwrap();
    let record = records(&a).remove(0);
    let before = fingerprint(&b, Audience::Store).await;
    assert!(b
        .apply_downloaded_stream(
            stream(&record),
            coven_format::value::EntryPositions(Vec::new()),
            || Err(DbError::SyncStream(Box::new(std::io::Error::other(
                "bad signature"
            ))))
        )
        .await
        .is_err());
    assert_eq!(count(&b, "notes"), 0);
    assert_eq!(count(&b, "_coven_writes"), 0);
    assert_eq!(count(&b, "_coven_positions"), 0);
    assert_eq!(fingerprint(&b, Audience::Store).await, before);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), observed.next())
            .await
            .is_err()
    );
    b.apply_downloaded_stream(
        stream(&record),
        coven_format::value::EntryPositions(Vec::new()),
        || Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(observed.next().await.unwrap(), ["first", "second"]);
}

#[tokio::test]
async fn streamed_excluded_rows_accumulate_under_one_snapshot_header() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = open(&sb).await;
    sql(
        &a,
        "INSERT INTO notes VALUES('one','first',''),('two','second','')",
    )
    .await
    .unwrap();
    let mut record = records(&a).remove(0);
    record.header.disposition = WriteDisposition::Lost(1);
    b.apply_downloaded_stream(
        stream(&record),
        coven_format::value::EntryPositions(Vec::new()),
        || Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(count(&b, "notes"), 0);
    assert_eq!(count(&b, "_coven_lost"), 2);
    b.inspect_writer(|sql| {
        let bytes: Vec<u8> = sql
            .query_row("SELECT header FROM _coven_excluded_writes", [], |r| {
                r.get(0)
            })
            .unwrap();
        let header = coven_format::write_stream::WriteHeaderFrame::decode(&bytes).unwrap();
        assert_eq!(header.parts[0].record_count, 2);
        assert_eq!(
            header.parts[0].plaintext_length,
            WriteEncoder::new(&record).unwrap().header().parts[0].plaintext_length
        );
    });
}

#[tokio::test]
async fn a_write_must_include_the_transitive_past_of_its_frontier() {
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
    b.apply_downloaded(wa.clone().into()).await.unwrap();
    c.apply_downloaded(wa.into()).await.unwrap();
    sql(&b, "INSERT INTO notes VALUES('b','second','')")
        .await
        .unwrap();
    let wb = records(&b).remove(0);
    c.apply_downloaded(wb.clone().into()).await.unwrap();
    sql(&c, "INSERT INTO notes VALUES('c','third','')")
        .await
        .unwrap();
    let mut wc = records(&c).remove(0);
    wc.header.had_read = WritePositions(vec![wb.header.position]);
    a.apply_downloaded(wb.into()).await.unwrap();
    assert!(a
        .apply_downloaded_stream(
            stream(&wc),
            coven_format::value::EntryPositions(Vec::new()),
            || Ok(())
        )
        .await
        .is_err());
    assert_eq!(count(&a, "notes"), 2);
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
        assert!(b
            .apply_downloaded_stream(
                stream(&invalid),
                coven_format::value::EntryPositions(Vec::new()),
                || Ok(())
            )
            .await
            .is_err());
        assert_eq!(count(&b, "notes"), 0);
    }
}

#[tokio::test]
async fn a_stale_store_log_view_cannot_commit_a_download() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa).await;
    let b = open(&sb).await;
    sql(&a, "INSERT INTO notes VALUES('one','first','')")
        .await
        .unwrap();
    crate::store_log::tests::delete_circle(&b, CircleId(uuid::Uuid::from_u128(8)))
        .await
        .unwrap();
    let record = records(&a).remove(0);
    assert!(matches!(
        b.apply_downloaded_stream(
            stream(&record),
            coven_format::value::EntryPositions(Vec::new()),
            || Ok(())
        )
        .await,
        Err(DbError::StoreLogEntriesChanged)
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
            .apply_downloaded_stream(
                input,
                coven_format::value::EntryPositions(Vec::new()),
                || panic!("authenticator panicked"),
            )
            .await
    })
    .await
    .unwrap_err()
    .is_panic());
    assert_eq!(count(&b, "notes"), 0);
    b.apply_downloaded_stream(
        stream(&record),
        coven_format::value::EntryPositions(Vec::new()),
        || Ok(()),
    )
    .await
    .unwrap();
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

    #[test]
    fn connected_rows_do_not_retain_the_whole_streamed_write_in_memory() {
        const CHILD: &str = "COVEN_STREAM_HEAP_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "download::tests::streams::memory::connected_rows_do_not_retain_the_whole_streamed_write_in_memory", "--nocapture"])
            .env(CHILD, "1").output().unwrap();
            assert!(
                output.status.success(),
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
        let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE CASCADE,body TEXT NOT NULL,file TEXT,size INTEGER,hash BLOB,location TEXT); CREATE INDEX links_parent ON links(parent)";
        let tables = || {
            let mut declarations = references();
            declarations[1] = declarations[1].clone().carries_files(crate::FileDecl::new(
                "linked-files", crate::Provenance::AppProvided, crate::Uploads::WhenAsked, crate::CacheFill::CacheLazy,
            ).with_id_column("file"));
            declarations
        };
        let a = sa.schema(tables(), schema).await.unwrap();
        let b = sb.schema(tables(), schema).await.unwrap();
        a.write_with_files(|batch| {
            batch.put_file("linked-files", "shared", b"original".to_vec());
            Ok(())
        }, |sql| {
            sql.execute("INSERT INTO parents VALUES('parent')", [])?;
            let body = "x".repeat(64 * 1024);
            for i in 0..512 {
                sql.execute("INSERT INTO links(id,parent,body,file) VALUES(?1,'parent',?2,'shared')", (i.to_string(), &body))?;
            }
            Ok(())
        }).await.unwrap();
        for (source, receiver, statement, expected) in [
            (&a, &b, None, 512),
            (&b, &a, Some("UPDATE links SET body=replace(body,'x','y')"), 512),
            (&b, &a, Some("DELETE FROM parents"), 0),
        ] {
            if let Some(statement) = statement { sql(source, statement).await.unwrap(); }
            let prepared = stream(&records(source).pop().unwrap());
            // The source bytes represent the remote object: keep them alive
            // throughout measurement so releasing input cannot hide a retained
            // copy in the database's merge views.
            let held: Vec<std::sync::Arc<[u8]>> = prepared.parts.into_iter().map(|part| {
                let DownloadedPartStream::Opened(input) = part else { panic!("readable part") };
                input.into_inner().into()
            }).collect();
            assert!(held.iter().map(|bytes| bytes.len()).sum::<usize>() > 32 * 1024 * 1024);
            let input = || DownloadedWriteStream {
                header: prepared.header.clone(),
                parts: held.iter().map(|bytes| DownloadedPartStream::Opened(Cursor::new(bytes.clone()))).collect(),
            };
            let baseline = LIVE.load(Ordering::SeqCst);
            PEAK.store(baseline, Ordering::SeqCst);
            let sqlite_baseline = unsafe {
                rusqlite::ffi::sqlite3_memory_highwater(1);
                rusqlite::ffi::sqlite3_memory_used()
            };
            assert!(sqlite_baseline > 0, "SQLite memory accounting must be enabled");
            let prior = fingerprint(receiver, Audience::Store).await;
            let rows = count(receiver, "links");
            assert!(receiver.apply_downloaded_stream(input(), coven_format::value::EntryPositions(Vec::new()), || Err(DbError::TransactionEnded)).await.is_err());
            assert_eq!(count(receiver, "links"), rows);
            assert_eq!(fingerprint(receiver, Audience::Store).await, prior);
            receiver.apply_downloaded_stream(input(), coven_format::value::EntryPositions(Vec::new()), || Ok(())).await.unwrap();
            let rust_extra = PEAK.load(Ordering::SeqCst).checked_sub(baseline).unwrap();
            let sqlite_extra = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) } - sqlite_baseline;
            let extra = rust_extra + usize::try_from(sqlite_extra).unwrap();
            // Adding separate peaks is a conservative bound even when they occur
            // at different instants. It includes SQLite's page and temp caches.
            assert!(extra < 16 * 1024 * 1024, "{statement:?}: streamed merge retained {extra} additional heap bytes (Rust {rust_extra}, SQLite {sqlite_extra})");
            drop(held);
            assert_eq!(count(receiver, "links"), expected);
            assert_eq!(fingerprint(&a, Audience::Store).await, fingerprint(&b, Audience::Store).await);
            assert_eq!(crate::file_write::tests::owned_paths(&sa).len(), usize::from(expected != 0));
        }
    });
    }
}

#[tokio::test]
async fn retargeting_a_reference_replaces_its_retained_pre_null_value() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL); CREATE INDEX links_parent ON links(parent)";
    let a = sa.schema(references(), schema).await.unwrap();
    let b = sb.schema(references(), schema).await.unwrap();
    for statement in [
        "INSERT INTO parents VALUES('old'),('new'); INSERT INTO links VALUES('link','old')",
        "DELETE FROM parents WHERE id='old'",
        "UPDATE links SET parent='new'",
    ] {
        sql(&a, statement).await.unwrap();
        let mut record = records(&a).pop().unwrap();
        if statement == "DELETE FROM parents WHERE id='old'" {
            // A parent-only write must also update children its author cannot read.
            record.parts[0]
                .rows
                .retain(|row| row.row.table == "parents");
        }
        b.apply_downloaded_stream(
            stream(&record),
            coven_format::value::EntryPositions(Vec::new()),
            || Ok(()),
        )
        .await
        .unwrap();
        if statement == "DELETE FROM parents WHERE id='old'" {
            assert_eq!(count(&b, "_coven_reference_values"), 1);
        }
    }
    assert_eq!(
        fingerprint(&a, Audience::Store).await,
        fingerprint(&b, Audience::Store).await
    );
}

#[tokio::test]
async fn a_default_parent_created_in_the_same_write_is_visible_by_its_unique_column() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,code TEXT NOT NULL UNIQUE); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,parent TEXT DEFAULT 'Inbox' REFERENCES parents(code) ON DELETE SET DEFAULT); CREATE INDEX links_parent ON links(parent)";
    let a = sa.schema(references(), schema).await.unwrap();
    let b = sb.schema(references(), schema).await.unwrap();
    for statement in [
        "INSERT INTO parents VALUES('old','old'); INSERT INTO links VALUES('link','old')",
        "INSERT INTO parents VALUES('new','Inbox'); DELETE FROM parents WHERE id='old'",
    ] {
        sql(&a, statement).await.unwrap();
        b.apply_downloaded_stream(
            stream(&records(&a).pop().unwrap()),
            coven_format::value::EntryPositions(Vec::new()),
            || Ok(()),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        fingerprint(&a, Audience::Store).await,
        fingerprint(&b, Audience::Store).await
    );
}
