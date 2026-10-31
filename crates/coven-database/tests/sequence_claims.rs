//! Durable claim identity and settlement through the database API.
use coven_database::{
    ClaimAttempt, ClaimRead, ClaimSettlement, ClaimWork, Database, DatabaseBuilder, NewOperation,
};
use coven_format::path::ObjectPath;
use coven_foundation::{
    files::StoreDirectories,
    id_source::{IdSource, SequentialIds, StoreId},
};

fn builder(directory: coven_foundation::files::StoreDir) -> DatabaseBuilder {
    DatabaseBuilder::new(directory)
        .synced_tables(vec![coven_database::SyncedTable::new(
            "items",
            coven_database::RowIdentity::SharedKey,
        )])
        .migrations(vec![coven_database::Migration::addition(
            1,
            "items",
            "CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY)",
        )])
}

async fn open() -> (tempfile::TempDir, coven_foundation::files::StoreDir, Database) {
    let temporary = tempfile::tempdir().unwrap();
    let ids = SequentialIds::new();
    let directory = StoreDirectories::new(temporary.path().into())
        .create_store_dir(StoreId(ids.new_id()), &ids)
        .unwrap();
    let db = builder(directory.clone()).open().await.unwrap();
    (temporary, directory, db)
}
fn bytes(value: &[u8]) -> Box<dyn std::io::Read + Send> {
    Box::new(std::io::Cursor::new(value.to_vec()))
}

fn attempt(store: StoreId, slot: u64, bytes: &[u8]) -> ClaimAttempt<std::io::Cursor<Vec<u8>>> {
    ClaimAttempt {
        path: ObjectPath::slot(store, slot.try_into().unwrap()),
        bytes: std::io::Cursor::new(bytes.to_vec()),
    }
}
async fn operation(db: &Database) -> coven_database::OperationId {
    db.start_operation(NewOperation {
        kind: "sequence".into(),
        data: vec![],
        started_by: "test".into(),
        pauses_writes: false,
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn exact_attempts_and_claim_numbers_survive_reopening() {
    let (_temp, directory, db) = open().await;
    let id = operation(&db).await;
    let first = attempt(directory.id(), 1, b"first exact bytes");
    let expected = first.path.clone();
    let claim = db.begin_claim(ClaimWork::Operation(id), move |_| Ok(first)).await.unwrap();
    assert_eq!(claim.number, 1);
    db.close().await.unwrap();
    let db = builder(directory.clone()).open().await.unwrap();
    assert_eq!(db.current_claim().await.unwrap(), Some(claim));
    let saved = db
        .read_current_attempt(|chunks| {
            let mut saved = Vec::new();
            for chunk in chunks {
                saved.extend(chunk?);
            }
            Ok::<_, coven_database::DbError>(saved)
        })
        .await
        .unwrap();
    assert_eq!(saved, b"first exact bytes");
    assert_eq!(db.settle_claim(ClaimRead::Kept(None)).await.unwrap(), ClaimSettlement::Retry);
    assert!(db.retry_claim(attempt(directory.id(), 2, b"changed")).await.is_err());
    assert_eq!(
        db.settle_claim(ClaimRead::Kept(Some(bytes(b"other")))).await.unwrap(),
        ClaimSettlement::NotLanded
    );
    let second = attempt(directory.id(), 2, b"second exact bytes");
    db.retry_claim(attempt(directory.id(), 2, b"second exact bytes")).await.unwrap();
    assert_eq!(
        db.current_claim().await.unwrap().unwrap().attempts,
        vec![expected.clone(), second.path.clone()]
    );
    assert_eq!(
        db.settle_claim(ClaimRead::Kept(Some(bytes(b"second exact bytes")))).await.unwrap(),
        ClaimSettlement::Landed
    );
    db.finish_claim(None).await.unwrap();
    db.finish_operation(id).await.unwrap();
    assert!(!db.attempted_claim(expected).await.unwrap());
    assert!(!db.attempted_claim(second.path).await.unwrap());
    let id = operation(&db).await;
    let next = attempt(directory.id(), 3, b"third");
    assert_eq!(
        db.begin_claim(ClaimWork::Operation(id), move |_| Ok(next)).await.unwrap().number,
        2
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn compacted_claim_settlement_matches_sequence_settle_examples() {
    // Sequence.Settle: both the number and the complete attempted bytes matter.
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(b"claim one");
    let ours = *hash.finish().as_bytes();
    for (included, digest, expected) in [
        (0, None, ClaimSettlement::NotLanded),
        (1, Some(ours), ClaimSettlement::Landed),
        (1, Some([0x42; 32]), ClaimSettlement::DeviceIdInUse),
        (2, Some(ours), ClaimSettlement::DeviceIdInUse),
        (2, Some([0x42; 32]), ClaimSettlement::DeviceIdInUse),
    ] {
        let (_temp, directory, db) = open().await;
        let id = operation(&db).await;
        let object = attempt(directory.id(), 3, b"claim one");
        db.begin_claim(ClaimWork::Operation(id), move |_| Ok(object)).await.unwrap();
        for (first, covers) in [(3, 5), (4, 2)] {
            assert!(db
                .settle_claim(ClaimRead::Compacted { first, covers, last_claim: included, digest })
                .await
                .is_err());
        }
        assert_eq!(
            db.settle_claim(ClaimRead::Compacted {
                first: 4,
                covers: 5,
                last_claim: included,
                digest,
            })
            .await
            .unwrap(),
            expected
        );
        db.close().await.unwrap();
        let db = builder(directory).open().await.unwrap();
        assert_eq!(db.settle_claim(ClaimRead::Kept(None)).await.unwrap(), expected);
        db.finish_claim(None).await.unwrap();
        assert!(db.current_claim().await.unwrap().is_none());
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn terminal_settlement_survives_repeated_reads() {
    for landed in [false, true] {
        let (_temp, directory, db) = open().await;
        let id = operation(&db).await;
        let object = attempt(directory.id(), 1, b"attempt");
        db.begin_claim(ClaimWork::Operation(id), move |_| Ok(object)).await.unwrap();
        let result = db
            .settle_claim(ClaimRead::Kept(Some(bytes(if landed { b"attempt" } else { b"other" }))))
            .await
            .unwrap();
        assert_eq!(db.settle_claim(ClaimRead::Kept(None)).await.unwrap(), result);
        db.finish_claim(None).await.unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn writes_and_operations_share_the_counter_and_failed_sealing_reserves_nothing() {
    let (_temp, directory, db) = open().await;
    db.write(|sql| {
        sql.execute("INSERT INTO items VALUES('queued')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let original = db
        .read_oldest_upload(|u| Ok::<_, coven_database::DbError>(u.header))
        .await
        .unwrap()
        .unwrap();
    assert!(db
        .begin_claim(ClaimWork::Writes(original.header.position.number), |_| Err::<
            ClaimAttempt<std::io::Empty>,
            _,
        >(
            coven_database::DbError::TransactionEnded
        ))
        .await
        .is_err());
    assert!(db.current_claim().await.unwrap().is_none());
    let object = attempt(directory.id(), 1, b"writes");
    let claim = db
        .begin_claim(ClaimWork::Writes(original.header.position.number), move |_| Ok(object))
        .await
        .unwrap();
    assert_eq!(claim.number, 1);
    db.close().await.unwrap();
    let db = builder(directory.clone()).open().await.unwrap();
    assert_eq!(db.current_claim().await.unwrap(), Some(claim));
    assert_eq!(
        db.read_oldest_upload(|u| Ok::<_, coven_database::DbError>(u.header))
            .await
            .unwrap()
            .unwrap(),
        original
    );
    db.settle_claim(ClaimRead::Kept(Some(bytes(b"other")))).await.unwrap();
    db.finish_claim(None).await.unwrap();
    assert!(db
        .read_oldest_upload(|_| Ok::<_, coven_database::DbError>(()))
        .await
        .unwrap()
        .is_some());
    let operation = operation(&db).await;
    let object = attempt(directory.id(), 2, b"admin");
    assert_eq!(
        db.begin_claim(ClaimWork::Operation(operation), move |_| Ok(object)).await.unwrap().number,
        2
    );
    db.settle_claim(ClaimRead::Kept(Some(bytes(b"admin")))).await.unwrap();
    db.finish_claim(None).await.unwrap();
    let object = attempt(directory.id(), 3, b"writes again");
    assert_eq!(
        db.begin_claim(ClaimWork::Writes(original.header.position.number), move |_| Ok(object))
            .await
            .unwrap()
            .number,
        3
    );
    db.settle_claim(ClaimRead::Kept(Some(bytes(b"writes again")))).await.unwrap();
    assert!(db.finish_claim(None).await.is_err());
    assert!(db
        .read_oldest_upload(|_| Ok::<_, coven_database::DbError>(()))
        .await
        .unwrap()
        .is_some());
    db.close().await.unwrap();
}

#[tokio::test]
async fn different_bytes_at_an_attempted_path_are_contention() {
    let (_temp, directory, db) = open().await;
    let id = operation(&db).await;
    let first = attempt(directory.id(), 1, b"ours");
    db.begin_claim(ClaimWork::Operation(id), move |_| Ok(first)).await.unwrap();
    assert!(db.attempted_claim(attempt(directory.id(), 1, b"theirs").path).await.unwrap());
    assert!(!db.attempted_claim(attempt(directory.id(), 2, b"ours").path).await.unwrap());
    db.close().await.unwrap();
}

struct BoundedBytes {
    remaining: usize,
    fail_at_end: bool,
}
impl std::io::Read for BoundedBytes {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        assert!(out.len() <= coven_format::chunks::CHUNK_SIZE);
        if self.remaining == 0 && self.fail_at_end {
            return Err(std::io::Error::other("stream failed"));
        }
        let count = self.remaining.min(out.len());
        out[..count].fill(7);
        self.remaining -= count;
        Ok(count)
    }
}

#[tokio::test]
async fn claim_streams_are_bounded_and_failed_reads_preserve_the_journal() {
    let (_temp, directory, db) = open().await;
    let id = operation(&db).await;
    let length = 3 * coven_format::chunks::CHUNK_SIZE + 17;
    let path = attempt(directory.id(), 1, b"").path;
    let failed_path = path.clone();
    assert!(db
        .begin_claim(ClaimWork::Operation(id), move |_| Ok(ClaimAttempt {
            path: failed_path,
            bytes: Box::new(BoundedBytes { remaining: length, fail_at_end: true }),
        }))
        .await
        .is_err());
    assert!(db.current_claim().await.unwrap().is_none());
    assert!(!db.attempted_claim(path.clone()).await.unwrap());
    let claim = db
        .begin_claim(ClaimWork::Operation(id), move |_| {
            Ok(ClaimAttempt {
                path,
                bytes: Box::new(BoundedBytes { remaining: length, fail_at_end: false }),
            })
        })
        .await
        .unwrap();
    assert_eq!(claim.number, 1);
    db.close().await.unwrap();
    let db = builder(directory).open().await.unwrap();
    let count = db
        .read_current_attempt(|chunks| {
            let mut count = 0;
            for chunk in chunks {
                let chunk = chunk?;
                assert!(chunk.len() <= coven_format::chunks::CHUNK_SIZE);
                assert!(chunk.iter().all(|b| *b == 7));
                count += chunk.len();
            }
            Ok::<_, coven_database::DbError>(count)
        })
        .await
        .unwrap();
    assert_eq!(count, length);
    // A differing prefix followed by a failed read cannot settle contention.
    let failed = std::io::Read::chain(
        std::io::Cursor::new(vec![8]),
        BoundedBytes { remaining: length, fail_at_end: true },
    );
    assert!(db.settle_claim(ClaimRead::Kept(Some(Box::new(failed)))).await.is_err());
    assert_eq!(db.current_claim().await.unwrap().unwrap().settlement, ClaimSettlement::Unknown);
    assert_eq!(
        db.settle_claim(ClaimRead::Kept(Some(Box::new(BoundedBytes {
            remaining: length,
            fail_at_end: false
        }))))
        .await
        .unwrap(),
        ClaimSettlement::Landed
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn transfer_ranges_and_confirmed_session_survive_reopening() {
    use std::io::{Read, Seek, SeekFrom};
    let (_temp, directory, db) = open().await;
    let id = operation(&db).await;
    let content: Vec<u8> =
        (0..(3 * coven_format::chunks::CHUNK_SIZE + 19)).map(|index| (index % 251) as u8).collect();
    let object = attempt(directory.id(), 1, &content);
    db.begin_claim(ClaimWork::Operation(id), move |_| Ok(object)).await.unwrap();
    db.save_claim_upload(b"provider-confirmed-session".to_vec()).await.unwrap();
    db.close().await.unwrap();
    let db = builder(directory).open().await.unwrap();
    let metadata = db.claim_upload().await.unwrap();
    assert_eq!(metadata.size, content.len() as u64);
    assert_eq!(metadata.session.as_deref(), Some(b"provider-confirmed-session".as_slice()));
    for (offset, length) in [(0, 10), (65530, 23), (65537, 65537), (content.len() - 19, 19)] {
        assert_eq!(
            db.claim_range(offset as u64, length).await.unwrap(),
            content[offset..offset + length]
        );
    }
    assert!(db.claim_range(content.len() as u64 - 1, 2).await.is_err());
    let mut input = db.claim_input().await.unwrap();
    db.settle_claim(ClaimRead::Created).await.unwrap();
    db.finish_claim(None).await.unwrap();
    input.seek(SeekFrom::Start(65530)).unwrap();
    let mut bytes = vec![0; 23];
    input.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, content[65530..65553]);
    assert!(db.current_claim().await.unwrap().is_none());
    db.close().await.unwrap();
    assert!(input.read(&mut [0; 1]).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_sealing_and_streaming_leave_the_app_writer_available() {
    for during_seal in [true, false] {
        let (_temporary, directory, db) = open().await;
        let operation = operation(&db).await;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let claim_db = db.clone();
        let claim = tokio::spawn(async move {
            claim_db
                .begin_claim(ClaimWork::Operation(operation), move |_| {
                    let mut bytes = HeldBytes { entered: Some(entered), resume, sent: false };
                    if during_seal {
                        bytes.wait();
                    }
                    Ok(ClaimAttempt {
                        path: ObjectPath::slot(directory.id(), 1.try_into().unwrap()),
                        bytes,
                    })
                })
                .await
        });
        started.await.unwrap();
        let written = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            db.write(|sql| {
                sql.execute("INSERT INTO items VALUES('while sealing')", [])?;
                Ok(())
            }),
        )
        .await;
        release.send(()).unwrap();
        claim.await.unwrap().unwrap();
        db.close().await.unwrap();
        assert!(
            written.is_ok(),
            "claim preparation held the application writer, during_seal={during_seal}"
        );
        written.unwrap().unwrap();
    }
}

struct HeldBytes {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    resume: std::sync::mpsc::Receiver<()>,
    sent: bool,
}
impl HeldBytes {
    fn wait(&mut self) {
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
            self.resume.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        }
    }
}
impl std::io::Read for HeldBytes {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        self.wait();
        if self.sent {
            return Ok(0);
        }
        output[0] = 7;
        self.sent = true;
        Ok(1)
    }
}
