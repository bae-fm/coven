use crate::tests::TestStore;
use crate::{
    Database, DbError, FileColumn, FileDatabase, FileWork, Migration, Namespace, PendingValue,
    Place, Provenance, RowIdentity, SyncedTable,
};
use coven_foundation::id_source::{DeviceId, FileId, ObjectId};
use serde::{Deserialize, Serialize};

const SCHEMA: &str = "CREATE TABLE covers(id TEXT NOT NULL PRIMARY KEY,file TEXT);
    CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT);";

/// `covers` binds its file, which holds the write that sets it, and `photos`
/// trails (§17.4).
async fn open(store: &TestStore) -> Database {
    store
        .builder(
            vec![
                SyncedTable::new("covers", RowIdentity::SharedKey)
                    .file_column(FileColumn::new("file").bound()),
                SyncedTable::new("photos", RowIdentity::SharedKey)
                    .file_column(FileColumn::new("file")),
            ],
            vec![Migration::addition(1, "files", SCHEMA)],
        )
        .namespaces(vec![
            Namespace::new("covers", Provenance::AppProvided, 1 << 30),
            Namespace::new("photos", Provenance::AppProvided, 1 << 30),
        ])
        .open()
        .await
        .unwrap()
}

/// Stage `bytes` into `table`'s namespace and point row `id` at them.
async fn stage(db: &Database, table: &'static str, id: &'static str, bytes: Vec<u8>) -> FileId {
    db.staged_write::<_, _, _, _, DbError>(
        move |batch| Ok(batch.put_file(table, bytes, Place::Cloud)),
        move |sql, file| {
            sql.execute(&format!("INSERT INTO {table}(id,file) VALUES(?1,?2)"), (id, file))?;
            Ok(file)
        },
    )
    .await
    .unwrap()
}

/// The upload queue ids of `owner`'s uploads, oldest first.
async fn queue(owner: &FileDatabase) -> Vec<crate::FileUpload> {
    owner.uploads().await.unwrap()
}

/// The upload read starts from the pending records, the bound files in
/// flight and an id-ordered walk that stops at its first match, and reads
/// the queue's rows only through the primary key: no part scans the whole
/// queue, whose table is `_coven_file_uploads` (§19.1).
#[tokio::test]
async fn the_upload_read_does_not_scan_the_queue() {
    let store = TestStore::new();
    let db = open(&store).await;
    stage(&db, "covers", "a", vec![1]).await;
    let plan: Vec<String> = db.inspect_writer(|sql| {
        sql.query(&format!("EXPLAIN QUERY PLAN {}", super::UPLOADS), [], |r| r.get(3)).unwrap()
    });
    let scans: Vec<&str> = plan
        .iter()
        .map(String::as_str)
        .filter(|line| line.starts_with("SCAN") && !line.starts_with("SCAN (subquery"))
        .collect();
    // `f` is the id-ordered walk under LIMIT 1 over the unstored bound
    // uploads alone, `p` the pending records with an object, `j` the files
    // in flight, read once by each of the two walks that leave out the
    // upload being sent; `u`, the outer read of the queue, is only ever
    // searched by its primary key, and the retry rows are searched through
    // their upload index.
    assert_eq!(
        scans,
        [
            "SCAN p USING INDEX _coven_pending_object",
            "SCAN f USING INDEX _coven_file_uploads_bound_unstored",
            "SCAN j VIRTUAL TABLE INDEX 1:",
            "SCAN j VIRTUAL TABLE INDEX 1:"
        ],
        "{plan:#?}"
    );
    assert!(plan.contains(&"SEARCH u USING INTEGER PRIMARY KEY (rowid=?)".to_string()));
}

/// A pending record about an upload's stored bytes or about a move.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum Record {
    Upload(DeviceId, ObjectId),
    Move(crate::MoveId),
}

impl PendingValue for Record {
    fn writes_refused(_: crate::Slot, _: crate::Slot, _: crate::WriteCutoff) -> Self {
        unreachable!("no slot in this test")
    }
    fn reload(_: coven_merge::Audience) -> Self {
        unreachable!("no audience in this test")
    }
    fn provider_access(_: crate::OperationId, _: String, _: bool) -> Self {
        unreachable!("no operation in this test")
    }
    type Subject = Record;
    fn subject(&self) -> &Self::Subject {
        self
    }
    fn operation(&self) -> Option<crate::OperationId> {
        None
    }
    fn object(&self) -> Option<(FileWork, DeviceId, ObjectId)> {
        match self {
            Self::Upload(device, object) => Some((FileWork::Upload, *device, *object)),
            Self::Move(_) => None,
        }
    }
    fn move_of(&self) -> Option<crate::MoveId> {
        match self {
            Self::Move(id) => Some(*id),
            Self::Upload(..) => None,
        }
    }
    fn pin_of(&self) -> Option<FileId> {
        None
    }
    fn slot(&self) -> Option<crate::Slot> {
        None
    }
    fn ends_when_covered(&self) -> bool {
        false
    }
    const BUILD: u128 = 1;
    fn can_retry(&self, _: u128, _: u32) -> bool {
        false
    }
    fn cause(&self) -> Option<&str> {
        None
    }
    fn with_cause(self, _: Option<String>) -> Self {
        self
    }
}

fn upload_record(upload: &crate::FileUpload) -> Record {
    Record::Upload(upload.location.uploader(), upload.location.object())
}

/// Attach one file to each of `rows` of `table` in one write, so their
/// uploads share a move.
async fn attach(db: &Database, table: &'static str, rows: &[&'static str]) {
    let rows = rows.to_vec();
    db.staged_write::<_, _, _, _, DbError>(
        {
            let rows = rows.clone();
            move |batch| {
                Ok(rows
                    .iter()
                    .map(|row| batch.put_file(table, row.as_bytes().to_vec(), Place::Cloud))
                    .collect::<Vec<_>>())
            }
        },
        move |sql, files| {
            for (row, file) in rows.iter().zip(files) {
                sql.execute(&format!("INSERT INTO {table}(id,file) VALUES(?1,?2)"), (*row, file))?;
            }
            Ok(())
        },
    )
    .await
    .unwrap();
}

/// The facts list the first bound upload sending, the first queued one with
/// no record and no retry, the first with no record that waits out a retry,
/// and each one with a record, in id order, and no other: the later queued
/// ones with no record are left out, though the queue holds them (§19.1).
#[tokio::test]
async fn the_facts_list_the_first_sending_the_first_waiting_and_each_record() {
    let store = TestStore::new();
    let db = open(&store).await;
    for row in ["a", "b", "c", "d", "e"] {
        attach(&db, "covers", &[row]).await;
    }
    let owner = FileDatabase::new(db.clone());
    let queue = queue(&owner).await;
    assert_eq!(
        queue.iter().map(|u| (u.id, u.stored)).collect::<Vec<_>>(),
        [(1, false), (2, false), (3, false), (4, false), (5, false)]
    );
    let sending = &queue[2];
    let _attempt = db.uploads_in_flight().start(
        sending.location.uploader(),
        sending.location.object(),
        sending.files.clone(),
        crate::TransferProgress { bytes_transferred: 0, bytes_total: 1 },
    );
    let record = upload_record(&queue[4]);
    db.inspect_writer(|sql| crate::pending::save(sql, &record).unwrap());
    let facts = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    let listed: Vec<_> = facts
        .uploads
        .iter()
        .map(|upload| (upload.sending, upload.stored, upload.record.is_some()))
        .collect();
    assert_eq!(listed, [(false, false, false), (true, false, false), (false, false, true)]);
    assert!(facts.queued);
    // Two of the later ones wait out a retry: the first of them is listed
    // beside the first with no retry, since a retrying upload waits to sync
    // while one with its turn is syncing, and the state shows the higher.
    let wait = crate::RetryWait {
        failures: 1,
        not_before: std::time::UNIX_EPOCH + std::time::Duration::from_secs(9),
        wait: std::time::Duration::from_secs(2),
    };
    db.inspect_writer(|sql| {
        for upload in [&queue[1], &queue[3]] {
            crate::retry::save(sql, crate::RetrySubject::Upload(upload.id), &wait).unwrap();
        }
    });
    let facts = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    let listed: Vec<_> = facts
        .uploads
        .iter()
        .map(|upload| (upload.sending, upload.record.is_some(), upload.retry_at.is_some()))
        .collect();
    assert_eq!(
        listed,
        [(false, false, false), (false, false, true), (true, false, false), (false, true, false)]
    );
}

/// A trailing upload, its record and its move's record are not sync's: with
/// trailing files only in the queue, the facts hold no upload, no record, and
/// nothing queued, however the uploads stand, while the queue's facts still
/// list the upload and what is recorded about it, for its phase (§17.4,
/// §19.1).
#[tokio::test]
async fn a_trailing_upload_and_the_records_about_it_leave_the_facts() {
    let store = TestStore::new();
    let db = open(&store).await;
    attach(&db, "photos", &["a", "b"]).await;
    let owner = FileDatabase::new(db.clone());
    let uploads = queue(&owner).await;
    assert_eq!(uploads.len(), 2);
    assert!(uploads.iter().all(|upload| upload.holds.is_none()));
    let own = upload_record(&uploads[0]);
    let moving = Record::Move(uploads[0].of);
    for record in [&own, &moving] {
        db.inspect_writer(|sql| crate::pending::save(sql, record).unwrap());
    }
    let sending = &uploads[1];
    let _attempt = db.uploads_in_flight().start(
        sending.location.uploader(),
        sending.location.object(),
        sending.files.clone(),
        crate::TransferProgress { bytes_transferred: 0, bytes_total: 1 },
    );
    let facts = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    assert!(facts.uploads.is_empty(), "{:?}", facts.uploads);
    assert!(facts.records.is_empty(), "{:?}", facts.records);
    assert!(!facts.queued);
    let queued = db.upload_queue_facts::<Record, _, DbError>(Ok).await.unwrap();
    assert_eq!(queued.queue.uploads.len(), 2);
    let recorded: Vec<_> = queued
        .recorded
        .iter()
        .map(|recorded| (recorded.own.clone(), recorded.moving.clone()))
        .collect();
    assert_eq!(recorded, [(Some(own.clone()), Some(moving.clone())), (None, Some(moving.clone()))]);
}

/// A bound upload and the records about it are sync's, a trailing one beside
/// it is not, and a move holding a bound upload keeps its record in the
/// facts: the bound file's failure holds its write (§17.4, §19.1).
#[tokio::test]
async fn a_bound_upload_and_its_records_stay_in_the_facts_beside_a_trailing_one() {
    let store = TestStore::new();
    let db = open(&store).await;
    attach(&db, "photos", &["a"]).await;
    attach(&db, "covers", &["b"]).await;
    let owner = FileDatabase::new(db.clone());
    let uploads = queue(&owner).await;
    let (trailing, bound) = (&uploads[0], &uploads[1]);
    assert!(trailing.holds.is_none() && bound.holds.is_some());
    let records = [
        upload_record(trailing),
        Record::Move(trailing.of),
        upload_record(bound),
        Record::Move(bound.of),
    ];
    for record in &records {
        db.inspect_writer(|sql| crate::pending::save(sql, record).unwrap());
    }
    let facts = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    assert_eq!(facts.records, [records[2].clone(), records[3].clone()]);
    assert_eq!(facts.uploads.len(), 1);
    assert!(facts.uploads[0].record.is_some());
    assert!(facts.queued);
}

/// A bound upload counts as queued work while it has bytes to send, and no
/// longer once its completion retired it (§19.1).
#[tokio::test]
async fn a_completed_bound_upload_is_not_queued_work() {
    let store = TestStore::new();
    let db = open(&store).await;
    attach(&db, "covers", &["a", "b"]).await;
    let owner = FileDatabase::new(db.clone());
    let facts = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    assert!(facts.queued);
    owner.complete_upload(1, ()).await.unwrap();
    owner.complete_upload(2, ()).await.unwrap();
    let facts = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    assert!(!facts.queued);
}

/// The facts hold the sync scope's backoff, when a failed pass armed one, and
/// no other work's: an upload's wait is its own (§6.7, §19.1).
#[tokio::test]
async fn the_facts_hold_the_sync_scopes_backoff() {
    let store = TestStore::new();
    let db = open(&store).await;
    attach(&db, "covers", &["a"]).await;
    let none = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    assert_eq!(none.pass_retry, None);
    assert!(none.operation_retries.is_empty());
    let wait = crate::RetryWait {
        failures: 1,
        not_before: std::time::UNIX_EPOCH + std::time::Duration::from_secs(9),
        wait: std::time::Duration::from_secs(2),
    };
    db.inspect_writer(|sql| {
        crate::retry::save(sql, crate::RetrySubject::Upload(1), &wait).unwrap();
    });
    let upload = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    assert_eq!(upload.pass_retry, None);
    assert_eq!(upload.uploads[0].retry_at, Some(wait.not_before));
    db.inspect_writer(|sql| {
        crate::retry::save(sql, crate::RetrySubject::Sync, &wait).unwrap();
    });
    let pass = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    assert_eq!(pass.pass_retry, Some(wait.not_before));
    assert!(pass.operation_retries.is_empty());
}
