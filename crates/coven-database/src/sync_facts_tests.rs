use crate::file_write::tests::{open, stage};
use crate::tests::TestStore;
use crate::{Database, DbError, FileDatabase, FileWork, PendingValue, Provenance};
use coven_foundation::id_source::{DeviceId, FileId, ObjectId};
use serde::{Deserialize, Serialize};

/// The upload read starts from the pending records, the files in flight and
/// an id-ordered walk that stops at its first match, and reads the queue's
/// rows only through the primary key: no part scans the whole queue, whose
/// table is `_coven_file_uploads` (§19.1).
#[tokio::test]
async fn the_upload_read_does_not_scan_the_queue() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, vec![1], true).await.unwrap();
    let plan: Vec<String> = db.inspect_writer(|sql| {
        sql.query(&format!("EXPLAIN QUERY PLAN {}", super::UPLOADS), [], |r| r.get(3)).unwrap()
    });
    let scans: Vec<&str> = plan
        .iter()
        .map(String::as_str)
        .filter(|line| line.starts_with("SCAN") && !line.starts_with("SCAN (subquery"))
        .collect();
    // `f` is the id-ordered walk under LIMIT 1 over the unstored uploads alone, `p` the pending records
    // with an object, `j` the files in flight; `u`, the outer read of the
    // queue, is only ever searched by its primary key.
    assert_eq!(
        scans,
        [
            "SCAN p USING INDEX _coven_pending_object",
            "SCAN f USING INDEX _coven_file_uploads_unstored",
            "SCAN j VIRTUAL TABLE INDEX 1:"
        ],
        "{plan:#?}"
    );
    assert!(plan.contains(&"SEARCH u USING INTEGER PRIMARY KEY (rowid=?)".to_string()));
}

/// A pending record that names an upload, or no file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    upload: Option<(DeviceId, ObjectId)>,
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
    type Subject = (DeviceId, ObjectId);
    fn subject(&self) -> &Self::Subject {
        self.upload.as_ref().expect("an upload's")
    }
    fn operation(&self) -> Option<crate::OperationId> {
        None
    }
    fn object(&self) -> Option<(FileWork, DeviceId, ObjectId)> {
        self.upload.map(|(device, object)| (FileWork::Upload, device, object))
    }
    fn move_of(&self) -> Option<crate::MoveId> {
        None
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

/// Attach one file to each of `rows` in one write, so their uploads share a
/// move.
async fn attach(db: &Database, rows: &[&'static str]) {
    let rows = rows.to_vec();
    db.staged_write::<_, _, _, _, DbError>(
        {
            let rows = rows.clone();
            move |batch| {
                Ok(rows
                    .iter()
                    .map(|row| {
                        batch.put_file("files", row.as_bytes().to_vec(), crate::Place::Cloud)
                    })
                    .collect::<Vec<_>>())
            }
        },
        move |sql, files| {
            for (row, file) in rows.iter().zip(files) {
                sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (*row, file))?;
            }
            Ok(())
        },
    )
    .await
    .unwrap();
}

/// With a stored upload first, the facts list the first one sending, the
/// first queued one with no record and each one with a record, in id order,
/// and no other: the later queued one with no record is left out, though the
/// queue holds it (§19.1).
#[tokio::test]
async fn the_facts_list_the_first_sending_the_first_waiting_and_each_record() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    // Ids 1 and 2 share a move; completing the first stores it, and the
    // second waits queued.
    attach(&db, &["a", "b"]).await;
    for row in ["c", "d", "e"] {
        attach(&db, &[row]).await;
    }
    let owner = FileDatabase::new(db.clone());
    owner.complete_upload(1, ()).await.unwrap();
    let queue = owner.uploads().await.unwrap();
    assert_eq!(
        queue.iter().map(|u| (u.id, u.stored)).collect::<Vec<_>>(),
        [(1, true), (2, false), (3, false), (4, false), (5, false)]
    );
    let sending = &queue[2];
    let _attempt = db.uploads_in_flight().start(
        sending.location.uploader(),
        sending.location.object(),
        sending.files.clone(),
        crate::TransferProgress { bytes_transferred: 0, bytes_total: 1 },
    );
    let failing = &queue[4];
    let record = Record { upload: Some((failing.location.uploader(), failing.location.object())) };
    db.inspect_writer(|sql| crate::pending::save(sql, &record).unwrap());
    let facts = db.sync_facts::<Record, _, DbError>(Ok).await.unwrap();
    let listed: Vec<_> = facts
        .uploads
        .iter()
        .map(|upload| (upload.sending, upload.stored, upload.record.is_some()))
        .collect();
    assert_eq!(listed, [(false, false, false), (true, false, false), (false, false, true)]);
    assert!(facts.queued);
}
