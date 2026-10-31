use crate::snapshot_write::tests::{author, frames, id, load_one};
use crate::tests::TestStore;
use crate::write::tests::{notes, sql, NOTES};
use coven_merge::Audience;
use std::io::{Cursor, Read};

#[tokio::test]
async fn snapshot_installation_does_not_decode_prepared_records_under_the_writer() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','before','')").await.unwrap();
    let snapshot = frames(&db, Audience::Store).await;
    let counter = coven_foundation::test_work::Counter::default();
    let bytes = snapshot.concat();
    counter
        .run(load_one(&db, id(Audience::Store), snapshot.prefix, Cursor::new(bytes)))
        .await
        .unwrap();
    let counts = counter.take();
    assert!(counts.snapshot_decodes > 0);
    assert!(counts.decodes > 0, "the real snapshot decoder was measured");
    assert_eq!(counts.snapshot_decodes_with_writer, 0);
    db.close().await.unwrap();
}

/// Staging decodes each batch on the input connection and commits it there,
/// so the application can write between batches, before loading or checking
/// files takes the writer (§21.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_staging_leaves_the_application_writer_available() {
    for validating in [false, true] {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('n','before','')").await.unwrap();
        let snapshot = frames(&db, Audience::Store).await;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let input = HeldSnapshot {
            bytes: Cursor::new(snapshot.concat()),
            reads: 0,
            entered: Some(entered),
            resume,
        };
        let loading = db.clone();
        let load = tokio::spawn(async move {
            if validating {
                crate::snapshot_write::tests::validate_one(
                    &loading,
                    id(Audience::Store),
                    snapshot.prefix,
                    input,
                    author(),
                    std::sync::Arc::new(crate::test_utils::TestAuthorship),
                )
                .await
                .map(drop)
            } else {
                load_one(&loading, id(Audience::Store), snapshot.prefix, input).await
            }
        });
        started.await.unwrap();
        assert!(db.staged_snapshot_records().await.unwrap() > 0);
        let written = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sql(&db, "UPDATE notes SET title='while staging'"),
        )
        .await;
        release.send(()).unwrap();
        load.await.unwrap().unwrap();
        assert_eq!(db.staged_snapshot_records().await.unwrap(), 0);
        db.close().await.unwrap();
        written.expect("snapshot staging held the application writer").unwrap();
    }
}
/// Plaintext that arrives in two halves and stalls between them, once the
/// first half is staged.
struct HeldSnapshot {
    bytes: Cursor<Vec<u8>>,
    reads: usize,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    resume: std::sync::mpsc::Receiver<()>,
}
impl Read for HeldSnapshot {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        self.reads += 1;
        if self.reads == 2 {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.resume.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
            }
        }
        let length = output.len().min(self.bytes.get_ref().len().div_ceil(2));
        self.bytes.read(&mut output[..length])
    }
}
