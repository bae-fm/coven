use crate::snapshot_write::tests::{author, frames, id, load_one};
use crate::tests::TestStore;
use crate::write::tests::{notes, sql, NOTES};
use coven_merge::Audience;
use std::io::{Cursor, Read};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_loading_and_validation_leave_the_application_writer_available() {
    for validating in [false, true] {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('n','before','')")
            .await
            .unwrap();
        let snapshot = frames(&db, Audience::Store).await;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let input = HeldSnapshot {
            bytes: Cursor::new(snapshot.concat()),
            entered: Some(entered),
            resume,
        };
        let loading = db.clone();
        let load = tokio::spawn(async move {
            if validating {
                loading
                    .validate_snapshot(
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
        let written = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sql(&db, "UPDATE notes SET title='while decoding'"),
        )
        .await;
        release.send(()).unwrap();
        load.await.unwrap().unwrap();
        db.close().await.unwrap();
        written
            .expect("snapshot decoding held the application writer")
            .unwrap();
    }
}
struct HeldSnapshot {
    bytes: Cursor<Vec<u8>>,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    resume: std::sync::mpsc::Receiver<()>,
}
impl Read for HeldSnapshot {
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
