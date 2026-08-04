use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::*;

async fn temp_entries(dir: &Path) -> Vec<PathBuf> {
    let mut entries = tokio::fs::read_dir(dir).await.expect("read test directory");
    let mut temps = Vec::new();
    while let Some(entry) = entries.next_entry().await.expect("read test entry") {
        let path = entry.path();
        if is_temp_blob_path(&path) {
            temps.push(path);
        }
    }
    temps
}

#[tokio::test]
async fn direct_file_operations_preserve_bytes_and_report_presence() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let source = tmp.path().join("source").join("blob.bin");
    let copy = tmp.path().join("copy").join("blob.bin");
    let renamed = tmp.path().join("copy").join("renamed.bin");
    let bytes = b"0123456789";

    AtomicStagedFile::write_for_test(&source, bytes)
        .await
        .expect("write source");
    assert!(exists(&source).await.expect("source exists"));
    assert_eq!(file_len(&source).await.expect("source length"), 10);
    assert_eq!(read(&source).await.expect("read source"), bytes);

    AtomicStagedFile::write_for_test(&copy, b"old copy")
        .await
        .expect("seed copy");
    let staged = AtomicStagedFile::create(&copy)
        .await
        .expect("reserve replacement copy");
    let (staged, _, _) = staged.copy_from(&source).await.expect("copy source");
    staged.commit().await.expect("replace copy");
    assert_eq!(read(&copy).await.expect("read copy"), bytes);

    rename(&copy, &renamed).await.expect("rename copy");
    assert!(!exists(&copy).await.expect("old copy absent"));
    assert_eq!(read(&renamed).await.expect("read renamed"), bytes);
    assert!(remove_file(&renamed).await.expect("remove renamed"));
    assert!(!remove_file(&renamed).await.expect("renamed already absent"));
}

#[tokio::test]
async fn exact_read_keeps_bytes_and_identity_on_one_open_inode() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("blob.bin");
    let original = b"original exact bytes";
    let replacement = b"replacement bytes";
    AtomicStagedFile::write_for_test(&path, original)
        .await
        .expect("write original");
    let mut open = tokio::fs::File::open(&path).await.expect("open original");

    AtomicStagedFile::write_for_test(&path, replacement)
        .await
        .expect("replace path after open");
    let (bytes, size, hash) =
        read_open_file_with_facts(&mut open, &path, ExactReadSelection::Whole)
            .await
            .expect("read the already-open exact file");

    assert_eq!(bytes, original);
    assert_eq!(size, original.len() as u64);
    assert_eq!(
        crate::protocol::store_commit::ObjectHash::from_digest(hash),
        crate::protocol::store_commit::ObjectHash::digest(original)
    );
    assert_eq!(read(&path).await.expect("read replacement"), replacement);
}

/// An [`OpenFile`] serves every range from the descriptor it opened.
/// Replacing the path with same-length different bytes — the swap a
/// per-range re-open by name would silently follow — cannot change what the
/// handle reads.
#[tokio::test]
async fn an_open_file_serves_ranges_from_the_inode_it_opened() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("blob.bin");
    let original = b"original exact bytes";
    let replacement = b"replaced exact bytes";
    assert_eq!(original.len(), replacement.len());
    AtomicStagedFile::write_for_test(&path, original)
        .await
        .expect("write original");

    let open = OpenFile::open(&path).await.expect("open");
    assert_eq!(open.size(), original.len() as u64);

    AtomicStagedFile::write_for_test(&path, replacement)
        .await
        .expect("replace the path after the handle opened it");

    assert_eq!(
        open.read_at(9, 5).await.expect("mid-file range"),
        b"exact",
        "the range comes from the opened inode, not the file now at the name",
    );
    assert_eq!(
        open.read_at(0, original.len() as u64)
            .await
            .expect("whole opened file"),
        original,
    );
    assert_eq!(
        open.read_at(0, 8).await.expect("re-read the head"),
        &original[..8],
        "each read positions itself, so an earlier read leaves no cursor behind",
    );
    assert_eq!(
        open.read_at(4, 0).await.expect("zero-length range"),
        Vec::<u8>::new(),
    );
    let past_end = open
        .read_at(original.len() as u64 - 2, 5)
        .await
        .expect_err("a range past the end must fail, not short-read");
    assert!(
        past_end.contains("read 5 bytes at"),
        "the error names the range it could not serve: {past_end}",
    );

    assert_eq!(read(&path).await.expect("read replacement"), replacement);
}

#[tokio::test]
async fn exact_copy_keeps_bytes_and_identity_on_one_open_inode() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let source = tmp.path().join("source.bin");
    let destination = tmp.path().join("destination.bin");
    let original = b"original exact bytes";
    let replacement = b"replacement bytes";
    AtomicStagedFile::write_for_test(&source, original)
        .await
        .expect("write original");
    let open = tokio::fs::File::open(&source).await.expect("open original");

    AtomicStagedFile::write_for_test(&source, replacement)
        .await
        .expect("replace source path after open");
    let staged = AtomicStagedFile::create(&destination)
        .await
        .expect("reserve destination stage");
    let (staged, size, hash) = staged
        .write_open_file_with_facts(open, &source)
        .await
        .expect("copy the already-open exact file");
    staged.commit().await.expect("publish exact copy");

    assert_eq!(read(&destination).await.expect("read copy"), original);
    assert_eq!(size, original.len() as u64);
    assert_eq!(
        crate::protocol::store_commit::ObjectHash::from_digest(hash),
        crate::protocol::store_commit::ObjectHash::digest(original)
    );
    assert_eq!(read(&source).await.expect("read replacement"), replacement);
}

#[tokio::test]
async fn staged_file_is_invisible_until_verified_commit() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let destination = tmp.path().join("blob.bin");
    AtomicStagedFile::write_for_test(&destination, b"prior")
        .await
        .expect("seed destination");
    let mut staged = AtomicStagedFile::create(&destination)
        .await
        .expect("allocate staging path");
    staged
        .write_bytes(b"verified")
        .await
        .expect("write staged file");

    assert_eq!(read(&destination).await.unwrap(), b"prior");
    staged.commit().await.expect("commit staged file");
    assert_eq!(read(&destination).await.unwrap(), b"verified");
    assert!(temp_entries(tmp.path()).await.is_empty());
}

#[tokio::test]
async fn plaintext_stream_fills_the_reserved_stage_before_commit() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let source_path = tmp.path().join("source.bin");
    let destination = tmp.path().join("blob.bin");
    let bytes = b"verified plaintext";
    AtomicStagedFile::write_for_test(&source_path, bytes)
        .await
        .expect("write plaintext source");
    struct TestFileReader {
        file: tokio::fs::File,
        path: std::path::PathBuf,
    }

    #[async_trait]
    impl PlaintextChunkReader for TestFileReader {
        type Error = String;
        async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
            use tokio::io::AsyncReadExt;
            let mut chunk = vec![0u8; max];
            let read = self
                .file
                .read(&mut chunk)
                .await
                .map_err(|error| format!("read {}: {error}", self.path.display()))?;
            chunk.truncate(read);
            Ok(chunk)
        }
    }

    let mut source = TestFileReader {
        file: tokio::fs::File::open(&source_path)
            .await
            .expect("open plaintext source"),
        path: source_path,
    };
    let mut staged = AtomicStagedFile::create(&destination)
        .await
        .expect("reserve staging file");

    let written = staged
        .write_plaintext(&mut source)
        .await
        .expect("fill reserved staging file");
    assert_eq!(written, bytes.len() as u64);
    assert!(!destination.exists());

    staged.commit().await.expect("publish verified stage");
    assert_eq!(read(&destination).await.expect("read destination"), bytes);
    assert!(temp_entries(tmp.path()).await.is_empty());
}

#[tokio::test]
async fn byte_stream_fills_the_reserved_stage_before_commit() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let destination = tmp.path().join("blob.bin");
    let stream = futures_util::stream::iter([
        Ok::<_, &'static str>(Bytes::from_static(b"verified ")),
        Ok(Bytes::from_static(b"bytes")),
    ]);
    let staged = AtomicStagedFile::create(&destination)
        .await
        .expect("reserve staging file");

    let (staged, written) = staged
        .write_byte_stream(Box::pin(stream))
        .await
        .expect("fill reserved staging file");
    assert_eq!(written, b"verified bytes".len() as u64);
    assert!(!destination.exists());

    staged.commit().await.expect("publish verified stage");
    assert_eq!(
        read(&destination).await.expect("read destination"),
        b"verified bytes"
    );
    assert!(temp_entries(tmp.path()).await.is_empty());
}

#[tokio::test]
async fn staged_file_publish_rolls_back_when_directory_sync_fails() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let destination = tmp.path().join("blob.bin");
    let mut staged = AtomicStagedFile::create(&destination)
        .await
        .expect("allocate staging path");
    staged
        .write_bytes(b"verified")
        .await
        .expect("write staged file");
    let error = staged
        .commit_with_sync(|_| async { Err("injected directory sync failure".to_string()) })
        .await
        .expect_err("directory sync failure must reject publication");

    assert_eq!(error, "injected directory sync failure");
    assert!(!destination.exists());
    assert!(temp_entries(tmp.path()).await.is_empty());
}

#[tokio::test]
async fn staged_new_file_refuses_to_replace_an_existing_user_file() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let destination = tmp.path().join("blob.bin");
    AtomicStagedFile::write_for_test(&destination, b"user file")
        .await
        .expect("seed user destination");
    let mut staged = AtomicStagedFile::create(&destination)
        .await
        .expect("allocate staging path");
    staged
        .write_bytes(b"downloaded")
        .await
        .expect("write verified staged file");

    assert_eq!(
        staged.commit_new().await,
        Err(CommitNewFileError::DestinationExists(destination.clone()))
    );
    assert_eq!(read(&destination).await.unwrap(), b"user file");
    assert!(temp_entries(tmp.path()).await.is_empty());
}

#[tokio::test]
async fn staged_new_file_publishes_complete_verified_bytes() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let destination = tmp.path().join("blob.bin");
    let mut staged = AtomicStagedFile::create(&destination)
        .await
        .expect("allocate staging path");
    staged
        .write_bytes(b"downloaded")
        .await
        .expect("write verified staged file");

    staged.commit_new().await.expect("publish new user file");

    assert_eq!(read(&destination).await.unwrap(), b"downloaded");
    assert!(temp_entries(tmp.path()).await.is_empty());
}

#[tokio::test]
async fn staged_new_file_rolls_back_when_final_directory_sync_fails() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let destination = tmp.path().join("blob.bin");
    let mut staged = AtomicStagedFile::create(&destination)
        .await
        .expect("allocate staging path");
    staged
        .write_bytes(b"downloaded")
        .await
        .expect("write verified staged file");
    let staged_path = staged.path().to_path_buf();
    let sync_count = Arc::new(AtomicUsize::new(0));
    let sync_count_for_call = sync_count.clone();

    let error = staged
        .commit_new_with_sync(move |_| {
            let invocation = sync_count_for_call.fetch_add(1, Ordering::SeqCst);
            async move {
                if invocation == 1 {
                    Err("injected final directory sync failure".to_string())
                } else {
                    Ok(())
                }
            }
        })
        .await
        .expect_err("final directory sync failure must be reported");

    assert_eq!(
        error,
        CommitNewFileError::Filesystem("injected final directory sync failure".to_string())
    );
    assert_eq!(sync_count.load(Ordering::SeqCst), 2);
    assert!(!destination.exists());
    assert!(!staged_path.exists());
}

#[tokio::test]
async fn byte_stream_failure_preserves_destination_and_removes_temp() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("streamed.bin");
    AtomicStagedFile::write_for_test(&path, b"committed")
        .await
        .expect("seed destination");
    let stream =
        futures_util::stream::iter([Ok(Bytes::from_static(b"partial")), Err("source failed")]);

    let staged = AtomicStagedFile::create(&path)
        .await
        .expect("reserve destination stage");
    let error = match staged.write_byte_stream(Box::pin(stream)).await {
        Ok(_) => panic!("source failure must reject the staged write"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        ByteStreamWriteError::Source("source failed")
    ));
    assert_eq!(read(&path).await.expect("read destination"), b"committed");
    assert!(temp_entries(tmp.path()).await.is_empty());
}

#[tokio::test]
async fn canceled_byte_stream_preserves_destination_and_removes_temp() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("streamed.bin");
    AtomicStagedFile::write_for_test(&path, b"committed")
        .await
        .expect("seed destination");
    let first_yielded = Arc::new(tokio::sync::Notify::new());
    let first_yielded_for_stream = first_yielded.clone();
    let stream = futures_util::stream::once(async move {
        first_yielded_for_stream.notify_one();
        Ok::<Bytes, &'static str>(Bytes::from_static(b"partial"))
    })
    .chain(futures_util::stream::pending());
    let write_path = path.clone();
    let write = tokio::spawn(async move {
        let staged = AtomicStagedFile::create(&write_path)
            .await
            .map_err(ByteStreamWriteError::Local)?;
        staged.write_byte_stream(Box::pin(stream)).await
    });
    first_yielded.notified().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let temps = temp_entries(tmp.path()).await;
            if temps.iter().any(|temp| {
                std::fs::metadata(temp)
                    .is_ok_and(|metadata| metadata.len() == b"partial".len() as u64)
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("partial temp file was written");

    write.abort();
    let cancellation = match write.await {
        Ok(_) => panic!("write task must be canceled"),
        Err(error) => error,
    };
    assert!(cancellation.is_cancelled());

    assert_eq!(read(&path).await.expect("read destination"), b"committed");
    assert!(temp_entries(tmp.path()).await.is_empty());
}

#[tokio::test]
async fn absent_atomic_temp_cleanup_succeeds() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let temp = AtomicTempFile::create_in(tmp.path()).expect("create atomic temp");
    let path = temp.path.clone();
    tokio::fs::remove_file(path)
        .await
        .expect("remove atomic temp before cleanup");

    temp.cleanup()
        .await
        .expect("an already-absent atomic temp is clean");
}

#[tokio::test]
async fn failed_atomic_temp_cleanup_reports_the_remaining_target() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let temp = AtomicTempFile::create_in(tmp.path()).expect("create atomic temp");
    let path = temp.path.clone();
    tokio::fs::remove_file(&path)
        .await
        .expect("remove atomic temp");
    tokio::fs::create_dir(&path)
        .await
        .expect("create cleanup obstruction");

    let error = temp
        .cleanup()
        .await
        .expect_err("an unremovable atomic temp must fail cleanup");

    assert!(
        error.contains("remove temporary blob") && path.exists(),
        "{error}"
    );
    tokio::fs::remove_dir(path)
        .await
        .expect("remove cleanup obstruction");
}

#[tokio::test]
async fn write_atomic_durable_leaves_a_readable_file() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().join("nested").join("upload_staging.bin");
    let bytes = b"packed outgoing changeset".to_vec();

    AtomicStagedFile::write_for_test(&path, &bytes)
        .await
        .expect("durable write");

    assert_eq!(read(&path).await.expect("read back"), bytes);
}
