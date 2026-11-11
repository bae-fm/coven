//! A prepare that stops keeps the bytes it has downloaded, and the next
//! prepare of the file continues from them (§17.8).

use super::*;
use coven_storage::test_utils::MemoryRequest;

/// The size of a request to storage, which a download moves in steps of.
const REQUEST: usize = 1024 * 1024;

/// Twenty chunks take two requests of up to 1 MiB.
fn album() -> Vec<u8> {
    (0..CHUNK * 20).map(|i| (i % 251) as u8).collect()
}

/// The cache file of the partial download kept for `file`, and how many
/// bytes of it are kept.
async fn partial(f: &Fixture, file: &FileRef) -> Option<(std::path::PathBuf, u64)> {
    let (name, kept) = f.files.inner.database.pin_download_kept(file).await.unwrap()?;
    Some((
        f.directory.area_path(coven_foundation::files::FileArea::Cache).join(name.as_str()),
        kept,
    ))
}

/// The ranges storage was asked for since `from` requests, of `file`.
fn reads_since(f: &Fixture, file: &FileRef, from: usize) -> Vec<MemoryRequest> {
    f.storage
        .request_trace()
        .into_iter()
        .skip(from)
        .filter(|request| matches!(request, MemoryRequest::Read(path, _) if *path == f.path(file)))
        .collect()
}

/// Prepare `file`'s pin and stop it while its second request is held, so the
/// first request's bytes are all that was downloaded.
async fn stopped_partway(f: &Fixture, file: &FileRef) {
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_body_at(f.path(file), REQUEST as u64 + 1, observed, resumed).await;
    let prepare = f.files.prepare_pin(std::slice::from_ref(file));
    tokio::pin!(prepare);
    tokio::select! {
        result = &mut prepare => panic!("the download was not held: {result:?}"),
        held = held => held.unwrap(),
    }
    // Returning drops the prepare, with its download held.
    drop(resume);
}

/// Ben's app stops preparing an album's pin partway; preparing it again
/// fetches only the rest, and the pin commits the whole file.
#[tokio::test]
async fn a_stopped_prepare_continues_from_the_bytes_it_kept() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes = album();
    let file = f.uploaded("album", bytes.clone()).await;
    f.files.stop_background_for_counts().await;
    stopped_partway(&f, &file).await;
    let (_, kept) = partial(&f, &file).await.expect("the stopped prepare kept its bytes");
    assert!(kept > REQUEST as u64 / 2, "the first request's bytes are kept: {kept}");

    let before = f.storage.request_trace().len();
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(
        reads_since(&f, &file, before).len(),
        1,
        "only the rest is fetched: {:?}",
        reads_since(&f, &file, before)
    );
    assert!(partial(&f, &file).await.is_none(), "the pin made the partial the copy");
    assert_eq!(f.database.files_pinned(vec![file.id()]).await.unwrap(), vec![Some(true)]);
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    f.close().await;
}

/// A partial whose bytes changed on this device is deleted when the next
/// prepare checks it, and the file downloads again from the start.
#[tokio::test]
async fn a_partial_that_no_longer_matches_is_deleted_and_the_file_restarts() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes = album();
    let file = f.uploaded("album", bytes.clone()).await;
    f.files.stop_background_for_counts().await;
    stopped_partway(&f, &file).await;
    let (path, _) = partial(&f, &file).await.unwrap();
    let mut kept = std::fs::read(&path).unwrap();
    kept[coven_format::file::FILE_HEADER_LEN + 5] ^= 0xff;
    std::fs::write(&path, kept).unwrap();

    let before = f.storage.request_trace().len();
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(reads_since(&f, &file, before).len(), 2, "the whole file is fetched again");
    assert!(!path.exists(), "the damaged partial is deleted");
    assert!(partial(&f, &file).await.is_none());
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    f.close().await;
}

/// The app unpinning a file ends the partial download its pin waited on, and
/// its bytes go.
#[tokio::test]
async fn unpinning_a_file_deletes_its_partial_download() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("album", album()).await;
    f.files.stop_background_for_counts().await;
    stopped_partway(&f, &file).await;
    let (path, _) = partial(&f, &file).await.unwrap();
    let id = file.id();
    f.database.write(move |sql| sql.unpin(&[id])).await.unwrap();
    assert!(partial(&f, &file).await.is_none());
    assert!(!path.exists());
    f.close().await;
}

/// An unpin while a prepare of the file is downloading removes its partial
/// download and ends the prepare with `PinChanged`, recording nothing, so
/// nothing of the file stays on the device (§17.8).
#[tokio::test]
async fn an_unpin_during_a_prepare_fails_it_and_leaves_nothing() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("album", album()).await;
    f.files.stop_background_for_counts().await;
    let id = file.id();
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_body_at(f.path(&file), REQUEST as u64 + 1, observed, resumed).await;
    let mut preparing = Box::pin(f.files.prepare_pin(std::slice::from_ref(&file)));
    tokio::select! {
        result = &mut preparing => panic!("the download was not held: {:?}", result.err()),
        held = held => held.unwrap(),
    }
    assert!(partial(&f, &file).await.is_some());
    f.database.write(move |sql| sql.unpin(&[id])).await.unwrap();
    assert!(partial(&f, &file).await.is_none());
    resume.send(()).unwrap();
    let refused = preparing.await.err().expect("the prepare fails");
    assert!(
        matches!(refused, FileReadError::Database(DbError::PinChanged { file }) if file == id),
        "{refused:?}"
    );
    assert!(partial(&f, &file).await.is_none());
    assert_eq!(f.database.files_pinned(vec![id]).await.unwrap(), vec![Some(false)]);
    f.close().await;
}

/// Clearing a namespace's cache leaves the partial download a stopped pin
/// kept, and the next prepare continues from it (§17.8).
#[tokio::test]
async fn clearing_the_cache_leaves_a_partial_download() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("album", album()).await;
    f.files.stop_background_for_counts().await;
    stopped_partway(&f, &file).await;
    let (path, kept) = partial(&f, &file).await.unwrap();
    f.database.write(|sql| sql.clear_cache("files")).await.unwrap();
    let (still, size) = partial(&f, &file).await.expect("the partial stays");
    assert_eq!((still, size), (path.clone(), kept));
    assert!(path.exists());
    let before = f.storage.request_trace().len();
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    // The cleared header is read again, and the rest follows the kept bytes.
    let reads = reads_since(&f, &file, before);
    let starts: Vec<u64> = reads
        .iter()
        .map(|read| match read {
            MemoryRequest::Read(_, Some(range)) => range.start(),
            other => panic!("a ranged read: {other:?}"),
        })
        .collect();
    assert_eq!(starts, [0, kept]);
    f.close().await;
}

/// Once no record here names a file's stored bytes, the partial download of
/// them goes, as the cached copies do.
#[tokio::test]
async fn a_partial_download_goes_with_the_last_record_naming_its_bytes() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("album", album()).await;
    f.files.stop_background_for_counts().await;
    stopped_partway(&f, &file).await;
    let (path, _) = partial(&f, &file).await.unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='album'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(partial(&f, &file).await.is_none());
    assert!(!path.exists());
    f.close().await;
}
