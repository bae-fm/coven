//! A cached copy that fails its check (§17.8): damage on this device, which
//! says nothing about the stored file, so the stored bytes are fetched and
//! written back over it.
use super::*;
use coven_format::file::FILE_HEADER_LEN;

/// Change the bytes of the cache file of `size` bytes, as a disk might.
fn damage(f: &Fixture, size: usize, change: impl FnOnce(&mut Vec<u8>)) {
    let folder = f.directory.area_path(coven_foundation::files::FileArea::Cache);
    let path = std::fs::read_dir(folder)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| std::fs::metadata(path).unwrap().len() == size as u64)
        .expect("a cache file of that size");
    let mut bytes = std::fs::read(&path).unwrap();
    change(&mut bytes);
    std::fs::write(&path, bytes).unwrap();
}

/// A byte of the cached header or chunk changes on disk, in the separate
/// entries a read keeps or in a pin's whole copy. The next read or pin of
/// the file fetches those bytes from storage, records nothing, and writes
/// them back, so a pinned file stays pinned and a later read asks storage
/// for nothing. Refused instead, every later read and pin would fail on a
/// file storage holds intact.
#[tokio::test]
async fn a_damaged_cached_copy_is_fetched_and_written_back_not_refused() {
    let sealed = CHUNK + 16;
    // Whether the copy is pinned, the damaged cache file's size and byte,
    // and whether a pin rather than a read finds it.
    let cases = [
        (false, FILE_HEADER_LEN, 3, false),
        (false, sealed, 3, false),
        (false, sealed, 3, true),
        (true, FILE_HEADER_LEN + sealed, 3, false),
        (true, FILE_HEADER_LEN + sealed, FILE_HEADER_LEN + 3, false),
    ];
    for (pinned, size, at, pin) in cases {
        let f = Fixture::new(Provenance::AppProvided).await;
        let file = f.uploaded("damaged", vec![20; CHUNK]).await;
        if pinned {
            f.files.pin(std::slice::from_ref(&file)).await.unwrap();
        } else {
            assert_eq!(f.files.read_file(&file).await.unwrap(), vec![20; CHUNK]);
        }
        damage(&f, size, |bytes| bytes[at] ^= 1);
        let requests = f.storage.request_count();
        if pin {
            f.files.pin(std::slice::from_ref(&file)).await.unwrap();
        } else {
            assert_eq!(f.files.read_file(&file).await.unwrap(), vec![20; CHUNK]);
        }
        assert!(f.storage.request_count() > requests, "the damaged bytes are fetched");
        assert!(f.database.pending::<crate::PendingRecord>().await.unwrap().is_empty());
        let id = file.id();
        assert_eq!(f.database.files_pinned(vec![id]).await.unwrap(), vec![Some(pinned || pin)]);
        let requests = f.storage.request_count();
        assert_eq!(f.files.read_file(&file).await.unwrap(), vec![20; CHUNK]);
        assert_eq!(f.storage.request_count(), requests, "the copy holds the stored bytes again");
        f.close().await;
    }
}

/// A cache file loses its end, or gains bytes past it, on disk: in a read's
/// chunk entry or header entry, or in a pin's whole copy. It takes back the
/// size its entry records, and the next read fetches what it lost from
/// storage, records nothing, and writes it back, so a pinned file stays
/// pinned and a later read asks storage for nothing. Taken as a damaged
/// database instead, every later read failed.
#[tokio::test]
async fn a_cache_file_of_another_size_is_fetched_and_written_back() {
    let sealed = CHUNK + 16;
    // Whether the copy is pinned, the cache file's size, and its new size.
    let cases = [
        (false, FILE_HEADER_LEN, 5),
        (false, sealed, 100),
        (false, sealed, sealed + 7),
        (true, FILE_HEADER_LEN + sealed, FILE_HEADER_LEN + 100),
        (true, FILE_HEADER_LEN + sealed, 0),
    ];
    for (pinned, size, length) in cases {
        let f = Fixture::new(Provenance::AppProvided).await;
        let file = f.uploaded("shortened", vec![20; CHUNK]).await;
        if pinned {
            f.files.pin(std::slice::from_ref(&file)).await.unwrap();
        } else {
            assert_eq!(f.files.read_file(&file).await.unwrap(), vec![20; CHUNK]);
        }
        damage(&f, size, |bytes| bytes.resize(length, 9));
        let requests = f.storage.request_count();
        assert_eq!(f.files.read_file(&file).await.unwrap(), vec![20; CHUNK]);
        // Bytes past the end are no chunk's: a copy that only gained some
        // lost nothing to fetch.
        assert_eq!(f.storage.request_count() > requests, length < size, "what it lost is fetched");
        assert!(f.database.pending::<crate::PendingRecord>().await.unwrap().is_empty());
        let id = file.id();
        assert_eq!(f.database.files_pinned(vec![id]).await.unwrap(), vec![Some(pinned)]);
        let requests = f.storage.request_count();
        assert_eq!(f.files.read_file(&file).await.unwrap(), vec![20; CHUNK]);
        assert_eq!(f.storage.request_count(), requests, "the copy holds the stored bytes again");
        f.close().await;
    }
}
