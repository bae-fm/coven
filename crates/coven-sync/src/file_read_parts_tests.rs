//! On a provider whose ranged read moves whole parts, as CloudKit's does, an
//! open file fetches each part it needs once and reads the ranges inside it
//! from that one fetch (§17.7, Appendix E.1).

use super::*;
use coven_foundation::files::FileLease;
use std::num::NonZeroUsize;

/// CloudKit's part: each asset holds up to 8 MiB of an object.
const PART: usize = 8 * 1024 * 1024;

/// The deterministic provider standing in for CloudKit: objects past a part
/// go up in parts, and a read moves every part it touches whole.
async fn cloudkit() -> Fixture {
    let clock: ClockRef =
        Arc::new(ClosureClock(|| SystemTime::UNIX_EPOCH + Duration::from_secs(100)));
    let storage = MemoryStorage::builder()
        .provider(coven_storage::CloudProvider::CloudKit)
        .clock(clock.clone())
        .transfer_limits(PART as u64, PART)
        .reads_whole_parts();
    Fixture::with_storage(Provenance::AppProvided, clock, storage).await
}

fn downloads(f: &Fixture, at_once: usize) {
    f.files.set_transfer_limits(TransferLimits {
        uploads: NonZeroUsize::MIN,
        downloads: NonZeroUsize::new(at_once).unwrap(),
    });
}

fn content(length: usize) -> Vec<u8> {
    (0..length).map(|i| (i % 251) as u8).collect()
}

/// The encrypted object's size, which each part's bytes add up to once.
fn stored_size(file: &FileRef) -> u64 {
    coven_format::file::FileHeader::new(file.size()).encrypted_size().unwrap()
}

/// Ben pins a 20 MiB video and a 3 MiB song on iCloud, four requests at a
/// time: each of the video's three parts, and the song's record, which holds
/// it whole, moves once, though the pin asks for them in 1 MiB requests.
#[tokio::test]
async fn a_pin_moves_each_part_once() {
    let f = cloudkit().await;
    let video = f.uploaded("video", content(20 * 1024 * 1024)).await;
    let song = f.uploaded("song", content(3 * 1024 * 1024)).await;
    f.files.stop_background_for_counts().await;
    downloads(&f, 4);
    for file in [&video, &song] {
        let before = f.storage.moved_bytes(&f.path(file)).await;
        f.files.pin(std::slice::from_ref(file)).await.unwrap();
        let moved = f.storage.moved_bytes(&f.path(file)).await - before;
        assert_eq!(moved, stored_size(file), "{} moved each part once", file.id());
    }
    assert_eq!(
        f.files.read_file(&video, &FileLease::for_test().await).await.unwrap(),
        content(20 * 1024 * 1024)
    );
    f.close().await;
}

/// Ana's app reads the whole video, which coven fetches in requests of up
/// to 1 MiB: each part moves once.
#[tokio::test]
async fn a_whole_read_moves_each_part_once() {
    let f = cloudkit().await;
    let bytes = content(20 * 1024 * 1024);
    let video = f.uploaded("video", bytes.clone()).await;
    f.files.stop_background_for_counts().await;
    let before = f.storage.moved_bytes(&f.path(&video)).await;
    assert_eq!(f.files.read_file(&video, &FileLease::for_test().await).await.unwrap(), bytes);
    let moved = f.storage.moved_bytes(&f.path(&video)).await - before;
    assert_eq!(moved, stored_size(&video), "each part moved once");
    f.close().await;
}

/// A byte of the video's second part changes on the way to Ana's phone. The
/// check that fails asks storage again, not the part her open file holds,
/// so the read fails as a read rather than a refusal (§19.1), and the part
/// that second fetch brought is the one her next read of it uses.
#[tokio::test]
async fn a_part_changed_on_the_way_is_fetched_again_from_storage() {
    use coven_storage::test_utils::{Faults, Garble};
    let f = cloudkit().await;
    let bytes = content(20 * 1024 * 1024);
    let video = f.uploaded("video", bytes.clone()).await;
    f.files.stop_background_for_counts().await;
    let garbled = [(f.path(&video), Garble { at: PART as u64 + 200, reads: 1 })].into();
    f.storage.set_faults(Faults { garbled_reads: garbled, ..Faults::none() }).await;
    let stream = f.files.open_file_stream(&video, &FileLease::for_test().await).await.unwrap();
    // The chunk that reaches across the first part's end into the byte.
    let (offset, length) = (127 * 64 * 1024, 64 * 1024);
    let result = stream.read_at(offset, length).await;
    assert!(
        matches!(&result, Err(FileReadError::Storage(error))
            if error.failure() == coven_storage::StorageFailure::Corrupted),
        "{result:?}"
    );
    let read = stream.read_at(offset, length).await.unwrap();
    assert_eq!(read, bytes[offset as usize..(offset + length) as usize]);
    drop(stream);
    f.close().await;
}
