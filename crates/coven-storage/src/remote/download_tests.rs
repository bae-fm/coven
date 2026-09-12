//! Downloading one stored blob: the framing opens the provider's body as it
//! arrives, and the stage is handed back only after the source has ended with
//! the exact object the reference names.

use super::blob_io::*;
use super::tests::{ephemeral_stage, publish_sealed_blob, ramp, small_chunking};
use super::*;
use crate::cloud::test_utils::InMemoryCloudHome;
use coven_keys::encryption::TAG_SIZE;
use coven_protocol::blob::locator::{BlobLocator, StoredBlobRef};
use coven_protocol::objects::{BlobSpoolProtection, BlobWriteAuthority};
use coven_protocol::store_commit::ObjectHash;

/// Publish one blob into a browsable home, where the object holds the
/// plaintext in the clear and only the row's content hash can refuse it.
async fn publish_browsable_blob(
    home: &InMemoryCloudHome,
    store_id: &str,
    blob_id: &str,
    plaintext: &[u8],
) -> (CloudSyncConnection, StoredBlobRef, tempfile::TempDir) {
    let storage = CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        store_id,
        UserKeypair::generate(),
    );
    let registration = storage.blob_write_registration(store_id).await;
    let authority = BlobWriteAuthority::new(&registration);
    let locator = BlobLocator::browsable(
        "audio",
        blob_id,
        registration.reference().clone(),
        "Artist/Album/track.flac",
        plaintext.len() as u64,
        ObjectHash::digest(plaintext),
    )
    .expect("build browsable locator");
    let temp = tempfile::tempdir().expect("temporary blob directory");
    let source = temp.path().join("plaintext");
    let spool = temp.path().join("spool");
    tokio::fs::write(&source, plaintext)
        .await
        .expect("write plaintext source");
    storage
        .seal_blob_to_spool(
            &locator,
            &authority,
            BlobSpoolProtection::Browsable,
            &source,
            ephemeral_stage(&spool).await,
            crate::cloud::no_preparation_progress(),
        )
        .await
        .expect("stage the browsable spool");
    let slot = storage
        .allocate_blob_slot(&locator, &authority)
        .await
        .expect("allocate exact blob slot");
    let blob = storage
        .prepare_blob_object(&locator, &authority, slot, &spool)
        .await
        .expect("prepare exact blob");
    storage
        .create_blob_object_from_file(
            &blob,
            &authority,
            &spool,
            &crate::cloud::UploadControl::running(crate::cloud::no_progress()),
        )
        .await
        .expect("create exact blob");
    (storage, blob, temp)
}

/// A directory that holds only the download under test, so "no destination and
/// no temp sibling" is one assertion over its entries.
async fn empty_destination_dir(temp: &tempfile::TempDir) -> std::path::PathBuf {
    let directory = temp.path().join("materialized");
    tokio::fs::create_dir_all(&directory)
        .await
        .expect("create destination directory");
    directory
}

async fn directory_entries(directory: &std::path::Path) -> Vec<String> {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .expect("read destination directory");
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await.expect("read destination entry") {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    names
}

/// The refusal a download must produce. An unexpected success drops its stage
/// on the spot, so the directory assertions that follow see the same state
/// either way.
async fn refused_download(
    storage: &CloudSyncConnection,
    blob: &StoredBlobRef,
    protection: BlobSpoolProtection,
    destination: &std::path::Path,
    reason: &str,
) -> StorageError {
    match download(storage, blob, protection, destination).await {
        Ok(_) => panic!("{reason}: the download must be refused"),
        Err(error) => error,
    }
}

async fn download(
    storage: &CloudSyncConnection,
    blob: &StoredBlobRef,
    protection: BlobSpoolProtection,
    destination: &std::path::Path,
) -> Result<coven_foundation::local_file::AtomicStagedFile, StorageError> {
    let stage = ephemeral_stage(destination).await;
    storage
        .stage_verified_blob_plaintext(
            blob,
            protection,
            stage,
            crate::cloud::no_download_progress(),
        )
        .await
}

#[tokio::test]
async fn exact_blob_plaintext_is_published_only_after_both_verifications() {
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(150_000);
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "verified-blob-download",
        "verified-track",
        &plaintext,
        BlobChunking::DEFAULT,
    )
    .await;
    let destination = empty_destination_dir(&temp).await.join("blob");

    let staged = download(
        &storage,
        &blob,
        BlobSpoolProtection::Opaque(audience_key),
        &destination,
    )
    .await
    .expect("stage verified plaintext");
    assert!(!destination.exists());
    assert_eq!(tokio::fs::read(staged.path()).await.unwrap(), plaintext);
    staged.commit().await.expect("publish verified plaintext");
    assert_eq!(tokio::fs::read(&destination).await.unwrap(), plaintext);
}

/// A blob with no plaintext still has one chunk: its tag. That chunk is read
/// and authenticated like any other, so an empty blob is proven, not assumed.
#[tokio::test]
async fn an_empty_opaque_blob_downloads_and_verifies() {
    let home = InMemoryCloudHome::new();
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "empty-blob-download",
        "empty-track",
        &[],
        BlobChunking::DEFAULT,
    )
    .await;
    assert_eq!(
        blob.object().stored_size(),
        (KeyTag::LEN + SEALED_BLOB_HEADER_LEN + TAG_SIZE) as u64
    );
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    let staged = download(
        &storage,
        &blob,
        BlobSpoolProtection::Opaque(audience_key),
        &destination,
    )
    .await
    .expect("stage the empty blob");
    assert!(tokio::fs::read(staged.path()).await.unwrap().is_empty());
    staged.commit().await.expect("publish the empty blob");
    assert!(tokio::fs::read(&destination).await.unwrap().is_empty());
}

/// The provider chooses its buffer sizes, and they have nothing to do with the
/// framing: the key tag, the header and each sealed chunk can begin and end
/// anywhere inside a received buffer. So the reader is driven at two buffer
/// sizes that both straddle those boundaries — one smaller than every framing
/// span, so each exact read loops over several buffers, and one wider than a
/// sealed chunk, so a single buffer crosses a chunk boundary and its tail is
/// what begins the next chunk.
#[tokio::test]
async fn a_download_verifies_across_provider_buffer_boundaries() {
    const CHUNK: u32 = 4096;
    /// Divides no framing span and lands on no framing boundary, so every one
    /// of them falls inside a buffer.
    const NARROW_BUFFER: usize = 17;

    let home = InMemoryCloudHome::new();
    let plaintext = ramp(CHUNK as usize * 3 + 1024);
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "buffered-blob-download",
        "buffered-track",
        &plaintext,
        small_chunking(CHUNK),
    )
    .await;
    let sealed_chunk = CHUNK as usize + TAG_SIZE;
    let stored_size = blob.object().stored_size();
    assert_eq!(
        stored_size as usize,
        KeyTag::LEN + SEALED_BLOB_HEADER_LEN + sealed_chunk * 3 + 1024 + TAG_SIZE,
        "four sealed chunks behind the key tag and header",
    );
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    // One buffer wider than a sealed chunk still leaves every boundary inside
    // a buffer, because the prefix offsets it.
    for buffer in [NARROW_BUFFER, sealed_chunk + 5] {
        home.stream_exact_reads_in_chunks(buffer, std::time::Duration::ZERO);
        let staged = download(
            &storage,
            &blob,
            BlobSpoolProtection::Opaque(audience_key.clone()),
            &destination,
        )
        .await
        .unwrap_or_else(|error| panic!("download in {buffer}-byte buffers: {error}"));
        assert_eq!(
            tokio::fs::read(staged.path()).await.unwrap(),
            plaintext,
            "download in {buffer}-byte buffers"
        );
        drop(staged);
    }

    // One byte short of the last sealed chunk's end: every earlier chunk has
    // opened and still nothing is handed over. The failure keeps the
    // provider's own classification — a cut body is a transport fault a later
    // attempt may clear, not content this device must refuse forever.
    home.stream_exact_reads_in_chunks(NARROW_BUFFER, std::time::Duration::ZERO);
    home.fail_exact_stream_read_after_bytes(stored_size - 1);
    let error = refused_download(
        &storage,
        &blob,
        BlobSpoolProtection::Opaque(audience_key),
        &destination,
        "a body one byte short of its last chunk must be refused",
    )
    .await;
    assert!(
        matches!(
            error.backend_failure(),
            Some(coven_protocol::objects::StorageBackendFailure::Transport)
        ),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("armed exact stream-read failure"),
        "{error}"
    );
    assert!(!destination.exists());
    assert_eq!(directory_entries(&directory).await, Vec::<String>::new());
}

/// A body that stops anywhere inside the framing is refused wherever the cut
/// falls — the key tag, the header, a chunk, or a whole missing chunk — and
/// leaves nothing behind.
#[tokio::test]
async fn a_body_truncated_at_any_framing_boundary_is_refused() {
    const CHUNK: u32 = 4096;
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(CHUNK as usize * 2 + 1024);
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "truncated-blob-download",
        "truncated-track",
        &plaintext,
        small_chunking(CHUNK),
    )
    .await;
    let slot = blob.object().slot();
    let whole = home
        .stored_exact_bytes(slot)
        .expect("the published object is stored");
    let prefix = (KeyTag::LEN + SEALED_BLOB_HEADER_LEN) as u64;
    let sealed_chunk = u64::from(CHUNK) + TAG_SIZE as u64;
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    for (boundary, cut) in [
        ("inside the key tag", (KeyTag::LEN / 2) as u64),
        ("inside the header", prefix - 1),
        ("inside the first chunk", prefix + 5),
        ("after the last full chunk", prefix + sealed_chunk * 2),
    ] {
        home.truncate_exact_object(slot, cut as usize);
        let error = refused_download(
            &storage,
            &blob,
            BlobSpoolProtection::Opaque(audience_key.clone()),
            &destination,
            boundary,
        )
        .await;
        assert!(
            matches!(error, StorageError::InvalidContent(_)),
            "{boundary}: {error}"
        );
        assert!(!destination.exists(), "{boundary}");
        assert_eq!(
            directory_entries(&directory).await,
            Vec::<String>::new(),
            "{boundary}"
        );
        home.replace_exact_object(slot, whole.clone());
    }
}

/// A browsable home stores the plaintext in the clear, so a body that stops
/// short has no tag to fail — the declared plaintext simply never arrives.
#[tokio::test]
async fn a_truncated_browsable_body_is_refused() {
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(4096);
    let (storage, blob, temp) =
        publish_browsable_blob(&home, "truncated-browsable", "readable-track", &plaintext).await;
    home.truncate_exact_object(blob.object().slot(), plaintext.len() - 7);
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    let error = refused_download(
        &storage,
        &blob,
        BlobSpoolProtection::Browsable,
        &destination,
        "a short browsable body must be refused",
    )
    .await;
    assert!(matches!(error, StorageError::InvalidContent(_)), "{error}");
    assert!(!destination.exists());
    assert_eq!(directory_entries(&directory).await, Vec::<String>::new());
}

/// One flipped byte inside a sealed chunk fails that chunk's tag. Nothing the
/// provider can serve opens as this blob's plaintext.
#[tokio::test]
async fn an_invalid_chunk_tag_is_refused() {
    const CHUNK: u32 = 4096;
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(CHUNK as usize * 2);
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "tampered-blob-download",
        "tampered-track",
        &plaintext,
        small_chunking(CHUNK),
    )
    .await;
    let slot = blob.object().slot();
    let mut stored = home
        .stored_exact_bytes(slot)
        .expect("the published object is stored");
    stored[KeyTag::LEN + SEALED_BLOB_HEADER_LEN + 3] ^= 0x01;
    home.replace_exact_object(slot, stored);
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    let error = refused_download(
        &storage,
        &blob,
        BlobSpoolProtection::Opaque(audience_key),
        &destination,
        "a flipped sealed byte must be refused",
    )
    .await;
    assert!(matches!(error, StorageError::Decryption { .. }), "{error}");
    assert!(!destination.exists());
    assert_eq!(directory_entries(&directory).await, Vec::<String>::new());
}

/// The framing says where the chunks end; the source ending is what says the
/// object did. Anything after the last chunk is a different object.
#[tokio::test]
async fn trailing_bytes_after_the_last_chunk_are_refused() {
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(8192);
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "trailing-blob-download",
        "trailing-track",
        &plaintext,
        BlobChunking::DEFAULT,
    )
    .await;
    home.append_exact_object_bytes(blob.object().slot(), b"extra");
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    let error = refused_download(
        &storage,
        &blob,
        BlobSpoolProtection::Opaque(audience_key),
        &destination,
        "bytes after the framing must be refused",
    )
    .await;
    assert!(
        error.to_string().contains("stored body has trailing bytes"),
        "{error}"
    );
    assert!(matches!(error, StorageError::InvalidContent(_)), "{error}");
    assert!(!destination.exists());
    assert_eq!(directory_entries(&directory).await, Vec::<String>::new());
}

/// A browsable body whose bytes hash to something other than what the
/// reference names is refused even though its plaintext matches the row: the
/// stored object is not the one the reference pins.
#[tokio::test]
async fn a_readable_body_with_the_wrong_stored_identity_is_refused() {
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(4096);
    let (storage, blob, temp) =
        publish_browsable_blob(&home, "mismatched-identity", "readable-track", &plaintext).await;
    let misnamed = StoredBlobRef::new(
        blob.locator().clone(),
        ExactObjectRef::new(
            blob.object().slot().clone(),
            blob.object().stored_size(),
            ObjectHash::digest(b"a different object"),
        ),
    )
    .expect("build a reference naming different stored bytes");
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    let error = refused_download(
        &storage,
        &misnamed,
        BlobSpoolProtection::Browsable,
        &destination,
        "a body that is not the named object must be refused",
    )
    .await;
    assert!(error.to_string().contains("its reference names"), "{error}");
    assert!(matches!(error, StorageError::InvalidContent(_)), "{error}");
    assert!(!destination.exists());
    assert_eq!(directory_entries(&directory).await, Vec::<String>::new());
}

/// Every plaintext byte has been handed out and the framing is complete, and
/// the download still fails: completion is the source ending, and this source
/// ended with an error.
#[tokio::test]
async fn a_provider_error_after_the_last_plaintext_byte_fails_the_download() {
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(8192);
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "late-failure-download",
        "late-failure-track",
        &plaintext,
        BlobChunking::DEFAULT,
    )
    .await;
    home.fail_exact_stream_read_after_bytes(blob.object().stored_size());
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    let error = refused_download(
        &storage,
        &blob,
        BlobSpoolProtection::Opaque(audience_key),
        &destination,
        "a source that ends with an error has not delivered the object",
    )
    .await;
    assert!(
        error
            .to_string()
            .contains("armed exact stream-read failure"),
        "{error}"
    );
    assert!(!destination.exists());
    assert_eq!(directory_entries(&directory).await, Vec::<String>::new());
}

/// Dropping the download drops the stream and the stage together, which is the
/// whole cancellation path: there is no half-written file to find.
#[tokio::test]
async fn cancelling_a_download_leaves_no_files() {
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(8192);
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "cancelled-download",
        "cancelled-track",
        &plaintext,
        BlobChunking::DEFAULT,
    )
    .await;
    // A barrier wider than the one download in flight parks the body where a
    // caller can drop it.
    home.arm_exact_stream_read_concurrency_probe(2);
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");
    let stage = ephemeral_stage(&destination).await;

    let cancelled = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        storage.stage_verified_blob_plaintext(
            &blob,
            BlobSpoolProtection::Opaque(audience_key),
            stage,
            crate::cloud::no_download_progress(),
        ),
    )
    .await;

    assert!(cancelled.is_err(), "the download parks at the barrier");
    assert!(!destination.exists());
    assert_eq!(directory_entries(&directory).await, Vec::<String>::new());
}

#[tokio::test]
async fn stored_blob_corruption_never_creates_a_plaintext_stage() {
    let home = InMemoryCloudHome::new();
    let (storage, blob, audience_key, temp) = publish_sealed_blob(
        &home,
        "corrupt-blob-download",
        "corrupt-cover",
        b"signed blob plaintext",
        BlobChunking::DEFAULT,
    )
    .await;
    home.replace_exact_object(blob.object().slot(), b"corrupt".to_vec());
    let directory = empty_destination_dir(&temp).await;
    let destination = directory.join("blob");

    let error = refused_download(
        &storage,
        &blob,
        BlobSpoolProtection::Opaque(audience_key),
        &destination,
        "corrupt stored bytes must be refused",
    )
    .await;
    assert!(matches!(error, StorageError::InvalidContent(_)), "{error}");
    assert!(!destination.exists());
    assert_eq!(directory_entries(&directory).await, Vec::<String>::new());
}
