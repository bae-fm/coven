use super::blob_io::*;
use super::cipher::*;
use super::*;
use crate::cloud::test_utils::InMemoryCloudHome;
use coven_protocol::blob::locator::{BlobLocator, RemoteAudience};
use coven_protocol::blob::BlobScope;
use coven_protocol::objects::BlobWriteAuthority;
use coven_protocol::objects::{LocalRotation, RotationPendingState};
use coven_protocol::store_commit::ObjectHash;
use std::num::NonZeroU64;

async fn ephemeral_stage(
    destination: &std::path::Path,
) -> coven_foundation::local_file::AtomicStagedFile {
    let parent = destination
        .parent()
        .expect("test stage destination has a parent");
    coven_foundation::store_dir::StoreDir::new_ephemeral(parent)
        .stage_atomic_file(destination)
        .await
        .expect("create ephemeral file stage")
}

#[test]
fn encrypted_cloud_object_tag_carries_the_full_key_digest() {
    let encryption = EncryptionService::from_key([0xA5u8; 32]);
    let fingerprint = encryption.seal_key_fingerprint();
    let cipher = CloudCipher::Encrypted(encryption);
    let plaintext = b"full fingerprint cloud object".to_vec();
    let aad = b"full-fingerprint-test";

    let stored = cipher.seal(plaintext.clone(), aad);

    let (tagged, body) = KeyTag::read(&stored).expect("a stored object carries a key tag");
    assert_eq!(&tagged, fingerprint.as_bytes());
    assert_eq!(
        body.len() as u64,
        coven_keys::encryption::chunked_encrypted_len(plaintext.len() as u64),
        "a protocol object keeps the whole-object chunked format the blob namespace left",
    );
    assert_eq!(cipher.open(stored, aad).unwrap(), plaintext);
}

/// A committed local rotation is this device's own published fact. A peer
/// generation that happens to name the same number is not it, and cannot be
/// committed as though it were.
#[test]
fn peer_rotation_cannot_stand_in_for_the_exact_local_candidate() {
    let mutation = ObjectHash::digest(b"local rotation mutation");
    let gate = RotationGate::merge_peer_commit(None, 2).expect("record peer rotation");

    assert!(RotationGate::commit_candidate(Some(gate), 2, mutation).is_err());
}

#[test]
fn local_adoption_cannot_close_another_local_rotation() {
    let adopted = ObjectHash::digest(b"adopted local rotation");
    let other = ObjectHash::digest(b"other local rotation");
    let gate = RotationGate::Local(LocalRotation::Committed {
        generation: NonZeroU64::new(3).unwrap(),
        mutation: other,
    });

    assert!(gate.complete_local_adoption(2, adopted).is_err());
}

/// A gate reaches the type from its `protocol_state` row without passing
/// through any transition, so the refusals the transitions enforce have to
/// hold at parse. Naming no rotation, naming generation zero, and holding
/// both a candidate and a committed local rotation are all shapes the type
/// cannot express — deserializing one fails rather than yielding a gate.
#[test]
fn a_persisted_gate_that_names_no_real_rotation_fails_to_parse() {
    let mutation =
        serde_json::to_string(&ObjectHash::digest(b"rotation owner")).expect("serialize mutation");
    let candidate = format!(r#"{{"generation":2,"mutation":{mutation}}}"#);
    let zero = format!(r#"{{"generation":0,"mutation":{mutation}}}"#);
    for encoded in [
        // Names no rotation at all.
        "{}".to_string(),
        // Generation zero is no generation, local or peer.
        r#"{"peer":{"generation":0}}"#.to_string(),
        format!(r#"{{"local":{{"candidate":{zero}}}}}"#),
        // A candidate and a committed local rotation at once — the shape the
        // gate used to hold and a validator used to refuse.
        format!(r#"{{"candidate":{candidate},"local_committed":{candidate}}}"#),
    ] {
        assert!(
            serde_json::from_str::<RotationGate>(&encoded).is_err(),
            "parsed a gate that names no real rotation: {encoded}",
        );
    }
}

/// The gate a round trip through `protocol_state` must survive: parsing what
/// the transitions produce yields the same gate.
#[test]
fn a_persisted_gate_round_trips() {
    let mutation = ObjectHash::digest(b"round trip");
    let gate = RotationGate::merge_peer_commit(
        Some(RotationGate::with_candidate(None, 2, mutation).expect("stage candidate")),
        3,
    )
    .expect("record peer rotation");
    let encoded = serde_json::to_string(&gate).expect("serialize gate");
    assert_eq!(
        serde_json::from_str::<RotationGate>(&encoded).expect("parse gate"),
        gate
    );
    assert_eq!(
        gate.pending_state(),
        RotationPendingState::CandidateAndPeer {
            candidate_generation: 2,
            peer_generation: 3,
        }
    );
}

#[test]
fn local_adoption_clears_the_same_peer_fact_but_preserves_a_newer_one() {
    let mutation = ObjectHash::digest(b"local removal");
    let committed = RotationGate::commit_candidate(
        Some(RotationGate::with_candidate(None, 2, mutation).unwrap()),
        2,
        mutation,
    )
    .unwrap();
    assert_eq!(
        RotationGate::merge_peer_commit(Some(committed.clone()), 2)
            .unwrap()
            .complete_local_adoption(2, mutation)
            .unwrap(),
        None
    );
    assert_eq!(
        RotationGate::merge_peer_commit(Some(committed), 3)
            .unwrap()
            .complete_local_adoption(2, mutation)
            .unwrap()
            .unwrap()
            .pending_state(),
        RotationPendingState::PeerCommitted { generation: 3 }
    );
}

/// Publish one sealed blob into `home` and hand back the reference a reader
/// opens it through. `chunking` is the installation setting the blob is
/// sealed under; the reader honors whatever the stored header records.
async fn publish_sealed_blob(
    home: &InMemoryCloudHome,
    store_id: &str,
    blob_id: &str,
    plaintext: &[u8],
    chunking: BlobChunking,
) -> (
    CloudSyncConnection,
    coven_protocol::blob::locator::StoredBlobRef,
    EncryptionService,
    tempfile::TempDir,
) {
    let storage = CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
        BlobPathScheme::Hashed,
        store_id,
        UserKeypair::generate(),
    )
    .with_blob_chunking(chunking);
    let registration = storage.blob_write_registration(store_id).await;
    let authority = BlobWriteAuthority::new(&registration);
    let audience_key = EncryptionService::from_key([9u8; 32]);
    let locator = BlobLocator::opaque(
        "audio",
        blob_id,
        registration.reference().clone(),
        RemoteAudience::Store,
        BlobScope::Master,
        audience_key.seal_key_fingerprint(),
        plaintext.len() as u64,
        ObjectHash::digest(plaintext),
    )
    .expect("build locator");
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
            coven_protocol::objects::BlobSpoolProtection::Opaque(audience_key.clone()),
            &source,
            ephemeral_stage(&spool).await,
            crate::cloud::no_preparation_progress(),
        )
        .await
        .expect("seal exact spool");
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
    (storage, blob, audience_key, temp)
}

fn ramp(len: usize) -> Vec<u8> {
    (0..len).map(|value| (value % 251) as u8).collect()
}

fn small_chunking(chunk: u32) -> BlobChunking {
    BlobChunking::new(
        std::num::NonZeroU32::new(chunk).expect("nonzero chunk"),
        std::num::NonZeroU64::new(1 << 20).expect("nonzero window"),
    )
}

#[tokio::test]
async fn sealing_reports_each_plaintext_buffer_before_upload() {
    const CHUNK: u32 = 4096;
    let home = InMemoryCloudHome::new();
    let storage = CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
        BlobPathScheme::Hashed,
        "preparation-progress",
        UserKeypair::generate(),
    )
    .with_blob_chunking(small_chunking(CHUNK));
    let registration = storage
        .blob_write_registration("preparation-progress")
        .await;
    let authority = BlobWriteAuthority::new(&registration);
    let audience_key = EncryptionService::from_key([9u8; 32]);
    let plaintext = ramp(12 * CHUNK as usize + 37);
    let locator = BlobLocator::opaque(
        "audio",
        "progress-track",
        registration.reference().clone(),
        RemoteAudience::Store,
        BlobScope::Master,
        audience_key.seal_key_fingerprint(),
        plaintext.len() as u64,
        ObjectHash::digest(&plaintext),
    )
    .expect("build locator");
    let temp = tempfile::tempdir().expect("temporary blob directory");
    let source = temp.path().join("plaintext");
    let spool = temp.path().join("spool");
    tokio::fs::write(&source, &plaintext)
        .await
        .expect("write plaintext source");
    let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
    let reported = progress.clone();

    storage
        .seal_blob_to_spool(
            &locator,
            &authority,
            coven_protocol::objects::BlobSpoolProtection::Opaque(audience_key),
            &source,
            ephemeral_stage(&spool).await,
            Arc::new(move |bytes| reported.lock().unwrap().push(bytes)),
        )
        .await
        .expect("seal exact spool");

    let progress = progress.lock().unwrap();
    assert!(
        progress.len() > 2,
        "preparation must expose advancing source-buffer progress: {progress:?}",
    );
    assert!(
        progress.windows(2).all(|window| window[0] < window[1]),
        "preparation progress must advance monotonically: {progress:?}",
    );
    assert_eq!(progress.last().copied(), Some(plaintext.len() as u64));
    assert_eq!(home.exact_stream_read_count(), 0, "upload has not started");
}

/// The receipt the whole design exists for: many small ranges across one
/// stream transfer only the chunks those ranges touch, and never the object.
///
/// The sabotage this fails under is a whole-object fetch reintroduced
/// anywhere on the read path — that shows up as a full or streamed exact
/// read, both asserted at zero, rather than hiding inside the ranged total.
#[tokio::test]
async fn ranged_reads_transfer_only_the_chunks_they_cover() {
    const CHUNK: u32 = 4096;
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(400 * CHUNK as usize);
    let (storage, blob, key, _temp) = publish_sealed_blob(
        &home,
        "o-range-receipt",
        "big-track",
        &plaintext,
        small_chunking(CHUNK),
    )
    .await;

    // Keep publication's whole-read counters as the baseline so the receipt
    // below detects any whole-object fetch introduced on the range path.
    let published_whole_reads = (home.exact_full_read_count(), home.exact_stream_read_count());
    home.clear_exact_range_reads();

    let reader = storage
        .open_blob_range_reader(
            &blob,
            coven_protocol::objects::BlobSpoolProtection::Opaque(key),
        )
        .await
        .expect("open a ranged reader");
    // Opening reads the prefix that names the key and the chunk size. Every
    // range below is measured against a cleared ledger, so the receipt is
    // about the ranges, not the open.
    let opened_bytes = home.exact_range_read_bytes();
    assert_eq!(
        opened_bytes,
        (KeyTag::LEN + SEALED_BLOB_HEADER_LEN) as u64,
        "opening costs one prefix read and nothing else",
    );
    home.clear_exact_range_reads();

    // A codec header, a seek to the middle, and a tail — the shape a player
    // issues to start a track.
    let ranges = [
        (0u64, 64u64),
        (200 * CHUNK as u64 + 7, 300),
        (plaintext.len() as u64 - 128, 128),
        (CHUNK as u64 - 1, 2),
    ];
    for (offset, len) in ranges {
        assert_eq!(
            reader.read_at(offset, len).await.expect("serve range"),
            &plaintext[offset as usize..(offset + len) as usize],
        );
    }

    // Each range covers one chunk except the boundary-straddling last, which
    // covers two. Every sealed chunk here is a full one.
    let sealed_chunk = (CHUNK + coven_keys::encryption::TAG_SIZE as u32) as u64;
    assert_eq!(
        home.exact_range_read_bytes(),
        5 * sealed_chunk,
        "ranged reads: {:?}",
        home.exact_range_reads(),
    );
    assert!(
        (home.exact_range_read_bytes() as usize) < plaintext.len() / 50,
        "four small ranges cost a fraction of the object, not the object",
    );
    assert_eq!(
        (home.exact_full_read_count(), home.exact_stream_read_count()),
        published_whole_reads,
        "neither publication nor ranged reading fetched a whole object",
    );
}

/// A browsable home stores the plaintext in the clear, so nothing in the
/// object can refuse a provider's answer to a range. Ranged reading is
/// refused there rather than serving unverified bytes — the caller
/// materializes the whole blob, where the row's content hash still applies.
#[tokio::test]
async fn a_blob_stored_in_the_clear_refuses_ranged_reading() {
    let home = InMemoryCloudHome::new();
    let identity = UserKeypair::generate();
    let storage = CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "browsable-range",
        identity,
    );
    let registration = storage.blob_write_registration("browsable-range").await;
    let authority = BlobWriteAuthority::new(&registration);
    let plaintext = ramp(4096);
    let locator = BlobLocator::browsable(
        "audio",
        "readable-track",
        registration.reference().clone(),
        "Artist/Album/track.flac",
        plaintext.len() as u64,
        ObjectHash::digest(&plaintext),
    )
    .expect("build browsable locator");
    let temp = tempfile::tempdir().expect("temporary blob directory");
    let source = temp.path().join("plaintext");
    let spool = temp.path().join("spool");
    tokio::fs::write(&source, &plaintext)
        .await
        .expect("write plaintext source");
    storage
        .seal_blob_to_spool(
            &locator,
            &authority,
            coven_protocol::objects::BlobSpoolProtection::Browsable,
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

    assert!(matches!(
        storage
            .open_blob_range_reader(
                &blob,
                coven_protocol::objects::BlobSpoolProtection::Browsable,
            )
            .await,
        Err(StorageError::Configuration(_))
    ));
    // The whole-blob path still serves it, checked against the row's hash.
    let destination = temp.path().join("materialized");
    let stage = ephemeral_stage(&destination).await;
    let staged = storage
        .stage_verified_blob_plaintext(
            &blob,
            coven_protocol::objects::BlobSpoolProtection::Browsable,
            stage,
            crate::cloud::no_download_progress(),
        )
        .await
        .expect("materialize the whole browsable blob");
    assert_eq!(tokio::fs::read(staged.path()).await.unwrap(), plaintext);
}

/// The reader honors each blob's own header, so an installation that changed
/// its chunk size keeps reading what it sealed before, and blobs at
/// different sizes coexist with no migration.
#[tokio::test]
async fn blobs_sealed_at_different_chunk_sizes_coexist() {
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(300_000);
    let (small_storage, small_blob, small_key, _small_temp) = publish_sealed_blob(
        &home,
        "mixed-chunk-sizes",
        "sealed-at-64k",
        &plaintext,
        small_chunking(64 * 1024),
    )
    .await;
    let (big_storage, big_blob, big_key, _big_temp) = publish_sealed_blob(
        &home,
        "mixed-chunk-sizes",
        "sealed-at-4m",
        &plaintext,
        small_chunking(4 * 1024 * 1024),
    )
    .await;
    assert_ne!(
        small_blob.object().stored_size(),
        big_blob.object().stored_size(),
        "different chunk counts mean different tag counts, so different objects",
    );

    for (storage, blob, key, chunk) in [
        (&small_storage, &small_blob, small_key, 64u64 * 1024),
        (&big_storage, &big_blob, big_key, 4 * 1024 * 1024),
    ] {
        let reader = storage
            .open_blob_range_reader(
                blob,
                coven_protocol::objects::BlobSpoolProtection::Opaque(key),
            )
            .await
            .expect("open a ranged reader");
        home.clear_exact_range_reads();
        assert_eq!(
            reader.read_at(1000, 100).await.expect("serve range"),
            &plaintext[1000..1100],
        );
        let fetched = home.exact_range_read_bytes();
        let covering = chunk.min(plaintext.len() as u64) + coven_keys::encryption::TAG_SIZE as u64;
        assert_eq!(
            fetched, covering,
            "the read fetched one chunk of this blob's own declared size",
        );
    }
}

/// A flipped byte fails exactly the ranges whose chunk holds it. Every other
/// range still serves — the tag is per chunk, so damage does not spread.
#[tokio::test]
async fn a_tampered_chunk_fails_only_the_ranges_that_touch_it() {
    const CHUNK: u32 = 4096;
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(10 * CHUNK as usize);
    let (storage, blob, key, _temp) = publish_sealed_blob(
        &home,
        "chunk-tamper",
        "tampered-track",
        &plaintext,
        small_chunking(CHUNK),
    )
    .await;

    // Flip one byte inside chunk 3's ciphertext.
    let mut stored = home.stored_exact_object(blob.object().slot());
    let victim = KeyTag::LEN + SEALED_BLOB_HEADER_LEN + 3 * (CHUNK as usize + 16) + 10;
    stored[victim] ^= 0xff;
    home.replace_exact_object(blob.object().slot(), stored);

    let reader = storage
        .open_blob_range_reader(
            &blob,
            coven_protocol::objects::BlobSpoolProtection::Opaque(key),
        )
        .await
        .expect("open a ranged reader");
    for chunk in 0..10u64 {
        let offset = chunk * CHUNK as u64;
        let read = reader.read_at(offset, 16).await;
        if chunk == 3 {
            assert!(
                matches!(read, Err(StorageError::Decryption { .. })),
                "chunk 3 must refuse, got {read:?}",
            );
        } else {
            assert_eq!(
                read.expect("an untouched chunk still serves"),
                &plaintext[offset as usize..offset as usize + 16],
            );
        }
    }
}

/// The header is bound into every chunk's AAD, so rewriting it does not
/// re-frame the object — it makes the first chunk fail to open.
#[tokio::test]
async fn a_tampered_header_fails_the_first_open() {
    const CHUNK: u32 = 4096;
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(4 * CHUNK as usize);
    let (storage, blob, key, _temp) = publish_sealed_blob(
        &home,
        "header-tamper",
        "rewritten-header",
        &plaintext,
        small_chunking(CHUNK),
    )
    .await;

    // Halve the declared chunk size. The object's length no longer matches
    // what that header implies, which is caught before a chunk is opened.
    let mut stored = home.stored_exact_object(blob.object().slot());
    stored[KeyTag::LEN + 2..KeyTag::LEN + 6].copy_from_slice(&(CHUNK / 2).to_le_bytes());
    home.replace_exact_object(blob.object().slot(), stored.clone());
    assert!(
        matches!(
            storage
                .open_blob_range_reader(
                    &blob,
                    coven_protocol::objects::BlobSpoolProtection::Opaque(key.clone()),
                )
                .await,
            Err(StorageError::InvalidContent(_))
        ),
        "a header the object's length cannot produce is refused at open",
    );

    // Shorten the declared plaintext length. The row pins the stored object's
    // exact size, so a header whose framing implies a different one is
    // refused before a chunk is opened.
    let mut stored = home.stored_exact_object(blob.object().slot());
    stored[KeyTag::LEN + 2..KeyTag::LEN + 6].copy_from_slice(&CHUNK.to_le_bytes());
    let shorter = plaintext.len() as u64 - CHUNK as u64;
    stored[KeyTag::LEN + 6..KeyTag::LEN + 14].copy_from_slice(&shorter.to_le_bytes());
    stored.truncate(KeyTag::LEN + SEALED_BLOB_HEADER_LEN + 3 * (CHUNK as usize + 16));
    home.replace_exact_object(blob.object().slot(), stored);
    assert!(
        matches!(
            storage
                .open_blob_range_reader(
                    &blob,
                    coven_protocol::objects::BlobSpoolProtection::Opaque(key.clone()),
                )
                .await,
            Err(StorageError::InvalidContent(_))
        ),
        "a header that disagrees with the row's declared size is refused",
    );

    // The case only the AAD can catch. Nudging the chunk size by one byte
    // leaves every length check satisfied — 16384 plaintext bytes still take
    // four chunks at 4097 as at 4096, so chunk count, sealed length, and the
    // row's declared size all agree — and re-frames where each chunk starts.
    // Nothing but the tag over the header refuses this.
    const NUDGED: u32 = CHUNK + 1;
    let derived = coven_keys::encryption::NoncePolicy::DerivedFromContext {
        context: b"unused by the length arithmetic".to_vec(),
    };
    let unaltered = SealedBlobHeader::new(
        std::num::NonZeroU32::new(CHUNK).unwrap(),
        plaintext.len() as u64,
        &derived,
    );
    let nudged = SealedBlobHeader::new(
        std::num::NonZeroU32::new(NUDGED).unwrap(),
        plaintext.len() as u64,
        &derived,
    );
    assert_eq!(
        (nudged.chunk_count(), nudged.sealed_len()),
        (unaltered.chunk_count(), unaltered.sealed_len()),
        "the nudge must survive every length check, or it proves nothing about the AAD",
    );

    let mut stored = home.stored_exact_object(blob.object().slot());
    stored[KeyTag::LEN + 2..KeyTag::LEN + 6].copy_from_slice(&NUDGED.to_le_bytes());
    stored[KeyTag::LEN + 6..KeyTag::LEN + 14]
        .copy_from_slice(&(plaintext.len() as u64).to_le_bytes());
    stored.truncate(KeyTag::LEN + unaltered.sealed_len() as usize);
    home.replace_exact_object(blob.object().slot(), stored);

    let reader = storage
        .open_blob_range_reader(
            &blob,
            coven_protocol::objects::BlobSpoolProtection::Opaque(key),
        )
        .await
        .expect("every length check passes, so the reader opens");
    assert!(
        matches!(
            reader.read_at(0, 16).await,
            Err(StorageError::Decryption { .. })
        ),
        "the first chunk's tag covers the header, so a re-framed header fails the open",
    );
}

/// A chunk cannot be moved: not into another blob, and not to another index
/// in its own. Its tag covers the blob's identity and its position.
#[tokio::test]
async fn a_spliced_chunk_refuses_to_open() {
    const CHUNK: u32 = 4096;
    const SEALED: usize = CHUNK as usize + 16;
    let home = InMemoryCloudHome::new();
    let plaintext = ramp(6 * CHUNK as usize);
    let (storage, victim, victim_key, _victim_temp) =
        publish_sealed_blob(&home, "splice", "victim", &plaintext, small_chunking(CHUNK)).await;
    // A second blob with identical plaintext and chunking: the only thing
    // separating the two objects is which blob they are.
    let (_donor_storage, donor, _donor_key, _donor_temp) =
        publish_sealed_blob(&home, "splice", "donor", &plaintext, small_chunking(CHUNK)).await;
    let donor_stored = home.stored_exact_object(donor.object().slot());
    let body = KeyTag::LEN + SEALED_BLOB_HEADER_LEN;

    // Cross-blob: the donor's chunk 2 in the victim's chunk 2.
    let mut spliced = home.stored_exact_object(victim.object().slot());
    spliced[body + 2 * SEALED..body + 3 * SEALED]
        .copy_from_slice(&donor_stored[body + 2 * SEALED..body + 3 * SEALED]);
    home.replace_exact_object(victim.object().slot(), spliced);
    let reader = storage
        .open_blob_range_reader(
            &victim,
            coven_protocol::objects::BlobSpoolProtection::Opaque(victim_key.clone()),
        )
        .await
        .expect("open a ranged reader");
    assert!(
        matches!(
            reader.read_at(2 * CHUNK as u64, 16).await,
            Err(StorageError::Decryption { .. })
        ),
        "another blob's chunk cannot stand in for this one's",
    );

    // Cross-position: the victim's own chunk 4 moved to index 2.
    let original = home.stored_exact_object(donor.object().slot());
    let mut moved = original.clone();
    let chunk_four = original[body + 4 * SEALED..body + 5 * SEALED].to_vec();
    moved[body + 2 * SEALED..body + 3 * SEALED].copy_from_slice(&chunk_four);
    home.replace_exact_object(donor.object().slot(), moved);
    let reader = storage
        .open_blob_range_reader(
            &donor,
            coven_protocol::objects::BlobSpoolProtection::Opaque(victim_key),
        )
        .await
        .expect("open a ranged reader");
    assert!(
        matches!(
            reader.read_at(2 * CHUNK as u64, 16).await,
            Err(StorageError::Decryption { .. })
        ),
        "a chunk cannot open at an index it was not sealed for",
    );
    assert_eq!(
        reader
            .read_at(5 * CHUNK as u64, 16)
            .await
            .expect("an untouched chunk still serves"),
        &plaintext[5 * CHUNK as usize..5 * CHUNK as usize + 16],
    );
}

/// Every range shape a stream produces, against the plaintext: boundaries,
/// single bytes, the tail, the whole blob, and the empty range.
#[tokio::test]
async fn ranged_reads_sweep_every_boundary() {
    const CHUNK: u32 = 1024;
    let home = InMemoryCloudHome::new();
    // Deliberately not a chunk multiple, so the last chunk is short.
    let plaintext = ramp(3 * CHUNK as usize + 37);
    let (storage, blob, key, _temp) = publish_sealed_blob(
        &home,
        "boundary-sweep",
        "swept",
        &plaintext,
        small_chunking(CHUNK),
    )
    .await;
    let reader = storage
        .open_blob_range_reader(
            &blob,
            coven_protocol::objects::BlobSpoolProtection::Opaque(key),
        )
        .await
        .expect("open a ranged reader");
    let size = plaintext.len() as u64;
    assert_eq!(reader.plaintext_size(), size);

    assert!(reader.read_at(0, 0).await.expect("empty range").is_empty());
    assert!(reader
        .read_at(size, 0)
        .await
        .expect("empty range at the end")
        .is_empty());
    assert_eq!(
        reader.read_at(0, size).await.expect("whole blob"),
        plaintext,
    );
    assert_eq!(
        reader.read_at(size - 1, 1).await.expect("last byte"),
        &plaintext[plaintext.len() - 1..],
    );
    for boundary in [CHUNK as u64, 2 * CHUNK as u64, 3 * CHUNK as u64] {
        for (offset, len) in [(boundary - 1, 2), (boundary - 1, 1), (boundary, 1)] {
            assert_eq!(
                reader.read_at(offset, len).await.expect("boundary range"),
                &plaintext[offset as usize..(offset + len) as usize],
                "range {offset}..{} straddling a chunk boundary",
                offset + len,
            );
        }
    }
    // Every single-byte read across the last chunk, where the short chunk
    // makes the arithmetic differ from every chunk before it.
    for offset in 3 * CHUNK as u64..size {
        assert_eq!(
            reader.read_at(offset, 1).await.expect("tail byte"),
            &plaintext[offset as usize..offset as usize + 1],
        );
    }
    assert!(
        reader.read_at(size, 1).await.is_err(),
        "a range past the end is an error, not a short read",
    );
    assert!(reader.read_at(size - 1, 2).await.is_err());
}

/// A window narrower than the range splits it into several requests whose
/// spans, together, are exactly the covering chunks — the window changes how
/// many round-trips a read costs, never which bytes it fetches.
#[tokio::test]
async fn the_fetch_window_splits_requests_without_changing_the_bytes() {
    const CHUNK: u32 = 1024;
    let sealed_chunk = CHUNK as u64 + coven_keys::encryption::TAG_SIZE as u64;
    let plaintext = ramp(20 * CHUNK as usize);
    let mut totals = Vec::new();
    for window in [sealed_chunk, 4 * sealed_chunk, 1 << 20] {
        let home = InMemoryCloudHome::new();
        let (storage, blob, key, _temp) = publish_sealed_blob(
            &home,
            "fetch-window",
            "windowed",
            &plaintext,
            BlobChunking::new(
                std::num::NonZeroU32::new(CHUNK).unwrap(),
                std::num::NonZeroU64::new(window).unwrap(),
            ),
        )
        .await;
        let reader = storage
            .open_blob_range_reader(
                &blob,
                coven_protocol::objects::BlobSpoolProtection::Opaque(key),
            )
            .await
            .expect("open a ranged reader");
        home.clear_exact_range_reads();
        assert_eq!(
            reader
                .read_at(0, 8 * CHUNK as u64)
                .await
                .expect("read eight chunks"),
            &plaintext[..8 * CHUNK as usize],
        );
        totals.push((
            home.exact_range_reads().len(),
            home.exact_range_read_bytes(),
        ));
    }
    assert_eq!(
        totals,
        vec![
            (8, 8 * sealed_chunk),
            (2, 8 * sealed_chunk),
            (1, 8 * sealed_chunk)
        ],
        "the same eight chunks, in eight, two, then one request",
    );
}

#[tokio::test]
async fn circle_blob_spool_uses_the_supplied_audience_key() {
    let home = InMemoryCloudHome::new();
    let identity = UserKeypair::generate();
    let storage = CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
        BlobPathScheme::Hashed,
        "circle-blob-spool",
        identity,
    );
    let registration = storage.blob_write_registration("circle-blob-spool").await;
    let authority = BlobWriteAuthority::new(&registration);
    let circle_key = EncryptionService::from_key([9u8; 32]);
    let plaintext = b"circle audience blob";
    let locator = BlobLocator::opaque(
        "covers",
        "circle-cover",
        registration.reference().clone(),
        RemoteAudience::Circle(coven_protocol::circle::CircleId::from_bytes([8; 16])),
        BlobScope::Master,
        circle_key.seal_key_fingerprint(),
        plaintext.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(plaintext),
    )
    .expect("build Circle locator");
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
            coven_protocol::objects::BlobSpoolProtection::Opaque(circle_key.clone()),
            &source,
            ephemeral_stage(&spool).await,
            crate::cloud::no_preparation_progress(),
        )
        .await
        .expect("seal Circle blob spool");

    let stored = tokio::fs::read(&spool).await.expect("read exact spool");
    let (fingerprint, header, sealed) =
        super::blob_io::split_sealed_blob(&stored).expect("parse sealed Circle blob");
    assert_eq!(fingerprint, circle_key.seal_key_fingerprint());
    let opened = circle_key
        .blob_opener(
            header,
            &coven_keys::encryption::NoncePolicy::DerivedFromContext {
                context: cloud_aad_context("circle-blob-spool", &locator.semantic_key()),
            },
            &cloud_aad_context("circle-blob-spool", &locator.semantic_key()),
        )
        .expect("a blob is sealed under the derived policy")
        .open_chunks(0..header.chunk_count(), sealed)
        .expect("open Circle blob with supplied key");
    assert_eq!(opened, plaintext);
}

#[tokio::test]
async fn blob_spool_rejects_a_key_that_differs_from_the_locator() {
    let home = InMemoryCloudHome::new();
    let identity = UserKeypair::generate();
    let storage = CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
        BlobPathScheme::Hashed,
        "blob-spool-key-mismatch",
        identity,
    );
    let registration = storage
        .blob_write_registration("blob-spool-key-mismatch")
        .await;
    let authority = BlobWriteAuthority::new(&registration);
    let declared_key = EncryptionService::from_key([9u8; 32]);
    let plaintext = b"audience blob";
    let locator = BlobLocator::opaque(
        "covers",
        "mismatched-cover",
        registration.reference().clone(),
        RemoteAudience::Store,
        BlobScope::Master,
        declared_key.seal_key_fingerprint(),
        plaintext.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(plaintext),
    )
    .expect("build locator");
    let temp = tempfile::tempdir().expect("temporary blob directory");
    let source = temp.path().join("plaintext");
    let spool = temp.path().join("spool");
    tokio::fs::write(&source, plaintext)
        .await
        .expect("write plaintext source");

    assert!(matches!(
        storage
            .seal_blob_to_spool(
                &locator,
                &authority,
                coven_protocol::objects::BlobSpoolProtection::Opaque(EncryptionService::from_key(
                    [10u8; 32]
                ),),
                &source,
                ephemeral_stage(&spool).await,
                crate::cloud::no_preparation_progress(),
            )
            .await,
        Err(StorageError::InvalidContent(_))
    ));
    assert!(!spool.exists());
}

#[tokio::test]
async fn exact_blob_plaintext_is_published_only_after_both_verifications() {
    let home = InMemoryCloudHome::new();
    let identity = UserKeypair::generate();
    let storage = CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
        BlobPathScheme::Hashed,
        "verified-blob-download",
        identity,
    );
    let registration = storage
        .blob_write_registration("verified-blob-download")
        .await;
    let authority = BlobWriteAuthority::new(&registration);
    let audience_key = EncryptionService::from_key([9u8; 32]);
    let plaintext: Vec<u8> = (0..150_000u32).map(|value| (value % 251) as u8).collect();
    let locator = BlobLocator::opaque(
        "audio",
        "verified-track",
        registration.reference().clone(),
        RemoteAudience::Store,
        BlobScope::Derived("album-a".to_string()),
        audience_key.seal_key_fingerprint(),
        plaintext.len() as u64,
        ObjectHash::digest(&plaintext),
    )
    .expect("build locator");
    let temp = tempfile::tempdir().expect("temporary blob directory");
    let source = temp.path().join("plaintext");
    let spool = temp.path().join("spool");
    let destination = temp.path().join("materialized");
    tokio::fs::write(&source, &plaintext)
        .await
        .expect("write plaintext source");
    storage
        .seal_blob_to_spool(
            &locator,
            &authority,
            coven_protocol::objects::BlobSpoolProtection::Opaque(audience_key.clone()),
            &source,
            ephemeral_stage(&spool).await,
            crate::cloud::no_preparation_progress(),
        )
        .await
        .expect("seal exact spool");
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

    let stage = ephemeral_stage(&destination).await;
    let staged = storage
        .stage_verified_blob_plaintext(
            &blob,
            coven_protocol::objects::BlobSpoolProtection::Opaque(audience_key),
            stage,
            crate::cloud::no_download_progress(),
        )
        .await
        .expect("stage verified plaintext");
    assert!(!destination.exists());
    assert_eq!(tokio::fs::read(staged.path()).await.unwrap(), plaintext);
    staged.commit().await.expect("publish verified plaintext");
    assert_eq!(tokio::fs::read(destination).await.unwrap(), plaintext);
}

#[tokio::test]
async fn stored_blob_corruption_never_creates_a_plaintext_stage() {
    let home = InMemoryCloudHome::new();
    let identity = UserKeypair::generate();
    let storage = CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
        BlobPathScheme::Hashed,
        "corrupt-blob-download",
        identity,
    );
    let registration = storage
        .blob_write_registration("corrupt-blob-download")
        .await;
    let authority = BlobWriteAuthority::new(&registration);
    let audience_key = EncryptionService::from_key([9u8; 32]);
    let plaintext = b"signed blob plaintext";
    let locator = BlobLocator::opaque(
        "covers",
        "corrupt-cover",
        registration.reference().clone(),
        RemoteAudience::Store,
        BlobScope::Master,
        audience_key.seal_key_fingerprint(),
        plaintext.len() as u64,
        ObjectHash::digest(plaintext),
    )
    .expect("build locator");
    let temp = tempfile::tempdir().expect("temporary blob directory");
    let source = temp.path().join("plaintext");
    let spool = temp.path().join("spool");
    let destination = temp.path().join("materialized");
    tokio::fs::write(&source, plaintext)
        .await
        .expect("write plaintext source");
    storage
        .seal_blob_to_spool(
            &locator,
            &authority,
            coven_protocol::objects::BlobSpoolProtection::Opaque(audience_key.clone()),
            &source,
            ephemeral_stage(&spool).await,
            crate::cloud::no_preparation_progress(),
        )
        .await
        .expect("seal exact spool");
    let slot = storage
        .allocate_blob_slot(&locator, &authority)
        .await
        .unwrap();
    let blob = storage
        .prepare_blob_object(&locator, &authority, slot, &spool)
        .await
        .unwrap();
    storage
        .create_blob_object_from_file(
            &blob,
            &authority,
            &spool,
            &crate::cloud::UploadControl::running(crate::cloud::no_progress()),
        )
        .await
        .unwrap();
    home.replace_exact_object(blob.object().slot(), b"corrupt".to_vec());

    let stage = ephemeral_stage(&destination).await;
    assert!(matches!(
        storage
            .stage_verified_blob_plaintext(
                &blob,
                coven_protocol::objects::BlobSpoolProtection::Opaque(audience_key),
                stage,
                crate::cloud::no_download_progress(),
            )
            .await,
        Err(StorageError::InvalidContent(_))
    ));
    assert!(!destination.exists());
}

#[tokio::test]
async fn reserved_protocol_slot_read_returns_its_completed_exact_reference() {
    let home = InMemoryCloudHome::new();
    let storage = CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
        BlobPathScheme::Hashed,
        "reserved-slot-read",
        UserKeypair::generate(),
    );
    let root = coven_protocol::store_commit::ObjectHash::digest(b"reserved slot root");
    let semantic = "store-v1/heads/device-a/1".to_string();
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        root,
        ProtocolObjectDomain::StoreHead,
    );
    let slot = storage
        .allocate_protocol_slot(&context, &semantic, ".json")
        .await
        .expect("reserve successor slot");
    let prepared = storage
        .prepare_protocol_object(
            &context,
            slot.clone(),
            &semantic,
            b"signed successor bytes".to_vec(),
        )
        .expect("prepare successor bytes");
    storage
        .create_protocol_object(&prepared)
        .await
        .expect("create successor");

    let (opened, completed) = storage
        .read_protocol_slot(&context, &slot, &semantic)
        .await
        .expect("read reserved successor slot");

    assert_eq!(opened, b"signed successor bytes");
    assert_eq!(&completed, prepared.reference());
}

#[tokio::test]
async fn protocol_publication_verifies_local_bytes_without_a_provider_body_read() {
    let home = InMemoryCloudHome::new();
    let storage = CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
        BlobPathScheme::Hashed,
        "local-protocol-verification",
        UserKeypair::generate(),
    );
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        ObjectHash::digest(b"local protocol verification root"),
        ProtocolObjectDomain::StoreHead,
    );
    let semantic = "store-v1/heads/device-a/1";
    let slot = storage
        .allocate_protocol_slot(&context, semantic, ".json")
        .await
        .expect("reserve protocol slot");
    let canonical = b"canonical protocol bytes";
    let prepared = storage
        .prepare_protocol_object(&context, slot, semantic, canonical.to_vec())
        .expect("prepare protocol object");

    storage
        .create_verified_protocol_object(&context, &prepared, semantic, canonical)
        .await
        .expect("verify and publish protocol object");

    assert!(home.contains_exact_object(prepared.reference()));
    assert_eq!(home.exact_full_read_count(), 0);
    assert_eq!(home.exact_stream_read_count(), 0);
}

#[tokio::test]
async fn protocol_publication_refuses_local_semantic_mismatch_before_upload() {
    let home = InMemoryCloudHome::new();
    let storage = CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
        BlobPathScheme::Hashed,
        "local-protocol-mismatch",
        UserKeypair::generate(),
    );
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        ObjectHash::digest(b"local protocol mismatch root"),
        ProtocolObjectDomain::StoreHead,
    );
    let semantic = "store-v1/heads/device-a/1";
    let slot = storage
        .allocate_protocol_slot(&context, semantic, ".json")
        .await
        .expect("reserve protocol slot");
    let prepared = storage
        .prepare_protocol_object(
            &context,
            slot,
            semantic,
            b"different retained bytes".to_vec(),
        )
        .expect("prepare internally valid competing bytes");

    assert!(matches!(
        storage
            .create_verified_protocol_object(
                &context,
                &prepared,
                semantic,
                b"canonical journal bytes",
            )
            .await,
        Err(StorageError::PreparedObjectMismatch(_))
    ));
    assert!(!home.contains_exact_object(prepared.reference()));
    assert_eq!(home.exact_full_read_count(), 0);
    assert_eq!(home.exact_stream_read_count(), 0);
}

#[tokio::test]
async fn blob_publication_uses_the_spool_without_a_provider_body_read() {
    let home = InMemoryCloudHome::new();
    let (_storage, blob, _key, _temp) = publish_sealed_blob(
        &home,
        "local-blob-verification",
        "published-track",
        b"blob bytes retained in the local spool",
        small_chunking(4096),
    )
    .await;

    assert!(home.contains_exact_object(blob.object()));
    assert_eq!(home.exact_full_read_count(), 0);
    assert_eq!(home.exact_stream_read_count(), 0);
}

#[test]
fn protocol_object_prepare_rejects_a_path_outside_its_domain() {
    let storage = CloudSyncConnection::new(
        Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
        BlobPathScheme::Hashed,
        "prepare-domain-path",
        UserKeypair::generate(),
    );
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        ObjectHash::digest(b"prepare domain root"),
        ProtocolObjectDomain::StoreHead,
    );
    let invalid_semantic = "store-v1/commits/device-a/1";
    let slot =
        ObjectSlot::logical(format!("{invalid_semantic}.json")).expect("valid logical object slot");

    assert!(matches!(
        storage
            .prepare_protocol_object(&context, slot, invalid_semantic, b"signed bytes".to_vec(),),
        Err(StorageError::Parse(_))
    ));
}

#[tokio::test]
async fn exact_delete_refuses_to_remove_different_bytes_in_the_same_slot() {
    let home = InMemoryCloudHome::new();
    let storage = CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
        BlobPathScheme::Hashed,
        "exact-delete-identity",
        UserKeypair::generate(),
    );
    let root = ObjectHash::digest(b"exact delete root");
    let semantic = "store-v1/heads/device-a/1";
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        root,
        ProtocolObjectDomain::StoreHead,
    );
    let slot = storage
        .allocate_protocol_slot(&context, semantic, ".json")
        .await
        .expect("allocate exact slot");
    let prepared = storage
        .prepare_protocol_object(&context, slot.clone(), semantic, b"original".to_vec())
        .expect("prepare exact object");
    storage
        .create_protocol_object(&prepared)
        .await
        .expect("create exact object");
    home.replace_exact_object(&slot, b"competing stored bytes".to_vec());

    assert!(matches!(
        storage.delete_protocol_object(prepared.reference()).await,
        Err(StorageError::SlotCollision(_))
    ));
    assert_eq!(
        home.get(slot.logical_key()),
        Some(b"competing stored bytes".to_vec())
    );
}

#[tokio::test]
async fn reserved_protocol_slot_rejects_a_mismatched_semantic_path_before_read() {
    let home = InMemoryCloudHome::new();
    let storage = CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
        BlobPathScheme::Hashed,
        "reserved-slot-relocation",
        UserKeypair::generate(),
    );
    let root = coven_protocol::store_commit::ObjectHash::digest(b"reserved slot root");
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        root,
        ProtocolObjectDomain::StoreHead,
    );
    let original = "store-v1/heads/device-a/1".to_string();
    let relocated = "store-v1/heads/device-b/1".to_string();
    let slot = storage
        .allocate_protocol_slot(&context, &original, ".json")
        .await
        .expect("reserve successor slot");

    assert!(matches!(
        storage
            .read_protocol_slot(&context, &slot, &relocated)
            .await,
        Err(StorageError::Parse(_))
    ));
}

#[tokio::test]
async fn protocol_object_read_rejects_domain_and_path_substitution() {
    let home = InMemoryCloudHome::new();
    let storage = CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Encrypted(EncryptionService::from_key([8u8; 32])),
        BlobPathScheme::Hashed,
        "aad-store",
        UserKeypair::generate(),
    );
    let root = coven_protocol::store_commit::ObjectHash::digest(b"root-a");
    let other_root = coven_protocol::store_commit::ObjectHash::digest(b"root-b");
    let commit_hash = coven_protocol::store_commit::ObjectHash::digest(b"commit");
    let family = coven_protocol::store_commit::CandidateFamilyId::from_hash(
        coven_protocol::store_commit::ObjectHash::digest(b"cloud test family"),
    );
    let semantic =
        coven_protocol::store_commit::commit_semantic_prefix(family, "device", 1, commit_hash);
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        root,
        ProtocolObjectDomain::StoreCommit,
    );
    let slot = storage
        .allocate_protocol_slot(&context, &semantic, ".json")
        .await
        .expect("allocate root-bound Store commit slot");
    let prepared = storage
        .prepare_protocol_object(&context, slot, &semantic, b"signed commit".to_vec())
        .expect("prepare root-bound Store commit");
    storage
        .create_protocol_object(&prepared)
        .await
        .expect("create root-bound Store commit");
    let object = prepared.reference().clone();

    assert_eq!(
        storage
            .read_protocol_object(&context, &object, &semantic)
            .await
            .expect("read with the exact authenticated context"),
        b"signed commit",
    );
    let other_root_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        other_root,
        ProtocolObjectDomain::StoreCommit,
    );
    assert_eq!(
        storage
            .read_protocol_object(&other_root_context, &object, &semantic)
            .await
            .expect("signed plaintext bytes are opened before their root signature is parsed"),
        b"signed commit",
    );

    let other_semantic =
        coven_protocol::store_commit::commit_semantic_prefix(family, "device", 2, commit_hash);
    assert!(matches!(
        storage
            .read_protocol_object(&context, &object, &other_semantic)
            .await,
        Err(coven_protocol::objects::StorageError::Parse(_))
    ));

    let other_domain_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        root,
        ProtocolObjectDomain::StoreHead,
    );
    assert!(matches!(
        storage
            .read_protocol_object(&other_domain_context, &object, &semantic)
            .await,
        Err(coven_protocol::objects::StorageError::Parse(_))
    ));
}

#[tokio::test]
async fn signed_control_is_readable_across_store_key_rotations_but_packages_are_not() {
    let home = Arc::new(InMemoryCloudHome::new());
    let writer = CloudSyncConnection::new(
        home.clone(),
        CloudCipher::Encrypted(EncryptionService::from_key([8u8; 32])),
        BlobPathScheme::Hashed,
        "control-plane-rotation",
        UserKeypair::generate(),
    );
    let stale_reader = CloudSyncConnection::new(
        home,
        CloudCipher::Encrypted(EncryptionService::from_key([9u8; 32])),
        BlobPathScheme::Hashed,
        "control-plane-rotation",
        UserKeypair::generate(),
    );
    let root = ObjectHash::digest(b"control plane root");
    let head_semantic = "store-v1/heads/device-a/1";
    let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        root,
        ProtocolObjectDomain::StoreHead,
    );
    let head_slot = writer
        .allocate_protocol_slot(&head_context, head_semantic, ".json")
        .await
        .expect("allocate signed head");
    let head = writer
        .prepare_protocol_object(
            &head_context,
            head_slot,
            head_semantic,
            b"signed control bytes".to_vec(),
        )
        .expect("prepare signed head");
    writer
        .create_protocol_object(&head)
        .await
        .expect("create signed head");
    assert_eq!(
        stale_reader
            .read_protocol_object(&head_context, head.reference(), head_semantic)
            .await
            .expect("read signed control with a different Store key"),
        b"signed control bytes",
    );

    let family = coven_protocol::store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(
        b"control plane package family",
    ));
    let package_hash = ObjectHash::digest(b"encrypted package");
    let package_semantic = format!(
        "store-v1/candidates/{}/packages/device-a/1/{package_hash}",
        family.as_hash()
    );
    let package_context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
        root,
        ProtocolObjectDomain::StorePackage,
    );
    let package_slot = writer
        .allocate_protocol_slot(&package_context, &package_semantic, ".pkg")
        .await
        .expect("allocate encrypted package");
    let package = writer
        .prepare_protocol_object(
            &package_context,
            package_slot,
            &package_semantic,
            b"encrypted package".to_vec(),
        )
        .expect("prepare encrypted package");
    writer
        .create_protocol_object(&package)
        .await
        .expect("create encrypted package");
    assert!(matches!(
        stale_reader
            .read_protocol_object(&package_context, package.reference(), &package_semantic,)
            .await,
        Err(StorageError::Decryption { .. })
    ));
}
