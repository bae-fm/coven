use super::*;
use coven_foundation::store_dir::temp_store_dir;

fn payload(byte: u8, len: usize) -> Vec<u8> {
    vec![byte; len]
}

fn incompressible_payload(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn payload_store() -> (tempfile::TempDir, StoreDir, Connection) {
    let (directory, store_dir) = temp_store_dir();
    let conn = Connection::open_in_memory().expect("open payload database");
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("enable payload foreign keys");
    crate::apply_coven_schema(&conn).expect("apply payload schema");
    (directory, store_dir, conn)
}

fn storage_row(conn: &Connection, hash: ObjectHash) -> (String, i64, Option<Vec<u8>>, i64) {
    conn.query_row(
        "SELECT storage, payload_size, compressed_bytes, compressed_size
         FROM payload_storage WHERE payload_hash = ?1",
        [hash.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )
    .expect("read payload storage row")
}

fn install(conn: &Connection, store_dir: &StoreDir, bytes: &[u8]) -> ObjectHash {
    let transaction = conn
        .unchecked_transaction()
        .expect("begin payload installation");
    let hash = PayloadStore::new(&transaction, store_dir)
        .install(bytes)
        .expect("install payload");
    transaction.commit().expect("commit payload installation");
    hash
}

#[test]
fn protocol_sized_payloads_live_inline_in_the_database() {
    let (_directory, store_dir, conn) = payload_store();
    let bytes = payload(2, 16 * 1024);
    let hash = install(&conn, &store_dir, &bytes);
    let (storage, payload_size, compressed, compressed_size) = storage_row(&conn, hash);
    let compressed = compressed.expect("inline compressed payload");

    assert_eq!(storage, "inline");
    assert_eq!(payload_size, bytes.len() as i64);
    assert_eq!(compressed_size, compressed.len() as i64);
    assert!(compressed_size < payload_size);
    assert!(!store_dir.payload_spool_path(hash).exists());
    assert_eq!(
        PayloadStore::new(&conn, &store_dir)
            .read(hash)
            .expect("read inline payload"),
        bytes
    );
}

#[test]
fn payload_storage_compresses_bytes_without_changing_their_content_address() {
    let (_directory, store_dir, conn) = payload_store();
    let bytes = payload(12, 16 * 1024);
    let hash = install(&conn, &store_dir, &bytes);
    let (_, _, stored_bytes, _) = storage_row(&conn, hash);

    assert_eq!(hash, ObjectHash::digest(&bytes));
    assert_ne!(stored_bytes.as_deref(), Some(bytes.as_slice()));
    assert_eq!(
        PayloadStore::new(&conn, &store_dir)
            .read_verified(hash)
            .expect("read compressed payload"),
        bytes
    );
}

#[test]
fn compressed_size_selects_inline_storage() {
    let (_directory, store_dir, conn) = payload_store();
    let bytes = payload(13, INLINE_PAYLOAD_LIMIT * 4);
    let hash = install(&conn, &store_dir, &bytes);

    assert_eq!(storage_row(&conn, hash).0, "inline");
    assert!(!store_dir.payload_spool_path(hash).exists());
    assert_eq!(
        PayloadStore::new(&conn, &store_dir)
            .read(hash)
            .expect("read compressed inline payload"),
        bytes
    );
}

#[test]
fn payload_installation_requires_an_owning_transaction() {
    let (_directory, store_dir, conn) = payload_store();
    let bytes = b"unowned payload";
    let hash = ObjectHash::digest(bytes);

    let error = PayloadStore::new(&conn, &store_dir)
        .install(bytes)
        .expect_err("a bare connection must not install payload storage");

    assert!(error.to_string().contains("transaction"), "{error}");
    assert!(PayloadStore::new(&conn, &store_dir)
        .stored(hash)
        .expect("check unowned payload storage")
        .is_none());
}

#[test]
fn compressed_payloads_over_the_inline_limit_live_in_the_file_spool() {
    let (_directory, store_dir, conn) = payload_store();
    let inline = payload(3, INLINE_PAYLOAD_LIMIT * 4);
    let file = incompressible_payload(4, INLINE_PAYLOAD_LIMIT * 2);
    let inline_hash = install(&conn, &store_dir, &inline);
    let file_hash = install(&conn, &store_dir, &file);
    let (inline_storage, inline_size, inline_compressed, inline_compressed_size) =
        storage_row(&conn, inline_hash);
    let (file_storage, file_size, file_compressed, file_compressed_size) =
        storage_row(&conn, file_hash);

    assert_eq!(inline_storage, "inline");
    assert_eq!(inline_size, inline.len() as i64);
    assert!(inline_compressed.is_some());
    assert!(inline_compressed_size <= INLINE_PAYLOAD_LIMIT as i64);
    assert_eq!(file_storage, "file");
    assert_eq!(file_size, file.len() as i64);
    assert!(file_compressed.is_none());
    assert!(file_compressed_size > INLINE_PAYLOAD_LIMIT as i64);
    let stored = std::fs::read(store_dir.payload_spool_path(file_hash))
        .expect("read compressed file payload");
    assert_eq!(stored.len() as i64, file_compressed_size);
    assert_ne!(stored, file);
    assert_eq!(
        PayloadStore::new(&conn, &store_dir)
            .read(file_hash)
            .expect("decompress file payload"),
        file
    );
}

#[test]
fn reinstalling_a_file_payload_reuses_and_repairs_its_exact_path() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let (store_dir, sync_requests) =
        StoreDir::new_with_file_sync_observer_for_test(directory.path());
    let conn = Connection::open_in_memory().expect("open payload database");
    crate::apply_coven_schema(&conn).expect("apply payload schema");
    let bytes = incompressible_payload(5, INLINE_PAYLOAD_LIMIT * 2);

    let first = install(&conn, &store_dir, &bytes);
    let second = install(&conn, &store_dir, &bytes);
    assert_eq!(first, second);
    assert_eq!(
        sync_requests.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the exact reinstall performs no durability work"
    );

    std::fs::write(store_dir.payload_spool_path(first), b"changed")
        .expect("change installed payload");
    assert_eq!(install(&conn, &store_dir, &bytes), first);
    assert_eq!(
        PayloadStore::new(&conn, &store_dir)
            .read(first)
            .expect("read repaired payload"),
        bytes
    );
    assert_eq!(
        sync_requests.load(std::sync::atomic::Ordering::SeqCst),
        4,
        "repair atomically replaces the changed file"
    );
}

#[test]
fn conflicting_file_metadata_fails_without_replacing_the_file() {
    let (_directory, store_dir, conn) = payload_store();
    let bytes = incompressible_payload(17, INLINE_PAYLOAD_LIMIT * 2);
    let hash = install(&conn, &store_dir, &bytes);
    let path = store_dir.payload_spool_path(hash);
    std::fs::write(&path, b"changed").expect("change installed payload");
    conn.execute(
        "UPDATE payload_storage
         SET payload_size = ?2, compressed_size = 7
         WHERE payload_hash = ?1",
        rusqlite::params![hash.to_string(), bytes.len() as i64 - 1],
    )
    .expect("change catalog sizes");

    let transaction = conn
        .unchecked_transaction()
        .expect("begin conflicting reinstall");
    let error = PayloadStore::new(&transaction, &store_dir)
        .install(&bytes)
        .expect_err("conflicting metadata must reject reinstall");

    assert!(error.to_string().contains("catalog records"), "{error}");
    assert_eq!(
        std::fs::read(path).expect("read unchanged file"),
        b"changed"
    );
}

#[test]
fn verified_reads_reject_changed_inline_and_file_payloads() {
    let (_directory, store_dir, conn) = payload_store();
    let store = PayloadStore::new(&conn, &store_dir);
    let inline_hash = install(&conn, &store_dir, b"inline payload");
    let file_hash = install(
        &conn,
        &store_dir,
        &incompressible_payload(6, INLINE_PAYLOAD_LIMIT * 2),
    );

    conn.execute(
        "UPDATE payload_storage
         SET compressed_bytes = x'00', compressed_size = 1
         WHERE payload_hash = ?1",
        [inline_hash.to_string()],
    )
    .expect("change inline payload");
    std::fs::write(store_dir.payload_spool_path(file_hash), b"changed")
        .expect("change file payload");

    assert!(matches!(
        store.read_verified(inline_hash),
        Err(PayloadStoreError::Compression { hash, .. }) if hash == inline_hash
    ));
    assert!(matches!(
        store.read_verified(file_hash),
        Err(PayloadStoreError::Storage { hash, .. }) if hash == file_hash
    ));
}

#[test]
fn verified_reads_hash_the_decompressed_payload() {
    let (_directory, store_dir, conn) = payload_store();
    let expected = b"expected logical payload";
    let replacement = b"different logical bytes";
    let hash = install(&conn, &store_dir, expected);
    let compressed = compress_payload(hash, replacement).expect("compress replacement payload");
    conn.execute(
        "UPDATE payload_storage
         SET payload_size = ?2, compressed_bytes = ?3, compressed_size = ?4
         WHERE payload_hash = ?1",
        rusqlite::params![
            hash.to_string(),
            replacement.len() as i64,
            &compressed,
            compressed.len() as i64
        ],
    )
    .expect("replace compressed payload");

    assert!(matches!(
        PayloadStore::new(&conn, &store_dir).read_verified(hash),
        Err(PayloadStoreError::InlineContentMismatch { expected, actual })
            if expected == hash && actual == ObjectHash::digest(replacement)
    ));
}

#[test]
fn decompression_is_bounded_by_the_catalog_payload_size() {
    let (_directory, store_dir, conn) = payload_store();
    let bytes = payload(16, 4096);
    let hash = install(&conn, &store_dir, &bytes);
    conn.execute(
        "UPDATE payload_storage SET payload_size = 8 WHERE payload_hash = ?1",
        [hash.to_string()],
    )
    .expect("lower catalog payload size");

    let error = PayloadStore::new(&conn, &store_dir)
        .read(hash)
        .expect_err("decompression must stop beyond the catalog size");
    assert!(
        error
            .to_string()
            .contains("catalog records 8 payload bytes, but decompression produced 9"),
        "{error}"
    );
}

#[test]
fn streamed_protocol_payloads_finish_inline() {
    let (_directory, store_dir, conn) = payload_store();
    let bytes = payload(7, 4096);
    let transaction = conn
        .unchecked_transaction()
        .expect("begin streamed payload installation");
    let mut writer = PayloadStore::new(&transaction, &store_dir).writer();
    writer.write_all(&bytes).expect("stream payload");

    let (hash, size) = writer.commit().expect("commit payload");
    transaction
        .commit()
        .expect("commit streamed payload installation");

    assert_eq!(size, bytes.len() as u64);
    let (storage, payload_size, compressed, compressed_size) = storage_row(&conn, hash);
    assert_eq!(storage, "inline");
    assert_eq!(payload_size, bytes.len() as i64);
    assert_eq!(
        compressed_size,
        compressed.expect("inline compressed payload").len() as i64
    );
    assert!(!store_dir.payload_spool_path(hash).exists());
    assert_eq!(
        PayloadStore::new(&conn, &store_dir)
            .read_verified(hash)
            .expect("read streamed payload"),
        bytes
    );
}

#[test]
fn streamed_placement_uses_the_finished_compressed_size() {
    let (_directory, store_dir, conn) = payload_store();
    let inline = payload(14, INLINE_PAYLOAD_LIMIT * 4);
    let file = incompressible_payload(15, INLINE_PAYLOAD_LIMIT * 2);

    for (bytes, expected_storage) in [(inline, "inline"), (file, "file")] {
        let transaction = conn
            .unchecked_transaction()
            .expect("begin streamed payload installation");
        let mut writer = PayloadStore::new(&transaction, &store_dir).writer();
        for chunk in bytes.chunks(997) {
            writer.write_all(chunk).expect("stream payload chunk");
        }
        let (hash, size) = writer.commit().expect("commit streamed payload");
        transaction
            .commit()
            .expect("commit streamed payload installation");

        assert_eq!(hash, ObjectHash::digest(&bytes));
        assert_eq!(size, bytes.len() as u64);
        assert_eq!(storage_row(&conn, hash).0, expected_storage);
        assert_eq!(
            PayloadStore::new(&conn, &store_dir)
                .read_verified(hash)
                .expect("read streamed payload"),
            bytes
        );
    }
}

#[test]
fn reinstall_accepts_the_same_payload_from_different_compression_chunks() {
    let (_directory, store_dir, conn) = payload_store();
    for bytes in [
        payload(18, INLINE_PAYLOAD_LIMIT * 4),
        incompressible_payload(19, INLINE_PAYLOAD_LIMIT * 2),
    ] {
        let transaction = conn
            .unchecked_transaction()
            .expect("begin streamed payload installation");
        let mut writer = PayloadStore::new(&transaction, &store_dir).writer();
        for chunk in bytes.chunks(997) {
            writer.write_all(chunk).expect("stream payload chunk");
        }
        let (hash, _) = writer.commit().expect("commit streamed payload");
        transaction
            .commit()
            .expect("commit streamed payload installation");
        let row = storage_row(&conn, hash);
        let file = std::fs::read(store_dir.payload_spool_path(hash)).ok();

        assert_eq!(install(&conn, &store_dir, &bytes), hash);
        let transaction = conn
            .unchecked_transaction()
            .expect("begin differently chunked reinstall");
        let mut writer = PayloadStore::new(&transaction, &store_dir).writer();
        for chunk in bytes.chunks(4093) {
            writer.write_all(chunk).expect("reinstall payload chunk");
        }
        assert_eq!(writer.commit().expect("commit streamed reinstall").0, hash);
        transaction
            .commit()
            .expect("commit differently chunked reinstall");
        assert_eq!(storage_row(&conn, hash), row);
        assert_eq!(std::fs::read(store_dir.payload_spool_path(hash)).ok(), file);
    }
}

#[test]
fn an_owner_cannot_claim_bytes_that_were_never_installed() {
    let (_directory, _store_dir, conn) = payload_store();
    let absent = ObjectHash::digest(b"absent payload");

    let error = set_payload_owner_claims_on(&conn, "missing-owner", &BTreeSet::from([absent]))
        .expect_err("an owner cannot name absent storage");

    assert!(error.to_string().contains("FOREIGN KEY"), "{error}");
}

#[test]
fn last_claim_cleanup_removes_inline_and_file_storage() {
    for bytes in [
        payload(8, 64),
        incompressible_payload(9, INLINE_PAYLOAD_LIMIT * 2),
    ] {
        let (_directory, store_dir, mut conn) = payload_store();
        let hash = install(&conn, &store_dir, &bytes);
        let tx = conn.transaction().expect("begin claim");
        set_payload_owner_claims_on(&tx, "owner", &BTreeSet::from([hash])).expect("claim payload");
        tx.commit().expect("commit claim");
        let tx = conn.transaction().expect("begin release");
        release_payload_owner_on(&tx, "owner").expect("release payload");
        tx.commit().expect("commit release");

        pay_owed_payload_deletions_on(&conn, &store_dir).expect("pay deletion");

        let stored: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM payload_storage WHERE payload_hash = ?1)",
                [hash.to_string()],
                |row| row.get(0),
            )
            .expect("check storage");
        assert!(!stored);
        assert!(!store_dir.payload_spool_path(hash).exists());
        assert!(payload_cleanup_hashes_on(&conn)
            .expect("read cleanup")
            .is_empty());
    }
}

#[test]
fn a_second_owner_keeps_shared_payload_storage() {
    let (_directory, store_dir, mut conn) = payload_store();
    let bytes = payload(10, 128);
    let hash = install(&conn, &store_dir, &bytes);
    let tx = conn.transaction().expect("begin claims");
    set_payload_owner_claims_on(&tx, "owner-a", &BTreeSet::from([hash]))
        .expect("claim for owner a");
    set_payload_owner_claims_on(&tx, "owner-b", &BTreeSet::from([hash]))
        .expect("claim for owner b");
    tx.commit().expect("commit claims");
    let tx = conn.transaction().expect("begin release");
    release_payload_owner_on(&tx, "owner-a").expect("release owner a");
    tx.commit().expect("commit release");

    assert!(payload_cleanup_hashes_on(&conn)
        .expect("read cleanup")
        .is_empty());
    assert_eq!(
        PayloadStore::new(&conn, &store_dir)
            .read(hash)
            .expect("read shared payload"),
        bytes
    );
}

#[test]
fn a_file_deletion_retry_finishes_after_the_file_is_already_absent() {
    let (_directory, store_dir, mut conn) = payload_store();
    let hash = install(
        &conn,
        &store_dir,
        &incompressible_payload(11, INLINE_PAYLOAD_LIMIT * 2),
    );
    let tx = conn.transaction().expect("begin claim");
    set_payload_owner_claims_on(&tx, "owner", &BTreeSet::from([hash])).expect("claim payload");
    tx.commit().expect("commit claim");
    let tx = conn.transaction().expect("begin release");
    release_payload_owner_on(&tx, "owner").expect("release payload");
    tx.commit().expect("commit release");
    std::fs::remove_file(store_dir.payload_spool_path(hash)).expect("remove payload file");

    pay_owed_payload_deletions_on(&conn, &store_dir).expect("retry deletion");

    assert!(payload_cleanup_hashes_on(&conn)
        .expect("read cleanup")
        .is_empty());
}
