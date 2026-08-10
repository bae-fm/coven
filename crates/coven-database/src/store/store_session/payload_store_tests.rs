use super::*;
use coven_foundation::store_dir::temp_store_dir;

fn payload(byte: u8, len: usize) -> Vec<u8> {
    vec![byte; len]
}

fn payload_store() -> (tempfile::TempDir, StoreDir, Connection) {
    let (directory, store_dir) = temp_store_dir();
    let conn = Connection::open_in_memory().expect("open payload database");
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("enable payload foreign keys");
    crate::apply_coven_schema(&conn).expect("apply payload schema");
    (directory, store_dir, conn)
}

fn storage_row(conn: &Connection, hash: ObjectHash) -> (String, Option<Vec<u8>>, Option<i64>) {
    conn.query_row(
        "SELECT storage, inline_bytes, file_size
         FROM payload_storage WHERE payload_hash = ?1",
        [hash.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
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

    assert_eq!(
        storage_row(&conn, hash),
        ("inline".to_string(), Some(bytes.clone()), None)
    );
    assert!(!store_dir.payload_spool_path(hash).exists());
    assert_eq!(
        PayloadStore::new(&conn, &store_dir)
            .read(hash)
            .expect("read inline payload"),
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
fn payloads_over_the_inline_limit_live_in_the_file_spool() {
    let (_directory, store_dir, conn) = payload_store();
    let inline = payload(3, INLINE_PAYLOAD_LIMIT);
    let file = payload(4, INLINE_PAYLOAD_LIMIT + 1);
    let inline_hash = install(&conn, &store_dir, &inline);
    let file_hash = install(&conn, &store_dir, &file);

    assert_eq!(
        storage_row(&conn, inline_hash),
        ("inline".to_string(), Some(inline), None)
    );
    assert_eq!(
        storage_row(&conn, file_hash),
        (
            "file".to_string(),
            None,
            Some((INLINE_PAYLOAD_LIMIT + 1) as i64)
        )
    );
    assert_eq!(
        std::fs::read(store_dir.payload_spool_path(file_hash)).expect("read file payload"),
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
    let bytes = payload(5, INLINE_PAYLOAD_LIMIT + 1);

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
fn verified_reads_reject_changed_inline_and_file_payloads() {
    let (_directory, store_dir, conn) = payload_store();
    let store = PayloadStore::new(&conn, &store_dir);
    let inline_hash = install(&conn, &store_dir, b"inline payload");
    let file_hash = install(&conn, &store_dir, &payload(6, INLINE_PAYLOAD_LIMIT + 1));

    conn.execute(
        "UPDATE payload_storage SET inline_bytes = x'00' WHERE payload_hash = ?1",
        [inline_hash.to_string()],
    )
    .expect("change inline payload");
    std::fs::write(store_dir.payload_spool_path(file_hash), b"changed")
        .expect("change file payload");

    assert!(matches!(
        store.read_verified(inline_hash),
        Err(PayloadStoreError::InlineContentMismatch { expected, .. }) if expected == inline_hash
    ));
    assert!(store.read_verified(file_hash).is_err());
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
    assert_eq!(
        storage_row(&conn, hash),
        ("inline".to_string(), Some(bytes), None)
    );
    assert!(!store_dir.payload_spool_path(hash).exists());
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
    for bytes in [payload(8, 64), payload(9, INLINE_PAYLOAD_LIMIT + 1)] {
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
    let hash = install(&conn, &store_dir, &payload(11, INLINE_PAYLOAD_LIMIT + 1));
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
