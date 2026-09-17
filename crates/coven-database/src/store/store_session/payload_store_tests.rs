use super::*;

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

fn payload_store() -> Connection {
    let conn = Connection::open_in_memory().expect("open payload database");
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("enable payload foreign keys");
    crate::apply_coven_schema(&conn).expect("apply payload schema");
    conn
}

/// One payload's catalog row: payload size, compressed size, chunk count.
fn storage_row(conn: &Connection, hash: ObjectHash) -> (i64, i64, i64) {
    conn.query_row(
        "SELECT payload_size, compressed_size, chunk_count
         FROM payload_storage WHERE payload_hash = ?1",
        [hash.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .expect("read payload storage row")
}

/// The chunk rows behind one payload, in ordinal order.
fn chunks(conn: &Connection, hash: ObjectHash) -> Vec<(i64, Vec<u8>)> {
    crate::query_mapped_rows(
        conn,
        "SELECT ordinal, bytes FROM payload_chunks
         WHERE payload_hash = ?1 ORDER BY ordinal",
        [hash.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .expect("read payload chunks")
}

fn payload_row_counts(conn: &Connection) -> (i64, i64) {
    conn.query_row(
        "SELECT (SELECT COUNT(*) FROM payload_storage),
                (SELECT COUNT(*) FROM payload_chunks)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .expect("count payload rows")
}

fn install(conn: &Connection, bytes: &[u8]) -> ObjectHash {
    let transaction = conn
        .unchecked_transaction()
        .expect("begin payload installation");
    let hash = PayloadStore::new(&transaction)
        .install(bytes)
        .expect("install payload");
    transaction.commit().expect("commit payload installation");
    hash
}

/// Stream `bytes` in `step`-sized writes under the identity `expected` names.
fn stream(
    conn: &Connection,
    expected: ObjectHash,
    bytes: &[u8],
    step: usize,
) -> Result<u64, PayloadStoreError> {
    let transaction = conn
        .unchecked_transaction()
        .expect("begin streamed installation");
    let outcome = (|| {
        let mut writer = PayloadStore::new(&transaction).writer(expected)?;
        for chunk in bytes.chunks(step) {
            writer
                .write_all(chunk)
                .expect("stream one slice of the payload");
        }
        writer.commit()
    })();
    match outcome {
        Ok(size) => {
            transaction.commit().expect("commit streamed installation");
            Ok(size)
        }
        Err(error) => {
            transaction.rollback().expect("roll back a failed stream");
            Err(error)
        }
    }
}

#[test]
fn a_payload_smaller_than_one_chunk_is_one_row() {
    let conn = payload_store();
    let bytes = payload(2, 16 * 1024);
    let hash = install(&conn, &bytes);
    let (payload_size, compressed_size, chunk_count) = storage_row(&conn, hash);
    let stored = chunks(&conn, hash);

    assert_eq!(payload_size, bytes.len() as i64);
    assert_eq!(chunk_count, 1);
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].0, 0);
    assert_eq!(stored[0].1.len() as i64, compressed_size);
    assert!(compressed_size < payload_size);
    assert_eq!(
        PayloadStore::new(&conn).read(hash).expect("read payload"),
        bytes
    );
}

#[test]
fn a_payload_spanning_chunks_is_ordered_rows_summing_to_its_compressed_size() {
    let conn = payload_store();
    let bytes = incompressible_payload(15, PAYLOAD_CHUNK_BYTES * 5 / 2);
    let hash = install(&conn, &bytes);
    let (_, compressed_size, chunk_count) = storage_row(&conn, hash);
    let stored = chunks(&conn, hash);

    assert!(chunk_count > 1, "an incompressible payload spans chunks");
    assert_eq!(stored.len() as i64, chunk_count);
    assert_eq!(
        stored
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<Vec<_>>(),
        (0..chunk_count).collect::<Vec<_>>()
    );
    for (ordinal, chunk) in &stored[..stored.len() - 1] {
        assert_eq!(
            chunk.len(),
            PAYLOAD_CHUNK_BYTES,
            "chunk {ordinal} is not full"
        );
    }
    assert_eq!(
        stored
            .iter()
            .map(|(_, chunk)| chunk.len() as i64)
            .sum::<i64>(),
        compressed_size
    );
    assert_eq!(
        PayloadStore::new(&conn)
            .read_verified(hash)
            .expect("read payload"),
        bytes
    );
}

#[test]
fn payload_storage_compresses_bytes_without_changing_their_content_address() {
    let conn = payload_store();
    let bytes = payload(12, 16 * 1024);
    let hash = install(&conn, &bytes);
    let stored = chunks(&conn, hash);

    assert_eq!(hash, ObjectHash::digest(&bytes));
    assert_eq!(stored.len(), 1);
    assert_ne!(stored[0].1, bytes);
    assert!(stored[0].1.len() < bytes.len());
    assert_eq!(
        PayloadStore::new(&conn)
            .read_verified(hash)
            .expect("read compressed payload"),
        bytes
    );
}

#[test]
fn payload_installation_requires_an_owning_transaction() {
    let conn = payload_store();

    let error = PayloadStore::new(&conn)
        .install(b"autocommit payload")
        .expect_err("installation outside a transaction is refused");

    assert!(
        error
            .to_string()
            .contains("installation requires the owning database transaction"),
        "{error}"
    );
    assert_eq!(payload_row_counts(&conn), (0, 0));
}

#[test]
fn verified_reads_reject_a_changed_chunk() {
    let conn = payload_store();
    let bytes = incompressible_payload(6, PAYLOAD_CHUNK_BYTES * 2);
    let hash = install(&conn, &bytes);
    let stored = chunks(&conn, hash);
    let mut replacement = stored[0].1.clone();
    replacement[7] ^= 0xff;

    conn.execute(
        "UPDATE payload_chunks SET bytes = ?3
         WHERE payload_hash = ?1 AND ordinal = ?2",
        rusqlite::params![hash.to_string(), 0_i64, replacement],
    )
    .expect("change one chunk");

    PayloadStore::new(&conn)
        .read_verified(hash)
        .expect_err("a changed chunk must fail the verified read");
}

#[test]
fn a_truncated_chunk_set_fails_the_read() {
    let conn = payload_store();
    let bytes = incompressible_payload(7, PAYLOAD_CHUNK_BYTES * 2);
    let hash = install(&conn, &bytes);
    let (_, _, chunk_count) = storage_row(&conn, hash);

    conn.execute(
        "DELETE FROM payload_chunks WHERE payload_hash = ?1 AND ordinal = ?2",
        rusqlite::params![hash.to_string(), chunk_count - 1],
    )
    .expect("drop the last chunk");

    let error = PayloadStore::new(&conn)
        .read(hash)
        .expect_err("a truncated chunk set must fail the read");
    assert!(
        error.to_string().contains("catalog records"),
        "the read names the disagreement: {error}"
    );
}

#[test]
fn a_reordered_chunk_set_fails_the_read() {
    let conn = payload_store();
    let bytes = incompressible_payload(8, PAYLOAD_CHUNK_BYTES * 3);
    let hash = install(&conn, &bytes);
    let stored = chunks(&conn, hash);
    assert!(stored.len() >= 3);

    // Swap two whole chunks, so every count and length the catalog states still
    // holds and only the order is wrong.
    conn.execute(
        "UPDATE payload_chunks SET bytes = ?3 WHERE payload_hash = ?1 AND ordinal = ?2",
        rusqlite::params![hash.to_string(), 0_i64, stored[1].1],
    )
    .expect("swap chunk 0");
    conn.execute(
        "UPDATE payload_chunks SET bytes = ?3 WHERE payload_hash = ?1 AND ordinal = ?2",
        rusqlite::params![hash.to_string(), 1_i64, stored[0].1],
    )
    .expect("swap chunk 1");

    PayloadStore::new(&conn)
        .read_verified(hash)
        .expect_err("a reordered chunk set must fail the read");
}

#[test]
fn verified_reads_hash_the_decompressed_payload() {
    let conn = payload_store();
    let expected = b"expected logical payload";
    let replacement = b"different logical bytes";
    let hash = install(&conn, expected);
    let other = install(&conn, replacement);
    let stored = chunks(&conn, other);
    let (other_size, other_compressed, _) = storage_row(&conn, other);

    conn.execute(
        "UPDATE payload_storage SET payload_size = ?2, compressed_size = ?3
         WHERE payload_hash = ?1",
        rusqlite::params![hash.to_string(), other_size, other_compressed],
    )
    .expect("restate the catalog row");
    conn.execute(
        "UPDATE payload_chunks SET bytes = ?3 WHERE payload_hash = ?1 AND ordinal = ?2",
        rusqlite::params![hash.to_string(), 0_i64, stored[0].1],
    )
    .expect("replace the chunk under a name it does not hash to");

    assert!(matches!(
        PayloadStore::new(&conn).read_verified(hash),
        Err(PayloadStoreError::ContentMismatch { expected, actual })
            if expected == hash && actual == ObjectHash::digest(replacement)
    ));
}

#[test]
fn decompression_is_bounded_by_the_catalog_payload_size() {
    let conn = payload_store();
    let bytes = payload(16, 4096);
    let hash = install(&conn, &bytes);
    conn.execute(
        "UPDATE payload_storage SET payload_size = 8 WHERE payload_hash = ?1",
        [hash.to_string()],
    )
    .expect("lower catalog payload size");

    let error = PayloadStore::new(&conn)
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
fn a_streamed_payload_is_the_one_its_caller_named() {
    let conn = payload_store();
    for bytes in [
        payload(7, 4096),
        incompressible_payload(9, PAYLOAD_CHUNK_BYTES * 2 + 17),
    ] {
        let hash = ObjectHash::digest(&bytes);

        let size = stream(&conn, hash, &bytes, 997).expect("stream the payload");

        assert_eq!(size, bytes.len() as u64);
        assert_eq!(storage_row(&conn, hash).0, bytes.len() as i64);
        assert_eq!(
            PayloadStore::new(&conn)
                .read_verified(hash)
                .expect("read the streamed payload"),
            bytes
        );
    }
}

#[test]
fn a_stream_whose_digest_differs_from_its_expected_identity_fails() {
    let conn = payload_store();
    let expected = ObjectHash::digest(b"the payload its owner declared");

    let error = stream(&conn, expected, b"something else entirely", 8)
        .expect_err("a stream that is not what its caller named must fail");

    assert!(matches!(
        error,
        PayloadStoreError::ContentMismatch { expected: named, .. } if named == expected
    ));
    assert_eq!(payload_row_counts(&conn), (0, 0));
}

/// A writer that never settles its payload poisons the transaction it wrote
/// into: the deferred reference from its chunks to the catalog row it never
/// wrote fails the commit, so the caller cannot mistake half a payload for one.
#[test]
fn a_writer_that_never_commits_fails_the_transaction_it_wrote_into() {
    let conn = payload_store();
    let bytes = incompressible_payload(21, PAYLOAD_CHUNK_BYTES * 2);
    let hash = ObjectHash::digest(&bytes);

    let transaction = conn.unchecked_transaction().expect("begin the transaction");
    let mut writer = PayloadStore::new(&transaction)
        .writer(hash)
        .expect("open the writer");
    writer.write_all(&bytes).expect("stream the payload");
    drop(writer);

    let error = transaction
        .commit()
        .expect_err("an unsettled payload must not commit");

    assert!(error.to_string().contains("FOREIGN KEY"), "{error}");
    assert_eq!(payload_row_counts(&conn), (0, 0));
}

#[test]
fn a_rolled_back_transaction_leaves_no_payload_rows() {
    let conn = payload_store();
    let bytes = incompressible_payload(22, PAYLOAD_CHUNK_BYTES * 2);

    let transaction = conn.unchecked_transaction().expect("begin the transaction");
    PayloadStore::new(&transaction)
        .install(&bytes)
        .expect("install the payload");
    transaction.rollback().expect("roll the transaction back");

    assert_eq!(payload_row_counts(&conn), (0, 0));
}

#[test]
fn restreaming_an_existing_payload_verifies_it_without_rewriting_chunks() {
    let conn = payload_store();
    let bytes = incompressible_payload(19, PAYLOAD_CHUNK_BYTES * 2);
    let hash = ObjectHash::digest(&bytes);
    let size = stream(&conn, hash, &bytes, 997).expect("stream the payload");
    let installed = chunks(&conn, hash);
    let row = storage_row(&conn, hash);

    // A different slicing of the same content produces a different compressed
    // frame; the committed chunks are what the catalog row describes, so they
    // stay exactly as they are.
    assert_eq!(
        stream(&conn, hash, &bytes, 4093).expect("re-stream the payload"),
        size
    );

    assert_eq!(chunks(&conn, hash), installed);
    assert_eq!(storage_row(&conn, hash), row);
}

#[test]
fn restreaming_rejects_content_that_is_not_the_installed_payload() {
    let conn = payload_store();
    let bytes = incompressible_payload(20, PAYLOAD_CHUNK_BYTES * 2);
    let hash = install(&conn, &bytes);
    let mut changed = bytes.clone();
    changed[PAYLOAD_CHUNK_BYTES] ^= 0xff;

    let error = stream(&conn, hash, &changed, 997)
        .expect_err("a changed source fails even where its payload is installed");

    assert!(matches!(
        error,
        PayloadStoreError::ContentMismatch { expected, actual }
            if expected == hash && actual == ObjectHash::digest(&changed)
    ));
    assert_eq!(
        PayloadStore::new(&conn)
            .read_verified(hash)
            .expect("the installed payload is untouched"),
        bytes
    );
}

#[test]
fn an_owner_cannot_claim_bytes_that_were_never_installed() {
    let conn = payload_store();
    let absent = ObjectHash::digest(b"absent payload");

    let error = set_payload_owner_claims_on(&conn, "missing-owner", &BTreeSet::from([absent]))
        .expect_err("an owner cannot name absent storage");

    assert!(error.to_string().contains("FOREIGN KEY"), "{error}");
}

#[test]
fn the_last_claim_leaving_deletes_the_payload_and_its_chunks() {
    for bytes in [
        payload(8, 64),
        incompressible_payload(9, PAYLOAD_CHUNK_BYTES * 2),
    ] {
        let mut conn = payload_store();
        let hash = install(&conn, &bytes);
        let tx = conn.transaction().expect("begin claim");
        set_payload_owner_claims_on(&tx, "owner", &BTreeSet::from([hash])).expect("claim payload");
        tx.commit().expect("commit claim");

        let tx = conn.transaction().expect("begin release");
        release_payload_owner_on(&tx, "owner").expect("release payload");
        // The deletion is this transaction's, not a later pass's.
        let stored: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM payload_storage WHERE payload_hash = ?1)",
                [hash.to_string()],
                |row| row.get(0),
            )
            .expect("check storage inside the releasing transaction");
        assert!(!stored);
        tx.commit().expect("commit release");

        assert_eq!(payload_row_counts(&conn), (0, 0));
    }
}

#[test]
fn a_second_owner_keeps_shared_payload_storage() {
    let mut conn = payload_store();
    let bytes = payload(10, 128);
    let hash = install(&conn, &bytes);
    let tx = conn.transaction().expect("begin claims");
    set_payload_owner_claims_on(&tx, "owner-a", &BTreeSet::from([hash]))
        .expect("claim for owner a");
    set_payload_owner_claims_on(&tx, "owner-b", &BTreeSet::from([hash]))
        .expect("claim for owner b");
    tx.commit().expect("commit claims");
    let tx = conn.transaction().expect("begin release");
    release_payload_owner_on(&tx, "owner-a").expect("release owner a");
    tx.commit().expect("commit release");

    assert_eq!(
        PayloadStore::new(&conn)
            .read(hash)
            .expect("read shared payload"),
        bytes
    );
}

#[test]
fn a_travelling_image_reports_and_sheds_its_payload_rows() {
    let mut conn = payload_store();
    let hash = install(&conn, b"a payload an image must not carry");
    let tx = conn.transaction().expect("begin claim");
    set_payload_owner_claims_on(&tx, "owner", &BTreeSet::from([hash])).expect("claim payload");
    tx.commit().expect("commit claim");

    assert_eq!(
        payload_rows_in_image(&conn).expect("count carried payload rows"),
        vec![
            ("payload_owners", 1),
            ("payload_chunks", 1),
            ("payload_storage", 1)
        ]
    );

    let tx = conn.transaction().expect("begin clearing");
    clear_payload_tables_on(&tx).expect("clear the payload tables");
    tx.commit().expect("commit clearing");

    assert_eq!(
        payload_rows_in_image(&conn).expect("count carried payload rows"),
        Vec::new()
    );
}
